use log::{info, warn};

use crate::clock::ClockSim;
use crate::common::kv::{ClientId, NodeId};
use crate::common::messages::{
    ClientMessage, CommitMessage, FastReply, FastReplyResult, ProxyMessage, ServerMessage,
};
use crate::dom::request::DomMessage;
use crate::proxy::config::{ProxyConfig, Server};
use crate::proxy::network::Network;
use crate::proxy::types::{ClientRequestKey, ReplySetState};
use std::collections::HashMap;
use std::net::SocketAddr;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Proxy {
    config: ProxyConfig,
    network: Network,
    pending: HashMap<ClientRequestKey, ()>,
    reply_sets: HashMap<ClientRequestKey, ReplySetState>,
    clock: ClockSim,
    f: usize, // Amount of replicas that can fail
}

impl Proxy {
    pub async fn new(config: ProxyConfig) -> Self {
        let servers: Vec<(NodeId, String)> =
            config.targets().iter().map(Server::as_endpoint).collect();
        let listen_address: SocketAddr = format!(
            "{}:{}",
            config.proxy_listen_address, config.proxy_listen_port
        )
        .parse()
        .expect("Invalid proxy listen address");


        // Clock
        let clock_config = config.clock.clone();
        let network = Network::new(listen_address, servers, NETWORK_BATCH_SIZE).await;
        
        let n_replicas = config.targets().len() - 1;
        let f = (n_replicas - 1) / 2;
        Self {
            config,
            network,
            pending: HashMap::new(),
            reply_sets: HashMap::new(),
            clock: ClockSim::new(
                clock_config.drift_rate,
                clock_config.uncertainty_bound,
                clock_config.sync_freq,
            ),
            f,
        }
    }

    /// Load proxy from proxy-config (PROXY_CONFIG_FILE env or default path).
    pub async fn from_proxy_config() -> Result<Self, config::ConfigError> {
        let path = std::env::var("PROXY_CONFIG_FILE")
            .unwrap_or_else(|_| "build_scripts/proxy-config.toml".to_string());
        Self::from_file(path).await
    }

    /// Load proxy config from a cluster-config-style TOML at the given path.
    pub async fn from_file(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, config::ConfigError> {
        let config = ProxyConfig::from_file(path)?;
        Ok(Self::new(config).await)
    }    

    pub async fn run(&mut self) {
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut server_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        loop {
            tokio::select! {
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
                _ = self.network.server_messages.recv_many(&mut server_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_server_messages(&mut server_msg_buf).await;
                },
            }
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (client_id, message) in messages.drain(..) {
            match &message {
                ClientMessage::Append(command_id, _) => {
                    self.pending
                        .insert(ClientRequestKey::new(client_id, *command_id), ());
                }
            }
            let send_time = self.clock.get_time();
            let deadline = self.get_deadline(send_time);
            let dom_message = DomMessage::new(client_id, message, deadline, send_time);

            // Multicast to all servers as ProxyMessage::Append
            for server in self.config.targets() {
                info!("Forward client {} -> server {}", client_id, server.id);
                self.network
                    .send_to_server(server.id, ProxyMessage::Append(dom_message.clone()))
                    .await;
            }
        }
    }

    async fn handle_server_messages(&mut self, messages: &mut Vec<ServerMessage>) {
        for message in messages.drain(..) {            
            match message {
                ServerMessage::ProxyResponse(client_id, inner) => match *inner {
                    inner @ (ServerMessage::Write(_) | ServerMessage::Read(_, _)) => {
                        let _ = self
                            .pending
                            .remove(&ClientRequestKey::new(client_id, inner.command_id()));
                        self.network.send_to_client(client_id, inner);
                    }
                    other => {
                        warn!("Unexpected proxy inner message: {other:?}");
                    }
                },
                ServerMessage::StartSignal(_) => {
                    self.network.send_to_all_clients(message);
                }
                ServerMessage::FastReply(fast_reply) => {
                    self.handle_fast_reply(fast_reply).await;
                }
                other => {
                    warn!("Unexpected server message on proxy connection: {other:?}");
                }
            }
        }
    }

    fn get_deadline(&self, send_time: u64) -> u64 {
        send_time + 1000 // TODO change this!!!
    }

    async fn handle_fast_reply(&mut self, reply: FastReply) {
        let key = ClientRequestKey::new(reply.client_id, reply.request_id);
        let state = self
            .reply_sets
            .entry(key)
            .or_insert_with(|| ReplySetState {
                current_ballot: reply.ballot.clone(),
                replies: Vec::new(),
            });

        // 1. Duplicate or previous ballot
        if reply.ballot < state.current_ballot {
            return;
        }
        if state.replies.iter().any(|r| r.replica_id == reply.replica_id) {
            return;
        }

        // 2. New ballot
        if reply.ballot > state.current_ballot {
            state.current_ballot = reply.ballot.clone();
            state.replies.clear();
        }
        // 3. Same ballot: insert
        state.replies.push(reply.clone());        

        if let Some(leader_reply) = self.can_commit(key) {
            self.reply_to_client(leader_reply, key);
            
            // Will send the commit message to all servers
            self.send_commit_message(CommitMessage {
                client_id: reply.client_id,
                command_id: reply.request_id,
            })
            .await;
        }
    }
    
    #[inline]
    fn get_super_quorum_size(&self) -> usize {
        1 + self.f + (self.f + 1) / 2// 1 + f + ceil(f/2)
    }

    fn is_super_quorum(&self, key: ClientRequestKey) -> bool {
        let replies_len = self
            .reply_sets
            .get(&key)
            .unwrap()
            .replies
            .len();
        replies_len >= self.get_super_quorum_size()
    }

    fn get_leader_reply(&self, key: ClientRequestKey) -> Option<FastReply> {
        // Leader reply is the one with the result that is not None
        let replies = self.reply_sets.get(&key).unwrap().replies.clone();
        let leader_reply = replies.iter().find(|r| r.result.is_some());
        leader_reply.cloned()
    }

    fn can_commit(
        &self,
        key: ClientRequestKey,
    )-> Option<FastReply> {

        if self.is_super_quorum(key) {
            return self.get_leader_reply(key);
        }
        None
    }   

    fn reply_to_client(&mut self, committed: FastReply, key: ClientRequestKey) {
        let client_id = committed.client_id;
        let msg = match committed.result {
            Some(FastReplyResult::Read(cmd_id, value)) => ServerMessage::Read(cmd_id, value),
            Some(FastReplyResult::Write(cmd_id)) => ServerMessage::Write(cmd_id),
            None => return,
        };
        self.pending.remove(&key);
        self.reply_sets.remove(&key);
        self.network.send_to_client(client_id, msg);
    }

    async fn send_commit_message(&mut self, commit_message: CommitMessage) {
        let targets = self.config.targets();
        for server in targets {
            self.network
                .send_to_server(server.id, ProxyMessage::Commit(commit_message.clone()))
                .await;
        }
    }
}
