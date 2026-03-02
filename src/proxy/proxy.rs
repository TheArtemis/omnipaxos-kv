use log::{info, warn};

use crate::clock::ClockSim;
use crate::common::kv::{ClientId, CommandId, NodeId};
use crate::common::messages::{ClientMessage, FastReply, FastReplyResult, ServerMessage};
use omnipaxos::ballot_leader_election::Ballot;
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
        let clock_config = config.clock.clone();
        let network = Network::new(listen_address, servers, NETWORK_BATCH_SIZE).await;
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

            // Multicast to all servers
            for server in self.config.targets() {
                info!("Forward client {} -> server {}", client_id, server.id);
                self.network
                    .send_to_server(server.id, dom_message.clone())
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
                    self.handle_fast_reply(fast_reply);
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

    // Todo. I think we can write it cleaner but im tired :,)
    // Also, can we directly use the ballot number instead of the whole ballot object?

    fn handle_fast_reply(&mut self, reply: FastReply) {
        let key = ClientRequestKey::new(reply.client_id, reply.request_id);
        let n = self.config.targets().len();
        if n == 0 {
            return;
        }
        let f = (n - 1) / 2;
        let replica_ids: Vec<NodeId> = self.config.targets().iter().map(|s| s.id).collect();

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

        // 4. Check committed
        if let Some(leader_reply) =
            Self::check_committed(&reply, &state.replies, n, f, &replica_ids)
        {
            self.reply_to_client(leader_reply, key);
        }
    }

    /// Deterministic leader for a ballot: same ballot => same leader index.
    fn leader_from_ballot(ballot: &Ballot, replica_ids: &[NodeId]) -> NodeId {
        let bytes = bincode::serialize(ballot).expect("Ballot serialization");
        let idx = bytes
            .iter()
            .fold(0usize, |a, &b| a.wrapping_add(b as usize))
            % replica_ids.len();
        replica_ids[idx]
    }

    fn check_committed(
        reply: &FastReply,
        replies: &[FastReply],
        n: usize,
        f: usize,
        replica_ids: &[NodeId],
    ) -> Option<FastReply> {
        let quorum: Vec<&FastReply> = replies
            .iter()
            .filter(|r| {
                r.ballot == reply.ballot
                    && r.client_id == reply.client_id
                    && r.request_id == reply.request_id
            })
            .collect();

        let leader_node_id = Self::leader_from_ballot(&reply.ballot, replica_ids);
        let leader_reply: FastReply = quorum
            .iter()
            .find(|r| r.replica_id == leader_node_id && r.is_leader_reply())
            .map(|r| (*r).clone())?;

        let mut fast_reply_num = 0usize;
        let mut slow_reply_num = 0usize;
        for r in 0..n {
            let rid = replica_ids[r];
            let replica_reply = quorum.iter().find(|msg| msg.replica_id == rid);
            if let Some(rr) = replica_reply {
                if rr.is_replica_reply() {
                    slow_reply_num += 1;
                    fast_reply_num += 1;
                } else if rr.hash == leader_reply.hash {
                    fast_reply_num += 1;
                }
            }
        }

        let fast_quorum_size = 1 + f + (f + 1) / 2; // 1 + f + ceil(f/2)
        if fast_reply_num >= fast_quorum_size {
            return Some(leader_reply);
        }
        if slow_reply_num >= f {
            return Some(leader_reply);
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
}
