use log::{debug, info, warn};

use crate::clock::ClockSim;
use crate::common::kv::{ClientId, NodeId};
use crate::telemetry::TelemetryWriter;
use crate::common::log_hash::LogHash;
use crate::common::messages::{
    ClientMessage, CommitMessage, FastReply, ServerResult, ProxyMessage, ServerMessage, SlowPathReply,
};
use crate::dom::request::DomMessage;
use crate::proxy::config::{ProxyConfig, Server};
use crate::proxy::network::Network;
use crate::proxy::types::{ClientRequestKey, ReplySetState, SlowReplySetState};
use std::collections::HashMap;
use std::net::SocketAddr;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Proxy {
    config: ProxyConfig,
    network: Network,
    pending: HashMap<ClientRequestKey, ()>,
    reply_sets: HashMap<ClientRequestKey, ReplySetState>,
    slow_reply_sets: HashMap<ClientRequestKey, SlowReplySetState>,
    clock: ClockSim,
    f: usize, // Amount of replicas that can fail
    n_servers: usize,
    telemetry: TelemetryWriter,
    client_deadlines: HashMap<ClientId, u64>,
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
        let n_servers = config.targets().len();
        let telemetry = TelemetryWriter::new(config.metrics_filepath.clone());
        let client_deadlines = HashMap::new();
        Self {
            config,
            network,
            pending: HashMap::new(),
            reply_sets: HashMap::new(),
            slow_reply_sets: HashMap::new(),
            clock: ClockSim::new(
                clock_config.drift_rate,
                clock_config.uncertainty_bound,
                clock_config.sync_freq,
            ),
            f,
            n_servers,
            telemetry,
            client_deadlines
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
        let shutdown_signal = wait_for_shutdown_signal();
        tokio::pin!(shutdown_signal);
        let start = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let mut metrics_flush_interval = tokio::time::interval_at(start, std::time::Duration::from_secs(1));
        metrics_flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut deadline_request_interval = tokio::time::interval(std::time::Duration::from_secs(3));
        deadline_request_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
                _ = self.network.server_messages.recv_many(&mut server_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_server_messages(&mut server_msg_buf).await;
                },
                _ = metrics_flush_interval.tick() => {
                    self.flush_metrics();
                },
                _ = deadline_request_interval.tick() => {
                    self.send_deadline_length_request().await;
                },
                _ = &mut shutdown_signal => {
                    self.flush_metrics();
                    self.network.shutdown();
                    return;
                },
            }
        }
    }

    fn flush_metrics(&mut self) {
        self.telemetry.flush();
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (client_id, message) in messages.drain(..) {
            match &message {
                ClientMessage::Append(command_id, _) => {
                    let key = ClientRequestKey::new(client_id, *command_id);
                    self.pending.insert(key, ());
                    self.telemetry.record_client_request(client_id, *command_id);
                    self.client_deadlines.entry(client_id).or_insert(2*self.clock.get_uncertainty() as u64);
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
                ServerMessage::SlowPathReply(sr) => {
                    self.handle_slow_path_reply(sr);
                }
                ServerMessage::StartSignal(_) => {
                    self.network.send_to_all_clients(message);
                }
                ServerMessage::FastReply(fast_reply) => {
                    self.handle_fast_reply(fast_reply).await;
                }
                ServerMessage::DeadlineLengthRequestReply(node_id, deadline_length) => {
                    self.handle_deadline_length_request_reply(node_id, deadline_length);
                }
                other => {
                    warn!("Unexpected server message on proxy connection: {other:?}");
                }
            }
        }
    }

    fn get_deadline(&mut self, send_time: u64) -> u64 {
        let epsilon = self.clock.get_uncertainty() as u64;
        send_time + 2 * epsilon
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

        self.telemetry.record_fast_reply(reply.replica_id, reply.client_id, reply.request_id);

        if let Some(leader_reply) = self.can_commit(key) {
            self.telemetry.record_fast_path_commit(reply.client_id, reply.request_id);
            self.reply_to_client(leader_reply, key);
            // Will send the commit message to all servers
            self.send_commit_message(CommitMessage {
                client_id: reply.client_id,
                command_id: reply.request_id,
            })
            .await;
        }
    }
    
    fn handle_slow_path_reply(&mut self, sr: SlowPathReply) {
        let key = ClientRequestKey::new(sr.client_id, sr.request_id);
        let state = self
            .slow_reply_sets
            .entry(key)
            .or_insert_with(|| SlowReplySetState {
                replies: Vec::new(),
                result: None,
            });

        if state.replies.contains(&sr.replica_id) {
            return;
        }
        state.replies.push(sr.replica_id);
        if let Some(res) = sr.result {
            state.result = Some(res);
        }

        // OmniPaxos already decided this entry (majority agreed); we only need the leader's result.
        if let Some(result) = state.result.take() {
            self.telemetry.record_slow_path_commit(sr.client_id, sr.request_id);
            self.reply_sets.remove(&key);
            self.slow_reply_sets.remove(&key);
            let _ = self.pending.remove(&key);
            let response = match result {
                ServerResult::Write(id) => ServerMessage::Write(id),
                ServerResult::Read(id, val) => ServerMessage::Read(id, val),
            };
            self.network.send_to_client(sr.client_id, response);
        }
    }

    #[inline]
    fn get_super_quorum_size(&self) -> usize {
        1 + self.f + (self.f + 1) / 2 // 1 + f + ceil(f/2)
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

    /// True iff all replies in the set for this key have the same log hash (replicas have consistent logs).
    fn logs_consistent(&self, key: ClientRequestKey) -> bool {
        let replies = match self.reply_sets.get(&key) {
            Some(state) => &state.replies,
            None => return false,
        };
        let first_hash: &LogHash = match replies.first().map(|r| &r.hash) {
            Some(h) => h,
            None => return false,
        };
        replies.iter().all(|r| r.hash == *first_hash)
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
            if !self.logs_consistent(key) {
                warn!(
                    "Fast path: super quorum for {:?} but replicas have inconsistent log hashes, not committing",
                    key
                );
                return None;
            }
            // Only log "committing" when we actually have a leader reply to commit (avoids
            // logging "committing" when we have e.g. two follower replies with same hash
            // and the leader reply arrives later with a different hash).
            if let Some(leader) = self.get_leader_reply(key) {
                info!("Super quorum for {:?}, committing", key);
                // Log the log
                // debug!("Log for {:?}: {:?}", key, self.reply_sets.get(&key).unwrap().replies);
                return Some(leader);
            }
        }
        None
    }   

    fn reply_to_client(&mut self, committed: FastReply, key: ClientRequestKey) {
        let client_id = committed.client_id;
        let msg = match committed.result {
            Some(ServerResult::Read(cmd_id, value)) => ServerMessage::Read(cmd_id, value),
            Some(ServerResult::Write(cmd_id)) => ServerMessage::Write(cmd_id),
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

    async fn send_deadline_length_request(&mut self) {
        let request = ProxyMessage::DeadlineLengthRequest(
            DEFAULT_PROXY_ADDRESS_KEY,
            "give me deadline".to_string(),
        );
        for server in self.config.targets() {
            self.network
                .send_to_server(server.id, request.clone())
                .await;
        }
    }

    fn handle_deadline_length_request_reply(&mut self, node_id: NodeId, deadline_length: u64) {
        debug!("Received deadline update from node {node_id}: {deadline_length}");
        // HashMap::insert performs upsert semantics: existing value is replaced.
        self.client_deadlines.insert(node_id, deadline_length);
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
