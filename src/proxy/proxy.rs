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
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Proxy {
    config: ProxyConfig,
    network: Network,
    pending: HashMap<ClientRequestKey, ()>,
    pending_messages: HashMap<ClientRequestKey, DomMessage>,
    fast_path_deadlines: HashMap<ClientRequestKey, u64>,
    aborted_fast_path: HashSet<ClientRequestKey>,
    reply_sets: HashMap<ClientRequestKey, ReplySetState>,
    slow_reply_sets: HashMap<ClientRequestKey, SlowReplySetState>,
    clock: ClockSim,
    f: usize, // Amount of replicas that can fail
    n_servers: usize,
    telemetry: TelemetryWriter,
    client_deadlines: HashMap<NodeId, u64>,
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
            pending_messages: HashMap::new(),
            fast_path_deadlines: HashMap::new(),
            aborted_fast_path: HashSet::new(),
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
            client_deadlines,
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
        let mut fast_path_timeout_interval = tokio::time::interval(std::time::Duration::from_millis(1));
        fast_path_timeout_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
                _ = self.network.server_messages.recv_many(&mut server_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_server_messages(&mut server_msg_buf).await;
                },
                _ = fast_path_timeout_interval.tick() => {
                    self.handle_fast_path_timeouts().await;
                },
                _ = metrics_flush_interval.tick() => {
                    self.flush_metrics();
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
                    let send_time = self.clock.get_time();
                    let adaptive_deadline = self.get_adaptive_deadline();
                    let deadline = self.get_deadline(send_time, adaptive_deadline);
                    let fast_path_timeout = self.get_fast_path_timeout(deadline, adaptive_deadline);
                    let dom_message = DomMessage::new(client_id, message.clone(), deadline, send_time);
                    self.pending_messages.insert(key, dom_message.clone());
                    self.fast_path_deadlines.insert(key, fast_path_timeout);

                    // Multicast to all servers as ProxyMessage::Append
                    for server in self.config.targets() {
                        debug!("Forward client {} -> server {}", client_id, server.id);
                        self.network
                            .send_to_server(server.id, ProxyMessage::Append(dom_message.clone()))
                            .await;
                    }
                    continue;
                }
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
                other => {
                    warn!("Unexpected server message on proxy connection: {other:?}");
                }
            }
        }
    }

    fn get_adaptive_deadline(&self) -> u64 {
        (self.client_deadlines.values().max().copied().unwrap_or(0) as f64 * 0.95) as u64
    }

    fn get_deadline(&self, send_time: u64, adaptive_deadline: u64) -> u64 {
        let epsilon = self.clock.get_uncertainty() as u64;
        adaptive_deadline + send_time + 2 * epsilon
    }

    fn get_fast_path_timeout(&self, deadline: u64, adaptive_deadline: u64) -> u64 {
        let epsilon = self.clock.get_uncertainty() as u64;
        let rtt = adaptive_deadline.saturating_mul(2);
        deadline + rtt + epsilon
    }

    fn should_abort_fast_path(&mut self, key: ClientRequestKey) -> bool {
        if self.aborted_fast_path.contains(&key) {
            return false;
        }
        if !self.pending.contains_key(&key) {
            return false;
        }
        let now = self.clock.get_time();
        let timeout_at = match self.fast_path_deadlines.get(&key) {
            Some(t) => *t,
            None => return false,
        };
        now >= timeout_at
    }

    async fn abort_fast_path(&mut self, key: ClientRequestKey) {
        if !self.pending.contains_key(&key) {
            return;
        }
        let abort_msg = match self.pending_messages.get(&key) {
            Some(msg) => msg.clone(),
            None => return,
        };
        self.aborted_fast_path.insert(key);
        self.reply_sets.remove(&key);
        self.fast_path_deadlines.remove(&key);
        self.telemetry
            .record_fast_path_abort(abort_msg.client_id, abort_msg.message.command_id());

        let mut any_sent = false;
        for server in self.config.targets() {
            debug!(
                "Fast path timeout for client {} command {}, sending slow-path abort to server {}",
                abort_msg.client_id, abort_msg.message.command_id(), server.id
            );
            self.network
                .send_to_server(server.id, ProxyMessage::AbortFastPath(abort_msg.clone()))
                .await;
            any_sent = true;
        }
        if !any_sent {
            warn!(
                "Fast path timeout for client {} command {}, but no servers configured to receive abort",
                abort_msg.client_id, abort_msg.message.command_id()
            );
        }
    }

    async fn handle_fast_path_timeouts(&mut self) {
        let keys: Vec<ClientRequestKey> = self
            .fast_path_deadlines
            .keys()
            .copied()
            .collect();
        for key in keys {
            if self.should_abort_fast_path(key) {
                self.abort_fast_path(key).await;
            }
        }
    }

    async fn handle_fast_reply(&mut self, reply: FastReply) {
        let d = reply.deadline_length;
        let old = self.client_deadlines.insert(reply.replica_id, d);
        if old != Some(d) {
            debug!(
                "changing deadline for node {} from {:?} to {}",
                reply.replica_id, old, d
            );
        }
        let key = ClientRequestKey::new(reply.client_id, reply.request_id);
        if self.aborted_fast_path.contains(&key) {
            return;
        }
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
        let d = sr.deadline_length;
        let old = self.client_deadlines.insert(sr.replica_id, d);
        if old != Some(d) {
            debug!(
                "changing deadline for node {} from {:?} to {}",
                sr.replica_id, old, d
            );
        }
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
            self.fast_path_deadlines.remove(&key);
            self.pending_messages.remove(&key);
            self.aborted_fast_path.remove(&key);
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
                debug!("Super quorum for {:?}, committing", key);
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
        self.pending_messages.remove(&key);
        self.fast_path_deadlines.remove(&key);
        self.aborted_fast_path.remove(&key);
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
