use log::{info, warn};

use crate::clock::ClockSim;
use crate::common::kv::{ClientId, NodeId};
use crate::common::messages::{
    ClientMessage, CommitMessage, FastReply, FastReplyResult, ProxyMessage, ServerMessage, SlowPathReply,
};
use crate::dom::request::DomMessage;
use crate::proxy::config::{ProxyConfig, Server};
use crate::proxy::network::Network;
use crate::proxy::types::{ClientRequestKey, ReplySetState, SlowReplySetState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Proxy {
    config: ProxyConfig,
    network: Network,
    pending: HashMap<ClientRequestKey, ()>,
    pending_timestamps: HashMap<ClientRequestKey, Instant>,
    reply_sets: HashMap<ClientRequestKey, ReplySetState>,
    slow_reply_sets: HashMap<ClientRequestKey, SlowReplySetState>,
    clock: ClockSim,
    f: usize, // Amount of replicas that can fail
    n_servers: usize,
    metrics: crate::common::kv::SystemMetrics,
    throughput_window_count: usize,
    throughput_window_start: Instant,
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
        Self {
            config,
            network,
            pending: HashMap::new(),
            pending_timestamps: HashMap::new(),
            reply_sets: HashMap::new(),
            slow_reply_sets: HashMap::new(),
            clock: ClockSim::new(
                clock_config.drift_rate,
                clock_config.uncertainty_bound,
                clock_config.sync_freq,
            ),
            f,
            n_servers,
            metrics: crate::common::kv::SystemMetrics::new(),
            throughput_window_count: 0,
            throughput_window_start: Instant::now(),
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
        let start = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let mut metrics_flush_interval = tokio::time::interval_at(start, std::time::Duration::from_secs(1));
        metrics_flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                _ = tokio::signal::ctrl_c() => {
                    self.flush_metrics();
                    return;
                },
            }
        }
    }

    fn flush_metrics(&mut self) {
        use std::io::Write;
        // Compute requests/second over the elapsed window, then reset.
        let elapsed_secs = self.throughput_window_start.elapsed().as_secs_f64();
        self.metrics.throughput_rps = if elapsed_secs > 0.0 {
            self.throughput_window_count as f64 / elapsed_secs
        } else {
            0.0
        };
        self.throughput_window_count = 0;
        self.throughput_window_start = Instant::now();

        let json = self.metrics.to_json();
        match std::fs::File::create(&self.config.metrics_filepath) {
            Ok(mut f) => { let _ = f.write_all(json.as_bytes()); }
            Err(e) => warn!("Failed to write metrics to {}: {e}", self.config.metrics_filepath),
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (client_id, message) in messages.drain(..) {
            match &message {
                ClientMessage::Append(command_id, _) => {
                    let key = ClientRequestKey::new(client_id, *command_id);
                    self.pending.insert(key, ());
                    self.pending_timestamps.insert(key, Instant::now());
                    self.metrics.total_sent += 1;
                    self.metrics.recompute_ratios();
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

        if let Some(leader_reply) = self.can_commit(key) {
            if let Some(ts) = self.pending_timestamps.remove(&key) {
                let latency_us = ts.elapsed().as_micros();
                let contributing_ids: Vec<NodeId> = self
                    .reply_sets
                    .get(&key)
                    .map(|s| s.replies.iter().map(|r| r.replica_id).collect())
                    .unwrap_or_default();
                for nid in contributing_ids {
                    let entry = self.metrics.nodes.entry(nid).or_default();
                    entry.fast_path_count += 1;
                    entry.push_fast_path_latency(latency_us);
                }
            }
            self.metrics.fast_path_committed += 1;
            self.metrics.recompute_ratios();
            self.throughput_window_count += 1;
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

        let majority = self.n_servers / 2 + 1;
        if state.replies.len() >= majority {
            if let Some(result) = state.result.take() {
                if let Some(ts) = self.pending_timestamps.remove(&key) {
                    let latency_us = ts.elapsed().as_micros();
                    self.metrics.push_slow_path_latency(latency_us);
                    self.metrics.slow_path_committed += 1;
                    self.metrics.recompute_ratios();
                    self.throughput_window_count += 1;
                }
                self.reply_sets.remove(&key);
                self.slow_reply_sets.remove(&key);
                let _ = self.pending.remove(&key);
                let response = match result {
                    FastReplyResult::Write(id) => ServerMessage::Write(id),
                    FastReplyResult::Read(id, val) => ServerMessage::Read(id, val),
                };
                self.network.send_to_client(sr.client_id, response);
            }
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
