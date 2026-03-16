use crate::{configs::OmniPaxosKVConfig, database::Database, network::Network};
use chrono::Utc;
use log::*;
use rand::Rng;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_kv::{common::{kv::*, log_hash::LogHash, messages::*, utils::Timestamp, DEFAULT_NODE_ID}};
use omnipaxos_kv::dom::dom::Dom;
use omnipaxos_kv::dom::config::DomConfig;
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::Serialize;
use std::{collections::HashSet, fs::File, io::Write, time::{Duration, Instant}};

use crate::commit_queue::{CommitQueue, CommitState};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const STATS_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const LATE_BUFFER_DRAIN_INTERVAL: Duration = Duration::from_millis(10);


#[derive(Debug, Serialize)]
struct ServerStats<'a> {
    config: &'a OmniPaxosKVConfig,
    early_buffer_rate_rps: f64,
    late_buffer_rate_rps: f64,
}

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    dom: Dom,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosKVConfig,
    peers: Vec<NodeId>,

    // Proxy fast path related
    proxy_command_ids: HashSet<(ClientId, CommandId)>,
    commit_queue: CommitQueue,
    /// Commands applied optimistically via the fast-path.  When the Paxos log
    /// later decides the same entry (because we append it for durability), we
    /// skip re-applying their DB writes and re-sending replies.
    fast_path_executed: HashSet<(ClientId, CommandId)>,

    log_hash: LogHash,

    stats_window_start: Instant,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        OmniPaxosServer {
            id: config.local.server_id,
            database: Database::new(),
            network,
            dom: Dom::new(DomConfig {
                clock: config.clock.clone(),
                owd: config.owd.clone(),
            }),
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
            proxy_command_ids: HashSet::new(),
            fast_path_executed: HashSet::new(),
            log_hash: LogHash::new(),
            commit_queue: CommitQueue::new(),
            stats_window_start: Instant::now(),
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut proxy_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let stats_start = tokio::time::Instant::now() + STATS_FLUSH_INTERVAL;
        let mut stats_interval = tokio::time::interval_at(stats_start, STATS_FLUSH_INTERVAL);
        stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut late_drain_interval = tokio::time::interval(LATE_BUFFER_DRAIN_INTERVAL);
        late_drain_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failure_interval = tokio::time::interval(Duration::from_millis(
            self.config.local.failure_check_interval_ms.max(1),
        ));
        failure_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failed_until: Option<tokio::time::Instant> = None;
        let mut injected_failures: u64 = 0;
        loop {
            if let Some(until) = failed_until {
                if tokio::time::Instant::now() >= until {
                    failed_until = None;
                    warn!("{}: simulated failure ended, server rejoined", self.id);
                }
            }
            let currently_failed = failed_until.is_some();

            // Compute when the next deadline is
            let duration = self.dom.duration_until_next_deadline();
            let mut deadline_sleep = duration.map(|d| Box::pin(tokio::time::sleep(d)));
            tokio::select! {
                _ = election_interval.tick() => {
                    if !currently_failed {
                        self.omnipaxos.tick();
                        self.send_outgoing_msgs();
                    }
                },
                _ = late_drain_interval.tick() => {
                    if !currently_failed {
                        self.drain_late_buffer();
                        self.send_outgoing_msgs();
                    }
                },
                _ = failure_interval.tick(), if self.failure_injection_enabled() => {
                    if !currently_failed
                        && self.can_inject_more_failures(injected_failures)
                        && self.should_halt_now()
                    {
                        error!(
                            "{}: failure injection triggered, server unavailable for {} ms (p={:.4})",
                            self.id,
                            self.config.local.failure_downtime_ms,
                            self.config.local.failure_probability
                        );
                        injected_failures = injected_failures.saturating_add(1);
                        self.flush_stats();
                        failed_until = Some(
                            tokio::time::Instant::now()
                                + Duration::from_millis(self.config.local.failure_downtime_ms.max(1)),
                        );
                    }
                },
                _ = stats_interval.tick() => {
                    self.flush_stats();
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    if currently_failed {
                        cluster_msg_buf.clear();
                    } else {
                        self.handle_cluster_messages(&mut cluster_msg_buf).await;
                    }
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    if currently_failed {
                        client_msg_buf.clear();
                    } else {
                        self.handle_client_messages(&mut client_msg_buf).await;
                    }
                },
                _ = self.network.proxy_messages.recv_many(&mut proxy_msg_buf, NETWORK_BATCH_SIZE) => {
                    if currently_failed {
                        proxy_msg_buf.clear();
                    } else {
                        self.handle_proxy_messages(&mut proxy_msg_buf).await;
                    }
                },                
                _ = async {
                    match &mut deadline_sleep {
                        Some(s) => s.as_mut().await,
                        None => std::future::pending::<()>().await,
                    }
                }, if deadline_sleep.is_some() => {
                    if !currently_failed {
                        // drain any pending Paxos decisions BEFORE executing
                        // deadline commands so reads observe all committed writes.
                        self.handle_decided_entries();
                        let committable_messages = self.dom.handle_deadline();
                        for msg in committable_messages {
                            match msg.message {
                                ClientMessage::Append(command_id, kv_command) => {
                                    debug!("{}: early path — processing message (client_id={}, command_id={})", self.id, msg.client_id, command_id);
                                    let command = Command {
                                        client_id: msg.client_id,
                                        coordinator_id: self.id,
                                        id: command_id,
                                        kv_cmd: kv_command,
                                    };

                                    if self.id == self.omnipaxos.get_current_leader().unwrap().0 {
                                        self.update_database_and_respond_fast(command);
                                    } else {
                                        // If we are a follower we just respond with a fast reply
                                        // We append the command to the command buffer to be committed later
                                        // We need to ensure that we will add the commands in order
                                        self.respond_fast(command);
                                    }
                                }
                            }
                        }
                        self.send_outgoing_msgs();
                    }
                },
            }

            if failed_until.is_none() {
                self.flush_safe_to_commit_commands();
            }
        }
    }

    fn should_halt_now(&self) -> bool {
        let p = self.config.local.failure_probability;
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        rand::thread_rng().gen_bool(p)
    }

    fn failure_injection_enabled(&self) -> bool {
        self.config.local.failure_injection || self.config.local.failure_probability > 0.0
    }

    fn can_inject_more_failures(&self, injected_failures: u64) -> bool {
        let max_events = self.config.local.failure_max_events;
        max_events == 0 || injected_failures < max_events
    }
    

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            self.send_client_start_signals(experiment_sync_start);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();

            // Update the log hash with the decided commands
            for cmd in &decided_commands {
                self.log_hash.add_entry(cmd);
            }
            self.update_database_and_respond(decided_commands);
        }
    }

    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        for command in commands {
            let key = (command.client_id, command.id);
            if self.proxy_command_ids.remove(&key) {
                if self.fast_path_executed.remove(&key) {
                    self.commit_queue.remove_by_key(command.client_id, command.id);
                    continue;
                }
                let read = self.database.handle_command(command.kv_cmd);
                self.commit_queue.remove_by_key(command.client_id, command.id);
                if command.coordinator_id == self.id {
                    let result = match read {
                        Some(r) => ServerResult::Read(command.id, r),
                        None => ServerResult::Write(command.id),
                    };
                    let deadline_length = self.compute_deadline_length();
                    let reply = SlowPathReply {
                        replica_id: self.id,
                        client_id: command.client_id,
                        request_id: command.id,
                        result: Some(result),
                        deadline_length,
                    };
                    debug!("{}: slow path — sending SlowPathReply (client_id={}, command_id={})", self.id, command.client_id, command.id);
                    self.network.send_to_proxy(ServerMessage::SlowPathReply(reply));
                }
            } else {
                if self.fast_path_executed.remove(&key) {
                    // The fast-path already applied the command and sent a FastReply.
                    // If the proxy subsequently aborted the fast-path (e.g., hash
                    // inconsistency), it is still waiting for a slow-path reply.
                    // Send one from the coordinator; the proxy discards it silently
                    // if the fast-path already committed (key no longer in `pending`).
                    if command.coordinator_id == self.id {
                        let result = match &command.kv_cmd {
                            KVCommand::Put(_, _) => ServerResult::Write(command.id),
                            _ => {
                                let read = self.database.handle_command(command.kv_cmd.clone());
                                ServerResult::Read(command.id, read.flatten())
                            }
                        };
                        let deadline_length = self.compute_deadline_length();
                        let reply = SlowPathReply {
                            replica_id: self.id,
                            client_id: command.client_id,
                            request_id: command.id,
                            result: Some(result),
                            deadline_length,
                        };
                        self.network.send_to_proxy(ServerMessage::SlowPathReply(reply));
                    }
                } else {
                    let read = self.database.handle_command(command.kv_cmd);
                    if command.coordinator_id == self.id {
                        let response = match read {
                            Some(r) => ServerMessage::Read(command.id, r),
                            None => ServerMessage::Write(command.id),
                        };
                        self.network.send_to_client(command.client_id, response);
                    }
                }
            }
        }
    }

    fn update_database_and_respond_fast(&mut self, command: Command) {
        // Clone kv_cmd so we can both apply it to the DB and append the full
        // Command to the Paxos log for durability (RC1).
        let read = self.database.handle_command(command.kv_cmd.clone());
        let ballot = self.omnipaxos.get_promise();
        let deadline_length = self.compute_deadline_length();
        let fast_reply = FastReply {
            ballot,
            replica_id: self.id,
            client_id: command.client_id,
            request_id: command.id,
            result: match read {
                Some(read_result) => Some(ServerResult::Read(command.id, read_result)),
                None => Some(ServerResult::Write(command.id)),
            },
            hash: self.log_hash.clone(), // Use current log hash as snapshot for the fast reply; ensures read-your-writes consistency for commands executed via the fast-path
            deadline_length,
        };

        let key = (command.client_id, command.id);
        let msg = ServerMessage::FastReply(fast_reply);
        if self.proxy_command_ids.remove(&key) {
            self.fast_path_executed.insert(key);
            self.omnipaxos
                .append(command)
                .expect("Append to Omnipaxos log failed");
            self.network.send_to_proxy(msg);
        } else {
            let client_id = command.client_id;
            let cmd_id = command.id;
            warn!("No proxy command id found for fast reply to client {client_id} with command id {cmd_id}");
        }
    }

    // Replicas just respond with a fast reply without updating the database
    fn respond_fast(&mut self, command: Command) {
        // Different from Niezha logic
        // We must have consistent log hashes and consistent logs across replicas for the fast-path
        // Incase of inconsistencies, omnipaxos will ensure agreement eventually across majority
        let read = self.database.handle_command(command.kv_cmd.clone());
        let ballot = self.omnipaxos.get_promise();
        let deadline_length = self.compute_deadline_length();
        let fast_reply = FastReply {
            ballot,
            replica_id: self.id,
            client_id: command.client_id,
            request_id: command.id,
            result: None,
            // Use arrival-time snapshot for the same reason as the leader path.
            hash: self.log_hash.clone(),
            deadline_length,
        };

        let msg = ServerMessage::FastReply(fast_reply);
        // Since we have not executed the command we still keep it in the proxy command ids
        self.network.send_to_proxy(msg);
        self.commit_queue.push(command, CommitState::Pending);
    }

    fn update_database(&mut self, command: Command) {
        if self.proxy_command_ids.remove(&(command.client_id, command.id)) {
            self.database.handle_command(command.kv_cmd);
        }
    }

    fn compute_deadline_length(&mut self) -> u64 {
        if self.config.local.adaptive_deadline {
            self.dom.request_deadline_from_owd(DEFAULT_NODE_ID)
        } else {
            self.config.local.default_deadline
        }
    }
        

    fn flush_safe_to_commit_commands(&mut self) {
        let commands: Vec<Command> = self.commit_queue.drain_safe_to_commit_commands();

        if commands.is_empty() {
            return;
        }

        for command in commands {
            self.update_database(command);
        }
    }


    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => {
                    self.append_to_log(from, command_id, kv_command)
                }
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_proxy_messages(&mut self, messages: &mut Vec<ProxyMessage>) {
        for proxy_msg in messages.drain(..) {
            match proxy_msg {
                ProxyMessage::Append(dom_message) => {
                    if self.config.local.use_proxy {
                        match &dom_message.message {
                            ClientMessage::Append(command_id, _) => {
                                self.proxy_command_ids
                                    .insert((dom_message.client_id, *command_id));
                            }
                        }
                    }
                    let message_passing_delay = self.dom.get_time() - dom_message.send_time; // TODO: what about uncertainty here?
                    self.dom
                        .add_element_to_owd(DEFAULT_NODE_ID, message_passing_delay);
                    self.dom.push_by_deadline(dom_message);
                }
                ProxyMessage::AbortFastPath(dom_message) => {
                    if let Some((leader_id, _)) = self.omnipaxos.get_current_leader() {
                        if self.id == leader_id {
                            if let ClientMessage::Append(command_id, _) = &dom_message.message {
                                let key = (dom_message.client_id, *command_id);
                                if self.fast_path_executed.contains(&key) {
                                    debug!(
                                        "{}: fast path abort ignored — command already executed (client_id={}, command_id={})",
                                        self.id, dom_message.client_id, command_id
                                    );
                                } else {
                                    if self.config.local.use_proxy {
                                        self.proxy_command_ids.insert(key);
                                    }
                                    debug!(
                                        "{}: fast path aborted — adding to slow-path buffer (client_id={}, command_id={})",
                                        self.id,
                                        dom_message.client_id,
                                        dom_message.message.command_id()
                                    );
                                    self.dom.push_to_late_buffer(dom_message);
                                }
                            }
                        } else {
                            if let ClientMessage::Append(command_id, _) = &dom_message.message {
                                self.commit_queue
                                    .remove_by_key(dom_message.client_id, *command_id);
                            }
                            debug!(
                                "{}: fast path abort received for (client_id={}, command_id={}) but not leader; ignoring",
                                self.id,
                                dom_message.client_id,
                                dom_message.message.command_id()
                            );
                        }
                    }
                }
            }
        }
        self.drain_late_buffer();
        self.send_outgoing_msgs();
    }

    fn drain_late_buffer(&mut self) {
        if let Some((leader_id, _)) = self.omnipaxos.get_current_leader() {
            if self.id == leader_id {
                while let Some(dom_message) = self.dom.pop_from_late_buffer() {
                    match dom_message.message {
                        ClientMessage::Append(command_id, kv_cmd) => {
                            self.append_to_log(dom_message.client_id, command_id, kv_cmd);
                        }
                    }
                }
            }
        }
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries();
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        if self.config.local.use_proxy {
            self.network.set_start_signal(start_time);
        }
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
        if self.config.local.use_proxy {
            self.network.send_to_proxy(ServerMessage::StartSignal(start_time));
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }

    fn flush_stats(&mut self) {
        let elapsed = self.stats_window_start.elapsed().as_secs_f64();
        self.stats_window_start = Instant::now();

        let (early, late) = self.dom.take_buffer_counts();
        let early_rps = if elapsed > 0.0 { early as f64 / elapsed } else { 0.0 };
        let late_rps  = if elapsed > 0.0 { late  as f64 / elapsed } else { 0.0 };

        let stats = ServerStats {
            config: &self.config,
            early_buffer_rate_rps: early_rps,
            late_buffer_rate_rps: late_rps,
        };
        match serde_json::to_string_pretty(&stats) {
            Ok(json) => {
                if let Ok(mut f) = File::create(&self.config.local.output_filepath) {
                    let _ = f.write_all(json.as_bytes());
                    let _ = f.flush();
                }
            }
            Err(e) => warn!("{}: Failed to serialize stats: {e}", self.id),
        }
    }}
