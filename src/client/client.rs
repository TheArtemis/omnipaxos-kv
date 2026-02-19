use crate::{configs::ClientConfig, data_collection::ClientData, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos_kv::common::{kv::*, messages::*};
use rand::Rng;
use std::time::Duration;
use tokio::time::interval;

#[cfg(feature = "correctness-check")]
use omnipaxos_kv::correctness::operation_history::{Input, Output};

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
    #[cfg(feature = "correctness-check")]
    operation_indices: std::collections::HashMap<CommandId, usize>, // Map command_id to operation_index
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let network = Network::new(
            vec![(config.server_id, config.server_address.clone())],
            NETWORK_BATCH_SIZE,
        )
        .await;
        Client {
            id: config.server_id,
            network,
            #[cfg(feature = "correctness-check")]
            client_data: ClientData::new_with_correctness(config.server_id),
            #[cfg(not(feature = "correctness-check"))]
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            final_request_count: None,
            next_request_id: 0,
            #[cfg(feature = "correctness-check")]
            operation_indices: std::collections::HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(&mut self.config, start_time).await;
                #[cfg(feature = "correctness-check")]
                {
                    // Set sync time for correctness tracking after waiting
                    // This ensures all clients use the same reference point (the actual scheduled start instant)
                    self.client_data.set_sync_time(start_time);
                }
            }
            _ => panic!("Error waiting for start signal"),
        }

        // Early end
        let intervals = self.config.requests.clone();
        if intervals.is_empty() {
            self.save_results().expect("Failed to save results");
            return;
        }

        // Initialize intervals
        let mut rng = rand::thread_rng();
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let mut read_ratio = first_interval.get_read_ratio();
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let _ = next_interval.tick().await;

        // Main event loop
        info!("{}: Starting requests", self.id);
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                }
                _ = request_interval.tick(), if self.final_request_count.is_none() => {
                    let is_write = rng.gen::<f64>() > read_ratio;
                    self.send_request(is_write).await;
                },
                _ = next_interval.tick() => {
                    match intervals.next() {
                        Some(new_interval) => {
                            read_ratio = new_interval.read_ratio;
                            next_interval = interval(new_interval.get_interval_duration());
                            next_interval.tick().await;
                            request_interval = interval(new_interval.get_request_delay());
                        },
                        None => {
                            self.final_request_count = Some(self.client_data.request_count());
                            if self.run_finished() {
                                break;
                            }
                        },
                    }
                },
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.network.shutdown();
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        debug!("Recieved {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let cmd_id = server_response.command_id();
                self.client_data.new_response(cmd_id);
                
                #[cfg(feature = "correctness-check")]
                {
                    if let Some(&op_index) = self.operation_indices.get(&cmd_id) {
                        let output = match &server_response {
                            ServerMessage::Read(_, value) => Output {
                                status: "ok".to_string(),
                                value: value.clone(),
                            },
                            ServerMessage::Write(_) => Output {
                                status: "ok".to_string(),
                                value: None,
                            },
                            ServerMessage::StartSignal(_) => unreachable!(),
                        };
                        self.client_data.complete_operation(op_index, output);
                        self.operation_indices.remove(&cmd_id);
                    }
                }
            }
        }
    }

    async fn send_request(&mut self, is_write: bool) {
        let key = self.next_request_id.to_string();
        let cmd = match is_write {
            true => KVCommand::Put(key.clone(), key.clone()),
            false => KVCommand::Get(key.clone()),
        };
        
        #[cfg(feature = "correctness-check")]
        {
            self.record_operation_for_cmd(&cmd);
        }
        
        let request = ClientMessage::Append(self.next_request_id, cmd);
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }

    #[cfg(feature = "correctness-check")]
    fn record_operation_for_cmd(&mut self, cmd: &KVCommand) {
        let input = match cmd {
            KVCommand::Put(k, v) => Input::Put {
                key: k.clone(),
                value: v.clone(),
            },
            KVCommand::Get(k) => Input::Get {
                key: k.clone(),
            },
            KVCommand::Delete(k) => Input::Delete {
                key: k.clone(),
            },
        };
        if let Some(op_index) = self.client_data.record_operation(input) {
            self.operation_indices.insert(self.next_request_id, op_index);
        }
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        return false;
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(config: &mut ClientConfig, scheduled_start_utc_ms: i64) {
        // // Desync the clients a bit
        // let mut rng = rand::thread_rng();
        // let scheduled_start_utc_ms = scheduled_start_utc_ms + rng.gen_range(1..100);
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        config.sync_time = Some(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data.save_summary(self.config.clone())?;
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        
        #[cfg(feature = "correctness-check")]
        {
            self.export_history_json();
        }
        
        Ok(())
    }

    #[cfg(feature = "correctness-check")]
    fn export_history_json(&self) {
        let history_path = if let Some(ref path) = self.config.history_output_path {
            path.clone()
        } else {
            // Default to logs directory with client ID
            // Since logs/ is mounted, write directly there
            format!("/app/logs/history-{}.json", self.id)
        };
        if let Err(e) = self.client_data.export_history_json(&history_path) {
            warn!("Failed to export history JSON: {}", e);
        } else {
            info!("Exported operation history to {}", history_path);
        }
    }
}
