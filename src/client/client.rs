use crate::{configs::ClientConfig, data_collection::ClientData, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos_kv::common::{kv::*, messages::*};
use rand::Rng;
use std::time::Duration;
use tokio::time::interval;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Client {
    id: ClientId,
    server_network: Network,
    proxy_network: Option<Network>,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let server_network = Network::new(
            vec![(config.server_id, config.server_address.clone())],
            NETWORK_BATCH_SIZE,
        )
        .await;
        let proxy_network = if config.use_proxy {
            Some(Network::new(
                vec![(config.server_id, config.proxy_address.clone())],
                NETWORK_BATCH_SIZE,
            )
            .await)
        } else {
            None
        };
        Client {
            id: config.server_id,
            server_network,
            proxy_network,
            active_server: config.server_id,
            config,
            client_data: ClientData::new(),
            final_request_count: None,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        let start_time = if self.config.use_proxy {
            let proxy_messages = &mut self
                .proxy_network
                .as_mut()
                .expect("Proxy network missing")
                .server_messages;
            tokio::select! {
                msg = self.server_network.server_messages.recv() => msg,
                msg = proxy_messages.recv() => msg,
            }
        } else {
            self.server_network.server_messages.recv().await
        };
        match start_time {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(&mut self.config, start_time).await;
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
            if self.config.use_proxy {
                tokio::select! {
                    biased;
                    Some(msg) = self.proxy_network.as_mut().unwrap().server_messages.recv() => {
                        self.handle_server_message(msg);
                        if self.run_finished() {
                            break;
                        }
                    }
                    Some(msg) = self.server_network.server_messages.recv() => {
                        if let ServerMessage::StartSignal(_) = msg {
                            // Ignore; handled already or forwarded via proxy.
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
            } else {
                tokio::select! {
                    biased;
                    Some(msg) = self.server_network.server_messages.recv() => {
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
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.server_network.shutdown();
        if let Some(proxy_network) = self.proxy_network.as_mut() {
            proxy_network.shutdown();
        }
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::StartSignal(_) => (),
            ServerMessage::Read(cmd_id, value) => {
                self.client_data.new_response(cmd_id, value);
            }
            ServerMessage::Write(cmd_id) => {
                self.client_data.new_response(cmd_id, None);
            }
            ServerMessage::FastReply(fr) => {
                let value = fr.result.and_then(|r| match r {
                    ServerResult::Read(_, v) => v,
                    ServerResult::Write(_) => None,
                });
                self.client_data.new_response(fr.request_id, value);
            }
            ServerMessage::SlowPathReply(sr) => {
                let value = sr.result.and_then(|r| match r {
                    ServerResult::Read(_, v) => v,
                    ServerResult::Write(_) => None,
                });
                self.client_data.new_response(sr.request_id, value);
            }
        }
    }

    async fn send_request(&mut self, is_write: bool) {
        let key = self.next_request_id.to_string();
        let write_value = if is_write { Some(key.clone()) } else { None };
        let cmd = match is_write {
            true => KVCommand::Put(key.clone(), key.clone()),
            false => KVCommand::Get(key.clone()),
        };
        
        let request = ClientMessage::Append(self.next_request_id, cmd);
        debug!("Sending {request:?}");

        if self.config.use_proxy {
            self.proxy_network
                .as_mut()
                .expect("Proxy network missing")
                .send(self.active_server, request)
                .await;
        } else {
            self.server_network.send(self.active_server, request).await;
        }

        self.client_data.new_request(is_write, key, write_value);
        self.next_request_id += 1
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.response_count() >= count {
                return true;
            }
        }
        false
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
        let history_path = self.config.summary_filepath.replace("client-", "history-");
        let client_id = self.config.summary_filepath
            .chars()
            .rev()
            .skip(5) // skip ".json"
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>()
            .parse::<u64>()
            .unwrap_or(self.id as u64);
        if let Err(e) = self.client_data.save_history(&history_path, client_id) {
            log::warn!("Failed to write history file {}: {}", history_path, e);
        }
        Ok(())
    }
}
