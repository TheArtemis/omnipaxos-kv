use log::{info, warn};

use crate::common::kv::{ClientId, CommandId, NodeId};
use crate::common::messages::{ClientMessage, ServerMessage};
use crate::dom::request::DomMessage;
use crate::proxy::config::{ProxyConfig, Server};
use crate::proxy::network::Network;
use std::collections::HashMap;
use std::net::SocketAddr;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Proxy {
    config: ProxyConfig,
    network: Network,
    pending: HashMap<(ClientId, CommandId), ()>,
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
        let network = Network::new(listen_address, servers, NETWORK_BATCH_SIZE).await;
        Self {
            config,
            network,
            pending: HashMap::new(),
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
                    self.pending.insert((client_id, *command_id), ());
                }
            }
            let send_time = 0;
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
                        let _ = self.pending.remove(&(client_id, inner.command_id()));
                        self.network.send_to_client(client_id, inner);
                    }
                    other => {
                        warn!("Unexpected proxy inner message: {other:?}");
                    }
                },
                ServerMessage::StartSignal(_) => {
                    self.network.send_to_all_clients(message);
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
}
