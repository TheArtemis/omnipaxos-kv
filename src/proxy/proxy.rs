use crate::common::messages::ClientMessage;
use crate::dom::request::DomMessage;
use crate::proxy::config::{ProxyConfig, Server};

pub struct Proxy {
    config: ProxyConfig,
}

impl Proxy {
    pub fn new(config: ProxyConfig) -> Self {
        Self { config }
    }

    /// Load proxy from cluster-config (CLUSTER_CONFIG_FILE env or default path).
    pub fn from_cluster_config() -> Result<Self, config::ConfigError> {
        let path = std::env::var("CLUSTER_CONFIG_FILE")
            .unwrap_or_else(|_| "build_scripts/cluster-config.toml".to_string());
        Self::from_file(path)
    }

    /// Load proxy config from a cluster-config-style TOML at the given path.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, config::ConfigError> {
        let config = ProxyConfig::from_file(path)?;
        Ok(Self { config })
    }    

    pub fn send(&self, client_message: ClientMessage, send_time: u64) {
        // Compute deadline
        let deadline = self.get_deadline(send_time);

        // Create message
        let message = DomMessage::new(client_message, deadline, send_time);
        
        // Multicast to all servers
        self.multicast(message);

    }

    pub fn multicast(&self, message: DomMessage) {
        for server in self.config.targets() {
            let id = server.id;
            let address = server.address;

            // SEND MESSAGE. We need a network tho!
        }
    }

    fn get_deadline(&self, send_time: u64) -> u64 {
        send_time + 1000 // TODO change this!!!
    }
}
