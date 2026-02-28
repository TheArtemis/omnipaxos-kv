use std::path::Path;

use config::{Config, ConfigError, Environment, File};
use omnipaxos::util::{FlexibleQuorum, NodeId};
use serde::{Deserialize, Serialize};

/// A target server: id and address. Used by the proxy instead of separate id/address lists.
#[derive(Debug, Clone)]
pub struct Server {
    pub id: NodeId,
    pub address: String,
}

impl Server {
    pub fn as_endpoint(&self) -> (NodeId, String) {
        (self.id, self.address.clone())
    }
}

/// Proxy config. Load from file (e.g. cluster-config.toml); extra keys in the file are ignored.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProxyConfig {
    pub cluster_name: Option<String>,
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    #[serde(default = "default_proxy_listen_address")]
    pub proxy_listen_address: String,
    #[serde(default = "default_proxy_listen_port")]
    pub proxy_listen_port: u16,
    #[serde(default)]
    pub initial_flexible_superquorum: Option<FlexibleQuorum>,
}

impl ProxyConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let config = Config::builder()
            .add_source(File::with_name(path.to_string_lossy().as_ref()))
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        let cfg: ProxyConfig = config.try_deserialize()?;
        if cfg.nodes.len() != cfg.node_addrs.len() {
            return Err(ConfigError::Message(format!(
                "Proxy config mismatch: nodes({}) != node_addrs({})",
                cfg.nodes.len(),
                cfg.node_addrs.len()
            )));
        }
        Ok(cfg)
    }

    pub fn targets(&self) -> Vec<Server> {
        self.nodes
            .iter()
            .cloned()
            .zip(self.node_addrs.iter().cloned())
            .map(|(id, address)| Server { id, address })
            .collect()
    }
}

fn default_proxy_listen_address() -> String {
    "0.0.0.0".to_string()
}

fn default_proxy_listen_port() -> u16 {
    9000
}
