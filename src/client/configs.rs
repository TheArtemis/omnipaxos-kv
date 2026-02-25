use std::{env, time::Duration};

use config::{Config, ConfigError, Environment, File};
use omnipaxos_kv::common::{kv::NodeId, utils::Timestamp};
use omnipaxos_kv::clock::ClockConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ClusterDiscoveryConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub location: String,
    pub server_id: NodeId,
    pub server_address: String,
    #[serde(default, rename = "isProxyOn")]
    pub is_proxy_on: bool,
    #[serde(default, rename = "clusterConfigFile")]
    pub cluster_config_file: Option<String>,
    pub requests: Vec<RequestInterval>,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
    pub clock: ClockConfig,
}

impl ClientConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config_file = match env::var("CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(Environment::with_prefix("OMNIPAXOS").try_parsing(true))
            .build()?;
        config.try_deserialize()
    }

    pub fn get_target_servers(&self) -> Result<Vec<(NodeId, String)>, ConfigError> {
        if !self.is_proxy_on {
            return Ok(vec![(self.server_id, self.server_address.clone())]);
        }

        let cluster_config_file = self
            .cluster_config_file
            .clone()
            .or_else(|| env::var("CLUSTER_CONFIG_FILE").ok())
            .unwrap_or_else(|| "build_scripts/cluster-config.toml".to_string());

        let cluster_cfg: ClusterDiscoveryConfig = Config::builder()
            .add_source(File::with_name(&cluster_config_file))
            .build()?
            .try_deserialize()?;

        if cluster_cfg.nodes.len() != cluster_cfg.node_addrs.len() {
            panic!(
                "Cluster config mismatch: nodes({}) != node_addrs({})",
                cluster_cfg.nodes.len(),
                cluster_cfg.node_addrs.len()
            );
        }

        Ok(cluster_cfg
            .nodes
            .into_iter()
            .zip(cluster_cfg.node_addrs)
            .collect())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestInterval {
    pub duration_sec: u64,
    pub requests_per_sec: u64,
    pub read_ratio: f64,
}

impl RequestInterval {
    pub fn get_read_ratio(&self) -> f64 {
        self.read_ratio
    }

    pub fn get_interval_duration(&self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    pub fn get_request_delay(&self) -> Duration {
        if self.requests_per_sec == 0 {
            return Duration::from_secs(999999);
        }
        let delay_ms = 1000 / self.requests_per_sec;
        assert!(delay_ms != 0);
        Duration::from_millis(delay_ms)
    }
}
