use std::env;

use config::{Config, ConfigError, Environment, File};
use omnipaxos::{
    util::{FlexibleQuorum, NodeId},
    ClusterConfig as OmnipaxosClusterConfig, OmniPaxosConfig,
    ServerConfig as OmnipaxosServerConfig,
};
use serde::{Deserialize, Serialize};
use omnipaxos_kv::clock::ClockConfig;
use omnipaxos_kv::owd::config::OwdConfig;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    pub initial_leader: NodeId,
    pub initial_flexible_quorum: Option<FlexibleQuorum>,
    /// Used by proxy (e.g. superquorum); server ignores.
    #[serde(default)]
    pub initial_flexible_superquorum: Option<FlexibleQuorum>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalConfig {
    pub location: Option<String>,
    pub server_id: NodeId,
    pub listen_address: String,
    pub listen_port: u16,
    pub num_clients: usize,
    pub proxy_address: String,
    #[serde(default)]
    pub use_proxy: bool,
    pub adaptive_deadline: bool,
    pub default_deadline: u64,
    pub output_filepath: String,
    /// crash-stop behavior for failure-injection experiments.
    #[serde(default)]
    pub failure_injection: bool,
    #[serde(default = "default_failure_probability")]
    pub failure_probability: f64,
    #[serde(default = "default_failure_check_interval_ms")]
    pub failure_check_interval_ms: u64,
    /// Duration of simulated outage before automatic rejoin.
    #[serde(default = "default_failure_downtime_ms")]
    pub failure_downtime_ms: u64,
    /// Maximum number of simulated failures. 0 means unlimited.
    #[serde(default = "default_failure_max_events")]
    pub failure_max_events: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosKVConfig {
    #[serde(flatten)]
    pub local: LocalConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
    pub clock: ClockConfig,
    #[serde(default)]
    pub owd: OwdConfig,
}

impl Into<OmniPaxosConfig> for OmniPaxosKVConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = OmnipaxosClusterConfig {
            configuration_id: 1,
            nodes: self.cluster.nodes,
            flexible_quorum: self.cluster.initial_flexible_quorum,
        };
        let server_config = OmnipaxosServerConfig {
            pid: self.local.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl OmniPaxosKVConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        let cfg: OmniPaxosKVConfig = config.try_deserialize()?;
        if !(0.0..=1.0).contains(&cfg.local.failure_probability) {
            return Err(ConfigError::Message(format!(
                "Invalid failure_probability {}: expected value in [0.0, 1.0]",
                cfg.local.failure_probability
            )));
        }
        Ok(cfg)
    }

    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.cluster
            .nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
    }
}

fn default_failure_probability() -> f64 {
    0.0
}

fn default_failure_check_interval_ms() -> u64 {
    1000
}

fn default_failure_downtime_ms() -> u64 {
    3000
}

fn default_failure_max_events() -> u64 {
    1
}
