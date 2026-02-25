use serde::{Deserialize, Serialize};

use crate::common::kv::NodeId;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProxyConfig {
    pub is_proxy_on: bool,
    pub target_servers: Vec<NodeId>,
}