use crate::proxy::config::ProxyConfig;
use crate::common::kv::NodeId;

pub struct Proxy {
    config: ProxyConfig,
}

impl Proxy {
    pub fn new(config: ProxyConfig) -> Self {
        Self { config }
    }

    pub fn is_on(&self) -> bool {
        self.config.is_proxy_on
    }

    // Multicast
    
    pub fn target_servers(&self) -> &[NodeId] {
        &self.config.target_servers
    }

    pub fn has_targets(&self) -> bool {
        !self.config.target_servers.is_empty()
    }

    pub fn multicast(&self) -> bool {
        self.is_on() && self.has_targets()
    }

    // Super quorum check 

    // Handle acknowledgments

}