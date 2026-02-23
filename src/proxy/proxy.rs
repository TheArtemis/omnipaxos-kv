use crate::proxy::config::ProxyConfig;

pub struct Proxy {
   config: ProxyConfig,

}

impl Proxy {
    pub fn new(config: ProxyConfig) -> Self {
        Self { config }
    }

    //TODO:

    // Multicast

    // Super quorum check for the proxy

    // Handle acknowledgement from the servers
}