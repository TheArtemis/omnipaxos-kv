// Implement a config for the proxy that takes in a list of servers addresses
// Most likely we want to implement a [proxy] config section in the TOML file

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProxyConfig {
    pub servers_addresses: Vec<String>, // Check the data type and match the client config
}