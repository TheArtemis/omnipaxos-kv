// Dom config

// We need to have the client (proxy) address

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DomConfig {
    pub proxy_address: String,
}