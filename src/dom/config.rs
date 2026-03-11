// Dom config

// We need to have the client (proxy) address

use serde::{Deserialize, Serialize};
use crate::owd::config::OwdConfig;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DomConfig {
    pub clock: crate::clock::ClockConfig,
    #[serde(default)]
    pub owd: OwdConfig,
}
