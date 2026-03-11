use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OwdConfig {
    pub default_value: u64,
    pub percentile: f64,
    pub window_size: u64,
}

impl Default for OwdConfig {
    fn default() -> Self {
        Self {
            default_value: 1000,
            percentile: 0.95,
            window_size: 1000,
        }
    }
}
