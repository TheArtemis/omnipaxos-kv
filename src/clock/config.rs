//! Clock configuration for simulator parameters.

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

/// Clock simulation parameters. All units aligned with spec.
///
/// | Field              | Unit           | Description                              | Example  |
/// |--------------------|----------------|------------------------------------------|----------|
/// | drift_rate         | μs/s           | Drift per real second; (+) fast (−) slow | 50       |
/// | uncertainty_bound  | μs             | ±ε sync uncertainty; time in [t−ε, t+ε]  | 10, 100  |
/// | sync_freq          | Hz             | Resyncs per second; interval = 1/freq    | 1000     |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClockConfig {
    /// Drift rate in μs per second
    #[serde(default = "ClockConfig::default_drift_rate")]
    pub drift_rate: f64,
    /// Sync uncertainty ±ε in μs (e.g. 10 = ±10μs, 1000 = ±1ms)
    #[serde(default = "ClockConfig::default_uncertainty_bound")]
    pub uncertainty_bound: f64,
    /// Sync frequency in Hz (e.g. 1000 = 1ms interval, 100 = 10ms, 10 = 100ms)
    #[serde(default = "ClockConfig::default_sync_freq")]
    pub sync_freq: f64,
}

impl ClockConfig {
    /// Load clock config from the file path in `CONFIG_FILE` env var.
    pub fn from_env() -> Result<Self, ConfigError> {
        let path = std::env::var("CONFIG_FILE")
            .map_err(|_| ConfigError::Message("CONFIG_FILE environment variable not set".into()))?;
        Self::from_file(&path)
    }

    /// Load clock config from a TOML file. Supports:
    /// - Files with a `[clock]` section (e.g. client/server configs)
    /// - Flat files with `drift_rate`, `uncertainty_bound`, `sync_freq` at root
    ///
    /// Environment variables `OMNIPAXOS_CLOCK_DRIFT_RATE`, `OMNIPAXOS_CLOCK_UNCERTAINTY_BOUND`,
    /// `OMNIPAXOS_CLOCK_SYNC_FREQ` override file values.
    pub fn from_file(config_file: &str) -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name(config_file))
            .add_source(Environment::with_prefix("OMNIPAXOS_CLOCK").try_parsing(true))
            .build()?;
        config.get("clock").or_else(|_| config.try_deserialize())
    }

    fn default_drift_rate() -> f64 {
        50.0 // 50 μs per second
    }
    fn default_uncertainty_bound() -> f64 {
        100.0 // ±100 μs (medium quality)
    }
    fn default_sync_freq() -> f64 {
        100.0 // 10 ms sync interval (medium quality)
    }
}

impl Default for ClockConfig {
    fn default() -> Self {
        Self {
            drift_rate: Self::default_drift_rate(),
            uncertainty_bound: Self::default_uncertainty_bound(),
            sync_freq: Self::default_sync_freq(),
        }
    }
}
