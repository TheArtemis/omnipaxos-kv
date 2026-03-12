use std::collections::HashMap;
use std::time::SystemTime;

use log::warn;
use omnipaxos::util::NodeId;
use serde::{Deserialize, Serialize};

use crate::common::kv::{ClientId, CommandId};

// ---------------------------------------------------------------------------
// Per-node metrics
// ---------------------------------------------------------------------------

/// Fast-path counters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeMetrics {}

// ---------------------------------------------------------------------------
// System-wide aggregated metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SystemMetrics {
    pub nodes: HashMap<NodeId, NodeMetrics>,
    pub total_sent: usize,
    pub fast_path_committed: usize,
    pub fast_path_aborted: usize,
    pub slow_path_committed: usize,
    pub fast_path_ratio: f64,
    pub slow_path_ratio: f64,
    pub avg_fast_path_latency_us: f64,
    pub avg_slow_path_latency_us: f64,
    pub avg_total_latency: f64,
    pub avg_throughput_rps: f64,
    pub max_owd_deadline: u64,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn recompute_ratios(&mut self) {
        let total = self.fast_path_committed + self.slow_path_committed;
        self.fast_path_ratio = if total > 0 {
            self.fast_path_committed as f64 / total as f64
        } else {
            0.0
        };
        self.slow_path_ratio = if total > 0 {
            self.slow_path_committed as f64 / total as f64
        } else {
            0.0
        };
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}

pub struct TelemetryWriter {
    pub metrics: SystemMetrics,
    filepath: String,
    created_at: SystemTime,
    fast_path_latency_total_us: u128,
    fast_path_latency_samples: usize,
    slow_path_latency_total_us: u128,
    slow_path_latency_samples: usize,
    /// Request send times for latency computation.
    pending_timestamps: HashMap<(ClientId, CommandId), SystemTime>,
}

impl TelemetryWriter {
    pub fn new(filepath: String) -> Self {
        let mut writer = Self {
            metrics: SystemMetrics::new(),
            filepath,
            created_at: SystemTime::now(),
            fast_path_latency_total_us: 0,
            fast_path_latency_samples: 0,
            slow_path_latency_total_us: 0,
            slow_path_latency_samples: 0,
            pending_timestamps: HashMap::new(),
        };

        // Clear stale metrics from previous runs as soon as telemetry starts.
        writer.flush();
        writer
    }

    /// Record that a client request was sent (timestamp + total_sent + ratios).
    pub fn record_client_request(&mut self, client_id: ClientId, command_id: CommandId) {
        self.pending_timestamps
            .insert((client_id, command_id), SystemTime::now());
        self.metrics.total_sent += 1;
        self.metrics.recompute_ratios();
    }

    /// Record latest OWD-based deadline from a replica.
    pub fn record_owd_deadline(&mut self, _replica_id: NodeId, deadline_length: u64) {
        if deadline_length > self.metrics.max_owd_deadline {
            self.metrics.max_owd_deadline = deadline_length;
        }
    }

    /// Record a fast-path commit (clear pending + committed count + ratios + throughput).
    pub fn record_fast_path_commit(&mut self, client_id: ClientId, request_id: CommandId) {
        if let Some(sent_at) = self.pending_timestamps.remove(&(client_id, request_id)) {
            if let Some(latency_us) = Self::elapsed_micros(sent_at) {
                self.fast_path_latency_total_us = self.fast_path_latency_total_us.saturating_add(latency_us);
                self.fast_path_latency_samples += 1;
                self.update_average_latencies();
            }
        }
        self.metrics.fast_path_committed += 1;
        self.metrics.recompute_ratios();
        self.update_average_throughput();
    }

    /// Record a slow-path commit (take latency + committed count + ratios + throughput).
    pub fn record_slow_path_commit(&mut self, client_id: ClientId, request_id: CommandId) {
        if let Some(sent_at) = self.pending_timestamps.remove(&(client_id, request_id)) {
            if let Some(latency_us) = Self::elapsed_micros(sent_at) {
                self.slow_path_latency_total_us =
                    self.slow_path_latency_total_us.saturating_add(latency_us);
                self.slow_path_latency_samples += 1;
                self.update_average_latencies();
            }
        }
        self.metrics.slow_path_committed += 1;
        self.metrics.recompute_ratios();
        self.update_average_throughput();
    }

    /// Record a fast-path abort (timeout fallback to slow path).
    pub fn record_fast_path_abort(&mut self, _client_id: ClientId, _request_id: CommandId) {
        // Keep the pending timestamp: the request continues on slow path and
        // latency is measured when the leader slow-path reply arrives.
        self.metrics.fast_path_aborted += 1;
        self.metrics.recompute_ratios();
    }

    fn update_average_throughput(&mut self) {
        let elapsed_secs = match self.created_at.elapsed() {
            Ok(duration) => duration.as_secs_f64(),
            Err(e) => {
                warn!("SystemTime drift while computing throughput: {e}");
                0.0
            }
        };
        let total_committed = self.metrics.fast_path_committed + self.metrics.slow_path_committed;
        self.metrics.avg_throughput_rps = if elapsed_secs > 0.0 {
            total_committed as f64 / elapsed_secs
        } else {
            0.0
        };
    }

    fn elapsed_micros(sent_at: SystemTime) -> Option<u128> {
        match sent_at.elapsed() {
            Ok(duration) => Some(duration.as_micros()),
            Err(e) => {
                warn!("SystemTime drift while computing latency: {e}");
                None
            }
        }
    }

    fn update_average_latencies(&mut self) {
        self.metrics.avg_fast_path_latency_us = if self.fast_path_latency_samples > 0 {
            self.fast_path_latency_total_us as f64 / self.fast_path_latency_samples as f64
        } else {
            0.0
        };
        self.metrics.avg_slow_path_latency_us = if self.slow_path_latency_samples > 0 {
            self.slow_path_latency_total_us as f64 / self.slow_path_latency_samples as f64
        } else {
            0.0
        };

        let total_samples = self.fast_path_latency_samples + self.slow_path_latency_samples;
        let total_latency_us = self
            .fast_path_latency_total_us
            .saturating_add(self.slow_path_latency_total_us);
        self.metrics.avg_total_latency = if total_samples > 0 {
            total_latency_us as f64 / total_samples as f64
        } else {
            0.0
        };
    }

    pub fn flush(&mut self) {
        use std::io::Write;

        self.update_average_throughput();

        let json = self.metrics.to_json();
        match std::fs::File::create(&self.filepath) {
            Ok(mut f) => {
                let _ = f.write_all(json.as_bytes());
            }
            Err(e) => warn!("Failed to write metrics to {}: {e}", self.filepath),
        }
    }
}
