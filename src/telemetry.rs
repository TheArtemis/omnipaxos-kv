use std::collections::HashMap;
use std::time::Instant;

use log::warn;
use omnipaxos::util::NodeId;
use serde::{Deserialize, Serialize};

pub const MAX_LATENCY_SAMPLES: usize = 1000;

// ---------------------------------------------------------------------------
// Per-node metrics
// ---------------------------------------------------------------------------

/// Fast-path counters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeMetrics {
    pub fast_path_count: usize,
    pub fast_path_latencies_us: Vec<u128>,
}

impl NodeMetrics {
    pub fn push_fast_path_latency(&mut self, latency: u128) {
        self.fast_path_latencies_us.push(latency);
        if self.fast_path_latencies_us.len() > MAX_LATENCY_SAMPLES {
            let excess = self.fast_path_latencies_us.len() - MAX_LATENCY_SAMPLES;
            self.fast_path_latencies_us.drain(0..excess);
        }
    }
}

// ---------------------------------------------------------------------------
// System-wide aggregated metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SystemMetrics {
    pub nodes: HashMap<NodeId, NodeMetrics>,
    pub total_sent: usize,
    pub fast_path_committed: usize,
    pub slow_path_committed: usize,
    pub fast_path_ratio: f64,
    pub slow_path_ratio: f64,
    pub fast_path_response_ratio: f64,
    pub slow_path_response_ratio: f64,
    pub overall_response_ratio: f64,
    pub throughput_rps: f64,
    pub slow_path_latencies_us: Vec<u128>,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_slow_path_latency(&mut self, latency: u128) {
        self.slow_path_latencies_us.push(latency);
        if self.slow_path_latencies_us.len() > MAX_LATENCY_SAMPLES {
            let excess = self.slow_path_latencies_us.len() - MAX_LATENCY_SAMPLES;
            self.slow_path_latencies_us.drain(0..excess);
        }
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
    pub throughput_window_count: usize,
    throughput_window_start: Instant,
}

impl TelemetryWriter {
    pub fn new(filepath: String) -> Self {
        Self {
            metrics: SystemMetrics::new(),
            filepath,
            throughput_window_count: 0,
            throughput_window_start: Instant::now(),
        }
    }

    pub fn flush(&mut self) {
        use std::io::Write;

        let elapsed_secs = self.throughput_window_start.elapsed().as_secs_f64();
        self.metrics.throughput_rps = if elapsed_secs > 0.0 {
            self.throughput_window_count as f64 / elapsed_secs
        } else {
            0.0
        };
        self.throughput_window_count = 0;
        self.throughput_window_start = Instant::now();

        let json = self.metrics.to_json();
        match std::fs::File::create(&self.filepath) {
            Ok(mut f) => {
                let _ = f.write_all(json.as_bytes());
            }
            Err(e) => warn!("Failed to write metrics to {}: {e}", self.filepath),
        }
    }
}
