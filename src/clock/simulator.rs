//! Clock simulator for synchronized clocks with configurable drift and uncertainty.
//!
//! ## Units (all aligned to spec)
//!
//! | Parameter    | Unit           | Description                                  |
//! |--------------|----------------|----------------------------------------------|
//! | Drift rate   | μs per second  | Clock gains (+) or loses (−) this many μs/s  |
//! | Uncertainty  | μs             | ±ε bound after sync; true time in [t−ε, t+ε] |
//! | Sync freq    | Hz             | Resyncs per second (interval = 1/freq s)     |
//! | get_time()   | μs             | Logical timestamp in microseconds            |

use std::time::{Duration, Instant};

/// Simulated clock with configurable drift and sync uncertainty.
pub struct ClockSim {
    /// Reference instant when the clock started
    start_instant: Instant,
    /// Logical time (μs) at last sync
    base_offset: i64,
    /// Drift rate in μs per second (positive = fast, negative = slow)
    drift_rate: f64,
    /// Synchronization uncertainty ±ε in μs
    uncertainty_bound: f64,
    /// How often to resync (derived from sync_freq)
    sync_interval: Duration,
    /// Last sync wall-clock time
    last_sync_time: Instant,
}

impl ClockSim {
    /// Creates a new clock simulator.
    ///
    /// # Arguments
    /// * `drift` - Drift rate in μs per second
    /// * `epsilon` - Synchronization uncertainty ±ε in μs
    /// * `sync_freq` - Sync frequency in Hz (interval in seconds = 1/sync_freq)
    pub fn new(drift: f64, epsilon: f64, sync_freq: f64) -> Self {
        assert!(sync_freq > 0.0, "sync_freq must be > 0.0");
        let interval_secs = 1.0 / sync_freq;
        let sync_interval = Duration::from_secs_f64(interval_secs);
        let now = Instant::now();

        Self {
            start_instant: now,
            base_offset: 0,
            drift_rate: drift,
            uncertainty_bound: epsilon,
            sync_interval,
            last_sync_time: now,
        }
    }

    /// Returns current logical time in microseconds (μs).
    /// Time advances with drift: each real second adds `1_000_000 + drift_rate` μs.
    pub fn get_time(&mut self) -> u64 {
        let now = Instant::now();
        if now.duration_since(self.last_sync_time) >= self.sync_interval {
            self.synchronize(now);
        }

        let elapsed_us = now.duration_since(self.last_sync_time).as_micros() as f64;
        // Drift: drift_rate μs per real second → (elapsed_us / 1e6) * drift_rate
        let drift_us = (elapsed_us / 1_000_000.0) * self.drift_rate;
        let simulated = (self.base_offset as f64) + elapsed_us + drift_us;
        simulated as u64
    }

    /// Logical synchronization: resets to real elapsed time since start.
    /// (A real sync would exchange messages for RTT/offset estimation.)
    fn synchronize(&mut self, now: Instant) {
        let real_now = now.duration_since(self.start_instant).as_micros() as i64;
        self.base_offset = real_now;
        self.last_sync_time = now;
    }

    /// Returns the synchronization uncertainty ±ε in microseconds (μs).
    /// True logical time lies in [get_time() − ε, get_time() + ε].
    pub fn get_uncertainty(&self) -> f64 {
        self.uncertainty_bound
    }
}
