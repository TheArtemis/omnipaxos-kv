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

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A snapshot of both clocks captured at the same moment during synchronization.
/// We need two clocks because:
/// - `wall_us` (SystemTime) gives us the real-world anchor timestamp
/// - `instant` (Instant) gives us a monotonic reference for measuring elapsed time safely
struct SyncPoint {
    /// Wall clock time at sync, in μs since UNIX_EPOCH
    wall_us: i64,
    /// Monotonic instant at sync (never jumps; used to measure elapsed time)
    instant: Instant,
}

impl SyncPoint {
    fn now() -> Self {
        Self {
            wall_us: Self::system_time_micros(SystemTime::now()),
            instant: Instant::now(),
        }
    }

    fn elapsed_us(&self) -> f64 {
        Instant::now().duration_since(self.instant).as_micros() as f64
    }

    fn system_time_micros(t: SystemTime) -> i64 {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_micros() as i64,
            Err(e) => -(e.duration().as_micros() as i64),
        }
    }
}

/// Simulated clock with configurable drift and sync uncertainty.
pub struct ClockSim {
    /// The last synchronization point (wall clock + monotonic instant, captured together)
    sync_point: SyncPoint,
    /// Drift rate in μs per second (positive = fast, negative = slow)
    drift_rate: f64,
    /// Synchronization uncertainty ±ε in μs (at the moment of sync)
    uncertainty_bound: f64,
    /// How often to resync (derived from sync_freq)
    sync_interval: Duration,
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
        Self {
            sync_point: SyncPoint::now(),
            drift_rate: drift,
            uncertainty_bound: epsilon,
            sync_interval: Duration::from_secs_f64(1.0 / sync_freq),
        }
    }

    /// Returns current logical time in microseconds (μs) since UNIX_EPOCH.
    /// Time advances with drift: each real second adds `1_000_000 + drift_rate` μs.
    pub fn get_time(&mut self) -> u64 {
        if Instant::now().duration_since(self.sync_point.instant) >= self.sync_interval {
            self.synchronize();
        }

        let elapsed_us = self.sync_point.elapsed_us();
        // Drift: drift_rate μs per real second → (elapsed_us / 1e6) * drift_rate
        let drift_us = (elapsed_us / 1_000_000.0) * self.drift_rate;
        (self.sync_point.wall_us as f64 + elapsed_us + drift_us) as u64
    }

    /// Logical synchronization: anchors the sync point to the current system time.
    /// (A real sync would exchange messages for RTT/offset estimation.)
    fn synchronize(&mut self) {
        self.sync_point = SyncPoint::now();
    }

    /// Returns the synchronization uncertainty in microseconds (μs).
    /// True logical time lies in [get_time() − get_uncertainty(), get_time() + get_uncertainty()].
    /// Uncertainty grows over time as drift accumulates, regardless of drift direction.
    pub fn get_uncertainty(&self) -> f64 {
        let elapsed_us = self.sync_point.elapsed_us();
        self.uncertainty_bound + self.drift_rate.abs() * (elapsed_us / 1_000_000.0)
    }
}
