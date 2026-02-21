use std::time::{Duration, Instant};



pub struct ClockSim{
    start_instant: Instant,   //The reference
    base_offset: i64,
    drift_rate: f64,
    uncertainty_bound: f64,  // precision of the sync
    sync_interval: Duration,  // I need to obtain it from sync frequency
    last_sync_time: Instant,
}

impl ClockSim{

    pub fn new(drift: f64, epsilon: f64, sync_freq: f64) -> Self{
        assert!(sync_freq > 0.0, "sync_freq must be > 0.0");
        let interval = 1.0 / sync_freq; // obtain sync time from frequency
        let sync_interval = Duration::from_secs_f64(interval);
        let now = Instant::now();

        Self { start_instant: now,
            base_offset: 0,
            drift_rate: drift,
            uncertainty_bound: epsilon,
            sync_interval, 
            last_sync_time: now 
        }
    }

    pub fn get_time(&mut self) -> u64{
        let now = Instant::now();
        // Synchronize if needed
        if now.duration_since(self.last_sync_time) >= self.sync_interval{
            self.synchronize(now); 
        }

        let elapsed_since_last_sync = now.duration_since(self.last_sync_time).as_micros() as f64;
        let simulated = (self.base_offset as f64) + (elapsed_since_last_sync * self.drift_rate);
        simulated as u64
    }

    // Logical synchronization
    // Real sync procedure would involve:
    // 1. Exchanging messages to estimate round-trip time (RTT) and clock offset
    // 2. Linearly increasing uncertainty bound based on time since last sync and drift rate
    fn synchronize(&mut self, now:Instant){
        let real_now = now.duration_since(self.start_instant).as_micros() as i64;
        self.base_offset = real_now;
        self.last_sync_time = now;
    }

    // I have some doubts here related to how rust treats variables passed by value
    pub fn get_uncertainty(&self) -> f64{
        self.uncertainty_bound
    }

}

#[cfg(test)]
mod tests {
    use super::ClockSim;
    use std::time::Duration;

    #[test]
    fn clock_is_monotonic() {
        let mut clock = ClockSim::new(1.0, 0.0, 10_000.0);
        let t1 = clock.get_time();
        std::thread::sleep(Duration::from_millis(2));
        let t2 = clock.get_time();
        assert!(t2 >= t1);
    }

    #[test]
    fn synchronization_reduces_large_drift_error() {
        let mut clock = ClockSim::new(2.0, 0.0, 2.0);

        std::thread::sleep(Duration::from_millis(300));
        let before_sync = clock.get_time();

        std::thread::sleep(Duration::from_millis(350));
        let after_sync = clock.get_time();

        let delta = after_sync.saturating_sub(before_sync);
        println!("Delta after sync: {} microseconds", delta);
        assert!(delta < 500_000);
    }

    #[test]
    #[should_panic(expected = "sync_freq must be > 0.0")]
    fn zero_sync_frequency_panics() {
        let _ = ClockSim::new(1.0, 0.0, 0.0);
    }
}