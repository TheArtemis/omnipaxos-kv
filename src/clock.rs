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

        Self { start_instant: Instant::now(),
            base_offset: 0,
            drift_rate: drift,
            uncertainty_bound: epsilon,
            sync_interval, 
            last_sync_time: Instant::now() 
        }
    }

    pub fn get_time(&mut self) -> u64{
        let now = Instant::now();
        // Synchronize if needed
        if now.duration_since(self.last_sync_time) >= self.sync_interval{
            self.synchronize(now); 
        }

        
        let real_elapsed = now.duration_since(self.start_instant).as_micros() as f64;
        let simulated = (self.base_offset as f64) + (real_elapsed * self.drift_rate);
        simulated as u64
    }

    // Logical synchronization
    /*
    There is still a bit of delay between the synchronization and the sync time as saved in self
    TO BE SOLVED
     */
    fn synchronize(&mut self, now:Instant){
        let real_now = now.duration_since(self.start_instant).as_micros() as i64;
        self.base_offset = real_now;
        self.last_sync_time = now;
    }

    // I have some doubts here related to how rust treats variables passed by value
    pub fn get_uncertainty(self) -> f64{
        self.uncertainty_bound
    }

}