use omnipaxos_kv::clock::ClockSim;
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
