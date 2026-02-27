
use omnipaxos_kv::clock::{ClockConfig, ClockSim};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn tests_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data")
}

fn spin_for(duration: Duration) {
    let start = Instant::now();
    while start.elapsed() < duration {}
}

#[test]
fn clock_is_monotonic() {
    // drift 0, sync every 100ms
    let mut clock = ClockSim::new(0.0, 10.0, 10.0);
    let t1 = clock.get_time();
    std::thread::sleep(Duration::from_millis(2));
    let t2 = clock.get_time();
    assert!(t2 >= t1);
}

#[test]
fn synchronization_reduces_large_drift_error() {
    // 100 ms/s drift, sync every 500ms
    let mut clock = ClockSim::new(100_000.0, 0.0, 2.0);

    std::thread::sleep(Duration::from_millis(300));
    let before_sync = clock.get_time();

    std::thread::sleep(Duration::from_millis(350));
    let after_sync = clock.get_time();

    // Without sync, clock would have run ~100ms/s fast; sync corrects to real time.
    // Delta should be ~350ms (350_000 μs), well under 500_000
    let delta = after_sync.saturating_sub(before_sync);
    assert!(delta < 500_000);
}

#[test]
#[should_panic(expected = "sync_freq must be > 0.0")]
fn zero_sync_frequency_panics() {
    let _ = ClockSim::new(50.0, 100.0, 0.0);
}

#[test]
fn clock_config_from_file() {
    let path = tests_data_dir().join("clock-config");
    let config = ClockConfig::from_file(path.to_str().unwrap()).expect("load config");
    assert_eq!(config.drift_rate, 25.0);
    assert_eq!(config.uncertainty_bound, 50.0);
    assert_eq!(config.sync_freq, 200.0);

    let mut clock = ClockSim::new(
        config.drift_rate,
        config.uncertainty_bound,
        config.sync_freq,
    );
    let t1 = clock.get_time();
    std::thread::sleep(Duration::from_millis(2));
    let t2 = clock.get_time();
    assert!(t2 >= t1);
    assert_eq!(clock.get_uncertainty(), 50.0);
}

#[test]
fn clocks_with_different_drift_sync_to_same_time() {
    // Very different drift rates, but synchronize frequently.
    let mut fast = ClockSim::new(500_000.0, 0.0, 50.0); // +0.5s/s drift
    let mut slow = ClockSim::new(-800_000.0, 0.0, 50.0); // -0.8s/s drift

    // Let some time pass without a sync interval elapsing to observe drift.
    spin_for(Duration::from_millis(5));
    let t_fast = fast.get_time();
    let t_slow = slow.get_time();
    let drift_delta = t_fast.abs_diff(t_slow);
    assert!(
        drift_delta > 1_000,
        "expected noticeable drift, got {drift_delta}μs"
    );

    println!("Drift delta: {drift_delta}μs");

    // Advance time beyond the sync interval and read again to force resync.
    spin_for(Duration::from_millis(25));
    let t_fast_sync = fast.get_time();
    let t_slow_sync = slow.get_time();
    let sync_delta = t_fast_sync.abs_diff(t_slow_sync);
    assert!(
        sync_delta <= 5_000,
        "expected synchronized times, got delta {sync_delta}μs"
    );
    println!("Sync delta: {sync_delta}μs");
}
