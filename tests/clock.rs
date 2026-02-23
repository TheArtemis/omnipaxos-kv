
use omnipaxos_kv::clock::{ClockConfig, ClockSim};
use std::path::PathBuf;
use std::time::Duration;

fn tests_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data")
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
