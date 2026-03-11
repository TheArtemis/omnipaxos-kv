use omnipaxos_kv::owd::owd::Owd;

// init owd
fn setup_owd(default_value: u64, percentile: f64, window_size: u64) -> Owd {
    Owd::new(default_value, percentile, window_size)
}

#[test]
fn test_add_element_and_window_size() {
    let mut owd = setup_owd(100, 0.5, 3);
    let client_id = 1;

    owd.add_element(client_id, 10);
    assert_eq!(owd.get_size(client_id), 1);

    owd.add_element(client_id, 20);
    owd.add_element(client_id, 30);
    assert_eq!(owd.get_size(client_id), 3);

    owd.add_element(client_id, 40);
    assert_eq!(owd.get_size(client_id), 3);

    // Window should keep the latest 3 elements: [20, 30, 40].
    assert_eq!(owd.get_adaptive_deadline(client_id), 30);
}

#[test]
fn test_get_percentile_odd() {
    let mut owd = setup_owd(100, 0.5, 5);
    let client_id = 2;
    owd.add_element(client_id, 30);
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    assert_eq!(owd.get_adaptive_deadline(client_id), 20);
}

#[test]
fn test_get_percentile_even() {
    let mut owd = setup_owd(100, 0.5, 5);
    let client_id = 3;
    owd.add_element(client_id, 40);
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    owd.add_element(client_id, 30);
    // For even count with p=0.5, rank rounds to index 2 => 30.
    assert_eq!(owd.get_adaptive_deadline(client_id), 30);
}

#[test]
fn test_get_percentile_high() {
    let mut owd = setup_owd(100, 0.95, 5);
    let client_id = 4;
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    owd.add_element(client_id, 30);
    owd.add_element(client_id, 40);
    owd.add_element(client_id, 50);
    // p95 => rank rounds to last element (index 4)
    assert_eq!(owd.get_adaptive_deadline(client_id), 50);
}

#[test]
fn test_get_adaptive_deadline() {
    let mut owd = setup_owd(100, 0.5, 5);
    let client_id = 5;
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    assert_eq!(owd.get_adaptive_deadline(client_id), 20);
}

#[test]
fn test_empty_client() {
    let owd = setup_owd(100, 0.5, 5);
    let client_id = 7;
    assert_eq!(owd.get_adaptive_deadline(client_id), 100);
}
