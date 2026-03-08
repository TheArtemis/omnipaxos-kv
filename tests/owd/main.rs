use omnipaxos_kv::owd::owd::Owd;

// init owd
fn setup_owd(default_value: u64, beta: f64, window_size: u64) -> Owd {
    Owd::new(default_value, beta, window_size)
}

#[test]
fn test_add_element_and_window_size() {
    let mut owd = setup_owd(100, 1.0, 3);
    let client_id = 1;

    owd.add_element(client_id, 10);
    assert_eq!(owd.getSize(client_id), 1);

    owd.add_element(client_id, 20);
    owd.add_element(client_id, 30);
    assert_eq!(owd.getSize(client_id), 3);

    owd.add_element(client_id, 40);
    assert_eq!(owd.getSize(client_id), 3);

    // Window should keep the latest 3 elements: [20, 30, 40].
    assert_eq!(owd.get_median(client_id), 30);
}

#[test]
fn test_get_median_odd() {
    let mut owd = setup_owd(100, 1.0, 5);
    let client_id = 2;
    owd.add_element(client_id, 30);
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    assert_eq!(owd.get_median(client_id), 20);
}

#[test]
fn test_get_median_even() {
    let mut owd = setup_owd(100, 1.0, 5);
    let client_id = 3;
    owd.add_element(client_id, 40);
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    owd.add_element(client_id, 30);
    assert_eq!(owd.get_median(client_id), 25);
}

#[test]
fn test_get_std_dev() {
    let mut owd = setup_owd(100, 1.0, 5);
    let client_id = 4;
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 10);
    assert_eq!(owd.get_std_dev(client_id), 0);

    let mut owd2 = setup_owd(100, 1.0, 5);
    let client_id2 = 5;
    owd2.add_element(client_id2, 10);
    owd2.add_element(client_id2, 20);
    

    assert_eq!(owd2.get_std_dev(client_id2), 5);

    let client_id3 = 6;
    let client_id4 = 7;
    let client_id5 = 8;

    owd2.add_element(client_id3, 10);
    owd2.add_element(client_id4, 10);
    owd2.add_element(client_id5, 10);
    
    assert_eq!(owd2.get_std_dev(client_id2), 2);
}

#[test]
fn test_get_adaptive_deadline() {
    let mut owd = setup_owd(100, 2.0, 5);
    let client_id = 5;
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    assert_eq!(owd.get_adaptive_deadline(client_id), 25);
}

#[test]
fn test_get_adaptive_deadline_fallback_to_default() {
    let mut owd = setup_owd(15, 2.0, 5);
    let client_id = 6;
    owd.add_element(client_id, 10);
    owd.add_element(client_id, 20);
    assert_eq!(owd.get_adaptive_deadline(client_id), 15);
}

#[test]
fn test_empty_client() {
    let owd = setup_owd(100, 1.0, 5);
    let client_id = 7;
    assert_eq!(owd.get_median(client_id), 0);
    assert_eq!(owd.get_std_dev(client_id), 0);
    assert_eq!(owd.get_adaptive_deadline(client_id), 0);
}
