use omnipaxos_kv::clock::ClockConfig;
use omnipaxos_kv::common::kv::KVCommand;
use omnipaxos_kv::common::messages::ClientMessage;
use omnipaxos_kv::dom::config::DomConfig;
use omnipaxos_kv::dom::dom::Dom;
use omnipaxos_kv::dom::request::DomMessage;

fn dom_with_uncertainty(uncertainty: f64) -> Dom {
    let clock = ClockConfig {
        drift_rate: 0.0,
        uncertainty_bound: uncertainty,
        sync_freq: 1000.0,
    };
    let config = DomConfig { clock };
    Dom::new(config)
}

fn make_msg(client_id: u64, command_id: usize, deadline: u64, send_time: u64) -> DomMessage {
    DomMessage::new(
        client_id,
        ClientMessage::Append(command_id, KVCommand::Put("k".into(), "v".into())),
        deadline,
        send_time,
    )
}

/// Overlapping messages (within uncertainty) should go to late buffer;
/// non-overlapping messages should be released as due.
#[test]
fn handle_deadline_overlapping_uncertainty() {
    // uncertainty_bound=10 → overlap_threshold=20 (|d1-d2|<=20 means overlap)
    let mut dom = dom_with_uncertainty(10.0);

    // All deadlines in the past (clock returns UNIX epoch μs ~1e15)
    let msg1 = make_msg(1, 1, 100, 0); // overlaps msg2 (|100-101|=1<=20)
    let msg2 = make_msg(1, 2, 101, 0); // overlaps msg1
    let msg3 = make_msg(1, 3, 500, 0); // far from msg1,2 (|500-100|=400>20)

    dom.push_to_early_buffer(msg1);
    dom.push_to_early_buffer(msg2);
    dom.push_to_early_buffer(msg3);

    let due = dom.handle_deadline();

    // msg1 and msg2 overlap each other → late buffer
    assert_eq!(dom.get_late_buffer_size(), 2, "overlapping messages should go to late buffer");
    // msg3 does not overlap anyone → due
    assert_eq!(due.len(), 1, "non-overlapping message should be released");
    assert_eq!(due[0].deadline, 500);
    assert_eq!(due[0].message.command_id(), 3);
}

/// Non-overlapping messages should all be released as due.
#[test]
fn handle_deadline_non_overlapping() {
    let mut dom = dom_with_uncertainty(10.0); // overlap_threshold=20

    let msg1 = make_msg(1, 1, 100, 0);  // |100-500|=400 > 20
    let msg2 = make_msg(1, 2, 500, 0);  // |500-100|=400 > 20
    let msg3 = make_msg(1, 3, 1000, 0); // far from both

    dom.push_to_early_buffer(msg1);
    dom.push_to_early_buffer(msg2);
    dom.push_to_early_buffer(msg3);

    let due = dom.handle_deadline();

    assert_eq!(dom.get_late_buffer_size(), 0, "no overlapping messages");
    assert_eq!(due.len(), 3, "all messages should be released");
    assert_eq!(due[0].deadline, 100);
    assert_eq!(due[1].deadline, 500);
    assert_eq!(due[2].deadline, 1000);
}
