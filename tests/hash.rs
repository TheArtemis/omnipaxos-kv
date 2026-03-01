use omnipaxos_kv::common::kv::{Command, ClientId, CommandId, KVCommand, NodeId};
use omnipaxos_kv::common::log_hash::LogHash;

fn node_id(n: u64) -> NodeId {
    n
}

fn command(client_id: ClientId, cmd_id: CommandId, coordinator: NodeId, kv: KVCommand) -> Command {
    Command {
        client_id,
        coordinator_id: coordinator,
        id: cmd_id,
        kv_cmd: kv,
    }
}

#[test]
fn new_is_all_zeros() {
    let h = LogHash::new();
    assert!(h.0.iter().all(|&b| b == 0));
}

#[test]
fn add_entry_changes_hash() {
    let mut h = LogHash::new();
    let cmd = command(1, 0, node_id(0), KVCommand::Put("k".into(), "v".into()));
    h.add_entry(&cmd);
    assert!(!h.0.iter().all(|&b| b == 0));
}

#[test]
fn add_entry_is_deterministic() {
    let cmd = command(1, 0, node_id(0), KVCommand::Put("a".into(), "b".into()));
    let mut h1 = LogHash::new();
    let mut h2 = LogHash::new();
    h1.add_entry(&cmd);
    h2.add_entry(&cmd);
    assert_eq!(h1, h2);
}

#[test]
fn add_then_remove_restores_empty() {
    let mut h = LogHash::new();
    let cmd = command(1, 0, node_id(0), KVCommand::Put("x".into(), "y".into()));
    h.add_entry(&cmd);
    h.remove_entry(&cmd);
    assert_eq!(h, LogHash::new());
}

#[test]
fn same_entries_same_order_same_hash() {
    let cmd1 = command(1, 0, node_id(0), KVCommand::Put("a".into(), "1".into()));
    let cmd2 = command(1, 1, node_id(0), KVCommand::Get("a".into()));
    let mut h1 = LogHash::new();
    let mut h2 = LogHash::new();
    h1.add_entry(&cmd1);
    h1.add_entry(&cmd2);
    h2.add_entry(&cmd1);
    h2.add_entry(&cmd2);
    assert_eq!(h1, h2);
}

#[test]
fn same_entries_different_order_same_hash() {
    let cmd1 = command(1, 0, node_id(0), KVCommand::Put("k".into(), "v".into()));
    let cmd2 = command(2, 1, node_id(0), KVCommand::Delete("k".into()));
    let mut h1 = LogHash::new();
    let mut h2 = LogHash::new();
    h1.add_entry(&cmd1);
    h1.add_entry(&cmd2);
    h2.add_entry(&cmd2);
    h2.add_entry(&cmd1);
    assert_eq!(h1, h2);
}

#[test]
fn replace_entry_equivalent_to_remove_add() {
    let old_cmd = command(1, 0, node_id(0), KVCommand::Put("a".into(), "old".into()));
    let new_cmd = command(1, 0, node_id(0), KVCommand::Put("a".into(), "new".into()));
    let mut h_replace = LogHash::new();
    let mut h_remove_add = LogHash::new();
    h_replace.add_entry(&old_cmd);
    h_replace.replace_entry(&old_cmd, &new_cmd);
    h_remove_add.add_entry(&old_cmd);
    h_remove_add.remove_entry(&old_cmd);
    h_remove_add.add_entry(&new_cmd);
    assert_eq!(h_replace, h_remove_add);
}

#[test]
fn different_entries_different_hash() {
    let cmd1 = command(1, 0, node_id(0), KVCommand::Put("a".into(), "1".into()));
    let cmd2 = command(1, 0, node_id(0), KVCommand::Put("b".into(), "2".into()));
    let mut h1 = LogHash::new();
    let mut h2 = LogHash::new();
    h1.add_entry(&cmd1);
    h2.add_entry(&cmd2);
    assert_ne!(h1, h2);
}

#[test]
fn as_ref_returns_inner() {
    let mut h = LogHash::new();
    let cmd = command(1, 0, node_id(0), KVCommand::Get("x".into()));
    h.add_entry(&cmd);
    assert_eq!(h.as_ref(), &h.0);
}
