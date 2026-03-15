use crate::common::{kv::ClientId, log_hash::LogHash, messages::ClientMessage};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Ordered by `deadline` only (for use in priority queues).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomMessage {
    pub client_id: ClientId,
    pub message: ClientMessage,
    pub deadline: u64,
    pub send_time: u64,
    #[serde(default)]
    pub arrival_hash: LogHash,
}

impl PartialEq for DomMessage {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl Eq for DomMessage {}

impl PartialOrd for DomMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DomMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline.cmp(&other.deadline)
    }
}

impl DomMessage {
    pub fn new(
        client_id: ClientId,
        message: ClientMessage,
        deadline: u64, // Will be set by the client
        send_time: u64, // Will be set by the client's clock simulator
    ) -> Self {
        Self {
            client_id,
            message,
            deadline,
            send_time,
            arrival_hash: LogHash::default(),
        }
    }
}
