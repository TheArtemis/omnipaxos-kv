use crate::common::{kv::ClientId, messages::ClientMessage};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct DomRequest {
    pub client_id: ClientId,
    pub message: ClientMessage,      
    pub deadline: Instant,
    pub send_time: Instant,
}

impl DomRequest {
    pub fn new(client_id: ClientId, message: ClientMessage, deadline: Instant) -> Self {
        Self {
            client_id,
            message,
            deadline,
            send_time: Instant::now(),
        }
    }
}