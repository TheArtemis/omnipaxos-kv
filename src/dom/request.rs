use crate::common::{kv::ClientId, messages::ClientMessage};
use crate::dom::RequestIdx;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct DomRequest {
    pub request_idx: RequestIdx,
    pub client_id: ClientId,
    pub message: ClientMessage,      
    pub deadline: Instant,
    pub send_time: Instant,
}

impl DomRequest {
    pub fn new(
        request_idx: RequestIdx,
        client_id: ClientId,
        message: ClientMessage,
        deadline: Instant,
    ) -> Self {
        Self {
            request_idx,
            client_id,
            message,
            deadline,
            send_time: Instant::now(),
        }
    }
}