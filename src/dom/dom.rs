use crate::common::kv::CommandId;
use crate::dom::config::DomConfig;
use crate::dom::request::DomMessage;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

pub struct Dom {
    config: DomConfig,

    // Early buffer: min-heap by DomMessage::deadline
    early_buffer: BinaryHeap<Reverse<DomMessage>>,
    
    // Late buffer
    late_buffer: HashMap<CommandId, DomMessage>,

    // Release messages job

    // Slow path if late

    // Ack Proxy on log append   

}

impl Dom {
    pub fn new(config: DomConfig) -> Self {
        Self {
            config,
            early_buffer: BinaryHeap::new(),
            late_buffer: HashMap::new(),
        }
    }
}