use crate::dom::config::DomConfig;
use crate::dom::request::DomRequest;
use crate::dom::RequestIdx;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;

pub struct Dom {
    config: DomConfig,


    // Early buffer with a priority queue (min-heap by deadline)
    early_buffer: BinaryHeap<Reverse<(Instant, RequestIdx)>>,
    
    // Late buffer
    late_buffer: HashMap<RequestIdx, DomRequest>,

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