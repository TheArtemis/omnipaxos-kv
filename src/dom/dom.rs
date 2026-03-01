use crate::clock::ClockSim;
use crate::common::kv::{ClientId, CommandId};
use crate::dom::config::DomConfig;
use crate::dom::request::DomMessage;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

pub struct Dom {
    config: DomConfig,

    // Early buffer: min-heap by DomMessage::deadline
    early_buffer: BinaryHeap<Reverse<DomMessage>>,
    
    // Late buffer
    late_buffer: HashMap<(ClientId, CommandId), DomMessage>,

    clock: ClockSim,

    // Release messages job

    // Slow path if late

    // Ack Proxy on log append 
    

}

impl Dom {
    pub fn new(config: DomConfig) -> Self {
        let clock = ClockSim::new(
            config.clock.drift_rate,
            config.clock.uncertainty_bound,
            config.clock.sync_freq,
        );
        Self {
            config,
            early_buffer: BinaryHeap::new(),
            late_buffer: HashMap::new(),
            clock,
        }
    }

    pub fn push_by_deadline(&mut self, message: DomMessage) {
        let now = self.clock.get_time();
        if message.deadline <= now {
            self.push_to_early_buffer(message);
        } else {
            self.push_to_late_buffer(message);
        }
    }

    pub fn push_to_late_buffer(&mut self, message: DomMessage) {
        let key = (message.client_id, message.message.command_id());
        self.late_buffer.insert(key, message);
    }

    pub fn push_to_early_buffer(&mut self, message: DomMessage) {
        self.early_buffer.push(Reverse(message));
    }

}