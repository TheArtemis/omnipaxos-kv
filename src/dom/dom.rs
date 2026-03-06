use crate::clock::{self, ClockSim};
use crate::common::kv::{ClientId, CommandId};
use crate::dom::config::DomConfig;
use crate::dom::request::DomMessage;
use crate::owd::owd::Owd;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;

pub struct Dom {
    config: DomConfig,

    // Early buffer: min-heap by DomMessage::deadline
    early_buffer: BinaryHeap<Reverse<DomMessage>>,
    
    // Late buffer
    late_buffer: HashMap<(ClientId, CommandId), DomMessage>,

    clock: ClockSim,

    // moment in time where the last message from the early buffer has been released
    last_released_command: u64, 

    // owd data strcture
    owd: Owd,

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
            last_released_command: 0,
            owd: Owd::new(100, 0.01, 1000),
        }
    }

    // Needed to verify whether late_buffer is empty 
    pub fn get_late_buffer_size(&self) -> u64{
        self.late_buffer.len() as u64
    }

    // Add an element into owd
    pub fn add_element_to_owd(&mut self, client_id: ClientId, new_elem: u64){
        self.owd.add_element(client_id, new_elem)
    }

    // get time
    pub fn get_time(&mut self) -> u64{
        self.clock.get_time()
    }
    // request deadline to owd
    pub fn request_deadline_from_owd(&mut self, client_id: ClientId) -> u64{
        self.owd.get_adaptive_deadline(client_id)
    }

    // Implemented for testing
    pub fn get_early_buffer_size(&self) -> u64{
        self.early_buffer.len() as u64
    }

    // pop from late buffer
    pub fn pop_from_late_buffer(&mut self) -> Option<DomMessage> {
        // find the entry with the smallest deadline and remove it from the late buffer
        if let Some((key, _)) = self.late_buffer.iter().min_by_key(|(_, msg)| msg.deadline) {
            // Copy the key before mutating the map to avoid overlapping borrows.
            let key = *key;
            let (_k, v) = self.late_buffer.remove_entry(&key)?;
            return Some(v);
        }
        None
    }


    pub fn get_next_deadline(&self) -> Option<u64> {
        self.early_buffer.peek().map(|Reverse(message)| message.deadline)
    }

    pub fn push_by_deadline(&mut self, message: DomMessage) {
        // TODO: MODIFY THE if statement to check if the message is late or early
        if message.deadline > self.last_released_command {
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


    /// Returns how long to wait (real time) until the next deadline, or `None` if no deadline.
    /// Caller should wake immediately when the returned duration is zero.
    pub fn duration_until_next_deadline(&mut self) -> Option<Duration> {
        let deadline = self.get_next_deadline()?;
        let now = self.clock.get_time();
        let delta = deadline.saturating_sub(now);
        Some(Duration::from_micros(delta))
    }

    /// Pops all messages from the early buffer whose deadline has been reached.
    /// Caller is responsible for appending to the log and updating proxy_command_ids.
    pub fn handle_deadline(&mut self) -> Vec<DomMessage> {
        let now = self.clock.get_time();
        let mut due = Vec::new();
        while let Some(Reverse(msg)) = self.early_buffer.peek() {
            if msg.deadline > now {
                break;
            }
            due.push(self.early_buffer.pop().unwrap().0);
            self.last_released_command = self.clock.get_time();
        }
        due
    }

    // TODO: Server handles the late buffer:

    // a) If I'm a leader I edit the deadline so that i can still do the fast path

    // b) If I'm a follower I process it with the slow path???
    // But since we are doing omnipaxos maybe we don't do that???

}