use crate::clock::ClockSim;
use crate::common::kv::{ClientId, CommandId};
use crate::dom::config::DomConfig;
use crate::dom::request::DomMessage;
use crate::owd::owd::Owd;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;

pub struct Dom {
    // Early buffer: min-heap by DomMessage::deadline
    early_buffer: BinaryHeap<Reverse<DomMessage>>,
    
    // Late buffer
    late_buffer: HashMap<(ClientId, CommandId), DomMessage>,

    clock: ClockSim,

    // moment in time where the last message from the early buffer has been released
    last_released_command: u64, 

    owd: Owd,

    // Used by the server to compute fast-path / slow-path invocation rates.
    pub early_insertions: usize,
    pub late_insertions: usize,
    

}

impl Dom {
    pub fn new(config: DomConfig) -> Self {
        let clock = ClockSim::new(
            config.clock.drift_rate,
            config.clock.uncertainty_bound,
            config.clock.sync_freq,
        );
        Self {
            early_buffer: BinaryHeap::new(),
            late_buffer: HashMap::new(),
            clock,
            last_released_command: 0,
            early_insertions: 0,
            late_insertions: 0,
            owd: Owd::new(
                config.owd.default_value,
                config.owd.percentile,
                config.owd.window_size,
            ),
        }
    }

    // Needed to verify whether late_buffer is empty 
    pub fn get_late_buffer_size(&self) -> u64{
        self.late_buffer.len() as u64
    }

    // Add an element into owd
    pub fn add_element_to_owd(&mut self, node_id: u64, new_elem: u64){
        self.owd.add_element(node_id, new_elem)
    }

    pub fn get_default_deadline(&mut self) -> u64 {
        return self.owd.get_default_deadline()
    }

    // get time
    pub fn get_time(&mut self) -> u64{
        self.clock.get_time()
    }

    // request deadline to owd
    pub fn request_deadline_from_owd(&mut self, node_id: u64) -> u64{
        let deadline = self.owd.get_adaptive_deadline(node_id);
        let uncertainty = self.clock.get_uncertainty() as u64;
        deadline + 2 * uncertainty
    }

    // return size of element inside owd for client_id
    pub fn get_size(&mut self, node_id: u64) -> u64{
        return self.owd.get_size(node_id)
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
        if message.deadline > self.last_released_command {
            self.push_to_early_buffer(message);
        } else {
            self.push_to_late_buffer(message);
        }
    }

    pub fn push_to_late_buffer(&mut self, message: DomMessage) {
        let key = (message.client_id, message.message.command_id());
        self.late_buffer.insert(key, message);
        self.late_insertions += 1;
    }

    pub fn push_to_early_buffer(&mut self, message: DomMessage) {
        self.early_buffer.push(Reverse(message));
        self.early_insertions += 1;
    }

    pub fn take_buffer_counts(&mut self) -> (usize, usize) {
        let counts = (self.early_insertions, self.late_insertions);
        self.early_insertions = 0;
        self.late_insertions = 0;
        counts
    }


    /// Returns how long to wait (real time) until the next deadline, or `None` if no deadline.
    pub fn duration_until_next_deadline(&mut self) -> Option<Duration> {
        let deadline = self.get_next_deadline()?;
        let now = self.clock.get_time();
        let uncertainty = self.clock.get_uncertainty() as u64;
        let target = deadline.saturating_add(uncertainty);
        let delta = target.saturating_sub(now);
        Some(Duration::from_micros(delta))
    }

    /// Pops all messages from the early buffer whose deadline has been reached.
    /// Caller is responsible for appending to the log and updating proxy_command_ids.
    pub fn handle_deadline(&mut self) -> Vec<DomMessage> {
        let now = self.clock.get_time();
        let uncertainty = self.clock.get_uncertainty() as u64;
        let mut candidates = Vec::new();
        while let Some(Reverse(msg)) = self.early_buffer.peek() {
            if msg.deadline.saturating_add(uncertainty) > now {
                break;
            }
            candidates.push(self.early_buffer.pop().unwrap().0);
        }

        let due = self.handle_overlapping_uncertainty(candidates);
        // Last released command in the early buffer (due is ordered by deadline)
        if let Some(last) = due.last() {
            self.last_released_command = last.deadline;
        }
        due
    }

    /// Partitions messages by uncertainty overlap: if two messages have deadlines within
    /// each other's uncertainty window, we cannot order them, so they go to the late buffer.
    /// Otherwise they go to the early buffer.
    /// Uses binary search over the already-sorted (by deadline) messages for O(n log n)
    pub fn handle_overlapping_uncertainty(&mut self, messages: Vec<DomMessage>) -> Vec<DomMessage> {
        if messages.is_empty() {
            return Vec::new();
        }
        let overlap_threshold = (self.clock.get_uncertainty() as u64).saturating_mul(2);
        // Messages are already ordered by deadline (from early_buffer min-heap).
        let deadlines: Vec<u64> = messages.iter().map(|m| m.deadline).collect();
        let mut due = Vec::new();
        for msg in messages.iter() {
            let d = msg.deadline;
            let left = deadlines.partition_point(|&x| x < d.saturating_sub(overlap_threshold));
            let right = deadlines
                .partition_point(|&x| x <= d + overlap_threshold)
                .saturating_sub(1);
            // More than one message in [d - threshold, d + threshold] => overlaps another
            let overlaps_other = right > left;
            if overlaps_other {
                self.push_to_late_buffer(msg.clone());
            } else {
                due.push(msg.clone());
            }
        }
        due
    }

}
