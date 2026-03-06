use crate::clock::ClockSim;
use crate::common::kv::{ClientId, CommandId};
use crate::dom::config::DomConfig;
use crate::dom::request::DomMessage;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;

/// True iff two deadlines are within each other's uncertainty window
/// (intervals [d-u, d+u] overlap, i.e. |d1 - d2| <= 2*uncertainty).
fn deadlines_overlap(overlap_threshold: u64, d1: u64, d2: u64) -> bool {
    d1.saturating_sub(d2) <= overlap_threshold && d2.saturating_sub(d1) <= overlap_threshold
}

pub struct Dom {
    // Early buffer: min-heap by DomMessage::deadline
    early_buffer: BinaryHeap<Reverse<DomMessage>>,
    
    // Late buffer
    late_buffer: HashMap<(ClientId, CommandId), DomMessage>,

    clock: ClockSim,

    // moment in time where the last message from the early buffer has been released
    last_released_command: u64, 

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
        }
    }

    // Needed to verify whether late_buffer is empty 
    pub fn get_late_buffer_size(&self) -> u64{
        self.late_buffer.len() as u64
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
        let now = self.clock.get_time();
        let uncertainty = self.clock.get_uncertainty() as u64;
        if message.deadline > now + uncertainty {
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
        let mut candidates = Vec::new();
        while let Some(Reverse(msg)) = self.early_buffer.peek() {
            if msg.deadline > now {
                break;
            }
            // TODO: Should this be the last released command or the last released early buffer command?
            self.last_released_command = msg.deadline;
            candidates.push(self.early_buffer.pop().unwrap().0);
        }

        let due = self.handle_overlapping_uncertainty(candidates);
        due
    }

    /// Partitions messages by uncertainty overlap: if two messages have deadlines within
    /// each other's uncertainty window, we cannot order them, so they go to the late buffer.
    /// Otherwise they go to the early buffer.
    pub fn handle_overlapping_uncertainty(&mut self, messages: Vec<DomMessage>) -> Vec<DomMessage> {
        if messages.is_empty() {
            return Vec::new();
        }
        let mut due = Vec::new();
        let overlap_threshold = (self.clock.get_uncertainty() as u64).saturating_mul(2);
        for (i, msg) in messages.iter().enumerate() {
            let overlaps_other = messages.iter().enumerate().any(|(j, other)| {
                i != j && deadlines_overlap(overlap_threshold, msg.deadline, other.deadline)
            });
            if overlaps_other {
                self.push_to_late_buffer(msg.clone());
            } else {
                due.push(msg.clone());
            }
        }

        return due;
    }

}