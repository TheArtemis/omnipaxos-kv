use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::Entry;

pub struct Owd {
    // HashMap one for each client sending messages
    // Currently used for a single proxy node (default id 0), but structured
    // to support multiple OWD nodes in future improvements.
    node_data: HashMap<u64, VecDeque<u64>>,
    default_value: u64,
    percentile: f64,
    window_size: u64,
}

impl Owd {
    pub fn new(default_value: u64, percentile: f64, window_size: u64) -> Self {
        Self {
            node_data: HashMap::new(),
            default_value: default_value,
            percentile: percentile,
            window_size: window_size,
        }
    }

    // Return default value when there is no adaptive deadline
    pub fn get_default_deadline(&self) -> u64 {
        self.default_value
    }

    pub fn get_size(&mut self, node_id: u64) -> u64 {
        match self.node_data.get(&node_id) {
            Some(deque) => deque.len() as u64,
            None => 0,
        }
    }

    pub fn add_element(&mut self, key: u64, new_elem: u64) {

        match self.node_data.entry(key) {
            Entry::Occupied(mut occ) => {
                let deque = occ.get_mut();
                if (deque.len() as u64) < self.window_size {
                    deque.push_back(new_elem);
                } else {
                    // window is full: drop oldest and append new
                    deque.pop_front();
                    deque.push_back(new_elem);
                }
            }
            Entry::Vacant(vac) => {
                let mut dq = VecDeque::new();
                dq.push_back(new_elem);
                vac.insert(dq);
            }
        }
    }

    pub fn get_adaptive_deadline(&self, key: u64) -> u64 {
        match self.node_data.get(&key) {
            Some(values) if !values.is_empty() => {
                let mut vals: Vec<u64> = values.iter().copied().collect();
                let len = vals.len();
                let p = self.percentile.clamp(0.0, 1.0);
                let rank = ((len - 1) as f64 * p).round() as usize;
                let (_, nth, _) = vals.select_nth_unstable(rank);
                *nth
            }
            _ => self.default_value,
        }
    }
}
