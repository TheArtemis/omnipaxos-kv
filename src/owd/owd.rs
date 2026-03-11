use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::Entry;

pub struct Owd {
    // HashMap one for each client sending messages
    proxy_data: HashMap<u64, VecDeque<u64>>,
    default_value: u64,
    beta: f64,
    window_size: u64,
}

impl Owd {
    pub fn new(default_value: u64, beta: f64, window_size: u64) -> Self {
        Self {
            proxy_data: HashMap::new(),
            default_value: default_value,
            beta: beta,
            window_size: window_size,
        }
    }

    pub fn get_std_dev(&self, key: u64) -> u64 {
        match self.proxy_data.get(&key) {
            Some(values) if !values.is_empty() => {
                let n = values.len() as f64;
                let sum: f64 = values.iter().map(|&v| v as f64).sum();
                let mean = sum / n;
                let var_sum: f64 = values.iter().map(|&v| {
                    let d = v as f64 - mean;
                    d * d
                }).sum();
                let variance = var_sum / n;
                // standard deviation by the sqrt of the number of keys in the hashmap -> huygens
                let num_keys = (self.proxy_data.len() as f64).max(1.0);
                let std_dev = variance.sqrt() / num_keys.sqrt();
                std_dev as u64
            }
            _ => 0,
        }
    }

    // Return default value when there is no adaptive deadline
    pub fn get_default_deadline(&mut self) -> u64{
        self.default_value
    }

    pub fn getSize(&mut self, proxy_address: u64)-> u64{
        match self.proxy_data.get(&proxy_address) {
            Some(deque) => deque.len() as u64,
            None => 0,
        }
    }


    pub fn add_element(&mut self, key: u64, new_elem: u64) {

        match self.proxy_data.entry(key) {
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

    pub fn get_median(&self, key: u64) -> u64 {
        // Retrieve the deque of values for the given client key
        match self.proxy_data.get(&key) {
            Some(values) if !values.is_empty() => {
                let mut vals: Vec<u64> = values.iter().copied().collect();
                vals.sort_unstable();
                let len = vals.len();
                if len % 2 == 1 {
                    vals[len / 2] //middle element
                } else {
                    let a = vals[len / 2 - 1];
                    let b = vals[len / 2];
                    a.saturating_add(b) / 2 //saturating add to prevent calculations overflow
                }
            }
            _ => 0
        }
    }

    pub fn get_adaptive_deadline(&self, key: u64) -> u64 {
        let median = self.get_median(key);
        let std_dev = self.get_std_dev(key);
        let result = (median as f64) + self.beta * (std_dev as f64);
        if result < (self.default_value as f64) {
            return result.round() as u64;
        } else {
            return self.default_value;
        }
    }
}
