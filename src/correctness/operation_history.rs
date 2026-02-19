use serde::{Deserialize, Serialize};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use crate::common::kv::ClientId;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Input {
    Put { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Output {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Operation {
    pub client_id: u64,
    pub input: Input,
    #[serde(rename = "call")]
    pub call: i64, // Absolute timestamp in nanoseconds since epoch
    pub output: Output,
    #[serde(rename = "return_time")]
    pub return_time: i64, // Absolute timestamp in nanoseconds since epoch
}

pub struct OperationHistory {
    operations: Vec<Operation>,
    client_id: ClientId,
    start_time: Instant,
    sync_time_nanos: i64, // Absolute sync time in nanoseconds since epoch
}

impl OperationHistory {
    pub fn new(client_id: ClientId) -> Self {
        // Calculate sync time as nanoseconds since epoch
        // We'll update this when we receive the sync signal
        let sync_time_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        
        Self {
            operations: Vec::new(),
            client_id,
            start_time: Instant::now(),
            sync_time_nanos,
        }
    }

    pub fn set_sync_time(&mut self, sync_time_ms: i64) {
        // Convert milliseconds to nanoseconds
        self.sync_time_nanos = sync_time_ms * 1_000_000;
        // Reset start_time so elapsed time is measured from sync point
        self.start_time = Instant::now();
    }

    pub fn record_operation(&mut self, input: Input) -> usize {
        // Calculate absolute timestamp: sync_time + elapsed time since start
        let elapsed_nanos = self.start_time.elapsed().as_nanos() as i64;
        let call_time = self.sync_time_nanos + elapsed_nanos;
        let op_index = self.operations.len();
        
        // Create operation with placeholder output (will be updated on completion)
        let operation = Operation {
            client_id: self.client_id,
            input: input.clone(),
            call: call_time,
            output: Output {
                status: "pending".to_string(),
                value: None,
            },
            return_time: 0,
        };
        
        self.operations.push(operation);
        op_index
    }

    pub fn complete_operation(&mut self, op_index: usize, output: Output) {
        if let Some(op) = self.operations.get_mut(op_index) {
            // Calculate absolute timestamp: sync_time + elapsed time since start
            let elapsed_nanos = self.start_time.elapsed().as_nanos() as i64;
            let return_time = self.sync_time_nanos + elapsed_nanos;
            op.output = output;
            op.return_time = return_time;
        }
    }

    pub fn export_json(&self, file_path: &str) -> Result<(), std::io::Error> {
        // Filter out incomplete operations (those without return_time)
        let completed_ops: Vec<&Operation> = self.operations
            .iter()
            .filter(|op| op.return_time > 0)
            .collect();
        
        let json = serde_json::to_string_pretty(&completed_ops)?;
        std::fs::write(file_path, json)?;
        Ok(())
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }

    pub fn completed_count(&self) -> usize {
        self.operations.iter().filter(|op| op.return_time > 0).count()
    }
}
