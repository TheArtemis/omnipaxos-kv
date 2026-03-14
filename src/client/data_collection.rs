use std::{fs::File, io::Write};

use chrono::Utc;
use csv::Writer;
use omnipaxos_kv::common::{kv::CommandId, utils::Timestamp};
use serde::Serialize;

use crate::configs::ClientConfig;

use omnipaxos_kv::correctness::operation_history::{OperationHistory, Input, Output};

#[derive(Debug, Serialize, Clone, Copy)]
struct RequestData {
    request_time: Timestamp,
    write: bool,
    response_time: Option<Timestamp>,
    response_count: usize,
}

pub struct ClientData {
    request_data: Vec<RequestData>,
    response_count: usize,
    correctness_tracker: Option<OperationHistory>,
}

impl ClientData {

    /* Correctness Check Extras */
    
    pub fn new() -> Self {
        Self {
            request_data: Vec::new(),
            response_count: 0,
            correctness_tracker: None,
        }
    }

    pub fn new_with_correctness(client_id: omnipaxos_kv::common::kv::ClientId) -> Self {
        ClientData {
            request_data: Vec::new(),
            response_count: 0,
            correctness_tracker: Some(OperationHistory::new(client_id)),
        }
    }

    pub fn record_operation(&mut self, input: Input) -> Option<usize> {
        if let Some(ref mut tracker) = self.correctness_tracker {
            Some(tracker.record_operation(input))
        } else {
            None
        }
    }

    pub fn complete_operation(&mut self, op_index: usize, output: Output) {
        if let Some(ref mut tracker) = self.correctness_tracker {
            tracker.complete_operation(op_index, output);
        }
    }

    pub fn set_sync_time(&mut self, sync_time_ms: i64) {
        if let Some(ref mut tracker) = self.correctness_tracker {
            tracker.set_sync_time(sync_time_ms);
        }
    }

    pub fn export_history_json(&self, file_path: &str) -> Result<(), std::io::Error> {
        if let Some(ref tracker) = self.correctness_tracker {
            tracker.export_json(file_path)
        } else {
            Ok(())
        }
    }

    /* Normal Client Data Collection */

    pub fn new_request(&mut self, is_write: bool) {
        let data = RequestData {
            request_time: Utc::now().timestamp_millis(),
            write: is_write,
            response_time: None,
            response_count: 0,
        };
        self.request_data.push(data);
    }

    pub fn new_response(&mut self, command_id: CommandId) {
        let response_time = Utc::now().timestamp_millis();
        if let Some(request_data) = self.request_data.get_mut(command_id) {
            if request_data.response_time.is_none() {
                request_data.response_time = Some(response_time);
            }
            request_data.response_count += 1;
            self.response_count += 1;
        }
    }

    pub fn response_count(&self) -> usize {
        self.response_count
    }

    pub fn request_count(&self) -> usize {
        self.request_data.len()
    }

    pub fn save_summary(&self, config: ClientConfig) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&config)?;
        let mut summary_file = File::create(config.summary_filepath)?;
        summary_file.write_all(config_json.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    pub fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for data in &self.request_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }
}
