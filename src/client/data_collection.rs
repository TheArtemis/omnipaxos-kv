use std::{fs::File, io::Write};

use chrono::Utc;
use csv::Writer;
use omnipaxos_kv::common::{kv::CommandId, utils::Timestamp};
use serde::Serialize;

use crate::configs::ClientConfig;

use omnipaxos_kv::correctness::operation_history::{OperationHistory, Input, Output};

#[derive(Debug, Serialize, Clone)]
struct RequestData {
    request_time: Timestamp,
    write: bool,
    response_time: Option<Timestamp>,
    response_count: usize,

    // History fields
    #[serde(skip)]
    key: String,
    #[serde(skip)]
    write_value: Option<String>,
    #[serde(skip)]
    call_time_ns: i64,
    #[serde(skip)]
    return_time_ns: Option<i64>,
    #[serde(skip)]
    response_value: Option<String>,
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

    pub fn new_request(&mut self, is_write: bool, key: String, write_value: Option<String>) {
        let now_ms = Utc::now().timestamp_millis();
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(now_ms * 1_000_000);
        let data = RequestData {
            request_time: now_ms,
            write: is_write,
            response_time: None,
            response_count: 0,
            key,
            write_value,
            call_time_ns: now_ns,
            return_time_ns: None,
            response_value: None,
        };
        self.request_data.push(data);
    }

    pub fn new_response(&mut self, command_id: CommandId, response_value: Option<String>) {
        let now_ms = Utc::now().timestamp_millis();
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(now_ms * 1_000_000);
        if let Some(request_data) = self.request_data.get_mut(command_id) {
            if request_data.response_time.is_none() {
                request_data.response_time = Some(now_ms);
                request_data.return_time_ns = Some(now_ns);
                request_data.response_value = response_value;
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

    pub fn save_history(&self, file_path: &str, client_id: u64) -> Result<(), std::io::Error> {
        #[derive(Serialize)]
        struct HistoryInput<'a> {
            #[serde(rename = "type")]
            op_type: &'a str,
            key: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            value: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct HistoryOutput<'a> {
            status: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            value: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct HistoryEntry<'a> {
            client_id: u64,
            input: HistoryInput<'a>,
            call: i64,
            output: HistoryOutput<'a>,
            return_time: i64,
        }

        let mut entries: Vec<HistoryEntry> = Vec::with_capacity(self.request_data.len());
        for req in &self.request_data {
            let Some(return_ns) = req.return_time_ns else { continue };
            let (op_type, value) = if req.write {
                ("Put", req.write_value.as_deref())
            } else {
                ("Get", None)
            };
            entries.push(HistoryEntry {
                client_id,
                input: HistoryInput { op_type, key: &req.key, value },
                call: req.call_time_ns,
                output: HistoryOutput {
                    status: "ok",
                    value: req.response_value.as_deref(),
                },
                return_time: return_ns,
            });
        }

        let json = serde_json::to_string_pretty(&entries)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(file_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = File::create(file_path)?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}
