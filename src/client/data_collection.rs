use std::{
    fs::File,
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use csv::Writer;
use omnipaxos_kv::common::{
    kv::{ClientId, CommandId, KVCommand},
    messages::ServerResult,
    utils::Timestamp,
};
use serde::Serialize;

use crate::configs::ClientConfig;

#[derive(Debug, Serialize, Clone, Copy)]
struct RequestData {
    request_time: Timestamp,
    write: bool,
    response_time: Option<Timestamp>,
    response_count: usize,
}

pub struct ClientData {
    request_data: Vec<RequestData>,
    pending_history: Vec<Option<HistoryOperation>>,
    completed_history: Vec<HistoryOperation>,
    client_id: ClientId,
    response_count: usize,
}

#[derive(Debug, Serialize, Clone)]
struct HistoryInput {
    #[serde(rename = "type")]
    op_type: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct HistoryOutput {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct HistoryOperation {
    client_id: ClientId,
    input: HistoryInput,
    call: i64,
    output: HistoryOutput,
    return_time: i64,
}

impl ClientData {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            request_data: Vec::new(),
            pending_history: Vec::new(),
            completed_history: Vec::new(),
            client_id,
            response_count: 0,
        }
    }

    pub fn new_request(&mut self, command_id: CommandId, cmd: &KVCommand) {
        let is_write = matches!(cmd, KVCommand::Put(_, _) | KVCommand::Delete(_));
        let data = RequestData {
            request_time: Utc::now().timestamp_millis(),
            write: is_write,
            response_time: None,
            response_count: 0,
        };
        self.request_data.push(data);

        if command_id >= self.pending_history.len() {
            self.pending_history.resize(command_id + 1, None);
        }
        self.pending_history[command_id] = Some(HistoryOperation {
            client_id: self.client_id,
            input: Self::history_input_from_command(cmd),
            call: Self::now_unix_nanos(),
            output: HistoryOutput {
                status: "ok".to_string(),
                value: None,
            },
            return_time: 0,
        });
    }

    pub fn new_response(&mut self, command_id: CommandId, result: &ServerResult) {
        let response_time = Utc::now().timestamp_millis();
        if let Some(request_data) = self.request_data.get_mut(command_id) {
            if request_data.response_time.is_none() {
                request_data.response_time = Some(response_time);
            }
            request_data.response_count += 1;
            self.response_count += 1;
        }

        if let Some(pending) = self.pending_history.get_mut(command_id) {
            if let Some(mut op) = pending.take() {
                op.return_time = Self::now_unix_nanos();
                match result {
                    ServerResult::Write(_) => {
                        op.output.value = None;
                    }
                    ServerResult::Read(_, value) => {
                        op.output.value = value.clone();
                    }
                }
                self.completed_history.push(op);
            }
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

    pub fn history_path_for_output(output_path: &str) -> String {
        let mut candidate = output_path.replace("client-", "history-");
        if let Some(stripped) = candidate.strip_suffix(".csv") {
            candidate = format!("{stripped}.json");
        } else if !candidate.ends_with(".json") {
            candidate = format!("{candidate}.json");
        }
        candidate
    }

    pub fn initialize_history_file(&self, file_path: &str) -> Result<(), std::io::Error> {
        let mut history_file = File::create(file_path)?;
        history_file.write_all(b"[]")?;
        history_file.flush()?;
        Ok(())
    }

    pub fn save_history(&self, file_path: String) -> Result<(), std::io::Error> {
        let mut history = self.completed_history.clone();
        history.sort_by_key(|op| op.call);

        let history_json = serde_json::to_string_pretty(&history)?;
        let mut history_file = File::create(file_path)?;
        history_file.write_all(history_json.as_bytes())?;
        history_file.flush()?;
        Ok(())
    }

    fn history_input_from_command(cmd: &KVCommand) -> HistoryInput {
        match cmd {
            KVCommand::Put(key, value) => HistoryInput {
                op_type: "Put".to_string(),
                key: key.clone(),
                value: Some(value.clone()),
            },
            KVCommand::Get(key) => HistoryInput {
                op_type: "Get".to_string(),
                key: key.clone(),
                value: None,
            },
            KVCommand::Delete(key) => HistoryInput {
                op_type: "Delete".to_string(),
                key: key.clone(),
                value: None,
            },
        }
    }

    fn now_unix_nanos() -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before UNIX epoch");
        now.as_nanos() as i64
    }
}
