use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalPath {
    Logs,
    Metrics,
}

impl SignalPath {
    pub fn http_path(&self) -> &'static str {
        match self {
            SignalPath::Logs => "/v1/logs",
            SignalPath::Metrics => "/v1/metrics",
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessagePayload {
    Json(serde_json::Value),
    Protobuf(Vec<u8>),
    MalformedJson(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    Valid,
    InvalidJson,
    InvalidMalformedJson,
}

#[derive(Debug, Clone)]
pub struct OTLPMessage {
    pub payload: MessagePayload,
    pub tenant_id: String,
    pub project_id: String,
    pub source: String,
    pub message_type: MessageType,
    pub signal_path: SignalPath,
}

impl OTLPMessage {
    pub fn payload_size_bytes(&self) -> usize {
        match &self.payload {
            MessagePayload::Json(json) => serde_json::to_vec(json).map(|v| v.len()).unwrap_or(0),
            MessagePayload::Protobuf(bytes) => bytes.len(),
            MessagePayload::MalformedJson(s) => s.len(),
        }
    }
}
