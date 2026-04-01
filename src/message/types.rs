use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OTLPLogMessageType {
    Valid,
    InvalidJson,
    InvalidMalformedJson,
}

#[derive(Debug, Clone)]
pub enum MessagePayload {
    Json(serde_json::Value),
    Protobuf(Vec<u8>),
    MalformedJson(String),
}

#[derive(Debug, Clone)]
pub struct OTLPLogMessage {
    pub message: MessagePayload,
    pub tenant_id: String,
    pub project_id: String,
    pub source: String,
    pub message_type: OTLPLogMessageType,
}

impl OTLPLogMessage {
    pub fn new(
        message: MessagePayload,
        tenant_id: String,
        project_id: String,
        source: String,
        message_type: OTLPLogMessageType,
    ) -> Self {
        Self {
            message,
            tenant_id,
            project_id,
            source,
            message_type,
        }
    }

    pub fn payload_size_bytes(&self) -> usize {
        match &self.message {
            MessagePayload::Json(json) => serde_json::to_vec(json).map(|v| v.len()).unwrap_or(0),
            MessagePayload::Protobuf(bytes) => bytes.len(),
            MessagePayload::MalformedJson(s) => s.len(),
        }
    }
}
