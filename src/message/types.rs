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
    pub project_id: String,
    pub source: String,
    pub message_type: OTLPLogMessageType,
}

impl OTLPLogMessage {
    pub fn new(
        message: MessagePayload,
        project_id: String,
        source: String,
        message_type: OTLPLogMessageType,
    ) -> Self {
        Self {
            message,
            project_id,
            source,
            message_type,
        }
    }
}
