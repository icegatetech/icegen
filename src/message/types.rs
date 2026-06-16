use serde::{Deserialize, Serialize};

/// Telemetry signal type carried by the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signal {
    Logs,
    Traces,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OTLPMessageType {
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

/// A single OTLP message (logs or traces), ready to be sent by the transport.
#[derive(Debug, Clone)]
pub struct OTLPMessage {
    pub message: MessagePayload,
    pub signal: Signal,
    pub tenant_id: Option<String>,
    pub project_id: String,
    pub source: String,
    pub message_type: OTLPMessageType,
}

impl OTLPMessage {
    pub fn new(
        message: MessagePayload,
        signal: Signal,
        tenant_id: Option<String>,
        project_id: String,
        source: String,
        message_type: OTLPMessageType,
    ) -> Self {
        Self {
            message,
            signal,
            tenant_id,
            project_id,
            source,
            message_type,
        }
    }

    pub fn payload_size_bytes(&self) -> usize {
        // Protobuf is usually 10-30% smaller than JSON for the same data. The main savings are the lack of keys ("severity_text": → field tag 1 byte) and more compact numbers (varint).
        match &self.message {
            MessagePayload::Json(json) => serde_json::to_vec(json).map(|v| v.len()).unwrap_or(0),
            MessagePayload::Protobuf(bytes) => bytes.len(),
            MessagePayload::MalformedJson(s) => s.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_message_carries_logs_signal() {
        let msg = OTLPMessage::new(
            MessagePayload::Json(serde_json::json!({"resourceLogs": []})),
            Signal::Logs,
            Some("tenant1".to_string()),
            "proj".to_string(),
            "src".to_string(),
            OTLPMessageType::Valid,
        );
        assert_eq!(msg.signal, Signal::Logs);
    }
}
