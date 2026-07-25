use serde::{Deserialize, Serialize};

/// Telemetry signal type carried by the message.
///
/// `Ord`/`Hash` are derived so a `Signal` can key per-signal statistics tables and endpoint
/// routing maps. The variant order (`Logs` before `Traces`) also gives a stable default ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Signal {
    Logs,
    Traces,
}

impl Signal {
    /// Lowercase wire/CLI name of the signal (`"logs"` / `"traces"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Traces => "traces",
        }
    }
}

/// One shard of a multi-service OTLP request: a single `ResourceLogs` entry for logs and a single
/// `ResourceSpans` entry for traces, holding the spans of all `num_traces` traces.
#[derive(Debug, Clone)]
pub struct ServiceShard {
    pub service_name: Option<String>,
    /// Log records emitted for this shard; on the correlated path they are spread over the shard's
    /// traces and then over each trace's spans.
    pub num_logs: usize,
    /// Traces emitted for this shard, each with its own `trace_id` and span tree. Always `>= 1` on
    /// the traces path; ignored when traces are not generated.
    pub num_traces: usize,
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
