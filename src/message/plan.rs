use crate::message::types::OTLPLogMessageType;

/// A single log record in format-neutral form.
///
/// Encoders consume this to produce wire-format bytes. Timestamp clamping (e.g. negative → 0 for
/// unsigned protobuf fields) is the encoder's responsibility, not the planner's.
#[derive(Debug, Clone)]
pub struct PlannedRecord {
    pub timestamp_ns: i64,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: String,
    /// 16 raw bytes; `JsonEncoder` hex-encodes, `ProtobufEncoder` takes `.to_vec()` directly.
    pub trace_id: [u8; 16],
    /// 8 raw bytes; `JsonEncoder` hex-encodes, `ProtobufEncoder` takes `.to_vec()` directly.
    pub span_id: [u8; 8],
    pub flags: u32,
    pub attributes: Vec<(String, String)>,
}

/// One shard of a planned OTLP request, mapping to a single `ResourceLogs` entry.
#[derive(Debug, Clone)]
pub struct PlannedShard {
    pub resource_attrs: Vec<(String, String)>,
    pub resource_dropped_attributes_count: u32,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attrs: Vec<(String, String)>,
    pub scope_dropped_attributes_count: u32,
    pub records: Vec<PlannedRecord>,
}

/// A fully planned OTLP request in format-neutral form, ready for encoding.
#[derive(Debug, Clone)]
pub struct PlannedRequest {
    pub project_id: String,
    pub shards: Vec<PlannedShard>,
    pub message_type: OTLPLogMessageType,
}
