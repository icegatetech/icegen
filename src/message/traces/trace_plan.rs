//! Format-neutral OTLP trace plan. The encoders (`TraceJsonEncoder`/`TraceProtobufEncoder`)
//! turn it into bytes. Mirrors `plan.rs` for logs, but the span tree and typed attribute
//! values (int/double/array) differ — spans carry numeric `gen_ai.*`.

use std::sync::Arc;

/// Typed OTLP attribute value (corresponds to `AnyValue`).
#[derive(Debug, Clone, PartialEq)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    StrArray(Vec<String>),
}

/// Key-value pairs of span/event attributes.
pub type SpanAttrs = Vec<(String, AttrValue)>;

/// Span kind (maps to `span::SpanKind` when encoding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// Span status code (maps to `status::StatusCode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatusCode {
    Unset,
    Ok,
    Error,
}

/// Span event (time-stamped annotation).
#[derive(Debug, Clone)]
pub struct PlannedEvent {
    pub time_ns: i64,
    pub name: String,
    pub attributes: SpanAttrs,
}

/// A single span in the tree.
#[derive(Debug, Clone)]
pub struct PlannedSpan {
    /// The trace this span belongs to. Lives on the span, as in OTLP: one `ResourceSpans` group
    /// carries the spans of every trace of its service shard, so the group itself has no trace id.
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: SpanKind,
    pub start_ns: i64,
    pub end_ns: i64,
    pub attributes: SpanAttrs,
    pub events: Vec<PlannedEvent>,
    pub status_code: SpanStatusCode,
    pub status_message: String,
}

/// `ScopeSpans`: one instrumentation scope + its spans.
#[derive(Debug, Clone)]
pub struct PlannedScopeSpans {
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attrs: Vec<(String, String)>,
    pub spans: Vec<PlannedSpan>,
}

/// `ResourceSpans`: one service shard + the spans of all its traces.
#[derive(Debug, Clone)]
pub struct PlannedResourceSpans {
    pub resource_attrs: Vec<(String, String)>,
    pub resource_dropped_attributes_count: u32,
    pub scope: PlannedScopeSpans,
}

/// Full trace request: project_id + a set of ResourceSpans (one per service shard).
#[derive(Debug, Clone)]
pub struct PlannedTraces {
    pub project_id: String,
    pub resource_spans: Vec<PlannedResourceSpans>,
}

/// One span a correlated log record can be attached to: its id and its time window.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SpanAnchor {
    pub span_id: [u8; 8],
    /// Start of the span window (inclusive lower bound for correlated log timestamps).
    pub start_ns: i64,
    /// End of the span window (inclusive upper bound for correlated log timestamps).
    pub end_ns: i64,
}

impl SpanAnchor {
    /// Window length in nanoseconds, floored at zero.
    pub fn duration_ns(&self) -> i64 {
        (self.end_ns - self.start_ns).max(0)
    }
}

/// Native correlation handle for one service shard's trace, threaded from the trace planner to the
/// log planner so a shard's log records can carry the trace's `trace_id`, the `span_id` of the
/// span they belong to, and a timestamp inside that span's window. This is not an OTLP wire type;
/// the fields already live on the trace's [`PlannedSpan`]s.
/// Crate-internal: it is an orchestration handle for the planners, not part of the public library
/// API.
#[derive(Debug, Clone)]
pub(crate) struct TraceCorrelation {
    /// The single `trace_id` shared by every span of the shard's trace.
    pub trace_id: [u8; 16],
    /// Every span of the trace, in layout order — index 0 is the root (layout contract). Shared
    /// because a correlation is cloned per shard and the tree can hold thousands of spans. A trace
    /// always has at least the root, so this is never empty: a single-span trace degenerates to
    /// the root alone and every record lands there.
    pub anchors: Arc<[SpanAnchor]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_defaults_and_attr_values() {
        let span = PlannedSpan {
            trace_id: [9u8; 16],
            span_id: [1u8; 8],
            parent_span_id: None,
            name: "chat gpt-4o".to_string(),
            kind: SpanKind::Client,
            start_ns: 10,
            end_ns: 20,
            attributes: vec![
                ("gen_ai.usage.input_tokens".to_string(), AttrValue::Int(42)),
                (
                    "gen_ai.request.temperature".to_string(),
                    AttrValue::Double(0.7),
                ),
                (
                    "gen_ai.response.finish_reasons".to_string(),
                    AttrValue::StrArray(vec!["stop".to_string()]),
                ),
            ],
            events: vec![],
            status_code: SpanStatusCode::Unset,
            status_message: String::new(),
        };
        assert!(span.parent_span_id.is_none());
        assert_eq!(span.attributes.len(), 3);
    }
}
