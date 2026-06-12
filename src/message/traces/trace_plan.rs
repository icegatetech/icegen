//! Format-neutral OTLP trace plan. The encoders (`TraceJsonEncoder`/`TraceProtobufEncoder`)
//! turn it into bytes. Mirrors `plan.rs` for logs, but the span tree and typed attribute
//! values (int/double/array) differ — spans carry numeric `gen_ai.*`.

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

/// `ResourceSpans`: one service + one trace_id + span tree.
#[derive(Debug, Clone)]
pub struct PlannedResourceSpans {
    pub resource_attrs: Vec<(String, String)>,
    pub resource_dropped_attributes_count: u32,
    pub trace_id: [u8; 16],
    pub scope: PlannedScopeSpans,
}

/// Full trace request: project_id + a set of ResourceSpans (one per service/trace).
#[derive(Debug, Clone)]
pub struct PlannedTraces {
    pub project_id: String,
    pub resource_spans: Vec<PlannedResourceSpans>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_defaults_and_attr_values() {
        let span = PlannedSpan {
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
