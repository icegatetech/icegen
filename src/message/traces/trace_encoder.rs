//! Encoding `PlannedTraces` into OTLP/JSON and protobuf. Separate from the log encoder: the span
//! tree and typed attributes have a different shape. KeyValue helpers are shared (`attrs.rs`).

use crate::error::Result;
use crate::message::attrs::{
    pairs_to_json_attrs, pairs_to_proto_kv, typed_pairs_to_json_attrs, typed_pairs_to_proto_kv,
};
use crate::message::traces::trace_plan::{
    PlannedResourceSpans, PlannedSpan, PlannedTraces, SpanKind, SpanStatusCode,
};
use crate::message::types::MessagePayload;
use serde_json::{json, Value};

// OTLP `schemaUrl` pins the OpenTelemetry *semantic-conventions* version (the `/schemas/<ver>`
// namespace) — numbered separately from the OTel *specification* (specs/otel, ~1.5x); the two are
// not interchangeable. 1.42.0 is the latest semconv release and the last one that still defines
// the `gen_ai.*` attributes: from 1.42.0 they are deprecated and relocated to the dedicated GenAI
// semconv repo (still in Development). Every `gen_ai.*` key produced by `LlmSpanProfile` exists
// under 1.42.0 and none were renamed since 1.37. Re-pin to the GenAI repo's own schema URL once it
// publishes one and stabilises.
const SCHEMA_URL: &str = "https://opentelemetry.io/schemas/1.42.0";

/// Encoder of a trace plan into wire format.
pub trait TraceEncoder: Send + Sync {
    #[allow(clippy::result_large_err)]
    fn encode(&self, request: &PlannedTraces) -> Result<MessagePayload>;
}

/// OTLP/JSON.
pub struct TraceJsonEncoder;

/// OTLP/protobuf.
pub struct TraceProtobufEncoder;

fn span_kind_to_i32(kind: SpanKind) -> i32 {
    match kind {
        SpanKind::Internal => 1,
        SpanKind::Server => 2,
        SpanKind::Client => 3,
        SpanKind::Producer => 4,
        SpanKind::Consumer => 5,
    }
}

fn status_code_to_i32(code: SpanStatusCode) -> i32 {
    match code {
        SpanStatusCode::Unset => 0,
        SpanStatusCode::Ok => 1,
        SpanStatusCode::Error => 2,
    }
}

impl TraceEncoder for TraceJsonEncoder {
    #[allow(clippy::result_large_err)]
    fn encode(&self, request: &PlannedTraces) -> Result<MessagePayload> {
        let resource_spans: Vec<Value> = request
            .resource_spans
            .iter()
            .map(encode_resource_spans_json)
            .collect();
        Ok(MessagePayload::Json(
            json!({ "resourceSpans": resource_spans }),
        ))
    }
}

fn encode_resource_spans_json(rs: &PlannedResourceSpans) -> Value {
    let trace_id_hex = hex::encode(rs.trace_id);
    let spans: Vec<Value> = rs
        .scope
        .spans
        .iter()
        .map(|s| encode_span_json(s, &trace_id_hex))
        .collect();
    json!({
        "resource": {
            "attributes": pairs_to_json_attrs(&rs.resource_attrs),
            "droppedAttributesCount": rs.resource_dropped_attributes_count
        },
        "scopeSpans": [{
            "scope": {
                "name": rs.scope.scope_name,
                "version": rs.scope.scope_version,
                "attributes": pairs_to_json_attrs(&rs.scope.scope_attrs),
            },
            "spans": spans,
            "schemaUrl": SCHEMA_URL
        }],
        "schemaUrl": SCHEMA_URL
    })
}

fn encode_span_json(span: &PlannedSpan, trace_id_hex: &str) -> Value {
    let mut obj = json!({
        "traceId": trace_id_hex,
        "spanId": hex::encode(span.span_id),
        "name": span.name,
        "kind": span_kind_to_i32(span.kind),
        "startTimeUnixNano": span.start_ns.max(0).to_string(),
        "endTimeUnixNano": span.end_ns.max(0).to_string(),
        "attributes": typed_pairs_to_json_attrs(&span.attributes),
        "events": span.events.iter().map(|e| json!({
            "timeUnixNano": e.time_ns.max(0).to_string(),
            "name": e.name,
            "attributes": typed_pairs_to_json_attrs(&e.attributes),
        })).collect::<Vec<_>>(),
        "status": { "code": status_code_to_i32(span.status_code), "message": span.status_message },
    });
    if let Some(parent) = span.parent_span_id {
        obj["parentSpanId"] = json!(hex::encode(parent));
    }
    obj
}

impl TraceEncoder for TraceProtobufEncoder {
    #[allow(clippy::result_large_err)]
    fn encode(&self, request: &PlannedTraces) -> Result<MessagePayload> {
        use crate::pb::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
        use crate::pb::opentelemetry::proto::common::v1::InstrumentationScope;
        use crate::pb::opentelemetry::proto::resource::v1::Resource;
        use crate::pb::opentelemetry::proto::trace::v1::{
            span, ResourceSpans, ScopeSpans, Span, Status,
        };
        use prost::Message;

        let resource_spans: Vec<ResourceSpans> = request
            .resource_spans
            .iter()
            .map(|rs| {
                let spans: Vec<Span> = rs
                    .scope
                    .spans
                    .iter()
                    .map(|s| Span {
                        trace_id: rs.trace_id.to_vec(),
                        span_id: s.span_id.to_vec(),
                        trace_state: String::new(),
                        parent_span_id: s.parent_span_id.map(|p| p.to_vec()).unwrap_or_default(),
                        flags: 0,
                        name: s.name.clone(),
                        kind: span_kind_to_i32(s.kind),
                        start_time_unix_nano: s.start_ns.max(0) as u64,
                        end_time_unix_nano: s.end_ns.max(0) as u64,
                        attributes: typed_pairs_to_proto_kv(&s.attributes),
                        dropped_attributes_count: 0,
                        events: s
                            .events
                            .iter()
                            .map(|e| span::Event {
                                time_unix_nano: e.time_ns.max(0) as u64,
                                name: e.name.clone(),
                                attributes: typed_pairs_to_proto_kv(&e.attributes),
                                dropped_attributes_count: 0,
                            })
                            .collect(),
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: s.status_message.clone(),
                            code: status_code_to_i32(s.status_code),
                        }),
                    })
                    .collect();

                ResourceSpans {
                    resource: Some(Resource {
                        attributes: pairs_to_proto_kv(&rs.resource_attrs),
                        dropped_attributes_count: rs.resource_dropped_attributes_count,
                    }),
                    scope_spans: vec![ScopeSpans {
                        scope: Some(InstrumentationScope {
                            name: rs.scope.scope_name.clone(),
                            version: rs.scope.scope_version.clone(),
                            attributes: pairs_to_proto_kv(&rs.scope.scope_attrs),
                            dropped_attributes_count: 0,
                        }),
                        spans,
                        schema_url: SCHEMA_URL.to_string(),
                    }],
                    schema_url: SCHEMA_URL.to_string(),
                }
            })
            .collect();

        let mut buf = Vec::new();
        ExportTraceServiceRequest { resource_spans }.encode(&mut buf)?;
        Ok(MessagePayload::Protobuf(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::traces::{AttrValue, PlannedScopeSpans};
    use crate::message::types::MessagePayload;
    use crate::pb::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
    use prost::Message;

    fn sample_traces() -> PlannedTraces {
        PlannedTraces {
            project_id: "proj-1".to_string(),
            resource_spans: vec![PlannedResourceSpans {
                resource_attrs: vec![("service.name".to_string(), "svc-a".to_string())],
                resource_dropped_attributes_count: 0,
                trace_id: [0xAB; 16],
                scope: PlannedScopeSpans {
                    scope_name: "io.trihub.svc-a".to_string(),
                    scope_version: "1.0.0".to_string(),
                    scope_attrs: vec![],
                    spans: vec![
                        PlannedSpan {
                            span_id: [0x01; 8],
                            parent_span_id: None,
                            name: "invoke_agent x".to_string(),
                            kind: SpanKind::Internal,
                            start_ns: 100,
                            end_ns: 900,
                            attributes: vec![(
                                "gen_ai.usage.input_tokens".to_string(),
                                AttrValue::Int(42),
                            )],
                            events: vec![],
                            status_code: SpanStatusCode::Unset,
                            status_message: String::new(),
                        },
                        PlannedSpan {
                            span_id: [0x02; 8],
                            parent_span_id: Some([0x01; 8]),
                            name: "chat gpt-4o".to_string(),
                            kind: SpanKind::Client,
                            start_ns: 150,
                            end_ns: 800,
                            attributes: vec![],
                            events: vec![],
                            status_code: SpanStatusCode::Ok,
                            status_message: String::new(),
                        },
                    ],
                },
            }],
        }
    }

    #[test]
    fn json_encoder_emits_resource_spans_with_hex_ids() {
        let MessagePayload::Json(json) = TraceJsonEncoder.encode(&sample_traces()).unwrap() else {
            panic!("expected JSON");
        };
        let spans = json["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(
            spans[0]["traceId"].as_str(),
            Some("abababababababababababababababab")
        );
        assert_eq!(spans[0]["spanId"].as_str(), Some("0101010101010101"));
        assert_eq!(spans[0]["startTimeUnixNano"].as_str(), Some("100"));
        assert_eq!(spans[1]["parentSpanId"].as_str(), Some("0101010101010101"));
        assert_eq!(
            spans[0]["attributes"][0]["value"]["intValue"].as_str(),
            Some("42")
        );
    }

    #[test]
    fn protobuf_encoder_round_trips() {
        let MessagePayload::Protobuf(bytes) =
            TraceProtobufEncoder.encode(&sample_traces()).unwrap()
        else {
            panic!("expected protobuf");
        };
        let decoded = ExportTraceServiceRequest::decode(bytes.as_slice()).unwrap();
        let spans = &decoded.resource_spans[0].scope_spans[0].spans;
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].trace_id, vec![0xAB; 16]);
        assert_eq!(spans[1].parent_span_id, vec![0x01; 8]);
        assert_eq!(spans[0].start_time_unix_nano, 100);
    }
}
