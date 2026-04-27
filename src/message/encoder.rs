use crate::error::Result;
use crate::message::plan::{PlannedRecord, PlannedRequest, PlannedShard};
use crate::message::types::MessagePayload;
use crate::pb::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValue};
use serde_json::{json, Value};

/// Serializes a [`PlannedRequest`] into a wire-format [`MessagePayload`].
///
/// The encoder is chosen once at generator construction time and never changes, following the same
/// pattern as [`crate::transport::Transport`] in [`crate::generator::OtelLogGenerator`].
///
/// # Errors
///
/// Returns [`crate::error::GeneratorError::ProtobufEncodeError`] from [`ProtobufEncoder`] if
/// protobuf serialization fails. [`JsonEncoder`] is infallible for well-formed plans.
pub trait OtlpEncoder: Send + Sync {
    #[allow(clippy::result_large_err)]
    fn encode(&self, request: &PlannedRequest) -> Result<MessagePayload>;
}

/// Serializes a planned request to OTLP JSON format.
pub struct JsonEncoder;

/// Serializes a planned request to OTLP protobuf binary format.
pub struct ProtobufEncoder;

impl OtlpEncoder for JsonEncoder {
    // Always returns Ok — serde_json::json! and string formatting are infallible for well-formed
    // plans. Result is required by the trait contract shared with ProtobufEncoder.
    #[allow(clippy::result_large_err)]
    fn encode(&self, request: &PlannedRequest) -> Result<MessagePayload> {
        let resource_logs: Vec<Value> = request.shards.iter().map(encode_shard_json).collect();
        Ok(MessagePayload::Json(
            json!({ "resourceLogs": resource_logs }),
        ))
    }
}

fn encode_shard_json(shard: &PlannedShard) -> Value {
    let log_records: Vec<Value> = shard.records.iter().map(encode_record_json).collect();
    json!({
        "resource": {
            "attributes": pairs_to_json_attrs(&shard.resource_attrs),
            "droppedAttributesCount": shard.resource_dropped_attributes_count
        },
        "scopeLogs": [{
            "scope": {
                "name": shard.scope_name,
                "version": shard.scope_version,
                "attributes": pairs_to_json_attrs(&shard.scope_attrs),
                "droppedAttributesCount": shard.scope_dropped_attributes_count
            },
            "logRecords": log_records,
            "schemaUrl": "https://opentelemetry.io/schemas/1.21.0"
        }],
        "schemaUrl": "https://opentelemetry.io/schemas/1.21.0"
    })
}

fn encode_record_json(record: &PlannedRecord) -> Value {
    let ts = record.timestamp_ns.max(0).to_string();
    json!({
        "timeUnixNano": ts,
        "observedTimeUnixNano": ts,
        "severityNumber": record.severity_number,
        "severityText": record.severity_text,
        "body": { "stringValue": record.body },
        "attributes": pairs_to_json_attrs(&record.attributes),
        "traceId": hex::encode(record.trace_id),
        "spanId": hex::encode(record.span_id),
        "flags": record.flags,
    })
}

fn pairs_to_json_attrs(pairs: &[(String, String)]) -> Vec<Value> {
    pairs
        .iter()
        .map(|(key, value)| json!({ "key": key, "value": { "stringValue": value } }))
        .collect()
}

fn pairs_to_proto_kv(pairs: &[(String, String)]) -> Vec<KeyValue> {
    pairs
        .iter()
        .map(|(key, value)| KeyValue {
            key: key.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.clone())),
            }),
        })
        .collect()
}

impl OtlpEncoder for ProtobufEncoder {
    #[allow(clippy::result_large_err)]
    fn encode(&self, request: &PlannedRequest) -> Result<MessagePayload> {
        use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
        use crate::pb::opentelemetry::proto::common::v1::InstrumentationScope;
        use crate::pb::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use crate::pb::opentelemetry::proto::resource::v1::Resource;
        use prost::Message;

        let resource_logs: Vec<ResourceLogs> = request
            .shards
            .iter()
            .map(|shard| {
                let log_records: Vec<LogRecord> = shard
                    .records
                    .iter()
                    .map(|record| {
                        let timestamp_ns = record.timestamp_ns.max(0) as u64;
                        LogRecord {
                            time_unix_nano: timestamp_ns,
                            observed_time_unix_nano: timestamp_ns,
                            severity_number: record.severity_number,
                            severity_text: record.severity_text.clone(),
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(record.body.clone())),
                            }),
                            attributes: pairs_to_proto_kv(&record.attributes),
                            trace_id: record.trace_id.to_vec(),
                            span_id: record.span_id.to_vec(),
                            flags: record.flags,
                            dropped_attributes_count: 0,
                        }
                    })
                    .collect();

                ResourceLogs {
                    resource: Some(Resource {
                        attributes: pairs_to_proto_kv(&shard.resource_attrs),
                        dropped_attributes_count: shard.resource_dropped_attributes_count,
                    }),
                    scope_logs: vec![ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: shard.scope_name.clone(),
                            version: shard.scope_version.clone(),
                            attributes: pairs_to_proto_kv(&shard.scope_attrs),
                            dropped_attributes_count: shard.scope_dropped_attributes_count,
                        }),
                        log_records,
                        schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
                    }],
                    schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
                }
            })
            .collect();

        let mut buf = Vec::new();
        ExportLogsServiceRequest { resource_logs }.encode(&mut buf)?;
        Ok(MessagePayload::Protobuf(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::types::OTLPLogMessageType;
    use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    fn make_record(timestamp_ns: i64, suffix: u8) -> PlannedRecord {
        PlannedRecord {
            timestamp_ns,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: format!("body-{suffix}"),
            trace_id: [suffix; 16],
            span_id: [suffix; 8],
            flags: 0,
            attributes: vec![("request.id".to_string(), format!("req-{suffix}"))],
        }
    }

    fn make_shard(service: &str, records: Vec<PlannedRecord>) -> PlannedShard {
        PlannedShard {
            resource_attrs: vec![
                ("project_id".to_string(), "proj-1".to_string()),
                ("service.name".to_string(), service.to_string()),
            ],
            resource_dropped_attributes_count: 0,
            scope_name: format!("io.trihub.{service}"),
            scope_version: "1.0.0".to_string(),
            scope_attrs: vec![("library.name".to_string(), format!("trihub-{service}"))],
            scope_dropped_attributes_count: 0,
            records,
        }
    }

    fn make_request(shards: Vec<PlannedShard>) -> PlannedRequest {
        PlannedRequest {
            project_id: "proj-1".to_string(),
            shards,
            message_type: OTLPLogMessageType::Valid,
        }
    }

    #[test]
    fn json_encoder_serializes_planned_request_with_hex_ids_and_string_timestamp() {
        let request = make_request(vec![
            make_shard("svc-a", vec![make_record(100, 0xAB), make_record(200, 0xCD)]),
            make_shard("svc-b", vec![make_record(300, 0xEF)]),
        ]);
        let payload = JsonEncoder.encode(&request).unwrap();
        let MessagePayload::Json(json) = payload else {
            panic!("expected JSON payload");
        };

        let resource_logs = json["resourceLogs"].as_array().unwrap();
        assert_eq!(resource_logs.len(), 2);

        let records_a = resource_logs[0]["scopeLogs"][0]["logRecords"].as_array().unwrap();
        assert_eq!(records_a.len(), 2);
        // timeUnixNano must be serialised as a string (OTLP/JSON spec for uint64-shaped fields).
        assert_eq!(records_a[0]["timeUnixNano"].as_str(), Some("100"));
        assert_eq!(records_a[0]["observedTimeUnixNano"].as_str(), Some("100"));
        assert_eq!(records_a[1]["timeUnixNano"].as_str(), Some("200"));
        // trace_id/span_id must be hex-encoded, not raw bytes.
        assert_eq!(
            records_a[0]["traceId"].as_str(),
            Some("abababababababababababababababab")
        );
        assert_eq!(records_a[0]["spanId"].as_str(), Some("abababababababab"));

        let records_b = resource_logs[1]["scopeLogs"][0]["logRecords"].as_array().unwrap();
        assert_eq!(records_b.len(), 1);
        assert_eq!(records_b[0]["timeUnixNano"].as_str(), Some("300"));
    }

    #[test]
    fn protobuf_encoder_round_trips_planned_request() {
        let request = make_request(vec![
            make_shard("svc-a", vec![make_record(1_000, 0x11), make_record(2_000, 0x22)]),
            make_shard("svc-b", vec![make_record(3_000, 0x33)]),
        ]);
        let payload = ProtobufEncoder.encode(&request).unwrap();
        let MessagePayload::Protobuf(bytes) = payload else {
            panic!("expected Protobuf payload");
        };
        let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();

        assert_eq!(decoded.resource_logs.len(), 2);
        let records_a = &decoded.resource_logs[0].scope_logs[0].log_records;
        assert_eq!(records_a.len(), 2);
        assert_eq!(records_a[0].time_unix_nano, 1_000);
        assert_eq!(records_a[0].observed_time_unix_nano, 1_000);
        assert_eq!(records_a[0].trace_id, vec![0x11; 16]);
        assert_eq!(records_a[0].span_id, vec![0x11; 8]);
        assert_eq!(records_a[1].time_unix_nano, 2_000);

        let records_b = &decoded.resource_logs[1].scope_logs[0].log_records;
        assert_eq!(records_b.len(), 1);
        assert_eq!(records_b[0].time_unix_nano, 3_000);
    }

    #[test]
    fn protobuf_encoder_clamps_negative_timestamp_to_zero() {
        // PlannedRecord.timestamp_ns is i64 (can be negative under aggressive jitter), but the
        // protobuf field is u64 — the encoder is contractually responsible for clamping.
        let request = make_request(vec![make_shard(
            "svc-a",
            vec![make_record(-1, 0x01), make_record(-1_000_000_000, 0x02), make_record(42, 0x03)],
        )]);
        let payload = ProtobufEncoder.encode(&request).unwrap();
        let MessagePayload::Protobuf(bytes) = payload else {
            panic!("expected Protobuf payload");
        };
        let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();
        let records = &decoded.resource_logs[0].scope_logs[0].log_records;

        assert_eq!(records[0].time_unix_nano, 0, "negative timestamp must clamp to 0");
        assert_eq!(records[0].observed_time_unix_nano, 0);
        assert_eq!(records[1].time_unix_nano, 0, "large negative timestamp must clamp to 0");
        assert_eq!(records[2].time_unix_nano, 42, "non-negative timestamp must pass through");
    }
}
