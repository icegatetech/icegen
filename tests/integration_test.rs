use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use otel_log_generator::config::AttributesCardinalityConfig;
use otel_log_generator::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use otel_log_generator::pb::opentelemetry::proto::common::v1::any_value;
use otel_log_generator::transport::Destination;
use otel_log_generator::{
    LogJsonEncoder, LogProtobufEncoder, MessagePayload, OTLPLogMessageGenerator, OTLPMessageType,
    OtelConfig, OtelGenerator, ServiceShard, Signal, SignalGenerator, TimestampJitterConfig,
};
use prost::Message;

fn single_shard(service_name: Option<&str>, num_records: usize) -> Vec<ServiceShard> {
    vec![ServiceShard {
        service_name: service_name.map(ToString::to_string),
        num_logs: num_records,
        num_traces: 1,
    }]
}

fn make_json_generator() -> OTLPLogMessageGenerator {
    OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        AttributesCardinalityConfig::default(),
        TimestampJitterConfig::default(),
        Arc::new(LogJsonEncoder),
    )
}

fn make_protobuf_generator() -> OTLPLogMessageGenerator {
    OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        AttributesCardinalityConfig::default(),
        TimestampJitterConfig::default(),
        Arc::new(LogProtobufEncoder),
    )
}

fn make_json_generator_with_cardinality(
    cardinality: AttributesCardinalityConfig,
) -> OTLPLogMessageGenerator {
    OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        cardinality,
        TimestampJitterConfig {
            across_batch_timestamp_jitter_ns: 1_000_000_000,
            intra_batch_timestamp_jitter_ns: 5_000_000,
            intra_batch_overlap_probability: 0.05,
        },
        Arc::new(LogJsonEncoder),
    )
}

#[test]
fn test_generate_valid_message() {
    let generator = make_json_generator();
    let result = generator.generate_message(
        Some("tenant1".to_string()),
        Some("tenant1-acc-01".to_string()),
        single_shard(Some("tenant1-svc-01"), 1),
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(message.tenant_id.is_some());
    assert_eq!(message.source, "test-source");
    assert_eq!(message.message_type, OTLPMessageType::Valid);

    match message.message {
        MessagePayload::Json(json) => {
            assert!(json.get("resourceLogs").is_some());
        }
        _ => panic!("Expected JSON payload"),
    }
}

#[test]
fn test_generate_aggregated_message() {
    let generator = make_json_generator();
    let result = generator.generate_message(
        Some("tenant3".to_string()),
        Some("tenant3-acc-02".to_string()),
        single_shard(Some("tenant3-svc-05"), 5),
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(message.tenant_id.is_some());

    match message.message {
        MessagePayload::Json(json) => {
            let resource_logs = json.get("resourceLogs").unwrap().as_array().unwrap();
            assert_eq!(resource_logs.len(), 1);

            let scope_logs = resource_logs[0]
                .get("scopeLogs")
                .unwrap()
                .as_array()
                .unwrap();
            let log_records = scope_logs[0].get("logRecords").unwrap().as_array().unwrap();
            assert_eq!(log_records.len(), 5);
        }
        _ => panic!("Expected JSON payload"),
    }
}

#[test]
fn test_json_payload_contains_tenant_aware_resource_attributes() {
    let generator = make_json_generator();
    let message = generator
        .generate_message(
            Some("tenant3".to_string()),
            Some("tenant3-acc-02".to_string()),
            single_shard(Some("tenant3-svc-05"), 1),
        )
        .unwrap();

    let MessagePayload::Json(json) = message.message else {
        panic!("Expected JSON payload");
    };

    let resource_attributes = json["resourceLogs"][0]["resource"]["attributes"]
        .as_array()
        .unwrap();

    let as_map = resource_attributes
        .iter()
        .map(|attribute| {
            (
                attribute["key"].as_str().unwrap(),
                attribute["value"]["stringValue"].as_str().unwrap(),
            )
        })
        .collect::<HashMap<_, _>>();

    assert_eq!(as_map.get("service.name"), Some(&"tenant3-svc-05"));
    assert_eq!(as_map.get("cloud.account.id"), Some(&"tenant3-acc-02"));
}

#[test]
fn test_generate_invalid_message() {
    let generator = make_json_generator();
    let result = generator.generate_invalid_message(Some("tenant1".to_string()));

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(message.tenant_id.is_some());

    // Should be either InvalidJson or InvalidMalformedJson
    assert!(
        message.message_type == OTLPMessageType::InvalidJson
            || message.message_type == OTLPMessageType::InvalidMalformedJson
    );
}

#[test]
fn test_generate_protobuf_message() {
    let generator = make_protobuf_generator();
    let result = generator.generate_message(
        Some("tenant2".to_string()),
        Some("tenant2-acc-04".to_string()),
        single_shard(Some("tenant2-svc-06"), 3),
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(message.tenant_id.is_some());
    assert_eq!(message.message_type, OTLPMessageType::Valid);

    match message.message {
        MessagePayload::Protobuf(bytes) => {
            assert!(!bytes.is_empty());
        }
        _ => panic!("Expected Protobuf payload"),
    }
}

#[test]
fn test_protobuf_payload_contains_tenant_aware_resource_attributes() {
    let generator = make_protobuf_generator();
    let message = generator
        .generate_message(
            Some("tenant2".to_string()),
            Some("tenant2-acc-04".to_string()),
            single_shard(Some("tenant2-svc-06"), 2),
        )
        .unwrap();

    let MessagePayload::Protobuf(bytes) = message.message else {
        panic!("Expected Protobuf payload");
    };

    let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();
    let attributes = &decoded.resource_logs[0]
        .resource
        .as_ref()
        .unwrap()
        .attributes;

    let as_map = attributes
        .iter()
        .map(|attribute| {
            let value = attribute
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref())
                .and_then(|value| match value {
                    any_value::Value::StringValue(value) => Some(value.as_str()),
                    _ => None,
                })
                .unwrap();
            (attribute.key.as_str(), value)
        })
        .collect::<HashMap<_, _>>();

    assert_eq!(as_map.get("service.name"), Some(&"tenant2-svc-06"));
    assert_eq!(as_map.get("cloud.account.id"), Some(&"tenant2-acc-04"));
}

#[test]
fn test_generate_message_preserves_tenant_id_across_encoders() {
    let json_gen = make_json_generator();
    let protobuf_gen = make_protobuf_generator();

    let valid = json_gen
        .generate_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            single_shard(Some("tenant1-svc-01"), 1),
        )
        .unwrap();
    assert!(valid.tenant_id.is_some());

    let aggregated = json_gen
        .generate_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            single_shard(Some("tenant1-svc-01"), 2),
        )
        .unwrap();
    assert!(aggregated.tenant_id.is_some());

    let invalid = json_gen
        .generate_invalid_message(Some("tenant1".to_string()))
        .unwrap();
    assert!(invalid.tenant_id.is_some());

    let protobuf = protobuf_gen
        .generate_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            single_shard(Some("tenant1-svc-01"), 2),
        )
        .unwrap();
    assert!(protobuf.tenant_id.is_some());
}

#[test]
fn test_trace_id_format() {
    use otel_log_generator::message::FakeDataGenerator;

    let trace_id = FakeDataGenerator::generate_trace_id();
    // 16 raw bytes; hex-encoded form is 32 lowercase hex characters
    assert_eq!(trace_id.len(), 16);
    let encoded = hex::encode(trace_id);
    assert_eq!(encoded.len(), 32);
    assert!(encoded.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_span_id_format() {
    use otel_log_generator::message::FakeDataGenerator;

    let span_id = FakeDataGenerator::generate_span_id();
    // 8 raw bytes; hex-encoded form is 16 lowercase hex characters
    assert_eq!(span_id.len(), 8);
    let encoded = hex::encode(span_id);
    assert_eq!(encoded.len(), 16);
    assert!(encoded.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_cardinality_limit_applies_to_resource_and_log_attributes() {
    let mut limits = HashMap::new();
    limits.insert("k8s.pod.name".to_string(), 3);
    limits.insert("request.id".to_string(), 5);

    let generator = make_json_generator_with_cardinality(AttributesCardinalityConfig {
        enabled: true,
        default_limit: None,
        limit_by_attr: limits,
    });

    let mut pod_values = HashSet::new();
    let mut request_values = HashSet::new();

    for _ in 0..150 {
        let message = generator
            .generate_message(
                Some("tenant1".to_string()),
                Some("tenant1-acc-01".to_string()),
                single_shard(Some("tenant1-svc-01"), 6),
            )
            .unwrap();
        let MessagePayload::Json(json) = message.message else {
            panic!("Expected JSON payload");
        };

        let resource_logs = json.get("resourceLogs").unwrap().as_array().unwrap();
        let resource_attributes = resource_logs[0]
            .get("resource")
            .unwrap()
            .get("attributes")
            .unwrap()
            .as_array()
            .unwrap();

        for attr in resource_attributes {
            if attr.get("key").unwrap().as_str().unwrap() == "k8s.pod.name" {
                let value = attr
                    .get("value")
                    .unwrap()
                    .get("stringValue")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string();
                pod_values.insert(value);
            }
        }

        let log_records = resource_logs[0]
            .get("scopeLogs")
            .unwrap()
            .as_array()
            .unwrap()[0]
            .get("logRecords")
            .unwrap()
            .as_array()
            .unwrap();

        for record in log_records {
            let attrs = record.get("attributes").unwrap().as_array().unwrap();
            for attr in attrs {
                if attr.get("key").unwrap().as_str().unwrap() == "request.id" {
                    let value = attr
                        .get("value")
                        .unwrap()
                        .get("stringValue")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string();
                    request_values.insert(value);
                }
            }
        }
    }

    assert!(
        pod_values.len() <= 3,
        "expected <= 3 unique k8s.pod.name values, got {}",
        pod_values.len()
    );
    assert!(
        request_values.len() <= 5,
        "expected <= 5 unique request.id values, got {}",
        request_values.len()
    );
}

#[test]
fn test_cardinality_disabled_keeps_original_values() {
    let mut limits = HashMap::new();
    limits.insert("request.id".to_string(), 1);

    let generator = make_json_generator_with_cardinality(AttributesCardinalityConfig {
        enabled: false,
        default_limit: None,
        limit_by_attr: limits,
    });

    let message = generator
        .generate_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            single_shard(Some("tenant1-svc-01"), 1),
        )
        .unwrap();
    let MessagePayload::Json(json) = message.message else {
        panic!("Expected JSON payload");
    };

    let resource_logs = json.get("resourceLogs").unwrap().as_array().unwrap();
    let log_records = resource_logs[0]
        .get("scopeLogs")
        .unwrap()
        .as_array()
        .unwrap()[0]
        .get("logRecords")
        .unwrap()
        .as_array()
        .unwrap();

    let attrs = log_records[0]
        .get("attributes")
        .unwrap()
        .as_array()
        .unwrap();
    let request_id = attrs
        .iter()
        .find(|attr| attr.get("key").unwrap().as_str().unwrap() == "request.id")
        .unwrap()
        .get("value")
        .unwrap()
        .get("stringValue")
        .unwrap()
        .as_str()
        .unwrap();

    assert!(
        !request_id.starts_with("bucket_"),
        "cardinality limiter should be disabled"
    );
}

#[test]
fn test_protobuf_omits_service_name_and_cloud_account_when_none() {
    let generator = make_protobuf_generator();
    let message = generator
        .generate_message(None, None, single_shard(None, 2))
        .unwrap();

    let MessagePayload::Protobuf(bytes) = message.message else {
        panic!("Expected Protobuf payload");
    };
    let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();
    let resource_attrs = &decoded.resource_logs[0]
        .resource
        .as_ref()
        .unwrap()
        .attributes;
    let attr_keys: Vec<&str> = resource_attrs.iter().map(|kv| kv.key.as_str()).collect();
    assert!(
        !attr_keys.contains(&"service.name"),
        "service.name must be absent when svc=None"
    );
    assert!(
        !attr_keys.contains(&"cloud.account.id"),
        "cloud.account.id must be absent when cloud_account_id=None"
    );
}

#[test]
fn test_protobuf_scope_name_and_attrs_when_service_omitted() {
    let generator = make_protobuf_generator();
    let message = generator
        .generate_message(None, None, single_shard(None, 2))
        .unwrap();

    let MessagePayload::Protobuf(bytes) = message.message else {
        panic!("Expected Protobuf payload");
    };
    let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();
    let scope = decoded.resource_logs[0].scope_logs[0]
        .scope
        .as_ref()
        .unwrap();
    assert_eq!(scope.name, "io.trihub.icegen");
    let scope_attr_keys: Vec<&str> = scope.attributes.iter().map(|kv| kv.key.as_str()).collect();
    assert!(
        !scope_attr_keys.contains(&"library.name"),
        "library.name must be absent when svc=None"
    );
    assert!(
        scope_attr_keys.contains(&"library.version"),
        "library.version must be present"
    );
}

/// Capture one OTLP-over-HTTP request on `listener`: the raw request text, read up to the end of
/// the declared body, answered with a bare `200 OK` so the client's send completes.
async fn capture_one_http_request(listener: tokio::net::TcpListener) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut stream, _) = listener.accept().await.expect("accept");
    let mut buf = vec![0u8; 8192];
    let mut data: Vec<u8> = Vec::new();
    loop {
        let read = stream.read(&mut buf).await.expect("read");
        if read == 0 {
            break;
        }
        data.extend_from_slice(&buf[..read]);
        let text = String::from_utf8_lossy(&data);
        let Some(header_end) = text.find("\r\n\r\n") else {
            continue;
        };
        // Read exactly as many body bytes as the client announced; otherwise a large payload would
        // be asserted against a truncated prefix.
        let content_length: usize = text[..header_end]
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length: ")
                    .or_else(|| line.strip_prefix("Content-Length: "))
            })
            .map(|value| value.trim().parse().expect("content-length"))
            .unwrap_or(0);
        if data.len() >= header_end + 4 + content_length {
            break;
        }
    }
    stream
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
        .await
        .expect("write response");
    String::from_utf8_lossy(&data).to_string()
}

/// One generation cycle with both signals must produce two real HTTP requests, each to its own
/// signal endpoint, both tagged with the same tenant. Nothing below the transport can prove this:
/// only the wire shows the routing and the headers together.
#[tokio::test]
async fn logs_and_traces_reach_their_own_http_endpoints_in_one_cycle() {
    use std::time::Duration;
    use tokio::net::TcpListener;

    let logs_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let traces_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let logs_addr = logs_listener.local_addr().unwrap();
    let traces_addr = traces_listener.local_addr().unwrap();

    let logs_server = tokio::spawn(capture_one_http_request(logs_listener));
    let traces_server = tokio::spawn(capture_one_http_request(traces_listener));

    let config = OtelConfig {
        destination: Destination::Http {
            endpoints: HashMap::from([
                (Signal::Logs, format!("http://{logs_addr}/v1/logs")),
                (Signal::Traces, format!("http://{traces_addr}/v1/traces")),
            ]),
            protobuf: false,
        },
        invalid_record_percent: 0.0,
        logs_per_message: 2,
        traces_per_message: 2,
        print_logs: false,
        count: 1,
        message_interval_ms: 0,
        concurrency: 1,
        continuous: false,
        // No retries: a listener that never answers must fail the test, not be papered over.
        retry_max_retries: 0,
        retry_base_delay_ms: 100,
        retry_max_delay_ms: 100,
        tenant_id: "tenant-wire".to_string(),
        tenant_count: 1,
        cloud_account_count_per_tenant: 1,
        service_count_per_tenant: 1,
        label_cardinality_enabled: false,
        label_cardinality_default_limit: None,
        label_cardinality_limits: String::new(),
        record_across_batch_timestamp_jitter_ms: 0,
        record_intra_batch_timestamp_jitter_ns: 0,
        record_intra_batch_overlap_probability: 0.0,
        service_shards_per_message: 1,
        signals: vec![Signal::Logs, Signal::Traces],
        llm_max_tool_calls: 1,
        llm_capture_content: false,
        llm_profile_weights: "simple_chat:1".to_string(),
        trace_min_spans: 0,
        trace_max_spans: 0,
        auth_headers: String::new(),
        auth_bearer: None,
        auth_basic: None,
    };

    let generator = OtelGenerator::new(config).await.unwrap();
    let stats = generator.send_messages_batch(1, 0).await.unwrap();
    assert_eq!(stats.cycles.success, 1, "the cycle must be delivered");
    for signal in [Signal::Logs, Signal::Traces] {
        let counters = stats
            .per_signal
            .get(&signal)
            .unwrap_or_else(|| panic!("no requests recorded for {}", signal.as_str()));
        assert_eq!(
            counters.success,
            1,
            "{} request not delivered",
            signal.as_str()
        );
    }

    let logs_request = tokio::time::timeout(Duration::from_secs(5), logs_server)
        .await
        .expect("logs endpoint received a request")
        .unwrap();
    let traces_request = tokio::time::timeout(Duration::from_secs(5), traces_server)
        .await
        .expect("traces endpoint received a request")
        .unwrap();

    assert!(
        logs_request.starts_with("POST /v1/logs "),
        "logs request went to the wrong path: {}",
        logs_request.lines().next().unwrap_or_default()
    );
    assert!(
        traces_request.starts_with("POST /v1/traces "),
        "traces request went to the wrong path: {}",
        traces_request.lines().next().unwrap_or_default()
    );
    assert!(
        logs_request.contains("\"resourceLogs\""),
        "logs endpoint did not receive a logs payload"
    );
    assert!(
        traces_request.contains("\"resourceSpans\""),
        "traces endpoint did not receive a traces payload"
    );
    assert!(
        !logs_request.contains("\"resourceSpans\""),
        "traces payload leaked into the logs endpoint"
    );
    for request in [&logs_request, &traces_request] {
        assert!(
            request
                .to_lowercase()
                .contains("x-scope-orgid: tenant-wire"),
            "missing tenant header on the wire: {request}"
        );
    }
}

#[test]
fn test_multi_shard_request_has_correct_resource_logs_count() {
    let generator = make_json_generator();
    let shards = vec![
        ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 3,
            num_traces: 1,
        },
        ServiceShard {
            service_name: Some("svc-b".to_string()),
            num_logs: 3,
            num_traces: 1,
        },
        ServiceShard {
            service_name: Some("svc-c".to_string()),
            num_logs: 3,
            num_traces: 1,
        },
    ];
    let message = generator
        .generate_message(Some("tenant1".to_string()), None, shards)
        .unwrap();

    let MessagePayload::Json(json) = message.message else {
        panic!("Expected JSON payload");
    };
    let resource_logs = json["resourceLogs"].as_array().unwrap();
    assert_eq!(resource_logs.len(), 3);
    let counts: Vec<usize> = resource_logs
        .iter()
        .map(|rl| rl["scopeLogs"][0]["logRecords"].as_array().unwrap().len())
        .collect();
    assert_eq!(counts, vec![3, 3, 3]);
}
