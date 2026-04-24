use std::collections::{HashMap, HashSet};

use otel_log_generator::config::LabelCardinalityConfig;
use otel_log_generator::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use otel_log_generator::pb::opentelemetry::proto::common::v1::any_value;
use otel_log_generator::{
    MessagePayload, OTLPLogMessageGenerator, OTLPLogMessageType, TimestampJitterConfig,
};
use prost::Message;

#[test]
fn test_generate_valid_message() {
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let result = generator.generate_valid_message(
        Some("tenant1".to_string()),
        Some("tenant1-acc-01".to_string()),
        Some("tenant1-svc-01".to_string()),
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(message.tenant_id.is_some());
    assert_eq!(message.source, "test-source");
    assert_eq!(message.message_type, OTLPLogMessageType::Valid);

    match message.message {
        MessagePayload::Json(json) => {
            assert!(json.get("resourceLogs").is_some());
        }
        _ => panic!("Expected JSON payload"),
    }
}

#[test]
fn test_generate_aggregated_message() {
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let result = generator.generate_aggregated_message(
        Some("tenant3".to_string()),
        Some("tenant3-acc-02".to_string()),
        Some("tenant3-svc-05".to_string()),
        5,
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
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let message = generator
        .generate_valid_message(
            Some("tenant3".to_string()),
            Some("tenant3-acc-02".to_string()),
            Some("tenant3-svc-05".to_string()),
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
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let result = generator.generate_invalid_message(Some("tenant1".to_string()));

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(message.tenant_id.is_some());

    // Should be either InvalidJson or InvalidMalformedJson
    assert!(
        message.message_type == OTLPLogMessageType::InvalidJson
            || message.message_type == OTLPLogMessageType::InvalidMalformedJson
    );
}

#[test]
fn test_generate_protobuf_message() {
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let result = generator.generate_protobuf_message(
        Some("tenant2".to_string()),
        Some("tenant2-acc-04".to_string()),
        Some("tenant2-svc-06".to_string()),
        3,
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(message.tenant_id.is_some());
    assert_eq!(message.message_type, OTLPLogMessageType::Valid);

    match message.message {
        MessagePayload::Protobuf(bytes) => {
            assert!(!bytes.is_empty());
        }
        _ => panic!("Expected Protobuf payload"),
    }
}

#[test]
fn test_protobuf_payload_contains_tenant_aware_resource_attributes() {
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let message = generator
        .generate_protobuf_message(
            Some("tenant2".to_string()),
            Some("tenant2-acc-04".to_string()),
            Some("tenant2-svc-06".to_string()),
            2,
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
fn test_public_generation_methods_return_some_tenant_id() {
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );

    let valid = generator
        .generate_valid_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            Some("tenant1-svc-01".to_string()),
        )
        .unwrap();
    assert!(valid.tenant_id.is_some());

    let aggregated = generator
        .generate_aggregated_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            Some("tenant1-svc-01".to_string()),
            2,
        )
        .unwrap();
    assert!(aggregated.tenant_id.is_some());

    let invalid = generator
        .generate_invalid_message(Some("tenant1".to_string()))
        .unwrap();
    assert!(invalid.tenant_id.is_some());

    let protobuf = generator
        .generate_protobuf_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            Some("tenant1-svc-01".to_string()),
            2,
        )
        .unwrap();
    assert!(protobuf.tenant_id.is_some());
}

#[test]
fn test_trace_id_format() {
    use otel_log_generator::message::FakeDataGenerator;

    let trace_id = FakeDataGenerator::generate_trace_id();
    assert_eq!(trace_id.len(), 32);
    assert!(trace_id.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_span_id_format() {
    use otel_log_generator::message::FakeDataGenerator;

    let span_id = FakeDataGenerator::generate_span_id();
    assert_eq!(span_id.len(), 16);
    assert!(span_id.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_cardinality_limit_applies_to_resource_and_log_attributes() {
    let mut limits = HashMap::new();
    limits.insert("k8s.pod.name".to_string(), 3);
    limits.insert("request.id".to_string(), 5);

    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig {
            enabled: true,
            default_limit: None,
            limits,
        },
        TimestampJitterConfig {
            across_batch_timestamp_jitter_ns: 1_000_000_000,
            intra_batch_timestamp_jitter_ns: 5_000_000,
            intra_batch_overlap_probability: 0.05,
        },
    );

    let mut pod_values = HashSet::new();
    let mut request_values = HashSet::new();

    for _ in 0..150 {
        let message = generator
            .generate_aggregated_message(
                Some("tenant1".to_string()),
                Some("tenant1-acc-01".to_string()),
                Some("tenant1-svc-01".to_string()),
                6,
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

    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig {
            enabled: false,
            default_limit: None,
            limits,
        },
        TimestampJitterConfig {
            across_batch_timestamp_jitter_ns: 1_000_000_000,
            intra_batch_timestamp_jitter_ns: 5_000_000,
            intra_batch_overlap_probability: 0.05,
        },
    );

    let message = generator
        .generate_valid_message(
            Some("tenant1".to_string()),
            Some("tenant1-acc-01".to_string()),
            Some("tenant1-svc-01".to_string()),
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
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let message = generator
        .generate_protobuf_message(None, None, None, 2)
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
    let generator = OTLPLogMessageGenerator::new(
        "test-source".to_string(),
        LabelCardinalityConfig::default(),
        TimestampJitterConfig::default(),
    );
    let message = generator
        .generate_protobuf_message(None, None, None, 2)
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
