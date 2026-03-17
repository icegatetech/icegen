use std::collections::{HashMap, HashSet};

use otel_log_generator::config::LabelCardinalityConfig;
use otel_log_generator::{MessagePayload, OTLPLogMessageGenerator, OTLPLogMessageType};

#[test]
fn test_generate_valid_message() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_valid_message();

    assert!(result.is_ok());
    let message = result.unwrap();
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
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_aggregated_message(5);

    assert!(result.is_ok());
    let message = result.unwrap();

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
fn test_generate_invalid_message() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_invalid_message();

    assert!(result.is_ok());
    let message = result.unwrap();

    // Should be either InvalidJson or InvalidMalformedJson
    assert!(
        message.message_type == OTLPLogMessageType::InvalidJson
            || message.message_type == OTLPLogMessageType::InvalidMalformedJson
    );
}

#[test]
fn test_generate_protobuf_message() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_protobuf_message(3);

    assert!(result.is_ok());
    let message = result.unwrap();
    assert_eq!(message.message_type, OTLPLogMessageType::Valid);

    match message.message {
        MessagePayload::Protobuf(bytes) => {
            assert!(!bytes.is_empty());
        }
        _ => panic!("Expected Protobuf payload"),
    }
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

    let generator = OTLPLogMessageGenerator::new_with_cardinality(
        "test-source".to_string(),
        LabelCardinalityConfig {
            enabled: true,
            default_limit: None,
            limits,
        },
    );

    let mut pod_values = HashSet::new();
    let mut request_values = HashSet::new();

    for _ in 0..150 {
        let message = generator.generate_aggregated_message(6).unwrap();
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

    let generator = OTLPLogMessageGenerator::new_with_cardinality(
        "test-source".to_string(),
        LabelCardinalityConfig {
            enabled: false,
            default_limit: None,
            limits,
        },
    );

    let message = generator.generate_valid_message().unwrap();
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
