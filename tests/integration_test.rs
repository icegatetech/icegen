use std::collections::{HashMap, HashSet};

use otel_log_generator::config::LabelCardinalityConfig;
use otel_log_generator::message::OTLPMetricsMessageGenerator;
use otel_log_generator::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use otel_log_generator::pb::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_log_generator::pb::opentelemetry::proto::common::v1::any_value;
use otel_log_generator::{MessagePayload, OTLPLogMessageGenerator, OTLPLogMessageType};
use prost::Message;

#[test]
fn test_generate_valid_message() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_valid_message(
        "tenant1".to_string(),
        "tenant1-acc-01".to_string(),
        "tenant1-svc-01".to_string(),
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(!message.tenant_id().is_empty());
    assert_eq!(message.source(), "test-source");
    assert_eq!(message.message_type(), OTLPLogMessageType::Valid);

    match message.payload() {
        MessagePayload::Json(json) => {
            assert!(json.get("resourceLogs").is_some());
        }
        _ => panic!("Expected JSON payload"),
    }
}

#[test]
fn test_generate_aggregated_message() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_aggregated_message(
        "tenant3".to_string(),
        "tenant3-acc-02".to_string(),
        "tenant3-svc-05".to_string(),
        5,
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(!message.tenant_id().is_empty());

    match message.payload() {
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
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let message = generator
        .generate_valid_message(
            "tenant3".to_string(),
            "tenant3-acc-02".to_string(),
            "tenant3-svc-05".to_string(),
        )
        .unwrap();

    let MessagePayload::Json(json) = message.payload() else {
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
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_invalid_message("tenant1".to_string());

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(!message.tenant_id().is_empty());

    // Should be either InvalidJson or InvalidMalformedJson
    assert!(
        message.message_type() == OTLPLogMessageType::InvalidJson
            || message.message_type() == OTLPLogMessageType::InvalidMalformedJson
    );
}

#[test]
fn test_generate_protobuf_message() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let result = generator.generate_protobuf_message(
        "tenant2".to_string(),
        "tenant2-acc-04".to_string(),
        "tenant2-svc-06".to_string(),
        3,
    );

    assert!(result.is_ok());
    let message = result.unwrap();
    assert!(!message.tenant_id().is_empty());
    assert_eq!(message.message_type(), OTLPLogMessageType::Valid);

    match message.payload() {
        MessagePayload::Protobuf(bytes) => {
            assert!(!bytes.is_empty());
        }
        _ => panic!("Expected Protobuf payload"),
    }
}

#[test]
fn test_protobuf_payload_contains_tenant_aware_resource_attributes() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());
    let message = generator
        .generate_protobuf_message(
            "tenant2".to_string(),
            "tenant2-acc-04".to_string(),
            "tenant2-svc-06".to_string(),
            2,
        )
        .unwrap();

    let MessagePayload::Protobuf(bytes) = message.payload() else {
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
fn test_public_generation_methods_return_non_empty_tenant_id() {
    let generator = OTLPLogMessageGenerator::new("test-source".to_string());

    let valid = generator
        .generate_valid_message(
            "tenant1".to_string(),
            "tenant1-acc-01".to_string(),
            "tenant1-svc-01".to_string(),
        )
        .unwrap();
    assert!(!valid.tenant_id().is_empty());

    let aggregated = generator
        .generate_aggregated_message(
            "tenant1".to_string(),
            "tenant1-acc-01".to_string(),
            "tenant1-svc-01".to_string(),
            2,
        )
        .unwrap();
    assert!(!aggregated.tenant_id().is_empty());

    let invalid = generator.generate_invalid_message("tenant1".to_string()).unwrap();
    assert!(!invalid.tenant_id().is_empty());

    let protobuf = generator
        .generate_protobuf_message(
            "tenant1".to_string(),
            "tenant1-acc-01".to_string(),
            "tenant1-svc-01".to_string(),
            2,
        )
        .unwrap();
    assert!(!protobuf.tenant_id().is_empty());
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
        let message = generator
            .generate_aggregated_message(
                "tenant1".to_string(),
                "tenant1-acc-01".to_string(),
                "tenant1-svc-01".to_string(),
                6,
            )
            .unwrap();
        let MessagePayload::Json(json) = message.payload() else {
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

    let message = generator
        .generate_valid_message(
            "tenant1".to_string(),
            "tenant1-acc-01".to_string(),
            "tenant1-svc-01".to_string(),
        )
        .unwrap();
    let MessagePayload::Json(json) = message.payload() else {
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
fn test_generate_gauge_metric_json() {
    let generator = OTLPMetricsMessageGenerator::new("test-source".to_string());
    let msg = generator.generate_gauge_message(
        "tenant1".to_string(), "tenant1-acc-01".to_string(), "tenant1-svc-01".to_string(),
    ).unwrap();

    let MessagePayload::Json(json) = msg.payload() else { panic!("Expected JSON") };
    assert!(json.get("resourceMetrics").is_some());
    let metrics = &json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
    assert_eq!(metrics.as_array().unwrap().len(), 1);
    assert!(metrics[0].get("gauge").is_some());
}

#[test]
fn test_generate_protobuf_metric() {
    let generator = OTLPMetricsMessageGenerator::new("test-source".to_string());
    let msg = generator.generate_protobuf_metric_message(
        "tenant1".to_string(), "tenant1-acc-01".to_string(), "tenant1-svc-01".to_string(),
    ).unwrap();

    let MessagePayload::Protobuf(bytes) = msg.payload() else { panic!("Expected Protobuf") };
    let decoded = ExportMetricsServiceRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded.resource_metrics.len(), 1);
}

#[test]
fn test_metrics_message_has_correct_signal_path() {
    use otel_log_generator::SignalPath;
    let generator = OTLPMetricsMessageGenerator::new("test-source".to_string());
    let msg = generator.generate_gauge_message(
        "tenant1".to_string(), "tenant1-acc-01".to_string(), "tenant1-svc-01".to_string(),
    ).unwrap();
    assert_eq!(msg.as_otlp_message().signal_path, SignalPath::Metrics);
}

#[test]
fn test_metrics_resource_attributes_contain_tenant_fields() {
    let generator = OTLPMetricsMessageGenerator::new("test-source".to_string());
    let msg = generator.generate_gauge_message(
        "tenant1".to_string(), "tenant1-acc-01".to_string(), "tenant1-svc-01".to_string(),
    ).unwrap();

    let MessagePayload::Json(json) = msg.payload() else { panic!() };
    let attrs = json["resourceMetrics"][0]["resource"]["attributes"].as_array().unwrap();
    let as_map: HashMap<&str, &str> = attrs.iter().map(|a| {
        (a["key"].as_str().unwrap(), a["value"]["stringValue"].as_str().unwrap())
    }).collect();
    assert_eq!(as_map.get("cloud.account.id"), Some(&"tenant1-acc-01"));
    assert_eq!(as_map.get("service.name"), Some(&"tenant1-svc-01"));
}

#[test]
fn test_config_rejects_both_signals_disabled() {
    use otel_log_generator::OtelConfig;
    let config = OtelConfig {
        ingest_endpoint: "http://localhost:4318".to_string(),
        healthcheck_endpoint: None,
        use_protobuf: false,
        transport: "http".to_string(),
        invalid_record_percent: 0.0,
        records_per_message: 1,
        print_logs: false,
        count: 1,
        message_interval_ms: 0,
        concurrency: 1,
        continuous: false,
        retry_max_retries: 3,
        retry_base_delay_ms: 1000,
        retry_max_delay_ms: 32000,
        tenant_id: "tenant1".to_string(),
        tenant_count: 1,
        cloud_account_count_per_tenant: 4,
        service_count_per_tenant: 6,
        label_cardinality_enabled: true,
        label_cardinality_default_limit: None,
        label_cardinality_limits: String::new(),
        enable_logs: false,
        enable_metrics: false,
    };
    assert!(config.validate().is_err());
}
