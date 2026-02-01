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
