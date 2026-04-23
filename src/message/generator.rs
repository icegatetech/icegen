use crate::config::{LabelCardinalityConfig, TimestampJitterConfig};
use crate::error::{GeneratorError, Result};
use crate::message::fake_data::FakeDataGenerator;
use crate::message::types::{MessagePayload, OTLPLogMessage, OTLPLogMessageType};
use chrono::Utc;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::{json, Value};

const DEFAULT_SERVICE_NAME: &str = "generator";

#[derive(Clone)]
pub struct OTLPLogMessageGenerator {
    source: String,
    label_cardinality: LabelCardinalityConfig,
    jitter: TimestampJitterConfig,
}

impl OTLPLogMessageGenerator {
    pub fn new(source: String) -> Self {
        Self {
            source,
            label_cardinality: LabelCardinalityConfig::default(),
            jitter: TimestampJitterConfig {
                across_batch_timestamp_jitter_ns: 1_000_000_000,
                intra_batch_timestamp_jitter_ns: 5_000_000,
                intra_batch_overlap_probability: 0.05,
            },
        }
    }

    /// Create a generator with explicit cardinality and timestamp jitter settings.
    ///
    /// `jitter.across_batch_timestamp_jitter_ns` shifts the whole batch backwards in time,
    /// while `jitter.intra_batch_timestamp_jitter_ns` controls spacing between neighbouring
    /// records inside that batch. `jitter.intra_batch_overlap_probability` controls how often
    /// the emitted timestamp for a record is moved backwards relative to the previous record.
    ///
    /// The generator expects jitter values in nanoseconds. When configuration comes from
    /// [`crate::config::OtelConfig`], use [`crate::config::OtelConfig::timestamp_jitter_config`]
    /// so CLI millisecond values are converted consistently.
    pub fn new_with_cardinality(
        source: String,
        label_cardinality: LabelCardinalityConfig,
        jitter: TimestampJitterConfig,
    ) -> Self {
        Self {
            source,
            label_cardinality,
            jitter,
        }
    }

    fn attributes_pairs_to_dict_list(pairs: &[(String, String)]) -> Vec<Value> {
        pairs
            .iter()
            .map(|(key, value)| {
                json!({
                    "key": key,
                    "value": {
                        "stringValue": value
                    }
                })
            })
            .collect()
    }

    fn generate_resource_attributes_pairs(
        &self,
        project_id: &str,
        cloud_account_id: Option<&str>,
        service_name: Option<&str>,
    ) -> Vec<(String, String)> {
        let mut attributes = vec![("project_id".to_string(), project_id.to_string())];
        if let Some(acc) = cloud_account_id {
            attributes.push(("cloud.account.id".to_string(), acc.to_string()));
        }
        if let Some(svc) = service_name {
            attributes.push(("service.name".to_string(), svc.to_string()));
        }
        attributes.push((
            "service.version".to_string(),
            FakeDataGenerator::generate_service_version(),
        ));
        attributes.push((
            "deployment.environment".to_string(),
            FakeDataGenerator::generate_deployment_environment(),
        ));
        attributes.push((
            "host.name".to_string(),
            FakeDataGenerator::generate_host_name(),
        ));
        attributes.push((
            "k8s.pod.name".to_string(),
            FakeDataGenerator::generate_k8s_pod_name(),
        ));
        attributes.push((
            "k8s.namespace.name".to_string(),
            FakeDataGenerator::generate_k8s_namespace(),
        ));
        attributes.push(("generator.source".to_string(), self.source.clone()));

        self.normalize_attribute_pairs(attributes)
    }

    fn generate_scope_attributes_pairs(&self, service_name: Option<&str>) -> Vec<(String, String)> {
        let mut rng = rand::thread_rng();
        let mut attrs = Vec::new();
        if let Some(svc) = service_name {
            attrs.push(("library.name".to_string(), format!("trihub-{}", svc)));
        }
        attrs.push((
            "library.version".to_string(),
            format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10)),
        ));
        attrs
    }

    fn generate_log_attributes_pairs(
        &self,
        request_id: &str,
        thread_id: &str,
    ) -> Vec<(String, String)> {
        let mut rng = rand::thread_rng();
        let mut log_attributes = Vec::new();

        if rng.gen::<f32>() > 0.5 {
            log_attributes.push((
                "http.method".to_string(),
                FakeDataGenerator::generate_http_method(),
            ));
        }

        if rng.gen::<f32>() > 0.6 {
            log_attributes.push((
                "http.status_code".to_string(),
                FakeDataGenerator::generate_http_status_code().to_string(),
            ));
        }

        if rng.gen::<f32>() > 0.7 {
            log_attributes.push(("user.id".to_string(), FakeDataGenerator::generate_uuid()));
        }

        log_attributes.push(("request.id".to_string(), request_id.to_string()));
        log_attributes.push(("thread.id".to_string(), thread_id.to_string()));

        self.normalize_attribute_pairs(log_attributes)
    }

    fn normalize_attribute_pairs(&self, pairs: Vec<(String, String)>) -> Vec<(String, String)> {
        pairs
            .into_iter()
            .map(|(key, value)| {
                let normalized = self.normalize_by_cardinality(&key, &value);
                (key, normalized)
            })
            .collect()
    }

    fn normalize_by_cardinality(&self, key: &str, value: &str) -> String {
        if !self.label_cardinality.enabled {
            return value.to_string();
        }

        let Some(limit) = self.label_cardinality.limit_for(key) else {
            return value.to_string();
        };

        if limit <= 1 {
            return "bucket_00".to_string();
        }

        let index = stable_bucket_index(key, value, limit);
        let width = num_digits(limit.saturating_sub(1));
        format!("bucket_{index:0width$}")
    }

    fn generate_log_body(severity_text: &str, service_name: Option<&str>) -> String {
        let mut rng = rand::thread_rng();

        let bodies = match severity_text {
            "INFO" => vec![
                format!(
                    "Request processed successfully in {}ms",
                    rng.gen_range(10..500)
                ),
                format!(
                    "User {} authenticated successfully",
                    FakeDataGenerator::generate_uuid()
                ),
                format!(
                    "Database connection established to {}",
                    FakeDataGenerator::generate_host_name()
                ),
                format!(
                    "Cache hit for key {}",
                    &FakeDataGenerator::generate_uuid()[..8]
                ),
                format!(
                    "Health check passed for service {}",
                    service_name.unwrap_or(DEFAULT_SERVICE_NAME)
                ),
            ],
            "WARN" => vec![
                format!("High memory usage detected: {}%", rng.gen_range(70..96)),
                format!("Slow query detected: {}ms", rng.gen_range(1000..5000)),
                format!(
                    "Connection pool near capacity: {}/100",
                    rng.gen_range(80..100)
                ),
                format!(
                    "Rate limit approaching for user {}",
                    FakeDataGenerator::generate_uuid()
                ),
                format!(
                    "Deprecated API endpoint accessed: /api/v1/{}",
                    FakeDataGenerator::generate_sentence()
                        .split_whitespace()
                        .next()
                        .unwrap_or("endpoint")
                ),
            ],
            "ERROR" => vec![
                format!(
                    "Database connection failed: {}",
                    FakeDataGenerator::generate_sentence()
                ),
                format!(
                    "Failed to process request: {}",
                    FakeDataGenerator::generate_sentence()
                ),
                format!(
                    "Authentication failed for user {}",
                    FakeDataGenerator::generate_email()
                ),
                format!(
                    "External API call failed: HTTP {}",
                    [500, 502, 503, 504].choose(&mut rng).unwrap()
                ),
                format!(
                    "Queue processing error: {}",
                    FakeDataGenerator::generate_sentence()
                ),
            ],
            _ => vec!["Generic log message".to_string()],
        };

        bodies.choose(&mut rng).unwrap().clone()
    }

    fn batch_timestamps_ns(&self, num_records: usize) -> Vec<i64> {
        if num_records == 0 {
            return vec![];
        }

        let mut rng = rand::thread_rng();
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let batch_offset_ns = if self.jitter.across_batch_timestamp_jitter_ns > 0 {
            rng.gen_range(0..self.jitter.across_batch_timestamp_jitter_ns)
        } else {
            0
        };

        let intra = self.jitter.intra_batch_timestamp_jitter_ns;
        let overlap_prob = self.jitter.intra_batch_overlap_probability;

        let mut result: Vec<i64> = Vec::with_capacity(num_records);
        let mut total_span_ns: i64 = 0;
        for _ in 0..num_records {
            let step = if intra > 0 {
                rng.gen_range(0..intra)
            } else {
                0
            };
            total_span_ns += step;
            result.push(step);
        }

        let mut prev_ns = now - batch_offset_ns - total_span_ns;
        for i in 0..num_records {
            let step = result[i];
            let candidate = prev_ns + step;
            result[i] = if i > 0 && intra > 0 && rng.gen::<f32>() < overlap_prob {
                prev_ns - rng.gen_range(0..intra)
            } else {
                candidate
            };
            prev_ns = candidate;
        }

        result
    }

    fn generate_single_log_record(&self, service_name: Option<&str>, timestamp_ns: i64) -> Value {
        let mut rng = rand::thread_rng();

        let (severity_number, severity_text) = FakeDataGenerator::generate_severity();
        let body = Self::generate_log_body(&severity_text, service_name);
        let trace_id = FakeDataGenerator::generate_trace_id();
        let span_id = FakeDataGenerator::generate_span_id();
        let request_id = FakeDataGenerator::generate_uuid();
        let thread_id = FakeDataGenerator::generate_thread_id();

        let log_attributes = self.generate_log_attributes_pairs(&request_id, &thread_id);

        json!({
            "timeUnixNano": timestamp_ns.to_string(),
            "observedTimeUnixNano": timestamp_ns.to_string(),
            "severityNumber": severity_number,
            "severityText": severity_text,
            "body": {
                "stringValue": body
            },
            "attributes": Self::attributes_pairs_to_dict_list(&log_attributes),
            "traceId": trace_id,
            "spanId": span_id,
            "flags": rng.gen_range(0..256),
        })
    }

    fn wrap_log_records_in_otlp(
        &self,
        project_id: &str,
        cloud_account_id: Option<&str>,
        service_name: Option<&str>,
        log_records: Vec<Value>,
    ) -> Value {
        let mut rng = rand::thread_rng();

        let resource_attributes =
            self.generate_resource_attributes_pairs(project_id, cloud_account_id, service_name);
        let scope_attributes = self.generate_scope_attributes_pairs(service_name);
        let scope_name_src = service_name.unwrap_or(DEFAULT_SERVICE_NAME);

        json!({
            "resource": {
                "attributes": Self::attributes_pairs_to_dict_list(&resource_attributes),
                "droppedAttributesCount": rng.gen_range(0..4)
            },
            "scopeLogs": [{
                "scope": {
                    "name": format!("io.trihub.{}", scope_name_src.replace('-', ".")),
                    "version": format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10)),
                    "attributes": Self::attributes_pairs_to_dict_list(&scope_attributes),
                    "droppedAttributesCount": rng.gen_range(0..3)
                },
                "logRecords": log_records,
                "schemaUrl": "https://opentelemetry.io/schemas/1.21.0"
            }],
            "schemaUrl": "https://opentelemetry.io/schemas/1.21.0"
        })
    }

    fn build_message(
        &self,
        message: MessagePayload,
        tenant_id: Option<String>,
        project_id: String,
        message_type: OTLPLogMessageType,
    ) -> OTLPLogMessage {
        OTLPLogMessage::new(
            message,
            tenant_id,
            project_id,
            self.source.clone(),
            message_type,
        )
    }

    /// Generate a single-record OTLP JSON payload.
    ///
    /// The record timestamp is generated with the configured batch-level jitter and is never set
    /// in the future. Because the payload contains one record, intra-batch spacing and overlap
    /// settings have no observable effect here.
    pub fn generate_valid_message(
        &self,
        tenant_id: Option<String>,
        cloud_account_id: Option<String>,
        service_name: Option<String>,
    ) -> Result<OTLPLogMessage> {
        self.build_valid_message(tenant_id, cloud_account_id, service_name)
    }

    fn build_valid_message(
        &self,
        tenant_id: Option<String>,
        cloud_account_id: Option<String>,
        service_name: Option<String>,
    ) -> Result<OTLPLogMessage> {
        let project_id = FakeDataGenerator::generate_project_id();

        let ts = self.batch_timestamps_ns(1)[0];
        let log_record = self.generate_single_log_record(service_name.as_deref(), ts);
        let resource_log = self.wrap_log_records_in_otlp(
            &project_id,
            cloud_account_id.as_deref(),
            service_name.as_deref(),
            vec![log_record],
        );

        let otlp_message = json!({
            "resourceLogs": [resource_log]
        });

        Ok(self.build_message(
            MessagePayload::Json(otlp_message),
            tenant_id,
            project_id,
            OTLPLogMessageType::Valid,
        ))
    }

    /// Generate a multi-record OTLP JSON payload.
    ///
    /// The whole batch is shifted backwards by `across_batch_timestamp_jitter_ns`, then record
    /// timestamps are planned using `intra_batch_timestamp_jitter_ns`. With
    /// `intra_batch_overlap_probability == 0.0`, emitted timestamps do not decrease. With
    /// `intra_batch_timestamp_jitter_ns == 0`, all records in the batch collapse to the same
    /// timestamp.
    pub fn generate_aggregated_message(
        &self,
        tenant_id: Option<String>,
        cloud_account_id: Option<String>,
        service_name: Option<String>,
        num_records: usize,
    ) -> Result<OTLPLogMessage> {
        if num_records == 1 {
            return self.build_valid_message(tenant_id, cloud_account_id, service_name);
        }

        if num_records < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "num_records must be >= 1".to_string(),
            ));
        }

        let project_id = FakeDataGenerator::generate_project_id();

        let ts_list = self.batch_timestamps_ns(num_records);
        let log_records: Vec<Value> = ts_list
            .into_iter()
            .map(|ts| self.generate_single_log_record(service_name.as_deref(), ts))
            .collect();

        let resource_log = self.wrap_log_records_in_otlp(
            &project_id,
            cloud_account_id.as_deref(),
            service_name.as_deref(),
            log_records,
        );

        let otlp_message = json!({
            "resourceLogs": [resource_log]
        });

        Ok(self.build_message(
            MessagePayload::Json(otlp_message),
            tenant_id,
            project_id,
            OTLPLogMessageType::Valid,
        ))
    }

    pub fn generate_invalid_message(&self, tenant_id: Option<String>) -> Result<OTLPLogMessage> {
        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();

        let invalid_types = [
            "empty_resource_logs",
            "missing_resource_logs",
            "null_resource_logs",
            "invalid_resource_logs_type",
            "malformed_json",
        ];

        let invalid_type = invalid_types.choose(&mut rng).unwrap();

        match *invalid_type {
            "empty_resource_logs" => {
                let invalid_message = json!({"resourceLogs": []});
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPLogMessageType::InvalidJson,
                ))
            }
            "missing_resource_logs" => {
                let invalid_message = json!({
                    "someOtherField": "value",
                    "timestamp": "2024-01-01T00:00:00Z"
                });
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPLogMessageType::InvalidJson,
                ))
            }
            "null_resource_logs" => {
                let invalid_message = json!({"resourceLogs": null});
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPLogMessageType::InvalidJson,
                ))
            }
            "invalid_resource_logs_type" => {
                let invalid_message = json!({"resourceLogs": "not-an-array"});
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPLogMessageType::InvalidJson,
                ))
            }
            "malformed_json" => Ok(self.build_message(
                MessagePayload::MalformedJson(r#"{"resourceLogs": [ invalid json"#.to_string()),
                tenant_id,
                project_id,
                OTLPLogMessageType::InvalidMalformedJson,
            )),
            _ => {
                let invalid_message = json!({"resourceLogs": []});
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPLogMessageType::InvalidJson,
                ))
            }
        }
    }

    /// Generate a protobuf OTLP payload with `num_records` log records.
    ///
    /// Timestamp planning matches [`Self::generate_aggregated_message`], but the final payload is
    /// serialized as protobuf and negative intermediate values are clamped to `0` before writing
    /// them into unsigned OTLP fields.
    pub fn generate_protobuf_message(
        &self,
        tenant_id: Option<String>,
        cloud_account_id: Option<String>,
        service_name: Option<String>,
        num_records: usize,
    ) -> Result<OTLPLogMessage> {
        use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
        use crate::pb::opentelemetry::proto::common::v1::{
            any_value, AnyValue, InstrumentationScope, KeyValue,
        };
        use crate::pb::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use crate::pb::opentelemetry::proto::resource::v1::Resource;
        use prost::Message;

        if num_records < 1 {
            return Err(GeneratorError::InvalidConfiguration(
                "num_records must be >= 1".to_string(),
            ));
        }

        let mut rng = rand::thread_rng();
        let project_id = FakeDataGenerator::generate_project_id();
        let svc = service_name.as_deref();

        let ts_list = self.batch_timestamps_ns(num_records);

        // Generate log records
        let log_records: Vec<LogRecord> = ts_list
            .iter()
            .map(|&ts_i64| {
                let timestamp_ns = ts_i64.max(0) as u64;
                let (severity_number, severity_text) = FakeDataGenerator::generate_severity();
                let body = Self::generate_log_body(&severity_text, svc);
                let trace_id = FakeDataGenerator::generate_trace_id();
                let span_id = FakeDataGenerator::generate_span_id();
                let request_id = FakeDataGenerator::generate_uuid();
                let thread_id = FakeDataGenerator::generate_thread_id();

                let log_attributes = self.generate_log_attributes_pairs(&request_id, &thread_id);
                let attributes: Vec<KeyValue> = log_attributes
                    .iter()
                    .map(|(key, value)| KeyValue {
                        key: key.clone(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(value.clone())),
                        }),
                    })
                    .collect();

                LogRecord {
                    time_unix_nano: timestamp_ns,
                    observed_time_unix_nano: timestamp_ns,
                    severity_number: severity_number as i32,
                    severity_text,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body)),
                    }),
                    attributes,
                    trace_id: hex::decode(&trace_id).unwrap_or_default(),
                    span_id: hex::decode(&span_id).unwrap_or_default(),
                    flags: rng.gen_range(0..256),
                    dropped_attributes_count: 0,
                }
            })
            .collect();

        // Resource attributes
        let resource_attributes_pairs =
            self.generate_resource_attributes_pairs(&project_id, cloud_account_id.as_deref(), svc);
        let resource_attributes: Vec<KeyValue> = resource_attributes_pairs
            .iter()
            .map(|(key, value)| KeyValue {
                key: key.clone(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(value.clone())),
                }),
            })
            .collect();

        // Scope attributes
        let scope_attributes_pairs = self.generate_scope_attributes_pairs(svc);
        let scope_attributes: Vec<KeyValue> = scope_attributes_pairs
            .iter()
            .map(|(key, value)| KeyValue {
                key: key.clone(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(value.clone())),
                }),
            })
            .collect();

        let resource = Resource {
            attributes: resource_attributes,
            dropped_attributes_count: rng.gen_range(0..4),
        };

        let scope_name_src = svc.unwrap_or(DEFAULT_SERVICE_NAME);
        let scope = InstrumentationScope {
            name: format!("io.trihub.{}", scope_name_src.replace('-', ".")),
            version: format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10)),
            attributes: scope_attributes,
            dropped_attributes_count: rng.gen_range(0..3),
        };

        let scope_logs = ScopeLogs {
            scope: Some(scope),
            log_records,
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        };

        let resource_logs = ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![scope_logs],
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        };

        let request = ExportLogsServiceRequest {
            resource_logs: vec![resource_logs],
        };

        let mut buf = Vec::new();
        request.encode(&mut buf)?;

        Ok(self.build_message(
            MessagePayload::Protobuf(buf),
            tenant_id,
            project_id,
            OTLPLogMessageType::Valid,
        ))
    }
}

fn stable_bucket_index(key: &str, value: &str, limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }

    // FNV-1a 64-bit for deterministic, stable bucket assignment across runs.
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for byte in key
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0xff))
        .chain(value.as_bytes().iter())
    {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }

    (hash as usize) % limit
}

fn num_digits(mut number: usize) -> usize {
    if number == 0 {
        return 1;
    }

    let mut digits = 0;
    while number > 0 {
        number /= 10;
        digits += 1;
    }
    digits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TimestampJitterConfig;
    use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use chrono::Utc;
    use prost::Message;

    fn gen_with_jitter(
        batch_jitter_ns: i64,
        intra_batch_jitter_ns: i64,
        intra_batch_overlap_probability: f32,
    ) -> OTLPLogMessageGenerator {
        OTLPLogMessageGenerator::new_with_cardinality(
            "test".to_string(),
            LabelCardinalityConfig::default(),
            TimestampJitterConfig {
                across_batch_timestamp_jitter_ns: batch_jitter_ns,
                intra_batch_timestamp_jitter_ns: intra_batch_jitter_ns,
                intra_batch_overlap_probability,
            },
        )
    }

    fn json_timestamps(message: OTLPLogMessage) -> Vec<i64> {
        let MessagePayload::Json(json) = message.message else {
            panic!("Expected JSON payload");
        };

        json["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| {
                record["timeUnixNano"]
                    .as_str()
                    .unwrap()
                    .parse::<i64>()
                    .unwrap()
            })
            .collect()
    }

    fn protobuf_timestamps(message: OTLPLogMessage) -> Vec<i64> {
        let MessagePayload::Protobuf(bytes) = message.message else {
            panic!("Expected protobuf payload");
        };

        ExportLogsServiceRequest::decode(bytes.as_slice())
            .unwrap()
            .resource_logs
            .into_iter()
            .flat_map(|resource_logs| resource_logs.scope_logs.into_iter())
            .flat_map(|scope_logs| scope_logs.log_records.into_iter())
            .map(|record| i64::try_from(record.time_unix_nano).unwrap())
            .collect()
    }

    #[test]
    fn generate_valid_message_keeps_single_timestamp_within_batch_window() {
        let batch_jitter_ns = 2_000_000_000_i64;
        let gen = gen_with_jitter(batch_jitter_ns, 5_000_000, 0.25);
        let now_before = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let timestamps = json_timestamps(
            gen.generate_valid_message(None, None, Some("svc".to_string()))
                .unwrap(),
        );
        let now_after = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        assert_eq!(timestamps.len(), 1);
        let ts = timestamps[0];
        assert!(
            ts <= now_after,
            "timestamp in future: {} > {}",
            ts,
            now_after
        );
        assert!(
            ts >= now_before - batch_jitter_ns,
            "timestamp too old: {} < {}",
            ts,
            now_before - batch_jitter_ns
        );
    }

    #[test]
    fn generate_aggregated_message_is_non_decreasing_when_overlap_disabled() {
        let gen = gen_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let ts = json_timestamps(
            gen.generate_aggregated_message(None, None, None, 50)
                .unwrap(),
        );
        for i in 1..ts.len() {
            assert!(
                ts[i - 1] <= ts[i],
                "non-monotonic at i={}: {} > {}",
                i,
                ts[i - 1],
                ts[i]
            );
        }
    }

    #[test]
    fn generate_aggregated_message_collapses_timestamps_when_intra_jitter_zero() {
        let gen = gen_with_jitter(0, 0, 0.0);
        let ts = json_timestamps(
            gen.generate_aggregated_message(None, None, None, 10)
                .unwrap(),
        );
        assert!(
            ts.windows(2).all(|w| w[0] == w[1]),
            "all timestamps should be equal when intra_jitter=0 and batch_jitter=0"
        );
    }

    #[test]
    fn generate_protobuf_message_keeps_timestamps_within_batch_window() {
        let batch_jitter_ns = 1_000_000_000_i64;
        let gen = gen_with_jitter(batch_jitter_ns, 0, 0.0);
        let now_before = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let ts = protobuf_timestamps(gen.generate_protobuf_message(None, None, None, 5).unwrap());
        let now_after = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        for &t in &ts {
            assert!(t <= now_after, "timestamp in future: {} > {}", t, now_after);
            assert!(
                t >= now_before - batch_jitter_ns,
                "timestamp too old: {} < {}",
                t,
                now_before - batch_jitter_ns
            );
        }
    }

    #[test]
    fn generate_protobuf_message_is_non_decreasing_when_overlap_disabled() {
        let gen = gen_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let ts = protobuf_timestamps(gen.generate_protobuf_message(None, None, None, 50).unwrap());
        for i in 1..ts.len() {
            assert!(
                ts[i - 1] <= ts[i],
                "non-monotonic at i={}: {} > {}",
                i,
                ts[i - 1],
                ts[i]
            );
        }
    }
}
