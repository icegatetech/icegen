use crate::config::{LabelCardinalityConfig, TimestampJitterConfig};
use crate::error::{GeneratorError, Result};
use crate::message::encoder::OtlpEncoder;
use crate::message::fake_data::FakeDataGenerator;
use crate::message::plan::{PlannedRecord, PlannedRequest, PlannedShard};
use crate::message::types::{MessagePayload, OTLPLogMessage, OTLPLogMessageType};
use chrono::Utc;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::json;
use std::sync::Arc;

const DEFAULT_SERVICE_NAME: &str = "icegen";

/// One shard of a multi-service OTLP request, mapping to a single `ResourceLogs` entry.
#[derive(Debug, Clone)]
pub struct ServiceShard {
    pub service_name: Option<String>,
    pub num_records: usize,
}

#[derive(Clone)]
pub struct OTLPLogMessageGenerator {
    source: String,
    label_cardinality: LabelCardinalityConfig,
    jitter: TimestampJitterConfig,
    encoder: Arc<dyn OtlpEncoder>,
}

impl OTLPLogMessageGenerator {
    /// Create a generator with explicit cardinality, timestamp jitter settings, and encoder.
    ///
    /// `jitter.across_batch_timestamp_jitter_ns` shifts the whole batch backwards in time,
    /// while `jitter.intra_batch_timestamp_jitter_ns` controls spacing between neighbouring
    /// records inside that batch. `jitter.intra_batch_overlap_probability` controls how often
    /// the emitted timestamp for a record is moved backwards relative to the previous record.
    ///
    /// The `encoder` is chosen once at construction time and determines the wire format of every
    /// [`Self::generate_message`] call. Use [`crate::message::JsonEncoder`] for HTTP JSON
    /// transport and [`crate::message::ProtobufEncoder`] for HTTP Protobuf or gRPC transport.
    pub fn new(
        source: String,
        label_cardinality: LabelCardinalityConfig,
        jitter: TimestampJitterConfig,
        encoder: Arc<dyn OtlpEncoder>,
    ) -> Self {
        Self {
            source,
            label_cardinality,
            jitter,
            encoder,
        }
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
            FakeDataGenerator::generate_k8s_pod_name(service_name.unwrap_or(DEFAULT_SERVICE_NAME)),
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

    fn sample_batch_offset_ns(&self) -> i64 {
        if self.jitter.across_batch_timestamp_jitter_ns > 0 {
            rand::thread_rng().gen_range(0..self.jitter.across_batch_timestamp_jitter_ns)
        } else {
            0
        }
    }

    fn plan_timestamps_with_offset(
        &self,
        request_now_ns: i64,
        batch_offset_ns: i64,
        num_records: usize,
    ) -> Vec<i64> {
        if num_records == 0 {
            return vec![];
        }

        let mut rng = rand::thread_rng();

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

        let mut prev_ns = request_now_ns - batch_offset_ns - total_span_ns;
        for (i, step_slot) in result.iter_mut().enumerate() {
            let step = *step_slot;
            let candidate = prev_ns + step;
            *step_slot = if i > 0 && intra > 0 && rng.gen::<f32>() < overlap_prob {
                prev_ns - rng.gen_range(0..intra)
            } else {
                candidate
            };
            prev_ns = candidate;
        }

        result
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

    /// Build a format-neutral plan for a set of shards, shared across all encoders.
    ///
    /// # Errors
    ///
    /// Returns [`GeneratorError::InvalidConfiguration`] if `shards` is empty or any shard has
    /// `num_records == 0`.
    #[allow(clippy::result_large_err)]
    fn plan_shards(
        &self,
        cloud_account_id: Option<&str>,
        shards: &[ServiceShard],
    ) -> Result<PlannedRequest> {
        if shards.is_empty() {
            return Err(GeneratorError::InvalidConfiguration(
                "shards must not be empty".to_string(),
            ));
        }
        if let Some(i) = shards.iter().position(|s| s.num_records == 0) {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "shard at index {i} has num_records=0; every shard must have num_records >= 1"
            )));
        }

        let project_id = FakeDataGenerator::generate_project_id();
        let batch_offset_ns = self.sample_batch_offset_ns();
        // Anchor every shard in this request to the same `now` so the documented
        // "all shards share one batch window" invariant holds; otherwise each shard
        // would re-sample `Utc::now()` and drift forward by the time spent planning
        // the previous shards.
        let request_now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let planned_shards: Vec<PlannedShard> = shards
            .iter()
            .map(|shard| {
                let mut rng = rand::thread_rng();
                let svc = shard.service_name.as_deref();
                let ts_list = self.plan_timestamps_with_offset(
                    request_now_ns,
                    batch_offset_ns,
                    shard.num_records,
                );
                let resource_attrs =
                    self.generate_resource_attributes_pairs(&project_id, cloud_account_id, svc);
                let scope_attrs = self.generate_scope_attributes_pairs(svc);
                let scope_name_src = svc.unwrap_or(DEFAULT_SERVICE_NAME);
                let scope_name = format!("io.trihub.{}", scope_name_src.replace('-', "."));
                let scope_version = format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10));
                let resource_dropped_attributes_count = rng.gen_range(0..4);
                let scope_dropped_attributes_count = rng.gen_range(0..3);

                let records: Vec<PlannedRecord> = ts_list
                    .into_iter()
                    .map(|timestamp_ns| {
                        let mut rng = rand::thread_rng();
                        let (sev_num, sev_text) = FakeDataGenerator::generate_severity();
                        let body = Self::generate_log_body(&sev_text, svc);
                        let trace_id = FakeDataGenerator::generate_trace_id();
                        let span_id = FakeDataGenerator::generate_span_id();
                        let request_id = FakeDataGenerator::generate_uuid();
                        let thread_id = FakeDataGenerator::generate_thread_id();
                        let attributes =
                            self.generate_log_attributes_pairs(&request_id, &thread_id);
                        PlannedRecord {
                            timestamp_ns,
                            severity_number: sev_num as i32,
                            severity_text: sev_text,
                            body,
                            trace_id,
                            span_id,
                            flags: rng.gen_range(0..256),
                            attributes,
                        }
                    })
                    .collect();

                PlannedShard {
                    resource_attrs,
                    resource_dropped_attributes_count,
                    scope_name,
                    scope_version,
                    scope_attrs,
                    scope_dropped_attributes_count,
                    records,
                }
            })
            .collect();

        Ok(PlannedRequest {
            project_id,
            shards: planned_shards,
            message_type: OTLPLogMessageType::Valid,
        })
    }

    /// Generate a multi-shard OTLP payload with one `ResourceLogs` entry per shard.
    ///
    /// The wire format is determined by the encoder provided at construction. All shards share
    /// a single anchor time (`Utc::now()` sampled once per request) and a single
    /// `across_batch_timestamp_jitter_ns` offset, so they fall inside one batch window regardless
    /// of how long planning takes. Each shard's timestamps are then planned independently using
    /// `intra_batch_timestamp_jitter_ns`; between shards, monotonicity is not guaranteed.
    ///
    /// # Errors
    ///
    /// Returns [`GeneratorError::InvalidConfiguration`] if `shards` is empty.
    /// Returns [`GeneratorError::ProtobufEncodeError`] if the configured encoder fails to
    /// serialize (only possible with [`crate::message::ProtobufEncoder`]).
    #[allow(clippy::result_large_err)]
    pub fn generate_message(
        &self,
        tenant_id: Option<String>,
        cloud_account_id: Option<String>,
        shards: Vec<ServiceShard>,
    ) -> Result<OTLPLogMessage> {
        let planned = self.plan_shards(cloud_account_id.as_deref(), &shards)?;
        let payload = self.encoder.encode(&planned)?;
        Ok(self.build_message(
            payload,
            tenant_id,
            planned.project_id,
            OTLPLogMessageType::Valid,
        ))
    }

    // TODO: align with OtlpEncoder abstraction. Currently hardcoded to emit JSON/malformed-JSON
    // regardless of the configured encoder. A protobuf-mode invalid-message variant (malformed
    // protobuf, truncated message, invalid field tags) should live as a separate encoder-specific
    // "invalid" path once we decide the contract with the receiver tests.
    #[allow(clippy::result_large_err)]
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
    use crate::message::encoder::{JsonEncoder, ProtobufEncoder};
    use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use crate::pb::opentelemetry::proto::common::v1::{any_value, KeyValue};
    use chrono::Utc;
    use prost::Message;
    use std::sync::Arc;

    fn pb_attr_str<'a>(attrs: &'a [KeyValue], key: &str) -> Option<&'a str> {
        attrs.iter().find_map(|kv| {
            if kv.key != key {
                return None;
            }
            match kv.value.as_ref()?.value.as_ref()? {
                any_value::Value::StringValue(v) => Some(v.as_str()),
                _ => None,
            }
        })
    }

    fn gen_json_with_jitter(
        batch_jitter_ns: i64,
        intra_batch_jitter_ns: i64,
        intra_batch_overlap_probability: f32,
    ) -> OTLPLogMessageGenerator {
        OTLPLogMessageGenerator::new(
            "test".to_string(),
            LabelCardinalityConfig::default(),
            TimestampJitterConfig {
                across_batch_timestamp_jitter_ns: batch_jitter_ns,
                intra_batch_timestamp_jitter_ns: intra_batch_jitter_ns,
                intra_batch_overlap_probability,
            },
            Arc::new(JsonEncoder),
        )
    }

    fn gen_protobuf_with_jitter(
        batch_jitter_ns: i64,
        intra_batch_jitter_ns: i64,
        intra_batch_overlap_probability: f32,
    ) -> OTLPLogMessageGenerator {
        OTLPLogMessageGenerator::new(
            "test".to_string(),
            LabelCardinalityConfig::default(),
            TimestampJitterConfig {
                across_batch_timestamp_jitter_ns: batch_jitter_ns,
                intra_batch_timestamp_jitter_ns: intra_batch_jitter_ns,
                intra_batch_overlap_probability,
            },
            Arc::new(ProtobufEncoder),
        )
    }

    fn single_shard(service_name: Option<&str>, num_records: usize) -> Vec<ServiceShard> {
        vec![ServiceShard {
            service_name: service_name.map(ToString::to_string),
            num_records,
        }]
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

    fn json_timestamps_for_shard(message: &OTLPLogMessage, shard_index: usize) -> Vec<i64> {
        let MessagePayload::Json(json) = &message.message else {
            panic!("Expected JSON payload");
        };

        json["resourceLogs"][shard_index]["scopeLogs"][0]["logRecords"]
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
    fn generate_message_keeps_single_timestamp_within_batch_window() {
        let batch_jitter_ns = 2_000_000_000_i64;
        let gen = gen_json_with_jitter(batch_jitter_ns, 5_000_000, 0.25);
        let now_before = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let timestamps = json_timestamps(
            gen.generate_message(None, None, single_shard(Some("svc"), 1))
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
    fn generate_message_json_is_non_decreasing_when_overlap_disabled() {
        let gen = gen_json_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let ts = json_timestamps(
            gen.generate_message(None, None, single_shard(None, 50))
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
    fn generate_message_json_collapses_timestamps_when_intra_jitter_zero() {
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let ts = json_timestamps(
            gen.generate_message(None, None, single_shard(None, 10))
                .unwrap(),
        );
        assert!(
            ts.windows(2).all(|w| w[0] == w[1]),
            "all timestamps should be equal when intra_jitter=0 and batch_jitter=0"
        );
    }

    #[test]
    fn generate_message_protobuf_keeps_timestamps_within_batch_window() {
        let batch_jitter_ns = 1_000_000_000_i64;
        let gen = gen_protobuf_with_jitter(batch_jitter_ns, 0, 0.0);
        let now_before = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let ts = protobuf_timestamps(
            gen.generate_message(None, None, single_shard(None, 5))
                .unwrap(),
        );
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
    fn generate_message_protobuf_is_non_decreasing_when_overlap_disabled() {
        let gen = gen_protobuf_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let ts = protobuf_timestamps(
            gen.generate_message(None, None, single_shard(None, 50))
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
    fn generate_message_json_with_multiple_shards_produces_one_resource_logs_per_shard() {
        let gen = gen_json_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_records: 2,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_records: 3,
            },
            ServiceShard {
                service_name: Some("svc-c".to_string()),
                num_records: 4,
            },
        ];
        let message = gen.generate_message(None, None, shards).unwrap();
        let MessagePayload::Json(json) = &message.message else {
            panic!("Expected JSON");
        };

        let resource_logs = json["resourceLogs"].as_array().unwrap();
        assert_eq!(resource_logs.len(), 3, "one ResourceLogs per shard");

        let counts: Vec<usize> = resource_logs
            .iter()
            .map(|rl| rl["scopeLogs"][0]["logRecords"].as_array().unwrap().len())
            .collect();
        assert_eq!(counts, vec![2, 3, 4]);
        assert_eq!(counts.iter().sum::<usize>(), 9);

        let service_names: Vec<&str> = resource_logs
            .iter()
            .map(|rl| {
                rl["resource"]["attributes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find_map(|a| {
                        (a["key"].as_str() == Some("service.name"))
                            .then(|| a["value"]["stringValue"].as_str().unwrap())
                    })
                    .unwrap()
            })
            .collect();
        assert_eq!(service_names, vec!["svc-a", "svc-b", "svc-c"]);
    }

    #[test]
    fn generate_message_json_shares_project_id_and_cloud_account_across_shards() {
        let gen = gen_json_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_records: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_records: 1,
            },
            ServiceShard {
                service_name: Some("svc-c".to_string()),
                num_records: 1,
            },
        ];
        let message = gen
            .generate_message(
                Some("tenant1".to_string()),
                Some("tenant1-acc-01".to_string()),
                shards,
            )
            .unwrap();

        let MessagePayload::Json(json) = &message.message else {
            panic!("Expected JSON");
        };

        let resource_logs = json["resourceLogs"].as_array().unwrap();
        let project_ids: Vec<&str> = resource_logs
            .iter()
            .map(|rl| {
                rl["resource"]["attributes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find_map(|a| {
                        (a["key"].as_str() == Some("project_id"))
                            .then(|| a["value"]["stringValue"].as_str().unwrap())
                    })
                    .unwrap()
            })
            .collect();
        assert_eq!(project_ids[0], project_ids[1]);
        assert_eq!(project_ids[1], project_ids[2]);
        assert_eq!(project_ids[0], message.project_id);

        let cloud_account_ids: Vec<&str> = resource_logs
            .iter()
            .map(|rl| {
                rl["resource"]["attributes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find_map(|a| {
                        (a["key"].as_str() == Some("cloud.account.id"))
                            .then(|| a["value"]["stringValue"].as_str().unwrap())
                    })
                    .unwrap()
            })
            .collect();
        assert!(cloud_account_ids.iter().all(|&id| id == "tenant1-acc-01"));
    }

    #[test]
    fn generate_message_protobuf_shares_project_id_and_cloud_account_across_shards() {
        let gen = gen_protobuf_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_records: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_records: 1,
            },
            ServiceShard {
                service_name: Some("svc-c".to_string()),
                num_records: 1,
            },
        ];
        let message = gen
            .generate_message(
                Some("tenant1".to_string()),
                Some("tenant1-acc-01".to_string()),
                shards,
            )
            .unwrap();

        let MessagePayload::Protobuf(bytes) = &message.message else {
            panic!("Expected protobuf payload");
        };
        let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();

        let project_ids: Vec<&str> = decoded
            .resource_logs
            .iter()
            .map(|rl| {
                pb_attr_str(&rl.resource.as_ref().unwrap().attributes, "project_id")
                    .expect("project_id missing")
            })
            .collect();
        assert_eq!(project_ids[0], project_ids[1]);
        assert_eq!(project_ids[1], project_ids[2]);
        assert_eq!(project_ids[0], message.project_id);

        let cloud_account_ids: Vec<&str> = decoded
            .resource_logs
            .iter()
            .map(|rl| {
                pb_attr_str(
                    &rl.resource.as_ref().unwrap().attributes,
                    "cloud.account.id",
                )
                .expect("cloud.account.id missing")
            })
            .collect();
        assert!(cloud_account_ids.iter().all(|&id| id == "tenant1-acc-01"));
    }

    #[test]
    fn generate_message_json_is_non_decreasing_within_each_shard() {
        let gen = gen_json_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_records: 10,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_records: 10,
            },
        ];
        let message = gen.generate_message(None, None, shards).unwrap();

        for shard_idx in 0..2 {
            let ts = json_timestamps_for_shard(&message, shard_idx);
            for i in 1..ts.len() {
                assert!(
                    ts[i - 1] <= ts[i],
                    "shard {shard_idx}: non-monotonic at i={i}: {} > {}",
                    ts[i - 1],
                    ts[i]
                );
            }
        }
    }

    #[test]
    fn generate_message_json_keeps_all_timestamps_within_single_batch_window() {
        let batch_jitter_ns = 2_000_000_000_i64;
        let gen = gen_json_with_jitter(batch_jitter_ns, 5_000_000, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_records: 5,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_records: 5,
            },
        ];
        let now_before = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let message = gen.generate_message(None, None, shards).unwrap();
        let now_after = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        for shard_idx in 0..2 {
            let ts = json_timestamps_for_shard(&message, shard_idx);
            for &t in &ts {
                assert!(t <= now_after, "shard {shard_idx}: timestamp in future");
                assert!(
                    t >= now_before - batch_jitter_ns,
                    "shard {shard_idx}: timestamp too old"
                );
            }
        }
    }

    #[test]
    fn generate_message_protobuf_shards_produce_correct_resource_logs_count() {
        let gen = gen_protobuf_with_jitter(1_000_000_000, 5_000_000, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_records: 3,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_records: 4,
            },
        ];
        let message = gen.generate_message(None, None, shards).unwrap();

        let MessagePayload::Protobuf(bytes) = &message.message else {
            panic!("Expected protobuf");
        };
        let decoded = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.resource_logs.len(), 2);
        assert_eq!(decoded.resource_logs[0].scope_logs[0].log_records.len(), 3);
        assert_eq!(decoded.resource_logs[1].scope_logs[0].log_records.len(), 4);
        assert_eq!(
            pb_attr_str(
                &decoded.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes,
                "service.name"
            ),
            Some("svc-a"),
        );
        assert_eq!(
            pb_attr_str(
                &decoded.resource_logs[1]
                    .resource
                    .as_ref()
                    .unwrap()
                    .attributes,
                "service.name"
            ),
            Some("svc-b"),
        );
    }

    #[test]
    fn generate_message_returns_error_for_empty_shards_json() {
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let result = gen.generate_message(None, None, vec![]);
        assert!(
            matches!(
                result,
                Err(crate::error::GeneratorError::InvalidConfiguration(_))
            ),
            "expected InvalidConfiguration for empty shards"
        );
    }

    #[test]
    fn generate_message_returns_error_for_zero_num_records_shard() {
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let shards = vec![ServiceShard { service_name: None, num_records: 0 }];
        let result = gen.generate_message(None, None, shards);
        assert!(
            matches!(result, Err(crate::error::GeneratorError::InvalidConfiguration(_))),
            "expected InvalidConfiguration for shard with num_records=0"
        );
    }

    #[test]
    fn generate_message_returns_error_when_second_shard_has_zero_records() {
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let shards = vec![
            ServiceShard { service_name: Some("svc-a".to_string()), num_records: 2 },
            ServiceShard { service_name: Some("svc-b".to_string()), num_records: 0 },
        ];
        let result = gen.generate_message(None, None, shards);
        assert!(
            matches!(result, Err(crate::error::GeneratorError::InvalidConfiguration(_))),
            "expected InvalidConfiguration when any shard has num_records=0"
        );
    }

    #[test]
    fn generate_message_returns_error_when_first_shard_has_zero_records() {
        // Guards against a future short-circuit refactor that only validates trailing shards;
        // the position-agnostic check in plan_shards must catch zero-records at index 0.
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let shards = vec![
            ServiceShard { service_name: Some("svc-a".to_string()), num_records: 0 },
            ServiceShard { service_name: Some("svc-b".to_string()), num_records: 2 },
            ServiceShard { service_name: Some("svc-c".to_string()), num_records: 3 },
        ];
        let result = gen.generate_message(None, None, shards);
        assert!(
            matches!(result, Err(crate::error::GeneratorError::InvalidConfiguration(_))),
            "expected InvalidConfiguration when first shard has num_records=0"
        );
    }
}
