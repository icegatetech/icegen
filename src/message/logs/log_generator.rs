use crate::config::{AttributesCardinalityConfig, TimestampJitterConfig};
use crate::error::{GeneratorError, Result};
use crate::message::fake_data::FakeDataGenerator;
use crate::message::logs::log_attrs;
use crate::message::logs::log_encoder::LogEncoder;
use crate::message::logs::log_plan::{PlannedRecord, PlannedRequest, PlannedShard};
use crate::message::logs::log_time::{self, RecordSlot};
use crate::message::resource_attrs::{ShardResourceAttrs, DEFAULT_SERVICE_NAME};
use crate::message::traces::TraceCorrelation;
use crate::message::types::{MessagePayload, OTLPMessage, OTLPMessageType, ServiceShard, Signal};
use chrono::Utc;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct OTLPLogMessageGenerator {
    source: String,
    attr_cardinality: AttributesCardinalityConfig,
    jitter: TimestampJitterConfig,
    encoder: Arc<dyn LogEncoder>,
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
    /// [`Self::generate_message`] call. Use [`crate::message::logs::LogJsonEncoder`] for HTTP JSON
    /// transport and [`crate::message::logs::LogProtobufEncoder`] for HTTP Protobuf or gRPC
    /// transport.
    pub fn new(
        source: String,
        attr_cardinality: AttributesCardinalityConfig,
        jitter: TimestampJitterConfig,
        encoder: Arc<dyn LogEncoder>,
    ) -> Self {
        Self {
            source,
            attr_cardinality,
            jitter,
            encoder,
        }
    }

    fn build_message(
        &self,
        message: MessagePayload,
        tenant_id: Option<String>,
        project_id: String,
        message_type: OTLPMessageType,
    ) -> OTLPMessage {
        OTLPMessage::new(
            message,
            Signal::Logs,
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
    /// Returns [`GeneratorError::InvalidConfiguration`] if `shards` is empty, any shard has
    /// `num_records == 0`, `resource_attrs.len() != shards.len()`, or `correlations` is `Some` with
    /// a length other than `shards.len()`, holding a shard without traces, or a correlation without
    /// spans.
    #[allow(clippy::result_large_err)]
    pub(crate) fn plan(
        &self,
        shards: &[ServiceShard],
        project_id: &str,
        now_ns: i64,
        correlations: Option<&[Vec<TraceCorrelation>]>,
        resource_attrs: &[ShardResourceAttrs],
    ) -> Result<PlannedRequest> {
        if shards.is_empty() {
            return Err(GeneratorError::InvalidConfiguration(
                "shards must not be empty".to_string(),
            ));
        }
        if let Some(i) = shards.iter().position(|s| s.num_logs == 0) {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "shard at index {i} has num_records=0; every shard must have num_records >= 1"
            )));
        }
        if resource_attrs.len() != shards.len() {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "shard resource attributes ({}) must match shard count ({})",
                resource_attrs.len(),
                shards.len()
            )));
        }
        if let Some(correlations) = correlations {
            if correlations.len() != shards.len() {
                return Err(GeneratorError::InvalidConfiguration(format!(
                    "trace correlations ({}) must match shard count ({})",
                    correlations.len(),
                    shards.len()
                )));
            }
            // Records are distributed over a shard's traces and then over their anchors, so an
            // empty set has nothing to attach to. The trace planner rejects an empty span tree and
            // plans at least one trace per shard upstream; fail explicitly rather than divide by or
            // index into nothing.
            if let Some(i) = correlations.iter().position(|c| c.is_empty()) {
                return Err(GeneratorError::InvalidConfiguration(format!(
                    "shard at index {i} has no traces to correlate log records with"
                )));
            }
            if let Some(i) = correlations
                .iter()
                .position(|c| c.iter().any(|corr| corr.anchors.is_empty()))
            {
                return Err(GeneratorError::InvalidConfiguration(format!(
                    "shard at index {i} carries a trace without spans to correlate log records with"
                )));
            }
        }

        let batch_offset_ns =
            log_time::sample_batch_offset_ns(&self.jitter, &mut rand::thread_rng());

        let planned_shards: Vec<PlannedShard> = shards
            .iter()
            .enumerate()
            .map(|(shard_index, shard)| {
                let mut rng = rand::thread_rng();
                let svc = shard.service_name.as_deref();
                let shard_correlations = correlations.map(|c| c[shard_index].as_slice());
                let slots: Vec<RecordSlot> = match shard_correlations {
                    Some(corrs) => log_time::plan_correlated_slots(
                        &self.jitter,
                        corrs,
                        shard.num_logs,
                        &mut rng,
                    ),
                    // Anchor every uncorrelated shard to the same `now_ns` so the documented
                    // "all shards share one batch window" invariant holds.
                    None => log_time::plan_timestamps_with_offset(
                        &self.jitter,
                        now_ns,
                        batch_offset_ns,
                        shard.num_logs,
                        &mut rng,
                    )
                    .into_iter()
                    .map(|timestamp_ns| RecordSlot {
                        timestamp_ns,
                        trace_id: None,
                        anchor: None,
                    })
                    .collect(),
                };
                // The shard's resource attributes are built (and normalized) once per cycle by the
                // caller, so this signal reports the same pod its correlated trace does.
                let shard_resource_attrs = resource_attrs[shard_index].pairs().to_vec();
                let scope_attrs = log_attrs::generate_scope_attributes_pairs(svc);
                let scope_name_src = svc.unwrap_or(DEFAULT_SERVICE_NAME);
                let scope_name = format!("io.trihub.{}", scope_name_src.replace('-', "."));
                let scope_version = format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10));
                let resource_dropped_attributes_count = rng.gen_range(0..4);
                let scope_dropped_attributes_count = rng.gen_range(0..3);

                let records: Vec<PlannedRecord> = slots
                    .into_iter()
                    .map(|slot| {
                        let mut rng = rand::thread_rng();
                        let (sev_num, sev_text) = FakeDataGenerator::generate_severity();
                        let body = log_attrs::generate_log_body(&sev_text, svc);
                        // Correlated records adopt the id of the trace they were split into and the
                        // id of the span they were planned into; the trace flags are encoded as 0,
                        // so use 0 here instead of a random log flags byte.
                        let (trace_id, span_id, flags) = match (slot.trace_id, slot.anchor) {
                            (Some(trace_id), Some(anchor)) => (trace_id, anchor.span_id, 0),
                            _ => (
                                FakeDataGenerator::generate_trace_id(),
                                FakeDataGenerator::generate_span_id(),
                                rng.gen_range(0..256),
                            ),
                        };
                        let timestamp_ns = slot.timestamp_ns;
                        let request_id = FakeDataGenerator::generate_uuid();
                        let thread_id = FakeDataGenerator::generate_thread_id();
                        let attributes = log_attrs::generate_log_attributes_pairs(
                            &self.attr_cardinality,
                            &request_id,
                            &thread_id,
                        );
                        PlannedRecord {
                            timestamp_ns,
                            severity_number: sev_num as i32,
                            severity_text: sev_text,
                            body,
                            trace_id,
                            span_id,
                            flags,
                            attributes,
                        }
                    })
                    .collect();

                PlannedShard {
                    resource_attrs: shard_resource_attrs,
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
            project_id: project_id.to_string(),
            shards: planned_shards,
            message_type: OTLPMessageType::Valid,
        })
    }

    /// Encode a planned log request into a wire-format [`OTLPMessage`] tagged [`Signal::Logs`].
    ///
    /// # Errors
    ///
    /// Returns [`GeneratorError::ProtobufEncodeError`] if the configured encoder fails to serialize
    /// (only possible with [`crate::message::logs::LogProtobufEncoder`]).
    #[allow(clippy::result_large_err)]
    pub(crate) fn encode_message(
        &self,
        planned: &PlannedRequest,
        tenant_id: Option<String>,
    ) -> Result<OTLPMessage> {
        let payload = self.encoder.encode(planned)?;
        Ok(self.build_message(
            payload,
            tenant_id,
            planned.project_id.clone(),
            OTLPMessageType::Valid,
        ))
    }

    /// Generate a multi-shard OTLP payload with one `ResourceLogs` entry per shard.
    ///
    /// Self-samples `project_id` and `now_ns` and plans without trace correlation; used by the
    /// logs-only callers and unit tests. The multi-signal factory instead calls the crate-internal
    /// `plan` (optionally with correlations) followed by `encode_message`.
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
    /// serialize (only possible with [`crate::message::logs::LogProtobufEncoder`]).
    #[allow(clippy::result_large_err)]
    pub fn generate_message(
        &self,
        tenant_id: Option<String>,
        cloud_account_id: Option<String>,
        shards: Vec<ServiceShard>,
    ) -> Result<OTLPMessage> {
        let project_id = FakeDataGenerator::generate_project_id();
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        // Logs-only path: apply this generator's cardinality to the shard attributes, matching what
        // the multi-signal factory does when logs are among the signals.
        let resource_attrs = ShardResourceAttrs::for_shards(
            &project_id,
            cloud_account_id.as_deref(),
            &shards,
            &self.source,
            Some(&self.attr_cardinality),
        );
        let planned = self.plan(&shards, &project_id, now_ns, None, &resource_attrs)?;
        self.encode_message(&planned, tenant_id)
    }

    // TODO: align with LogEncoder abstraction. Currently hardcoded to emit JSON/malformed-JSON
    // regardless of the configured encoder. A protobuf-mode invalid-message variant (malformed
    // protobuf, truncated message, invalid field tags) should live as a separate encoder-specific
    // "invalid" path once we decide the contract with the receiver tests.
    #[allow(clippy::result_large_err)]
    pub fn generate_invalid_message(&self, tenant_id: Option<String>) -> Result<OTLPMessage> {
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
                    OTLPMessageType::InvalidJson,
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
                    OTLPMessageType::InvalidJson,
                ))
            }
            "null_resource_logs" => {
                let invalid_message = json!({"resourceLogs": null});
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPMessageType::InvalidJson,
                ))
            }
            "invalid_resource_logs_type" => {
                let invalid_message = json!({"resourceLogs": "not-an-array"});
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPMessageType::InvalidJson,
                ))
            }
            "malformed_json" => Ok(self.build_message(
                MessagePayload::MalformedJson(r#"{"resourceLogs": [ invalid json"#.to_string()),
                tenant_id,
                project_id,
                OTLPMessageType::InvalidMalformedJson,
            )),
            _ => {
                let invalid_message = json!({"resourceLogs": []});
                Ok(self.build_message(
                    MessagePayload::Json(invalid_message),
                    tenant_id,
                    project_id,
                    OTLPMessageType::InvalidJson,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TimestampJitterConfig;
    use crate::message::logs::log_encoder::{LogJsonEncoder, LogProtobufEncoder};
    use crate::message::traces::SpanAnchor;
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
            AttributesCardinalityConfig::default(),
            TimestampJitterConfig {
                across_batch_timestamp_jitter_ns: batch_jitter_ns,
                intra_batch_timestamp_jitter_ns: intra_batch_jitter_ns,
                intra_batch_overlap_probability,
            },
            Arc::new(LogJsonEncoder),
        )
    }

    fn gen_protobuf_with_jitter(
        batch_jitter_ns: i64,
        intra_batch_jitter_ns: i64,
        intra_batch_overlap_probability: f32,
    ) -> OTLPLogMessageGenerator {
        OTLPLogMessageGenerator::new(
            "test".to_string(),
            AttributesCardinalityConfig::default(),
            TimestampJitterConfig {
                across_batch_timestamp_jitter_ns: batch_jitter_ns,
                intra_batch_timestamp_jitter_ns: intra_batch_jitter_ns,
                intra_batch_overlap_probability,
            },
            Arc::new(LogProtobufEncoder),
        )
    }

    fn single_shard(service_name: Option<&str>, num_records: usize) -> Vec<ServiceShard> {
        vec![ServiceShard {
            service_name: service_name.map(ToString::to_string),
            num_logs: num_records,
            num_traces: 1,
        }]
    }

    fn json_timestamps(message: OTLPMessage) -> Vec<i64> {
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

    fn json_timestamps_for_shard(message: &OTLPMessage, shard_index: usize) -> Vec<i64> {
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

    fn protobuf_timestamps(message: OTLPMessage) -> Vec<i64> {
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
                num_logs: 2,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 3,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-c".to_string()),
                num_logs: 4,
                num_traces: 1,
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
                num_logs: 1,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 1,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-c".to_string()),
                num_logs: 1,
                num_traces: 1,
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
                num_logs: 1,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 1,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-c".to_string()),
                num_logs: 1,
                num_traces: 1,
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
                num_logs: 10,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 10,
                num_traces: 1,
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
        let intra_jitter_ns = 5_000_000_i64;
        // A shard's plan starts at `now - batch_offset - total_span`, where `total_span` is the sum
        // of the per-record forward steps (each `< intra_jitter_ns`). The oldest timestamp is
        // therefore `batch_jitter_ns + records * intra_jitter_ns` behind `now`, not just
        // `batch_jitter_ns` — leaving the span out makes this assertion fail whenever the sampled
        // offset lands near its maximum.
        let records_per_shard = 5_i64;
        let oldest_allowed_offset_ns = batch_jitter_ns + records_per_shard * intra_jitter_ns;
        let gen = gen_json_with_jitter(batch_jitter_ns, intra_jitter_ns, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 5,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 5,
                num_traces: 1,
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
                    t >= now_before - oldest_allowed_offset_ns,
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
                num_logs: 3,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 4,
                num_traces: 1,
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

    /// Trace correlation over `count` back-to-back 1 ms spans starting at `start_ns`, mimicking the
    /// layout the trace planner produces (index 0 is the root).
    fn correlation_with(start_ns: i64, count: usize) -> TraceCorrelation {
        const SPAN_LEN_NS: i64 = 1_000_000;
        let anchors: Vec<SpanAnchor> = (0..count)
            .map(|i| SpanAnchor {
                span_id: [i as u8 + 1; 8],
                start_ns: start_ns + i as i64 * SPAN_LEN_NS,
                end_ns: start_ns + (i as i64 + 1) * SPAN_LEN_NS,
            })
            .collect();
        TraceCorrelation {
            trace_id: [7u8; 16],
            anchors: anchors.into(),
        }
    }

    fn correlated_plan(
        gen: &OTLPLogMessageGenerator,
        correlation: &TraceCorrelation,
        num_records: usize,
    ) -> PlannedRequest {
        let shards = single_shard(Some("svc-a"), num_records);
        let resource_attrs = ShardResourceAttrs::for_shards(
            "proj",
            None,
            &shards,
            "test",
            Some(&AttributesCardinalityConfig::default()),
        );
        let correlations = vec![vec![correlation.clone()]];
        gen.plan(
            &shards,
            "proj",
            1_700_000_000_000_000_000,
            Some(&correlations),
            &resource_attrs,
        )
        .unwrap()
    }

    #[test]
    fn every_anchor_gets_records_when_records_outnumber_spans() {
        let gen = gen_json_with_jitter(1_000_000_000, 5_000_000, 0.05);
        let correlation = correlation_with(1_700_000_000_000_000_000, 4);
        let planned = correlated_plan(&gen, &correlation, 20);

        let used: std::collections::HashSet<[u8; 8]> = planned.shards[0]
            .records
            .iter()
            .map(|record| record.span_id)
            .collect();
        assert_eq!(used.len(), 4, "records must reach every span of the trace");
    }

    #[test]
    fn correlated_records_land_inside_their_own_span_window() {
        let gen = gen_json_with_jitter(1_000_000_000, 5_000_000, 0.05);
        let correlation = correlation_with(1_700_000_000_000_000_000, 4);
        let planned = correlated_plan(&gen, &correlation, 37);

        for record in &planned.shards[0].records {
            let anchor = correlation
                .anchors
                .iter()
                .find(|anchor| anchor.span_id == record.span_id)
                .expect("record points at a span of the trace");
            assert_eq!(record.trace_id, correlation.trace_id);
            assert!(
                record.timestamp_ns >= anchor.start_ns && record.timestamp_ns <= anchor.end_ns,
                "record ts {} outside its span window [{},{}]",
                record.timestamp_ns,
                anchor.start_ns,
                anchor.end_ns
            );
        }
    }

    #[test]
    fn single_span_trace_pins_every_record_to_the_root() {
        let gen = gen_json_with_jitter(1_000_000_000, 5_000_000, 0.05);
        let correlation = correlation_with(1_700_000_000_000_000_000, 1);
        let planned = correlated_plan(&gen, &correlation, 10);

        // Layout contract: anchor 0 is the root, and a one-span trace has nothing else.
        let root = correlation.anchors[0];
        for record in &planned.shards[0].records {
            assert_eq!(record.span_id, root.span_id);
            assert!(record.timestamp_ns >= root.start_ns && record.timestamp_ns <= root.end_ns);
        }
    }

    /// The window each record must stay inside, looked up by the span it points at.
    fn window_of(correlation: &TraceCorrelation, span_id: [u8; 8]) -> (i64, i64) {
        let anchor = correlation
            .anchors
            .iter()
            .find(|anchor| anchor.span_id == span_id)
            .expect("record points at a span of the trace");
        (anchor.start_ns, anchor.end_ns)
    }

    #[test]
    fn correlated_records_are_non_decreasing_when_overlap_disabled() {
        // Merging per-span timestamps is time-ordered, and with overlap off nothing reintroduces
        // inversions — the same contract the log-only path has.
        let gen = gen_json_with_jitter(1_000_000_000, 50_000, 0.0);
        let correlation = correlation_with(1_700_000_000_000_000_000, 4);
        let planned = correlated_plan(&gen, &correlation, 100);

        let ts: Vec<i64> = planned.shards[0]
            .records
            .iter()
            .map(|record| record.timestamp_ns)
            .collect();
        for i in 1..ts.len() {
            assert!(
                ts[i - 1] <= ts[i],
                "non-monotonic at i={i}: {} > {}",
                ts[i - 1],
                ts[i]
            );
        }
    }

    /// Correlation must not silently disable `RECORD_INTRA_BATCH_OVERLAP_PROBABILITY`: sorting the
    /// merged slots by time erases the per-span inversions, so the nudge is re-applied afterwards.
    #[test]
    fn correlated_records_still_go_out_of_order_when_overlap_enabled() {
        // Isolate the overlap knob: the across-batch offset is capped by the span window, so a
        // large one would push most records onto the window start, where they collapse to one
        // timestamp and no nudge can reorder them. Intra jitter stays well inside the 1 ms window.
        let gen = gen_json_with_jitter(0, 20_000, 1.0);
        let correlation = correlation_with(1_700_000_000_000_000_000, 4);
        let planned = correlated_plan(&gen, &correlation, 40);

        let records = &planned.shards[0].records;
        let inversions = records
            .windows(2)
            .filter(|pair| pair[1].timestamp_ns < pair[0].timestamp_ns)
            .count();
        assert!(
            inversions > 0,
            "overlap_probability=1.0 must still produce out-of-order records when correlated"
        );
        // The nudge stays inside the span each record points at.
        for record in records {
            let (start_ns, end_ns) = window_of(&correlation, record.span_id);
            assert!(
                record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns,
                "nudged record ts {} outside its span window [{start_ns},{end_ns}]",
                record.timestamp_ns
            );
        }
    }

    /// A one-span trace must follow the same route as a big tree: same knobs, same emitted shape.
    #[test]
    fn single_span_trace_also_honours_overlap() {
        // Same isolation as the multi-span case; here all 40 records share the single 1 ms window.
        let gen = gen_json_with_jitter(0, 20_000, 1.0);
        let correlation = correlation_with(1_700_000_000_000_000_000, 1);
        let planned = correlated_plan(&gen, &correlation, 40);

        let records = &planned.shards[0].records;
        let inversions = records
            .windows(2)
            .filter(|pair| pair[1].timestamp_ns < pair[0].timestamp_ns)
            .count();
        assert!(
            inversions > 0,
            "a single-span trace must honour overlap_probability like any other"
        );
        let root = correlation.anchors[0];
        for record in records {
            assert!(record.timestamp_ns >= root.start_ns && record.timestamp_ns <= root.end_ns);
        }
    }

    #[test]
    fn correlation_without_spans_is_rejected() {
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let shards = single_shard(Some("svc-a"), 2);
        let resource_attrs = ShardResourceAttrs::for_shards("proj", None, &shards, "test", None);
        let correlation = TraceCorrelation {
            trace_id: [7u8; 16],
            anchors: Vec::new().into(),
        };
        let result = gen.plan(
            &shards,
            "proj",
            1_700_000_000_000_000_000,
            Some(&[vec![correlation]]),
            &resource_attrs,
        );
        assert!(
            matches!(
                result,
                Err(crate::error::GeneratorError::InvalidConfiguration(_))
            ),
            "expected InvalidConfiguration for a correlation carrying no spans"
        );
    }

    #[test]
    fn shard_without_traces_is_rejected() {
        // A shard with no correlation has nothing to split its records over; the trace planner
        // guarantees at least one trace per shard, so this must fail loudly rather than divide by
        // zero.
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let shards = single_shard(Some("svc-a"), 2);
        let resource_attrs = ShardResourceAttrs::for_shards("proj", None, &shards, "test", None);
        let result = gen.plan(
            &shards,
            "proj",
            1_700_000_000_000_000_000,
            Some(&[Vec::new()]),
            &resource_attrs,
        );
        assert!(
            matches!(
                result,
                Err(crate::error::GeneratorError::InvalidConfiguration(_))
            ),
            "expected InvalidConfiguration for a shard carrying no traces"
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
        let shards = vec![ServiceShard {
            service_name: None,
            num_logs: 0,
            num_traces: 1,
        }];
        let result = gen.generate_message(None, None, shards);
        assert!(
            matches!(
                result,
                Err(crate::error::GeneratorError::InvalidConfiguration(_))
            ),
            "expected InvalidConfiguration for shard with num_records=0"
        );
    }

    #[test]
    fn generate_message_returns_error_when_second_shard_has_zero_records() {
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 2,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 0,
                num_traces: 1,
            },
        ];
        let result = gen.generate_message(None, None, shards);
        assert!(
            matches!(
                result,
                Err(crate::error::GeneratorError::InvalidConfiguration(_))
            ),
            "expected InvalidConfiguration when any shard has num_records=0"
        );
    }

    #[test]
    fn generate_message_returns_error_when_first_shard_has_zero_records() {
        // Guards against a future short-circuit refactor that only validates trailing shards;
        // the position-agnostic check in plan_shards must catch zero-records at index 0.
        let gen = gen_json_with_jitter(0, 0, 0.0);
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 0,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 2,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-c".to_string()),
                num_logs: 3,
                num_traces: 1,
            },
        ];
        let result = gen.generate_message(None, None, shards);
        assert!(
            matches!(
                result,
                Err(crate::error::GeneratorError::InvalidConfiguration(_))
            ),
            "expected InvalidConfiguration when first shard has num_records=0"
        );
    }
}
