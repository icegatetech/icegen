//! Multi-signal message factory. The orchestrator (`OtelGenerator`) selects tenant/cloud/shards
//! plus a shared `project_id`/`now_ns` for one generation cycle and delegates message construction
//! to the factory, which builds one [`OTLPMessage`] per configured signal.
//!
//! Traces are planned before logs so a shard's log records can be spread across its traces' spans,
//! adopting their `trace_id` and their own span's `span_id` (see
//! [`crate::message::traces::TraceCorrelation`]). All messages are fully built
//! before any is handed to the transport, so a planning/encoding failure never yields a partially
//! sent cycle.

use crate::config::{AttributesCardinalityConfig, SignalFactorySpec};
use crate::error::{GeneratorError, Result};
use crate::message::logs::log_generator::OTLPLogMessageGenerator;
use crate::message::logs::{LogEncoder, LogJsonEncoder, LogProtobufEncoder};
use crate::message::resource_attrs::ShardResourceAttrs;
use crate::message::traces::span_profile::LlmSpanProfile;
use crate::message::traces::trace_generator::TraceMessageGenerator;
use crate::message::traces::{
    ConversationCursor, TraceEncoder, TraceJsonEncoder, TraceProtobufEncoder,
};
use crate::message::types::{OTLPMessage, Signal};
use crate::message::ServiceShard;
use std::sync::Arc;

/// Shared context of a single generation cycle, selected once by the orchestrator. Crate-internal:
/// it is an orchestration detail, not part of the public library API.
pub(crate) struct GenContext {
    pub tenant_id: Option<String>,
    pub cloud_account_id: Option<String>,
    /// Shared project id for every signal in this cycle.
    pub project_id: String,
    /// Shared anchor time (Unix nanos) for every signal in this cycle.
    pub now_ns: i64,
    pub shards: Vec<ServiceShard>,
    /// Emit an invalid message. Only ever `true` for logs-only runs (enforced by config).
    pub invalid: bool,
}

/// Builds one [`OTLPMessage`] per configured signal, in `signals` order, for a generation cycle.
///
/// Beyond dispatching to the per-signal generators, the factory owns the one thing they must agree
/// on: each shard's [`ShardResourceAttrs`], built exactly once per cycle and handed to both
/// planners so no two generators describe a shard's pod apart.
pub(crate) struct SignalFactory {
    /// Configured signals, in the order requested on the CLI; drives the order messages are built.
    signals: Vec<Signal>,
    log_generator: Option<OTLPLogMessageGenerator>,
    trace_generator: Option<TraceMessageGenerator>,
    /// `generator.source` value stamped on every signal's resource attributes (shared, so a
    /// correlated log and trace carry the same value).
    source: String,
    /// Cardinality policy applied to the shared resource attributes, or `None` when they must not be
    /// normalized. It is `Some` exactly when logs are among the signals: cardinality is a logs-only
    /// feature, but when logs and traces run together the trace must adopt whatever the log path
    /// bucketed the resource to.
    resource_cardinality: Option<AttributesCardinalityConfig>,
}

impl SignalFactory {
    /// Build a factory from a resolved [`SignalFactorySpec`]: choose the encoders, construct the
    /// per-signal generators, and pair them with the shared `source` and resource cardinality. This
    /// is where the generic OTEL generator's "which signals, encoded how" detail lives — the
    /// orchestrator ([`crate::generator::OtelGenerator`]) stays signal-neutral.
    ///
    /// # Errors
    ///
    /// [`GeneratorError::InvalidConfiguration`] if the spec is internally inconsistent (a configured
    /// signal without its resolved inputs); [`OtelConfig::signal_factory_spec`] never produces such
    /// a spec.
    #[allow(clippy::result_large_err)]
    pub(crate) fn from_spec(spec: SignalFactorySpec) -> Result<Self> {
        let SignalFactorySpec {
            signals,
            source,
            want_protobuf,
            log_cardinality,
            timestamp_jitter,
            llm,
        } = spec;

        let log_generator = if signals.contains(&Signal::Logs) {
            let encoder: Arc<dyn LogEncoder> = if want_protobuf {
                Arc::new(LogProtobufEncoder)
            } else {
                Arc::new(LogJsonEncoder)
            };
            let cardinality = log_cardinality.clone().ok_or_else(|| {
                GeneratorError::InvalidConfiguration(
                    "logs signal requires a cardinality spec".to_string(),
                )
            })?;
            Some(OTLPLogMessageGenerator::new(
                source.clone(),
                cardinality,
                timestamp_jitter,
                encoder,
            ))
        } else {
            None
        };

        let trace_generator = if signals.contains(&Signal::Traces) {
            let encoder: Arc<dyn TraceEncoder> = if want_protobuf {
                Arc::new(TraceProtobufEncoder)
            } else {
                Arc::new(TraceJsonEncoder)
            };
            let llm = llm.ok_or_else(|| {
                GeneratorError::InvalidConfiguration(
                    "traces signal requires an llm profile spec".to_string(),
                )
            })?;
            // TODO(med): for generic traces add GenericSpanProfile and create TRACE_PROFILE env to switch
            let conversations = ConversationCursor::shared();
            Some(TraceMessageGenerator::new(
                source.clone(),
                encoder,
                Arc::new(LlmSpanProfile {
                    max_tool_calls: llm.max_tool_calls,
                    capture_content: llm.capture_content,
                    weights: llm.weights,
                    conversations,
                    budget: llm.budget,
                }),
            ))
        } else {
            None
        };

        // Cardinality is a logs-only feature, so the factory normalizes the shared resource
        // attributes only when logs are configured; a correlated trace then adopts the same
        // bucketed values.
        Ok(Self::new(
            signals,
            log_generator,
            trace_generator,
            source,
            log_cardinality,
        ))
    }

    /// Create a factory for `signals`. A generator must be present for each configured signal; this
    /// pairing (and `resource_cardinality` being `Some` iff logs are configured) is set up by
    /// [`Self::from_spec`].
    pub(crate) fn new(
        signals: Vec<Signal>,
        log_generator: Option<OTLPLogMessageGenerator>,
        trace_generator: Option<TraceMessageGenerator>,
        source: String,
        resource_cardinality: Option<AttributesCardinalityConfig>,
    ) -> Self {
        Self {
            signals,
            log_generator,
            trace_generator,
            source,
            resource_cardinality,
        }
    }

    /// Build one message per configured signal for a single generation cycle.
    ///
    /// # Errors
    ///
    /// Propagates planning/encoding errors from the underlying generators, or
    /// [`GeneratorError::InvalidConfiguration`] if a configured signal has no matching generator.
    #[allow(clippy::result_large_err)]
    pub fn build(&self, ctx: &GenContext) -> Result<Vec<OTLPMessage>> {
        if ctx.invalid {
            // Invalid records are restricted to logs-only runs by config validation; a valid
            // config therefore always has the log generator here.
            let log_generator = self.log_generator.as_ref().ok_or_else(|| {
                GeneratorError::InvalidConfiguration(
                    "invalid records require the logs signal".to_string(),
                )
            })?;
            return Ok(vec![
                log_generator.generate_invalid_message(ctx.tenant_id.clone())?
            ]);
        }

        // Build each shard's resource attributes once, so logs and traces of this cycle describe
        // byte-identical pods: the identity keys plus `host.name`, `k8s.*`, `service.version`, and
        // `deployment.environment`.
        let resource_attrs = ShardResourceAttrs::for_shards(
            &ctx.project_id,
            ctx.cloud_account_id.as_deref(),
            &ctx.shards,
            &self.source,
            self.resource_cardinality.as_ref(),
        );

        // Plan traces first: the log planner needs the per-shard correlations.
        let trace_plan = match &self.trace_generator {
            Some(trace_generator) => Some(trace_generator.plan(
                &ctx.project_id,
                &ctx.shards,
                ctx.now_ns,
                &resource_attrs,
                &mut rand::thread_rng(),
            )?),
            None => None,
        };

        let log_plan = match &self.log_generator {
            Some(log_generator) => {
                let correlations = trace_plan
                    .as_ref()
                    .map(|(_, correlations)| correlations.as_slice());
                Some(log_generator.plan(
                    &ctx.shards,
                    &ctx.project_id,
                    ctx.now_ns,
                    correlations,
                    &resource_attrs,
                )?)
            }
            None => None,
        };

        let mut messages = Vec::with_capacity(self.signals.len());
        for signal in &self.signals {
            match signal {
                Signal::Logs => {
                    let log_generator = self.log_generator.as_ref().ok_or_else(|| {
                        GeneratorError::InvalidConfiguration(
                            "logs signal configured without a log generator".to_string(),
                        )
                    })?;
                    let planned = log_plan
                        .as_ref()
                        .expect("log plan built when logs configured");
                    messages.push(log_generator.encode_message(planned, ctx.tenant_id.clone())?);
                }
                Signal::Traces => {
                    let trace_generator = self.trace_generator.as_ref().ok_or_else(|| {
                        GeneratorError::InvalidConfiguration(
                            "traces signal configured without a trace generator".to_string(),
                        )
                    })?;
                    let (planned, _) = trace_plan
                        .as_ref()
                        .expect("trace plan built when traces configured");
                    messages.push(trace_generator.encode_message(planned, ctx.tenant_id.clone())?);
                }
            }
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::traces::conversation::ConversationCursor;
    use crate::message::traces::span_profile::{LlmSpanProfile, ProfileWeights};
    use crate::message::traces::trace_encoder::TraceJsonEncoder;
    use crate::message::MessagePayload;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    fn trace_generator() -> TraceMessageGenerator {
        TraceMessageGenerator::new(
            "src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(LlmSpanProfile {
                max_tool_calls: 1,
                capture_content: false,
                weights: ProfileWeights::default(),
                conversations: ConversationCursor::shared(),
                budget: None,
            }),
        )
    }

    fn ctx(shards: Vec<ServiceShard>) -> GenContext {
        GenContext {
            tenant_id: Some("t1".to_string()),
            cloud_account_id: None,
            project_id: "proj".to_string(),
            now_ns: 1_700_000_000_000_000_000,
            shards,
            invalid: false,
        }
    }

    fn single_shard() -> Vec<ServiceShard> {
        vec![ServiceShard {
            service_name: Some("svc".to_string()),
            num_logs: 1,
            num_traces: 1,
        }]
    }

    #[test]
    fn traces_only_factory_builds_one_trace_message() {
        // Traces-only: resource attributes are not normalized (cardinality is logs-only), so `None`.
        let factory = SignalFactory::new(
            vec![Signal::Traces],
            None,
            Some(trace_generator()),
            "src".to_string(),
            None,
        );
        let messages = factory.build(&ctx(single_shard())).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].signal, Signal::Traces);
    }

    /// Value of resource attribute `key` on an encoded `ResourceLogs`/`ResourceSpans` group.
    fn json_resource_attr(resource: &serde_json::Value, key: &str) -> Option<String> {
        resource["resource"]["attributes"]
            .as_array()?
            .iter()
            .find_map(|a| {
                (a["key"].as_str() == Some(key))
                    .then(|| a["value"]["stringValue"].as_str().unwrap().to_string())
            })
    }

    /// `(traceId, spanId) -> (start, end)` for every span of one encoded `ResourceSpans`. Keyed by
    /// the pair because a group holds the spans of all its shard's traces.
    fn span_windows(resource_spans: &serde_json::Value) -> HashMap<(String, String), (i64, i64)> {
        resource_spans["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|span| {
                let start: i64 = span["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
                let end: i64 = span["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
                (
                    (
                        span["traceId"].as_str().unwrap().to_string(),
                        span["spanId"].as_str().unwrap().to_string(),
                    ),
                    (start, end),
                )
            })
            .collect()
    }

    #[test]
    fn correlated_logs_and_traces_share_trace_and_span_ids() {
        use crate::config::{AttributesCardinalityConfig, TimestampJitterConfig};
        use crate::message::LogJsonEncoder;

        let log_generator = OTLPLogMessageGenerator::new(
            "src".to_string(),
            AttributesCardinalityConfig::default(),
            TimestampJitterConfig::default(),
            Arc::new(LogJsonEncoder),
        );
        let factory = SignalFactory::new(
            vec![Signal::Logs, Signal::Traces],
            Some(log_generator),
            Some(trace_generator()),
            "src".to_string(),
            Some(AttributesCardinalityConfig::default()),
        );

        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 3,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 2,
                num_traces: 1,
            },
        ];
        let messages = factory.build(&ctx(shards)).unwrap();
        assert_eq!(messages.len(), 2);
        // Output order follows the signals list: logs first, then traces.
        assert_eq!(messages[0].signal, Signal::Logs);
        assert_eq!(messages[1].signal, Signal::Traces);
        assert_eq!(messages[0].project_id, messages[1].project_id);

        let MessagePayload::Json(logs) = &messages[0].message else {
            panic!("expected json logs");
        };
        let MessagePayload::Json(traces) = &messages[1].message else {
            panic!("expected json traces");
        };

        let resource_logs = logs["resourceLogs"].as_array().unwrap();
        let resource_spans = traces["resourceSpans"].as_array().unwrap();
        assert_eq!(resource_logs.len(), resource_spans.len());

        for (rl, rs) in resource_logs.iter().zip(resource_spans.iter()) {
            let windows = span_windows(rs);

            let records = rl["scopeLogs"][0]["logRecords"].as_array().unwrap();
            assert!(!records.is_empty());
            for record in records {
                // Every record points at a span of a trace belonging to *this* shard's group...
                let trace_id = record["traceId"].as_str().unwrap().to_string();
                let span_id = record["spanId"].as_str().unwrap().to_string();
                let (start, end) = windows
                    .get(&(trace_id.clone(), span_id.clone()))
                    .unwrap_or_else(|| panic!("log points at unknown span {trace_id}/{span_id}"));
                // ...and lies inside that span's own window, not merely the root's.
                let ts: i64 = record["timeUnixNano"].as_str().unwrap().parse().unwrap();
                assert!(
                    ts >= *start && ts <= *end,
                    "log ts {ts} outside span {span_id} window [{start},{end}]"
                );
            }
        }
    }

    /// With several traces per shard, a log record must resolve to exactly one pod: its `traceId`
    /// may only appear in the `ResourceSpans` group of its own shard, and that group must describe
    /// the same pod as the record's own `ResourceLogs` — every resource attribute, not just the
    /// identity keys. This is the invariant `README.md` states for a shard.
    #[test]
    fn correlated_log_and_its_trace_describe_the_same_pod() {
        use crate::config::{AttributesCardinalityConfig, TimestampJitterConfig};
        use crate::message::LogJsonEncoder;

        // Every resource attribute a shard emits; a per-signal redraw of any of them would split
        // one shard into two pods.
        const RESOURCE_KEYS: [&str; 9] = [
            "project_id",
            "cloud.account.id",
            "service.name",
            "service.version",
            "deployment.environment",
            "host.name",
            "k8s.pod.name",
            "k8s.namespace.name",
            "generator.source",
        ];

        let log_generator = OTLPLogMessageGenerator::new(
            "src".to_string(),
            AttributesCardinalityConfig::default(),
            TimestampJitterConfig::default(),
            Arc::new(LogJsonEncoder),
        );
        let factory = SignalFactory::new(
            vec![Signal::Logs, Signal::Traces],
            Some(log_generator),
            Some(trace_generator()),
            "src".to_string(),
            Some(AttributesCardinalityConfig::default()),
        );
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 6,
                num_traces: 3,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 6,
                num_traces: 3,
            },
        ];
        // `cloud_ctx` supplies a cloud account, so `cloud.account.id` is actually present to compare.
        let messages = factory.build(&cloud_ctx(shards)).unwrap();

        let MessagePayload::Json(logs) = &messages[0].message else {
            panic!("expected json logs");
        };
        let MessagePayload::Json(traces) = &messages[1].message else {
            panic!("expected json traces");
        };
        let resource_logs = logs["resourceLogs"].as_array().unwrap();
        let resource_spans = traces["resourceSpans"].as_array().unwrap();
        assert_eq!(resource_logs.len(), 2);
        assert_eq!(
            resource_spans.len(),
            2,
            "one group per shard, not per trace"
        );

        let trace_ids_per_group: Vec<HashSet<&str>> = resource_spans
            .iter()
            .map(|rs| {
                let spans = rs["scopeSpans"][0]["spans"].as_array().unwrap();
                let roots = spans
                    .iter()
                    .filter(|s| s["parentSpanId"].as_str().is_none())
                    .count();
                assert_eq!(roots, 3, "one root per trace of the shard");
                spans
                    .iter()
                    .map(|s| s["traceId"].as_str().unwrap())
                    .collect()
            })
            .collect();
        assert_eq!(trace_ids_per_group[0].len(), 3);
        assert!(
            trace_ids_per_group[0].is_disjoint(&trace_ids_per_group[1]),
            "shards must not share trace ids"
        );

        for (index, rl) in resource_logs.iter().enumerate() {
            let rs = &resource_spans[index];
            for key in RESOURCE_KEYS {
                let log_value = json_resource_attr(rl, key).expect("log resource attr present");
                let trace_value = json_resource_attr(rs, key).expect("trace resource attr present");
                assert_eq!(
                    log_value, trace_value,
                    "shard {index} reports two different {key} values"
                );
            }
            let other = &trace_ids_per_group[1 - index];
            for record in rl["scopeLogs"][0]["logRecords"].as_array().unwrap() {
                let trace_id = record["traceId"].as_str().unwrap();
                assert!(
                    trace_ids_per_group[index].contains(trace_id),
                    "record points at a trace outside its own shard's group"
                );
                assert!(!other.contains(trace_id));
            }
        }
    }

    /// A shard with more records than its trace has spans must spread them over the whole tree:
    /// pinning every record to the root would leave the other spans without a single log and make
    /// the "span → logs" jump unrepresentative.
    #[test]
    fn shard_records_are_spread_across_the_traces_spans() {
        use crate::config::{AttributesCardinalityConfig, TimestampJitterConfig};
        use crate::message::LogJsonEncoder;

        let log_generator = OTLPLogMessageGenerator::new(
            "src".to_string(),
            AttributesCardinalityConfig::default(),
            TimestampJitterConfig::default(),
            Arc::new(LogJsonEncoder),
        );
        let factory = SignalFactory::new(
            vec![Signal::Logs, Signal::Traces],
            Some(log_generator),
            Some(trace_generator()),
            "src".to_string(),
            Some(AttributesCardinalityConfig::default()),
        );
        // The smallest tree the LLM profile builds is `invoke_agent → chat` (2 spans), so 50
        // records always exceed the span count.
        let shards = vec![ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 50,
            num_traces: 1,
        }];
        let messages = factory.build(&ctx(shards)).unwrap();

        let MessagePayload::Json(logs) = &messages[0].message else {
            panic!("expected json logs");
        };
        let MessagePayload::Json(traces) = &messages[1].message else {
            panic!("expected json traces");
        };
        let windows = span_windows(&traces["resourceSpans"][0]);
        assert!(windows.len() >= 2, "profile must build a multi-span tree");

        let records = logs["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
            .as_array()
            .unwrap();
        let used: HashSet<(&str, &str)> = records
            .iter()
            .map(|record| {
                (
                    record["traceId"].as_str().unwrap(),
                    record["spanId"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            used.len(),
            windows.len(),
            "every span of the trace must carry at least one log when records >= spans"
        );
    }

    #[test]
    fn distinct_shards_get_distinct_trace_ids() {
        use crate::config::{AttributesCardinalityConfig, TimestampJitterConfig};
        use crate::message::LogJsonEncoder;

        let log_generator = OTLPLogMessageGenerator::new(
            "src".to_string(),
            AttributesCardinalityConfig::default(),
            TimestampJitterConfig::default(),
            Arc::new(LogJsonEncoder),
        );
        let factory = SignalFactory::new(
            vec![Signal::Logs, Signal::Traces],
            Some(log_generator),
            Some(trace_generator()),
            "src".to_string(),
            Some(AttributesCardinalityConfig::default()),
        );
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
        ];
        let messages = factory.build(&ctx(shards)).unwrap();
        let MessagePayload::Json(logs) = &messages[0].message else {
            panic!("expected json logs");
        };
        let resource_logs = logs["resourceLogs"].as_array().unwrap();
        let trace_a = resource_logs[0]["scopeLogs"][0]["logRecords"][0]["traceId"]
            .as_str()
            .unwrap();
        let trace_b = resource_logs[1]["scopeLogs"][0]["logRecords"][0]["traceId"]
            .as_str()
            .unwrap();
        assert_ne!(trace_a, trace_b, "shards must get distinct trace ids");
    }

    #[test]
    fn protobuf_logs_and_traces_share_raw_id_bytes_per_shard() {
        use crate::config::{AttributesCardinalityConfig, TimestampJitterConfig};
        use crate::message::traces::trace_encoder::TraceProtobufEncoder;
        use crate::message::LogProtobufEncoder;
        use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
        use crate::pb::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
        use prost::Message;

        let log_generator = OTLPLogMessageGenerator::new(
            "src".to_string(),
            AttributesCardinalityConfig::default(),
            TimestampJitterConfig::default(),
            Arc::new(LogProtobufEncoder),
        );
        let trace_generator = TraceMessageGenerator::new(
            "src".to_string(),
            Arc::new(TraceProtobufEncoder),
            Arc::new(LlmSpanProfile {
                max_tool_calls: 1,
                capture_content: false,
                weights: ProfileWeights::default(),
                conversations: ConversationCursor::shared(),
                budget: None,
            }),
        );
        let factory = SignalFactory::new(
            vec![Signal::Logs, Signal::Traces],
            Some(log_generator),
            Some(trace_generator),
            "src".to_string(),
            Some(AttributesCardinalityConfig::default()),
        );
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 2,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 2,
                num_traces: 1,
            },
        ];
        let messages = factory.build(&ctx(shards)).unwrap();

        let MessagePayload::Protobuf(logs_bytes) = &messages[0].message else {
            panic!("expected protobuf logs");
        };
        let MessagePayload::Protobuf(traces_bytes) = &messages[1].message else {
            panic!("expected protobuf traces");
        };
        let logs = ExportLogsServiceRequest::decode(logs_bytes.as_slice()).unwrap();
        let traces = ExportTraceServiceRequest::decode(traces_bytes.as_slice()).unwrap();
        assert_eq!(logs.resource_logs.len(), traces.resource_spans.len());

        for (rl, rs) in logs.resource_logs.iter().zip(traces.resource_spans.iter()) {
            // One root per trace of the shard; the fixture gives every shard a single trace.
            let roots = rs.scope_spans[0]
                .spans
                .iter()
                .filter(|s| s.parent_span_id.is_empty())
                .count();
            assert_eq!(roots, 1, "one root span per trace of the shard");
            let ids: HashSet<(&[u8], &[u8])> = rs.scope_spans[0]
                .spans
                .iter()
                .map(|s| (s.trace_id.as_slice(), s.span_id.as_slice()))
                .collect();
            let records = &rl.scope_logs[0].log_records;
            assert!(!records.is_empty());
            for record in records {
                assert!(
                    ids.contains(&(record.trace_id.as_slice(), record.span_id.as_slice())),
                    "log ids are not one of the shard's spans"
                );
            }
        }
    }

    /// Cardinality config that buckets the identity keys, so a regression that normalized only the
    /// log path (and left traces raw) would make logs and traces disagree.
    #[cfg(test)]
    fn bucketing_cardinality() -> crate::config::AttributesCardinalityConfig {
        use std::collections::HashMap;
        crate::config::AttributesCardinalityConfig {
            enabled: true,
            // Collapse cloud.account.id / generator.source; bucket project_id and service.name.
            default_limit: Some(1),
            limit_by_attr: HashMap::from([
                ("project_id".to_string(), 4usize),
                ("service.name".to_string(), 2usize),
            ]),
        }
    }

    fn cloud_ctx(shards: Vec<ServiceShard>) -> GenContext {
        GenContext {
            tenant_id: Some("t1".to_string()),
            cloud_account_id: Some("acc-1".to_string()),
            project_id: "proj".to_string(),
            now_ns: 1_700_000_000_000_000_000,
            shards,
            invalid: false,
        }
    }

    const IDENTITY_ATTR_KEYS: [&str; 4] = [
        "project_id",
        "cloud.account.id",
        "service.name",
        "generator.source",
    ];

    fn two_shards() -> Vec<ServiceShard> {
        vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 2,
                num_traces: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 2,
                num_traces: 1,
            },
        ]
    }

    /// JSON: with cardinality bucketing the identity keys, every shared identity value must be
    /// byte-identical between the log and trace `resource` — and actually bucketed, not raw.
    #[test]
    fn json_logs_and_traces_share_normalized_identity_per_shard() {
        use crate::config::TimestampJitterConfig;
        use crate::message::LogJsonEncoder;

        let log_generator = OTLPLogMessageGenerator::new(
            "src".to_string(),
            bucketing_cardinality(),
            TimestampJitterConfig::default(),
            Arc::new(LogJsonEncoder),
        );
        let factory = SignalFactory::new(
            vec![Signal::Logs, Signal::Traces],
            Some(log_generator),
            Some(trace_generator()),
            "src".to_string(),
            Some(bucketing_cardinality()),
        );
        let messages = factory.build(&cloud_ctx(two_shards())).unwrap();

        let MessagePayload::Json(logs) = &messages[0].message else {
            panic!("expected json logs");
        };
        let MessagePayload::Json(traces) = &messages[1].message else {
            panic!("expected json traces");
        };
        let resource_logs = logs["resourceLogs"].as_array().unwrap();
        let resource_spans = traces["resourceSpans"].as_array().unwrap();
        assert_eq!(resource_logs.len(), resource_spans.len());

        for (rl, rs) in resource_logs.iter().zip(resource_spans.iter()) {
            for key in IDENTITY_ATTR_KEYS {
                let log_value = json_resource_attr(rl, key).expect("log identity attr present");
                let trace_value = json_resource_attr(rs, key).expect("trace identity attr present");
                assert_eq!(log_value, trace_value, "identity key {key} diverged");
                assert!(
                    log_value.starts_with("bucket_"),
                    "identity key {key} not normalized: {log_value}"
                );
            }
        }
    }

    /// Protobuf: same invariant on the wire — a per-signal normalization regression would surface
    /// as mismatched identity bytes here.
    #[test]
    fn protobuf_logs_and_traces_share_normalized_identity_per_shard() {
        use crate::config::TimestampJitterConfig;
        use crate::message::traces::trace_encoder::TraceProtobufEncoder;
        use crate::message::LogProtobufEncoder;
        use crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
        use crate::pb::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
        use crate::pb::opentelemetry::proto::common::v1::{any_value, KeyValue};
        use prost::Message;

        fn pb_attr(attrs: &[KeyValue], key: &str) -> Option<String> {
            attrs.iter().find_map(|kv| {
                if kv.key != key {
                    return None;
                }
                match kv.value.as_ref()?.value.as_ref()? {
                    any_value::Value::StringValue(v) => Some(v.clone()),
                    _ => None,
                }
            })
        }

        let log_generator = OTLPLogMessageGenerator::new(
            "src".to_string(),
            bucketing_cardinality(),
            TimestampJitterConfig::default(),
            Arc::new(LogProtobufEncoder),
        );
        let trace_generator = TraceMessageGenerator::new(
            "src".to_string(),
            Arc::new(TraceProtobufEncoder),
            Arc::new(LlmSpanProfile {
                max_tool_calls: 1,
                capture_content: false,
                weights: ProfileWeights::default(),
                conversations: ConversationCursor::shared(),
                budget: None,
            }),
        );
        let factory = SignalFactory::new(
            vec![Signal::Logs, Signal::Traces],
            Some(log_generator),
            Some(trace_generator),
            "src".to_string(),
            Some(bucketing_cardinality()),
        );
        let messages = factory.build(&cloud_ctx(two_shards())).unwrap();

        let MessagePayload::Protobuf(logs_bytes) = &messages[0].message else {
            panic!("expected protobuf logs");
        };
        let MessagePayload::Protobuf(traces_bytes) = &messages[1].message else {
            panic!("expected protobuf traces");
        };
        let logs = ExportLogsServiceRequest::decode(logs_bytes.as_slice()).unwrap();
        let traces = ExportTraceServiceRequest::decode(traces_bytes.as_slice()).unwrap();
        assert_eq!(logs.resource_logs.len(), traces.resource_spans.len());

        for (rl, rs) in logs.resource_logs.iter().zip(traces.resource_spans.iter()) {
            let log_attrs = &rl.resource.as_ref().unwrap().attributes;
            let trace_attrs = &rs.resource.as_ref().unwrap().attributes;
            for key in IDENTITY_ATTR_KEYS {
                let log_value = pb_attr(log_attrs, key).expect("log identity attr present");
                let trace_value = pb_attr(trace_attrs, key).expect("trace identity attr present");
                assert_eq!(log_value, trace_value, "identity key {key} diverged");
                assert!(
                    log_value.starts_with("bucket_"),
                    "identity key {key} not normalized: {log_value}"
                );
            }
        }
    }
}
