//! Trace message generator: builds `shard.num_traces` traces per service shard (each with its own
//! trace_id and a span tree from `SpanProfile`), assigns time via `trace_time::layout_span_tree`,
//! collects a shard's traces into a single `ResourceSpans` group, and encodes with `TraceEncoder`.

use crate::error::{GeneratorError, Result};
use crate::message::fake_data::FakeDataGenerator;
use crate::message::resource_attrs::{ShardResourceAttrs, DEFAULT_SERVICE_NAME};
use crate::message::traces::span_profile::SpanProfile;
use crate::message::traces::trace_encoder::TraceEncoder;
use crate::message::traces::trace_plan::{
    PlannedEvent, PlannedResourceSpans, PlannedScopeSpans, PlannedSpan, PlannedTraces, SpanAnchor,
    TraceCorrelation,
};
use crate::message::traces::trace_time::{layout_span_tree, resolve_event_time};
use crate::message::types::{OTLPMessage, OTLPMessageType, Signal};
use crate::message::ServiceShard;
use chrono::Utc;
use rand::seq::SliceRandom;
use rand::Rng;
use std::sync::Arc;

const CHILD_GAP_NS: i64 = 1_000_000;

#[derive(Clone)]
pub struct TraceMessageGenerator {
    source: String,
    encoder: Arc<dyn TraceEncoder>,
    profile: Arc<dyn SpanProfile + Send + Sync>,
}

impl TraceMessageGenerator {
    pub fn new(
        source: String,
        encoder: Arc<dyn TraceEncoder>,
        profile: Arc<dyn SpanProfile + Send + Sync>,
    ) -> Self {
        Self {
            source,
            encoder,
            profile,
        }
    }

    /// One request = `shard.num_traces` traces per service shard.
    ///
    /// Self-samples `project_id` and `now_ns`; used by the logs-agnostic (traces-only) callers and
    /// unit tests. The multi-signal factory instead calls the crate-internal [`Self::plan`] with a
    /// shared `project_id`/`now_ns` so logs and traces of one generation cycle agree on identity
    /// and time.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::GeneratorError::ProtobufEncodeError`] if the configured encoder
    /// fails to serialize (only possible with [`crate::TraceProtobufEncoder`]), or
    /// [`crate::error::GeneratorError::EmptySpanTree`] if a span profile yields no nodes.
    #[allow(clippy::result_large_err)]
    pub fn generate_message(
        &self,
        tenant_id: Option<String>,
        cloud_account_id: Option<String>,
        shards: Vec<ServiceShard>,
    ) -> Result<OTLPMessage> {
        let project_id = FakeDataGenerator::generate_project_id();
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Traces-only path: cardinality is a logs-only feature, so the shard attributes are carried
        // through raw (`None`). The shared normalization only kicks in when logs are co-configured.
        let resource_attrs = ShardResourceAttrs::for_shards(
            &project_id,
            cloud_account_id.as_deref(),
            &shards,
            &self.source,
            None,
        );
        let (planned, _correlations) = self.plan(
            &project_id,
            &shards,
            now_ns,
            &resource_attrs,
            &mut rand::thread_rng(),
        )?;
        self.encode_message(&planned, tenant_id)
    }

    /// Plan `shard.num_traces` traces per service shard from a shared `project_id`/`now_ns`,
    /// returning both the format-neutral plan and, per shard (in shard order), one
    /// [`TraceCorrelation`] per trace of that shard. The log planner consumes the correlations to
    /// attach a shard's log records to the spans of that shard's traces.
    ///
    /// A shard is emitted as exactly one `ResourceSpans` group — as an OTEL Collector batches a
    /// pod's traces — so the request holds `shards.len()` groups. The group's resource attributes
    /// are built once per shard, and the spans of all its traces are interleaved inside it; each
    /// span carries its own `trace_id`, so a shard's traces share a service and a pod but never a
    /// `trace_id`.
    ///
    /// `resource_attrs` carries one shard's complete resource-attribute set per index (built once by
    /// the caller); the trace reuses it verbatim, so a correlated log and its trace agree on the
    /// whole resource — identity keys and the pod-describing variable keys alike.
    ///
    /// `rng` drives every structural draw of the request (span shuffle, conversation pick, scope
    /// version), so a caller can seed the whole plan from one source.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::GeneratorError::EmptySpanTree`] if a span profile yields no nodes, or
    /// [`crate::error::GeneratorError::InvalidConfiguration`] if
    /// `resource_attrs.len() != shards.len()` or a shard has `num_traces == 0`.
    #[allow(clippy::result_large_err)]
    pub(crate) fn plan(
        &self,
        project_id: &str,
        shards: &[ServiceShard],
        now_ns: i64,
        resource_attrs: &[ShardResourceAttrs],
        rng: &mut dyn rand::RngCore,
    ) -> Result<(PlannedTraces, Vec<Vec<TraceCorrelation>>)> {
        if resource_attrs.len() != shards.len() {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "shard resource attributes ({}) must match shard count ({})",
                resource_attrs.len(),
                shards.len()
            )));
        }
        if let Some(i) = shards.iter().position(|s| s.num_traces == 0) {
            return Err(GeneratorError::InvalidConfiguration(format!(
                "shard at index {i} has num_traces=0; every shard must have num_traces >= 1"
            )));
        }
        let mut resource_spans = Vec::with_capacity(shards.len());
        let mut correlations = Vec::with_capacity(shards.len());
        for (shard, shard_attrs) in shards.iter().zip(resource_attrs) {
            let service_name = shard.service_name.as_deref();
            let mut shard_correlations = Vec::with_capacity(shard.num_traces);
            let mut spans: Vec<PlannedSpan> = Vec::new();
            for _ in 0..shard.num_traces {
                let (trace_spans, correlation) = self.plan_trace_spans(now_ns, rng)?;
                spans.extend(trace_spans);
                shard_correlations.push(correlation);
            }
            correlations.push(shard_correlations);

            // Emit the shard's spans in random order: parent/child links travel on span ids and
            // each span carries its own `trace_id`, so the wire order is free. Shuffling the whole
            // group interleaves the shard's traces, the way a collector batch arrives — a
            // root-first, trace-by-trace stream is not what a backend receives.
            spans.shuffle(rng);

            let svc = service_name.unwrap_or(DEFAULT_SERVICE_NAME);
            let scope_name = format!("io.trihub.{}", svc.replace('-', "."));
            let scope_version = format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10));
            resource_spans.push(PlannedResourceSpans {
                // TODO(high): extend cardinality to the span attributes; the resource attributes are
                // already normalized once per shard and shared with the log path.
                resource_attrs: shard_attrs.pairs().to_vec(),
                resource_dropped_attributes_count: rng.gen_range(0..4),
                scope: PlannedScopeSpans {
                    scope_name,
                    scope_version,
                    scope_attrs: vec![],
                    spans,
                },
            });
        }

        Ok((
            PlannedTraces {
                project_id: project_id.to_string(),
                resource_spans,
            },
            correlations,
        ))
    }

    /// Encode a planned trace request into a wire-format [`OTLPMessage`] tagged [`Signal::Traces`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::GeneratorError::ProtobufEncodeError`] if the configured encoder
    /// fails to serialize (only possible with [`crate::TraceProtobufEncoder`]).
    #[allow(clippy::result_large_err)]
    pub fn encode_message(
        &self,
        planned: &PlannedTraces,
        tenant_id: Option<String>,
    ) -> Result<OTLPMessage> {
        let payload = self.encoder.encode(planned)?;
        Ok(OTLPMessage::new(
            payload,
            Signal::Traces,
            tenant_id,
            planned.project_id.clone(),
            self.source.clone(),
            OTLPMessageType::Valid,
        ))
    }

    /// Plan the spans of a single trace: a fresh `trace_id`, a span tree from the profile, and the
    /// laid-out time windows. The spans are returned in layout order (index 0 is the root) for the
    /// caller to merge into its shard's group; the shard owns the resource and scope of that group.
    #[allow(clippy::result_large_err)]
    fn plan_trace_spans(
        &self,
        now_ns: i64,
        rng: &mut dyn rand::RngCore,
    ) -> Result<(Vec<PlannedSpan>, TraceCorrelation)> {
        let trace_id = FakeDataGenerator::generate_trace_id();
        // The profile owns all span semantics, including the per-trace `gen_ai.conversation.id`
        // it stamps on every node — this generator stays signal-agnostic.
        let nodes = self.profile.build_tree(rng);

        // Layout indexes the root at `relative[0]`; an empty tree would panic there. Enforce the
        // "index 0 is the root" contract as an explicit error so a misbehaving profile fails
        // message generation cleanly instead of crashing.
        if nodes.is_empty() {
            return Err(GeneratorError::EmptySpanTree);
        }

        // id for each node
        let span_ids: Vec<[u8; 8]> = (0..nodes.len())
            .map(|_| FakeDataGenerator::generate_span_id())
            .collect();

        // Time: lay the tree out from t=0 once to measure its full span — the root window
        // stretches to the end of its longest, deepest descendant, which for multi-turn trees
        // runs well past the root's own short duration. Then translate every window so the root
        // *ends* at `now`, placing the whole subtree in the past. Anchoring by the root's own
        // duration instead let long trees spill into the future, where some backends drop
        // far-future spans. Layout is translation-invariant, so a flat shift is exact. Nesting at
        // any depth (tool ⊂ chat ⊂ root) is preserved.
        let parents: Vec<Option<usize>> = nodes.iter().map(|n| n.parent).collect();
        let durations: Vec<i64> = nodes.iter().map(|n| n.duration_ns).collect();
        let parallel: Vec<bool> = nodes.iter().map(|n| n.parallel_children).collect();
        let relative = layout_span_tree(0, &parents, &durations, CHILD_GAP_NS, &parallel)?;
        let shift = now_ns - relative[0].1.max(1);
        let windows: Vec<(i64, i64)> = relative
            .iter()
            .map(|(s, e)| (s + shift, e + shift))
            .collect();

        // Capture one anchor per span so the log planner can spread a shard's records across the
        // whole tree instead of pinning them all to the root. Built from `span_ids`/`windows`,
        // which are indexed by node — unaffected by the wire-order shuffle of `spans` below — and
        // kept in layout order, so index 0 stays the root.
        let anchors: Arc<[SpanAnchor]> = span_ids
            .iter()
            .zip(&windows)
            .map(|(&span_id, &(start_ns, end_ns))| SpanAnchor {
                span_id,
                start_ns,
                end_ns,
            })
            .collect();
        let correlation = TraceCorrelation { trace_id, anchors };

        let mut spans: Vec<PlannedSpan> = Vec::with_capacity(nodes.len());
        for (i, node) in nodes.iter().enumerate() {
            let (start_ns, end_ns) = windows[i];
            // Resolve each relative event into an absolute timestamp inside the span window now
            // that layout has assigned `[start_ns, end_ns]`. Clamp guards against rounding drift.
            let events = node
                .events
                .iter()
                .map(|e| PlannedEvent {
                    time_ns: resolve_event_time(start_ns, end_ns, e.offset_frac),
                    name: e.name.clone(),
                    attributes: e.attributes.clone(),
                })
                .collect();
            spans.push(PlannedSpan {
                trace_id,
                span_id: span_ids[i],
                parent_span_id: node.parent.map(|p| span_ids[p]),
                name: node.name.clone(),
                kind: node.kind,
                start_ns,
                end_ns,
                attributes: node.attributes.clone(),
                events,
                status_code: node.status_code,
                status_message: node.status_message.clone(),
            });
        }

        Ok((spans, correlation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::traces::conversation::{ConversationCursor, MAX_TRACES_PER_CONVERSATION};
    use crate::message::traces::span_profile::{
        LlmSpanProfile, ProfileWeights, RelEvent, SpanNode,
    };
    use crate::message::traces::trace_encoder::TraceJsonEncoder;
    use crate::message::traces::trace_plan::AttrValue;
    use crate::message::traces::{SpanKind, SpanStatusCode};
    use crate::message::types::{MessagePayload, Signal};
    use crate::message::ServiceShard;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    /// Fixed anchor for plan-level tests that inspect structure, not wall-clock time.
    const TEST_NOW_NS: i64 = 1_700_000_000_000_000_000;

    fn test_cursor() -> Arc<ConversationCursor> {
        ConversationCursor::shared()
    }

    fn gen() -> TraceMessageGenerator {
        TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(LlmSpanProfile {
                max_tool_calls: 2,
                capture_content: false,
                weights: ProfileWeights::default(),
                conversations: test_cursor(),
                budget: None,
            }),
        )
    }

    /// Deterministic 3-level tree root → chat → tool, where tool is longer than chat:
    /// a flat layout would place tool after the end of chat — this profile catches the regression.
    struct FixedNestedTree;
    impl SpanProfile for FixedNestedTree {
        fn build_tree(&self, _rng: &mut dyn rand::RngCore) -> Vec<SpanNode> {
            let leaf = |parent, name: &str| SpanNode {
                parent,
                name: name.to_string(),
                kind: SpanKind::Internal,
                attributes: vec![],
                status_code: SpanStatusCode::Unset,
                status_message: String::new(),
                duration_ns: 0,
                events: vec![],
                parallel_children: false,
            };
            vec![
                SpanNode {
                    duration_ns: 100_000_000,
                    ..leaf(None, "invoke_agent")
                },
                SpanNode {
                    duration_ns: 50_000_000,
                    ..leaf(Some(0), "chat")
                },
                SpanNode {
                    duration_ns: 300_000_000, // > chat: chat's window must stretch
                    ..leaf(Some(1), "execute_tool")
                },
            ]
        }
    }

    /// A profile that violates the "at least a root" contract by returning no nodes.
    struct EmptyTree;
    impl SpanProfile for EmptyTree {
        fn build_tree(&self, _rng: &mut dyn rand::RngCore) -> Vec<SpanNode> {
            vec![]
        }
    }

    #[test]
    fn empty_span_tree_is_a_clean_error_not_a_panic() {
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(EmptyTree),
        );
        let err = generator
            .generate_message(
                Some("tenant1".to_string()),
                None,
                vec![ServiceShard {
                    service_name: Some("svc-a".to_string()),
                    num_logs: 1,
                    num_traces: 1,
                }],
            )
            .unwrap_err();
        assert!(matches!(err, GeneratorError::EmptySpanTree));
    }

    /// A service shard is a single `ResourceSpans` group no matter how many traces it carries —
    /// the shape an OTEL Collector batches for one pod. Each trace inside the group keeps its own
    /// `trace_id` and its own single root, and its spans stay inside that root's window.
    #[test]
    fn shard_emits_one_resource_spans_group_holding_all_its_traces() {
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_logs: 1,
                num_traces: 3,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_logs: 1,
                num_traces: 3,
            },
        ];
        let msg = gen()
            .generate_message(
                Some("tenant1".to_string()),
                Some("acc-1".to_string()),
                shards,
            )
            .unwrap();
        assert_eq!(msg.signal, Signal::Traces);
        let MessagePayload::Json(json) = msg.message else {
            panic!("expected json")
        };
        let groups = json["resourceSpans"].as_array().unwrap();
        assert_eq!(groups.len(), 2, "one ResourceSpans group per shard");

        let mut all_trace_ids: HashSet<String> = HashSet::new();
        for group in groups {
            let spans = group["scopeSpans"][0]["spans"].as_array().unwrap();
            // Spans arrive interleaved, so regroup them by the trace they belong to.
            let mut by_trace: HashMap<String, Vec<&serde_json::Value>> = HashMap::new();
            for span in spans {
                by_trace
                    .entry(span["traceId"].as_str().unwrap().to_string())
                    .or_default()
                    .push(span);
            }
            assert_eq!(by_trace.len(), 3, "three traces per shard");
            all_trace_ids.extend(by_trace.keys().cloned());

            for trace_spans in by_trace.values() {
                let roots: Vec<_> = trace_spans
                    .iter()
                    .filter(|s| s["parentSpanId"].as_str().is_none())
                    .collect();
                assert_eq!(roots.len(), 1, "exactly one root span per trace");
                let root_start: i64 = roots[0]["startTimeUnixNano"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                let root_end: i64 = roots[0]["endTimeUnixNano"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                for s in trace_spans
                    .iter()
                    .filter(|s| s["parentSpanId"].as_str().is_some())
                {
                    let cs: i64 = s["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
                    let ce: i64 = s["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
                    assert!(cs >= root_start && ce <= root_end, "child within root");
                }
            }
        }
        assert_eq!(
            all_trace_ids.len(),
            6,
            "every trace of every shard has its own trace_id"
        );
    }

    /// Regression: the resource attributes are built once per shard, so all traces of a shard
    /// report the same pod. Emitting one group per trace rebuilt them per trace, turning a single
    /// shard into several hosts/pods that shared one `service.name`.
    #[test]
    fn traces_of_one_shard_share_identical_resource_attributes() {
        const VARIABLE_KEYS: [&str; 5] = [
            "host.name",
            "k8s.pod.name",
            "service.version",
            "deployment.environment",
            "k8s.namespace.name",
        ];

        let shards = vec![ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 1,
            num_traces: 5,
        }];
        let msg = gen().generate_message(None, None, shards).unwrap();
        let MessagePayload::Json(json) = msg.message else {
            panic!("expected json")
        };
        let groups = json["resourceSpans"].as_array().unwrap();
        assert_eq!(groups.len(), 1);

        let attrs = groups[0]["resource"]["attributes"].as_array().unwrap();
        for key in VARIABLE_KEYS {
            let values: Vec<&str> = attrs
                .iter()
                .filter(|a| a["key"].as_str() == Some(key))
                .map(|a| a["value"]["stringValue"].as_str().unwrap())
                .collect();
            assert_eq!(values.len(), 1, "{key} must appear exactly once per shard");
        }
    }

    /// Any span with a `parentSpanId` must lie within the time window of its actual parent —
    /// at any tree depth, not only under the root. Regression test for the defect where
    /// `execute_tool` (a child of `chat`) was laid out flat and fell past the end of `chat`.
    #[test]
    fn every_child_span_is_nested_within_its_parent() {
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(FixedNestedTree),
        );
        let shards = vec![ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 1,
            num_traces: 1,
        }];
        let msg = generator.generate_message(None, None, shards).unwrap();
        let MessagePayload::Json(json) = msg.message else {
            panic!("expected json")
        };
        let spans = json["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap();

        // spanId → (start, end)
        let mut windows: HashMap<String, (i64, i64)> = HashMap::new();
        for s in spans {
            let id = s["spanId"].as_str().unwrap().to_string();
            let start: i64 = s["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
            let end: i64 = s["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
            windows.insert(id, (start, end));
        }

        // the tree really is 3-level: there is a span whose parent itself has a parent
        let mut saw_grandchild = false;
        for s in spans {
            let Some(parent_id) = s["parentSpanId"].as_str() else {
                continue;
            };
            let (cs, ce) = windows[s["spanId"].as_str().unwrap()];
            let (ps, pe) = windows[parent_id];
            assert!(
                ps <= cs && ce <= pe,
                "span {} not within parent {}: child [{cs},{ce}] parent [{ps},{pe}]",
                s["name"].as_str().unwrap(),
                parent_id,
            );
            // is this span's parent itself a child of someone?
            if let Some(parent_span) = spans
                .iter()
                .find(|p| p["spanId"].as_str() == Some(parent_id))
            {
                if parent_span["parentSpanId"].as_str().is_some() {
                    saw_grandchild = true;
                }
            }
        }
        assert!(
            saw_grandchild,
            "expected a 3-level tree (tool ⊂ chat ⊂ root)"
        );
    }

    /// A single-span profile carrying one relative event; lets the test assert the event is
    /// resolved into the span's absolute window after layout.
    struct EventProfile {
        offset_frac: f64,
    }
    impl SpanProfile for EventProfile {
        fn build_tree(&self, _rng: &mut dyn rand::RngCore) -> Vec<SpanNode> {
            vec![SpanNode {
                parent: None,
                name: "invoke_agent x".to_string(),
                kind: SpanKind::Internal,
                attributes: vec![],
                status_code: SpanStatusCode::Unset,
                status_message: String::new(),
                duration_ns: 100_000_000,
                events: vec![RelEvent {
                    offset_frac: self.offset_frac,
                    name: "exception".to_string(),
                    attributes: vec![],
                }],
                parallel_children: false,
            }]
        }
    }

    fn root_event_time(offset_frac: f64) -> (i64, i64, i64) {
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(EventProfile { offset_frac }),
        );
        let shards = vec![ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 1,
            num_traces: 1,
        }];
        let msg = generator.generate_message(None, None, shards).unwrap();
        let MessagePayload::Json(json) = msg.message else {
            panic!("expected json")
        };
        let span = &json["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        let start: i64 = span["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
        let end: i64 = span["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
        let ev: i64 = span["events"][0]["timeUnixNano"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        (start, end, ev)
    }

    #[test]
    fn relative_event_resolves_within_span_window() {
        let (start, end, ev) = root_event_time(0.5);
        assert!(ev >= start && ev <= end, "event within window");
    }

    #[test]
    fn relative_event_at_one_lands_on_span_end() {
        let (_, end, ev) = root_event_time(1.0);
        assert_eq!(ev, end, "offset_frac=1.0 maps to end_ns");
    }

    /// Read the `gen_ai.conversation.id` string attribute of an encoded span, if present.
    fn conversation_id(span: &serde_json::Value) -> Option<String> {
        span["attributes"].as_array()?.iter().find_map(|a| {
            (a["key"] == "gen_ai.conversation.id")
                .then(|| a["value"]["stringValue"].as_str().unwrap().to_string())
        })
    }

    fn encoded_spans(msg: OTLPMessage) -> Vec<serde_json::Value> {
        let MessagePayload::Json(json) = msg.message else {
            panic!("expected json")
        };
        json["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap()
            .clone()
    }

    #[test]
    fn all_spans_in_trace_share_one_conversation_id() {
        let shards = vec![ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 1,
            num_traces: 1,
        }];
        let msg = gen().generate_message(None, None, shards).unwrap();
        let spans = encoded_spans(msg);
        let first = conversation_id(&spans[0]).expect("conversation.id present");
        assert!(spans
            .iter()
            .all(|s| conversation_id(s).as_deref() == Some(first.as_str())));
    }

    /// Read the `gen_ai.conversation.id` string attribute off a planned span, if present.
    fn planned_conversation_id(span: &PlannedSpan) -> Option<&str> {
        span.attributes
            .iter()
            .find_map(|(k, v)| match (k.as_str(), v) {
                ("gen_ai.conversation.id", AttrValue::Str(s)) => Some(s.as_str()),
                _ => None,
            })
    }

    #[test]
    fn conversation_ids_are_bounded_and_reused_across_traces() {
        // Consecutive traces reuse one conversation id until its 1–3 budget is spent, then the
        // cursor mints a fresh one — so ids recur across traces but no id ever grows past its
        // budget. A single seeded rng drives every mint and budget draw, so the run is
        // deterministic.
        let mut rng = StdRng::seed_from_u64(20);
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(LlmSpanProfile {
                max_tool_calls: 0,
                capture_content: false,
                weights: ProfileWeights::default(),
                conversations: ConversationCursor::shared(),
                budget: None,
            }),
        );
        let mut counts: HashMap<String, usize> = HashMap::new();
        for _ in 0..200 {
            let (spans, _corr) = generator.plan_trace_spans(TEST_NOW_NS, &mut rng).unwrap();
            let id = planned_conversation_id(&spans[0]).expect("conversation.id present");
            *counts.entry(id.to_string()).or_insert(0) += 1;
        }
        // Reuse happened: fewer distinct ids than traces.
        assert!(
            counts.len() < 200,
            "expected conversation-id reuse across traces"
        );
        // But no conversation grew past its trace budget.
        assert!(
            counts
                .values()
                .all(|&c| c <= MAX_TRACES_PER_CONVERSATION as usize),
            "a conversation id exceeded the max trace budget"
        );
    }

    /// A shard's spans are emitted in random order, so its first span is not always a root.
    /// `FixedNestedTree` has 3 spans, so a root-first group has probability ~1/3 per plan; the
    /// seeded rng makes the run reproducible, and 20 plans leave no seed-dependent doubt.
    #[test]
    fn first_span_of_a_shard_is_not_always_the_root() {
        let mut rng = StdRng::seed_from_u64(20);
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(FixedNestedTree),
        );
        let shards = vec![ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 1,
            num_traces: 1,
        }];
        let resource_attrs =
            ShardResourceAttrs::for_shards("proj", None, &shards, "test-src", None);
        let root_first = (0..20)
            .filter(|_| {
                let (planned, _corr) = generator
                    .plan("proj", &shards, TEST_NOW_NS, &resource_attrs, &mut rng)
                    .unwrap();
                planned.resource_spans[0].scope.spans[0]
                    .parent_span_id
                    .is_none()
            })
            .count();
        assert!(
            root_first < 20,
            "spans never reordered: root was always first"
        );
    }

    /// The shuffle is per shard, not per trace: with several traces in one group their spans
    /// interleave instead of arriving trace-by-trace. `FixedNestedTree` gives 3 spans per trace, so
    /// a trace-by-trace stream has probability `1/C(6,3) = 1/20` per plan for 2 traces; 20 seeded
    /// plans make an interleaving certain for this seed.
    #[test]
    fn spans_of_a_shards_traces_are_interleaved() {
        let mut rng = StdRng::seed_from_u64(20);
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(FixedNestedTree),
        );
        let shards = vec![ServiceShard {
            service_name: Some("svc-a".to_string()),
            num_logs: 1,
            num_traces: 2,
        }];
        let resource_attrs =
            ShardResourceAttrs::for_shards("proj", None, &shards, "test-src", None);
        let interleaved = (0..20).any(|_| {
            let (planned, _corr) = generator
                .plan("proj", &shards, TEST_NOW_NS, &resource_attrs, &mut rng)
                .unwrap();
            let trace_ids: Vec<[u8; 16]> = planned.resource_spans[0]
                .scope
                .spans
                .iter()
                .map(|span| span.trace_id)
                .collect();
            // Contiguous per trace means exactly one switch between the two traces.
            trace_ids.windows(2).filter(|w| w[0] != w[1]).count() > 1
        });
        assert!(
            interleaved,
            "a shard's traces were never interleaved: the shuffle is still per trace"
        );
    }
}
