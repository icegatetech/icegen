//! Trace message generator: builds one trace per service shard (one trace_id, a span tree from
//! `SpanProfile`), assigns time via `trace_time::layout_span_tree`, and encodes with
//! `TraceEncoder`.

use crate::error::{GeneratorError, Result};
use crate::message::fake_data::FakeDataGenerator;
use crate::message::resource_attrs::{build_resource_attribute_pairs, DEFAULT_SERVICE_NAME};
use crate::message::traces::span_profile::SpanProfile;
use crate::message::traces::trace_encoder::TraceEncoder;
use crate::message::traces::trace_plan::{
    PlannedEvent, PlannedResourceSpans, PlannedScopeSpans, PlannedSpan, PlannedTraces,
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

    /// One request = one trace per service shard.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::GeneratorError::ProtobufEncodeError`] if the configured encoder
    /// fails to serialize (only possible with [`crate::message::TraceProtobufEncoder`]), or
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

        // One rng per message, threaded through every trace plan: keeps the structural draws
        // (span shuffle, conversation pick) on a single, seedable source instead of a fresh
        // `thread_rng` per shard.
        let mut rng = rand::thread_rng();
        let resource_spans = shards
            .iter()
            .map(|shard| {
                self.plan_one_trace(
                    &project_id,
                    cloud_account_id.as_deref(),
                    shard.service_name.as_deref(),
                    now_ns,
                    &mut rng,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let planned = PlannedTraces {
            project_id: project_id.clone(),
            resource_spans,
        };
        let payload = self.encoder.encode(&planned)?;
        Ok(OTLPMessage::new(
            payload,
            Signal::Traces,
            tenant_id,
            project_id,
            self.source.clone(),
            OTLPMessageType::Valid,
        ))
    }

    #[allow(clippy::result_large_err)]
    fn plan_one_trace(
        &self,
        project_id: &str,
        cloud_account_id: Option<&str>,
        service_name: Option<&str>,
        now_ns: i64,
        rng: &mut dyn rand::RngCore,
    ) -> Result<PlannedResourceSpans> {
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

        // Emit spans in random order: parent/child links are carried by span ids, so the wire
        // order is free. Real collectors do not receive a root-first stream.
        spans.shuffle(rng);

        let svc = service_name.unwrap_or(DEFAULT_SERVICE_NAME);
        let scope_name = format!("io.trihub.{}", svc.replace('-', "."));
        let scope_version = format!("1.{}.{}", rng.gen_range(0..10), rng.gen_range(0..10));

        Ok(PlannedResourceSpans {
            resource_attrs: build_resource_attribute_pairs(
                project_id,
                cloud_account_id,
                service_name,
                &self.source,
            ),
            resource_dropped_attributes_count: rng.gen_range(0..4),
            trace_id,
            scope: PlannedScopeSpans {
                scope_name,
                scope_version,
                scope_attrs: vec![],
                spans,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::traces::conversation::ConversationPool;
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
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Fixed anchor for plan-level tests that inspect structure, not wall-clock time.
    const TEST_NOW_NS: i64 = 1_700_000_000_000_000_000;

    fn test_pool() -> Arc<ConversationPool> {
        ConversationPool::shared_default(&mut rand::thread_rng())
    }

    fn gen() -> TraceMessageGenerator {
        TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(LlmSpanProfile {
                max_tool_calls: 2,
                capture_content: false,
                weights: ProfileWeights::default(),
                conversations: test_pool(),
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
                    num_records: 1,
                }],
            )
            .unwrap_err();
        assert!(matches!(err, GeneratorError::EmptySpanTree));
    }

    #[test]
    fn generates_one_resource_spans_per_shard_with_shared_trace_id_per_tree() {
        let shards = vec![
            ServiceShard {
                service_name: Some("svc-a".to_string()),
                num_records: 1,
            },
            ServiceShard {
                service_name: Some("svc-b".to_string()),
                num_records: 1,
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
        let rs = json["resourceSpans"].as_array().unwrap();
        assert_eq!(rs.len(), 2);
        // within each ResourceSpans all spans share one traceId
        for r in rs {
            let spans = r["scopeSpans"][0]["spans"].as_array().unwrap();
            let first = spans[0]["traceId"].as_str().unwrap();
            assert!(spans.iter().all(|s| s["traceId"].as_str() == Some(first)));
            // the root is the single parent-less span; spans may arrive in any order.
            let root = spans
                .iter()
                .find(|s| s["parentSpanId"].as_str().is_none())
                .expect("exactly one root span");
            let root_start: i64 = root["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
            let root_end: i64 = root["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
            // every other span is enclosed by the root window
            for s in spans
                .iter()
                .filter(|s| s["parentSpanId"].as_str().is_some())
            {
                let cs: i64 = s["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
                let ce: i64 = s["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
                assert!(cs >= root_start && ce <= root_end, "child within root");
            }
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
            num_records: 1,
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
            num_records: 1,
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
            num_records: 1,
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
    fn conversation_ids_are_reused_across_traces() {
        // A small pool forces reuse across independently planned traces. A single seeded rng
        // drives the pool build and every conversation pick, so the run is deterministic.
        let mut rng = StdRng::seed_from_u64(20);
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(LlmSpanProfile {
                max_tool_calls: 0,
                capture_content: false,
                weights: ProfileWeights::default(),
                conversations: Arc::new(ConversationPool::new(4, &mut rng)),
            }),
        );
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            let rs = generator
                .plan_one_trace("proj", None, Some("svc-a"), TEST_NOW_NS, &mut rng)
                .unwrap();
            let id = planned_conversation_id(&rs.scope.spans[0]).expect("conversation.id present");
            seen.insert(id.to_string());
        }
        // Reuse (cardinality below the draw count) but more than a single id.
        assert!(seen.len() > 1, "expected more than one conversation id");
        assert!(seen.len() <= 4, "cardinality must not exceed the pool size");
    }

    /// Spans are emitted in random order, so the root is not always first. `FixedNestedTree` has
    /// 3 spans, so a root-first stream has probability ~1/3 per trace; across many seeded traces
    /// at least one must place the root elsewhere.
    #[test]
    fn root_span_is_not_always_first() {
        let mut rng = StdRng::seed_from_u64(20);
        let generator = TraceMessageGenerator::new(
            "test-src".to_string(),
            Arc::new(TraceJsonEncoder),
            Arc::new(FixedNestedTree),
        );
        let root_first = (0..40)
            .filter(|_| {
                let rs = generator
                    .plan_one_trace("proj", None, Some("svc-a"), TEST_NOW_NS, &mut rng)
                    .unwrap();
                rs.scope.spans[0].parent_span_id.is_none()
            })
            .count();
        assert!(
            root_first < 40,
            "spans never reordered: root was always first"
        );
    }
}
