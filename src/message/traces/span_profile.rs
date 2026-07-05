//! Span population profiles. `LlmSpanProfile` produces an LLM-call tree with `gen_ai.*`
//! semantics (current OTel GenAI semconv). `trait SpanProfile` is the extension point for a
//! future `GenericSpanProfile` (non-LLM traces).

use crate::error::GeneratorError;
use crate::message::fake_data::{ExceptionKind, FakeDataGenerator};
use crate::message::traces::conversation::ConversationCursor;
use crate::message::traces::{AttrValue, SpanAttrs, SpanKind, SpanStatusCode};
use rand::rngs::ThreadRng;
use rand::{Rng, RngCore};
use std::collections::HashMap;
use std::sync::Arc;

/// Probability that a generated trace carries an error (one in ten). Deliberately a constant,
/// not config — the error rate is a fixed realism knob.
const ERROR_TRACE_PROB: f64 = 0.1;

/// Span event positioned relatively inside its span's window.
///
/// The profile works in relative time (no absolute timestamps yet), so an event records the
/// fraction of the span window at which it occurs. The generator resolves it to an absolute
/// `time_ns` only after [`crate::message::traces::trace_time::layout_span_tree`] assigns the
/// span's `[start_ns, end_ns]`.
#[derive(Debug, Clone)]
pub struct RelEvent {
    /// Position inside the span window, `0.0` (start) to `1.0` (end).
    pub offset_frac: f64,
    /// Event name (e.g. `"exception"`).
    pub name: String,
    /// Event attributes.
    pub attributes: SpanAttrs,
}

/// Tree node before id/time assignment: references its parent by index in the flat list.
#[derive(Debug, Clone)]
pub struct SpanNode {
    pub parent: Option<usize>,
    pub name: String,
    pub kind: SpanKind,
    pub attributes: Vec<(String, AttrValue)>,
    pub status_code: SpanStatusCode,
    pub status_message: String,
    /// Relative duration (ns) for the time allocator.
    pub duration_ns: i64,
    /// Events positioned relatively in the span window; resolved to absolute time post-layout.
    pub events: Vec<RelEvent>,
    /// When set, this node's direct children are laid out in parallel (overlapping windows)
    /// instead of sequentially. Used for concurrent tool calls within a chat turn.
    pub parallel_children: bool,
}

/// Strategy for populating a span tree.
pub trait SpanProfile {
    /// Build the node tree (index 0 is the root). Time/id are assigned later.
    ///
    /// `rng` drives only *structural* decisions (call form, number of turns/tools, error flags)
    /// so a seeded generator yields a reproducible tree shape. Fake string/numeric values
    /// (names, ids, token counts) keep their own `thread_rng` and are intentionally not seeded —
    /// tests assert structure, never exact fake values.
    fn build_tree(&self, rng: &mut dyn RngCore) -> Vec<SpanNode>;
}

/// Relative weights for the LLM call forms. Higher weight = more frequent. The total must be
/// positive. Mirrors the `LLM_PROFILE_WEIGHTS` config spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileWeights {
    pub simple_chat: u32,
    pub tool_loop: u32,
    pub plan_execute_reflect: u32,
    pub rag: u32,
}

impl Default for ProfileWeights {
    fn default() -> Self {
        Self {
            simple_chat: 1,
            tool_loop: 3,
            plan_execute_reflect: 2,
            rag: 1,
        }
    }
}

impl ProfileWeights {
    /// Build weights from parsed `name → weight` pairs. The config layer owns the string syntax
    /// (`name:weight,…`) and hands over neutral pairs; see `config::parse_profile_weights`. This is
    /// the *domain* layer: it maps known form names and enforces a positive total. Omitted forms
    /// default to weight 0.
    ///
    /// # Arguments
    ///
    /// * `pairs` - form name → relative weight, already tokenised by the config layer.
    ///
    /// # Errors
    ///
    /// Returns [`GeneratorError::InvalidConfiguration`] on an unknown form name or a zero total.
    #[allow(clippy::result_large_err)]
    pub fn from_pairs(pairs: &HashMap<String, u32>) -> Result<Self, GeneratorError> {
        let mut weights = Self {
            simple_chat: 0,
            tool_loop: 0,
            plan_execute_reflect: 0,
            rag: 0,
        };
        for (name, &value) in pairs {
            match name.as_str() {
                "simple_chat" => weights.simple_chat = value,
                "tool_loop" => weights.tool_loop = value,
                "plan_execute_reflect" => weights.plan_execute_reflect = value,
                "rag" => weights.rag = value,
                other => {
                    return Err(GeneratorError::InvalidConfiguration(format!(
                        "unknown llm profile form '{other}' in llm_profile_weights"
                    )))
                }
            }
        }
        if weights.total() == 0 {
            return Err(GeneratorError::InvalidConfiguration(
                "llm_profile_weights must have a positive total".to_string(),
            ));
        }
        Ok(weights)
    }

    fn total(&self) -> u32 {
        self.simple_chat + self.tool_loop + self.plan_execute_reflect + self.rag
    }
}

/// One of the supported LLM call shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallForm {
    SimpleChat,
    ToolLoop,
    PlanExecuteReflect,
    Rag,
}

/// Target span-count budget for a single trace. Domain-neutral: a `SpanProfile` grows its tree
/// until the node count lands in `[min, max]`. `min == max` pins an exact size. Held as
/// `Option` on the profile; `None` disables budgeting (the profile emits its natural shape).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanBudget {
    pub min: u32,
    pub max: u32,
}

/// LLM profile: dispatches to one of several call forms (simple chat, tool loop, plan/execute/
/// reflect, RAG) weighted by [`ProfileWeights`].
#[derive(Debug, Clone)]
pub struct LlmSpanProfile {
    pub max_tool_calls: u32,
    pub capture_content: bool,
    pub weights: ProfileWeights,
    /// Source of `gen_ai.conversation.id` values, budgeting a small number of traces per
    /// conversation. The conversation concept is LLM domain knowledge, so it lives in the profile,
    /// not the signal-agnostic trace generator.
    pub conversations: Arc<ConversationCursor>,
    /// When set, overrides call-form selection: the tree is grown to an exact span count drawn
    /// from this budget. `None` keeps the weighted per-form shapes.
    pub budget: Option<SpanBudget>,
}

impl SpanProfile for LlmSpanProfile {
    fn build_tree(&self, rng: &mut dyn RngCore) -> Vec<SpanNode> {
        // `rng` drives structural choices (call form, tool counts) so the tree shape is
        // reproducible under a seed. `val` produces non-seeded fake numeric values (durations,
        // token counts) — these are intentionally not part of the deterministic contract.
        let mut val = rand::thread_rng();
        let provider = FakeDataGenerator::generate_gen_ai_provider();
        let model = FakeDataGenerator::generate_gen_ai_model(&provider);

        let mut nodes = match self.budget {
            // Budget mode: ignore call-form weights and grow one agent-loop tree to an exact size
            // drawn from [min, max]. This is the "large / controllable trace" mode.
            Some(budget) => {
                let target = draw_target(budget, rng);
                self.build_budgeted(target, &mut val, rng, &provider, &model)
            }
            None => match self.pick_form(rng) {
                CallForm::SimpleChat => self.build_simple_chat(&mut val, &provider, &model),
                CallForm::ToolLoop => self.build_tool_loop(&mut val, rng, &provider, &model),
                CallForm::PlanExecuteReflect => {
                    self.build_plan_execute_reflect(&mut val, &provider, &model)
                }
                CallForm::Rag => self.build_rag(&mut val, &provider, &model),
            },
        };

        inject_error(&mut nodes, self.capture_content, rng);
        self.tag_conversation(&mut nodes, rng);
        nodes
    }
}

impl LlmSpanProfile {
    /// Stamp one `gen_ai.conversation.id` onto every span of the tree: one id per trace, shared by
    /// all spans — including the generic `workflow`/`task` wrappers, which are steps of the same
    /// agent conversation, not independent infrastructure spans. Consecutive traces reuse the id
    /// until the conversation's 1–3 trace budget is spent (multi-trace agent sessions).
    fn tag_conversation(&self, nodes: &mut [SpanNode], rng: &mut dyn RngCore) {
        let id = self.conversations.next_id(rng);
        for node in nodes.iter_mut() {
            node.attributes.push((
                "gen_ai.conversation.id".to_string(),
                AttrValue::Str(id.clone()),
            ));
        }
    }

    /// Pick a call form weighted by [`ProfileWeights`]. The total is positive (enforced by
    /// `from_spec`; `Default` also yields a positive total), so the fallback is never reached.
    fn pick_form(&self, rng: &mut dyn RngCore) -> CallForm {
        let w = &self.weights;
        let entries = [
            (CallForm::SimpleChat, w.simple_chat),
            (CallForm::ToolLoop, w.tool_loop),
            (CallForm::PlanExecuteReflect, w.plan_execute_reflect),
            (CallForm::Rag, w.rag),
        ];
        let total = w.total().max(1);
        let mut pick = rng.gen_range(0..total);
        for (form, weight) in entries {
            if pick < weight {
                return form;
            }
            pick -= weight;
        }
        CallForm::SimpleChat
    }

    /// `invoke_agent → chat`.
    fn build_simple_chat(&self, val: &mut ThreadRng, provider: &str, model: &str) -> Vec<SpanNode> {
        vec![
            agent_root_node(val, provider),
            chat_node(Some(0), val, provider, model, self.capture_content, false),
        ]
    }

    /// `invoke_agent → chat(turn1, parallel tools) → tool×N → chat(turn2) → tool×M → chat(final)`.
    /// Each turn's tools are children of that turn; turn 1's tools overlap in time.
    fn build_tool_loop(
        &self,
        val: &mut ThreadRng,
        rng: &mut dyn RngCore,
        provider: &str,
        model: &str,
    ) -> Vec<SpanNode> {
        let mut nodes = vec![agent_root_node(val, provider)];

        // Number of tools per turn (structural — at least one when tools are enabled).
        let mut tools_for_turn = || -> u32 {
            if self.max_tool_calls == 0 {
                0
            } else {
                rng.gen_range(1..=self.max_tool_calls)
            }
        };

        // Turn 1: tools run in parallel under this chat.
        let turn1 = nodes.len();
        nodes.push(chat_node(
            Some(0),
            val,
            provider,
            model,
            self.capture_content,
            true,
        ));
        for _ in 0..tools_for_turn() {
            nodes.push(tool_node(Some(turn1), val, self.capture_content));
        }

        // Turn 2: sequential tools.
        let turn2 = nodes.len();
        nodes.push(chat_node(
            Some(0),
            val,
            provider,
            model,
            self.capture_content,
            false,
        ));
        for _ in 0..tools_for_turn() {
            nodes.push(tool_node(Some(turn2), val, self.capture_content));
        }

        // Final synthesis turn.
        nodes.push(chat_node(
            Some(0),
            val,
            provider,
            model,
            self.capture_content,
            false,
        ));

        // Agent-lifecycle events on the root span of this multi-turn form.
        nodes[0].events = vec![
            rel_event("agent.turn", 0.3),
            rel_event("agent.turn", 0.6),
            rel_event("agent.final", 0.95),
        ];
        nodes
    }

    /// Build an agent-loop tree of exactly `target` spans (`target >= 1`). The `invoke_agent` root
    /// is one span; the remainder is filled with chat turns, each a `chat` span plus
    /// `0..=max_tool_calls` `execute_tool` children, until the exact count is reached. Turns are
    /// siblings under the root, so the tree stays 3 levels deep (root -> chat -> tool) at any size
    /// and the recursive time layout never deepens with span count.
    fn build_budgeted(
        &self,
        target: usize,
        val: &mut ThreadRng,
        rng: &mut dyn RngCore,
        provider: &str,
        model: &str,
    ) -> Vec<SpanNode> {
        let mut nodes = vec![agent_root_node(val, provider)];
        // A budget of 1 is a lone root span.
        let mut first_turn = true;
        while nodes.len() < target {
            let turn = nodes.len();
            // Room left after this turn's own chat span. Never emit more than that many tools, so
            // the total lands on `target` exactly and never overshoots.
            let room_for_tools = target - nodes.len() - 1;
            let cap = self.max_tool_calls as usize;
            let tools = if cap == 0 || room_for_tools == 0 {
                0
            } else {
                rng.gen_range(0..=cap.min(room_for_tools))
            };
            // First turn fans its tools out in parallel (like the classic tool_loop turn 1);
            // pointless without tools, so gate on `tools > 0`.
            nodes.push(chat_node(
                Some(0),
                val,
                provider,
                model,
                self.capture_content,
                first_turn && tools > 0,
            ));
            for _ in 0..tools {
                nodes.push(tool_node(Some(turn), val, self.capture_content));
            }
            first_turn = false;
        }
        nodes
    }

    /// `invoke_agent → workflow → [task(parse)→chat, task(plan)→chat, task(reflect)→chat]`.
    /// The workflow/task wrappers are generic spans (`trihub.span.kind`, no gen_ai operation).
    fn build_plan_execute_reflect(
        &self,
        val: &mut ThreadRng,
        provider: &str,
        model: &str,
    ) -> Vec<SpanNode> {
        let mut nodes = vec![agent_root_node(val, provider)];
        let workflow = nodes.len();
        nodes.push(generic_node(
            Some(0),
            val,
            "workflow plan_execute_reflect",
            "workflow",
        ));
        for stage in ["parse", "plan", "reflect"] {
            let task = nodes.len();
            nodes.push(generic_node(
                Some(workflow),
                val,
                &format!("task {stage}"),
                "task",
            ));
            nodes.push(chat_node(
                Some(task),
                val,
                provider,
                model,
                self.capture_content,
                false,
            ));
        }
        nodes
    }

    /// `invoke_agent → embeddings → chat` (retrieval-augmented generation).
    fn build_rag(&self, val: &mut ThreadRng, provider: &str, model: &str) -> Vec<SpanNode> {
        vec![
            agent_root_node(val, provider),
            embeddings_node(Some(0), val, provider),
            chat_node(Some(0), val, provider, model, self.capture_content, false),
        ]
    }
}

/// Build a relative event with no attributes (used for agent-lifecycle markers).
fn rel_event(name: &str, offset_frac: f64) -> RelEvent {
    RelEvent {
        offset_frac,
        name: name.to_string(),
        attributes: vec![],
    }
}

/// Build the `invoke_agent` root span.
fn agent_root_node(val: &mut ThreadRng, provider: &str) -> SpanNode {
    let agent = FakeDataGenerator::generate_agent_name();
    SpanNode {
        parent: None,
        name: format!("invoke_agent {agent}"),
        kind: SpanKind::Internal,
        attributes: vec![
            (
                "gen_ai.operation.name".to_string(),
                AttrValue::Str("invoke_agent".to_string()),
            ),
            (
                "gen_ai.provider.name".to_string(),
                AttrValue::Str(provider.to_string()),
            ),
            ("gen_ai.agent.name".to_string(), AttrValue::Str(agent)),
        ],
        status_code: SpanStatusCode::Unset,
        status_message: String::new(),
        duration_ns: val.gen_range(50_000_000..500_000_000),
        events: vec![],
        parallel_children: false,
    }
}

/// Build a `chat` span with the full set of `gen_ai.*` request/response/usage attributes.
fn chat_node(
    parent: Option<usize>,
    val: &mut ThreadRng,
    provider: &str,
    model: &str,
    capture_content: bool,
    parallel_children: bool,
) -> SpanNode {
    let input_tokens = val.gen_range(20..4000);
    let output_tokens = val.gen_range(5..1500);
    let finish_reason = FakeDataGenerator::generate_finish_reason();
    let mut attributes = vec![
        (
            "gen_ai.operation.name".to_string(),
            AttrValue::Str("chat".to_string()),
        ),
        (
            "gen_ai.provider.name".to_string(),
            AttrValue::Str(provider.to_string()),
        ),
        (
            "gen_ai.request.model".to_string(),
            AttrValue::Str(model.to_string()),
        ),
        (
            "gen_ai.request.temperature".to_string(),
            AttrValue::Double(val.gen_range(0.0..2.0)),
        ),
        (
            "gen_ai.request.max_tokens".to_string(),
            AttrValue::Int(val.gen_range(256..8192)),
        ),
        (
            "gen_ai.request.top_p".to_string(),
            AttrValue::Double(val.gen_range(0.1..1.0)),
        ),
        (
            "gen_ai.response.model".to_string(),
            AttrValue::Str(model.to_string()),
        ),
        (
            "gen_ai.response.id".to_string(),
            AttrValue::Str(FakeDataGenerator::generate_response_id()),
        ),
        (
            "gen_ai.response.finish_reasons".to_string(),
            AttrValue::StrArray(vec![finish_reason.clone()]),
        ),
        (
            "gen_ai.usage.input_tokens".to_string(),
            AttrValue::Int(input_tokens),
        ),
        (
            "gen_ai.usage.output_tokens".to_string(),
            AttrValue::Int(output_tokens),
        ),
        (
            "gen_ai.usage.total_tokens".to_string(),
            AttrValue::Int(input_tokens + output_tokens),
        ),
    ];
    if capture_content {
        // OTel GenAI semconv: messages are arrays of structured `ChatMessage` objects
        // (role + typed parts), serialized to a JSON string. A plain sentence is not
        // schema-valid and downstream consumers (e.g. Braintrust) silently drop it.
        let input_messages = serde_json::json!([{
            "role": "user",
            "parts": [{ "type": "text", "content": FakeDataGenerator::generate_sentence() }],
        }]);
        let output_messages = serde_json::json!([{
            "role": "assistant",
            "parts": [{ "type": "text", "content": FakeDataGenerator::generate_sentence() }],
            "finish_reason": finish_reason,
        }]);
        attributes.push((
            "gen_ai.input.messages".to_string(),
            AttrValue::Str(input_messages.to_string()),
        ));
        attributes.push((
            "gen_ai.output.messages".to_string(),
            AttrValue::Str(output_messages.to_string()),
        ));
    }
    SpanNode {
        parent,
        name: format!("chat {model}"),
        kind: SpanKind::Client,
        attributes,
        status_code: SpanStatusCode::Unset,
        status_message: String::new(),
        duration_ns: val.gen_range(100_000_000..3_000_000_000),
        events: vec![],
        parallel_children,
    }
}

/// Build an `execute_tool` span.
fn tool_node(parent: Option<usize>, val: &mut ThreadRng, capture_content: bool) -> SpanNode {
    let tool = FakeDataGenerator::generate_tool_name();
    let name = format!("execute_tool {tool}");
    let mut attributes = vec![
        (
            "gen_ai.operation.name".to_string(),
            AttrValue::Str("execute_tool".to_string()),
        ),
        ("gen_ai.tool.name".to_string(), AttrValue::Str(tool)),
        (
            "gen_ai.tool.type".to_string(),
            AttrValue::Str("function".to_string()),
        ),
        (
            "gen_ai.tool.call.id".to_string(),
            AttrValue::Str(FakeDataGenerator::generate_uuid()),
        ),
    ];
    if capture_content {
        // semconv: `arguments`/`result` are `any` — serialized to a JSON string when the
        // span format has no structured value type. These carry the tool I/O (PII!).
        let arguments = serde_json::json!({ "query": FakeDataGenerator::generate_sentence() });
        let result = serde_json::json!({ "output": FakeDataGenerator::generate_sentence() });
        attributes.push((
            "gen_ai.tool.call.arguments".to_string(),
            AttrValue::Str(arguments.to_string()),
        ));
        attributes.push((
            "gen_ai.tool.call.result".to_string(),
            AttrValue::Str(result.to_string()),
        ));
    }
    SpanNode {
        parent,
        name,
        kind: SpanKind::Internal,
        attributes,
        status_code: SpanStatusCode::Ok,
        status_message: String::new(),
        duration_ns: val.gen_range(1_000_000..200_000_000),
        events: vec![],
        parallel_children: false,
    }
}

/// Build an `embeddings` span.
fn embeddings_node(parent: Option<usize>, val: &mut ThreadRng, provider: &str) -> SpanNode {
    let embed_model = FakeDataGenerator::generate_gen_ai_model(provider);
    SpanNode {
        parent,
        name: format!("embeddings {embed_model}"),
        kind: SpanKind::Client,
        attributes: vec![
            (
                "gen_ai.operation.name".to_string(),
                AttrValue::Str("embeddings".to_string()),
            ),
            (
                "gen_ai.provider.name".to_string(),
                AttrValue::Str(provider.to_string()),
            ),
            (
                "gen_ai.request.model".to_string(),
                AttrValue::Str(embed_model),
            ),
            (
                "gen_ai.embeddings.dimension.count".to_string(),
                AttrValue::Int(val.gen_range(256..3072)),
            ),
            (
                "gen_ai.usage.input_tokens".to_string(),
                AttrValue::Int(val.gen_range(5..500)),
            ),
        ],
        status_code: SpanStatusCode::Unset,
        status_message: String::new(),
        duration_ns: val.gen_range(5_000_000..100_000_000),
        events: vec![],
        parallel_children: false,
    }
}

/// Build a generic workflow/task wrapper span: vendor-neutral `trihub.span.kind` marker, no
/// `gen_ai.operation.name`, `Internal` kind.
fn generic_node(
    parent: Option<usize>,
    val: &mut ThreadRng,
    name: &str,
    kind_label: &str,
) -> SpanNode {
    SpanNode {
        parent,
        name: name.to_string(),
        kind: SpanKind::Internal,
        attributes: vec![(
            "trihub.span.kind".to_string(),
            AttrValue::Str(kind_label.to_string()),
        )],
        status_code: SpanStatusCode::Unset,
        status_message: String::new(),
        duration_ns: val.gen_range(10_000_000..150_000_000),
        events: vec![],
        parallel_children: false,
    }
}

/// Operation name (`gen_ai.operation.name`) of a node, if present.
fn operation_name(node: &SpanNode) -> Option<&str> {
    node.attributes.iter().find_map(|(k, v)| match v {
        AttrValue::Str(s) if k == "gen_ai.operation.name" => Some(s.as_str()),
        _ => None,
    })
}

/// True if the node is a generic workflow/task wrapper (carries `trihub.span.kind`).
fn is_generic_wrapper(node: &SpanNode) -> bool {
    node.attributes.iter().any(|(k, _)| k == "trihub.span.kind")
}

/// Draw a concrete target span count from the budget. `min == max` yields a fixed size, which is
/// what the edge-case tests rely on.
fn draw_target(budget: SpanBudget, rng: &mut dyn RngCore) -> usize {
    rng.gen_range(budget.min..=budget.max) as usize
}

/// With probability [`ERROR_TRACE_PROB`], turn one span of the tree into a failure.
///
/// The failing span gets `status_code = Error`, the failure message, an `error.type` attribute,
/// and an `"exception"` event near its end. The failure category is chosen uniformly among the
/// kinds actually present in this tree (llm if a `chat` span exists, tool if an `execute_tool`
/// span exists, generic if a workflow/task wrapper exists).
///
/// Error propagation mirrors real tracing: an llm/generic failure bubbles up — every ancestor is
/// marked `Error` (without its own exception event) — while a tool failure stays isolated (the
/// agent recovered), so ancestors are left untouched.
///
/// # Arguments
///
/// * `nodes` - the span tree (mutated in place).
/// * `capture_content` - when set, attach an `exception.stacktrace` attribute to the event.
/// * `rng` - seeded source for the error flag, category, and target selection.
fn inject_error(nodes: &mut [SpanNode], capture_content: bool, rng: &mut dyn RngCore) {
    if !rng.gen_bool(ERROR_TRACE_PROB) {
        return;
    }

    // Candidate target spans by category.
    let chat_idxs: Vec<usize> = (0..nodes.len())
        .filter(|&i| operation_name(&nodes[i]) == Some("chat"))
        .collect();
    let tool_idxs: Vec<usize> = (0..nodes.len())
        .filter(|&i| operation_name(&nodes[i]) == Some("execute_tool"))
        .collect();
    let generic_idxs: Vec<usize> = (0..nodes.len())
        .filter(|&i| is_generic_wrapper(&nodes[i]))
        .collect();

    // Kinds available for this tree shape; pick one uniformly.
    let mut available: Vec<ExceptionKind> = Vec::with_capacity(3);
    if !chat_idxs.is_empty() {
        available.push(ExceptionKind::Llm);
    }
    if !tool_idxs.is_empty() {
        available.push(ExceptionKind::Tool);
    }
    if !generic_idxs.is_empty() {
        available.push(ExceptionKind::Generic);
    }
    if available.is_empty() {
        return;
    }
    let kind = available[rng.gen_range(0..available.len())];

    let (candidates, propagate): (&[usize], bool) = match kind {
        ExceptionKind::Llm => (&chat_idxs, true),
        ExceptionKind::Tool => (&tool_idxs, false),
        ExceptionKind::Generic => (&generic_idxs, true),
    };
    let target = candidates[rng.gen_range(0..candidates.len())];

    let (exc_type, exc_msg) = FakeDataGenerator::generate_exception(kind);
    nodes[target].status_code = SpanStatusCode::Error;
    nodes[target].status_message = exc_msg.clone();
    nodes[target]
        .attributes
        .push(("error.type".to_string(), AttrValue::Str(exc_type.clone())));

    let mut event_attrs: SpanAttrs = vec![
        (
            "exception.type".to_string(),
            AttrValue::Str(exc_type.clone()),
        ),
        ("exception.message".to_string(), AttrValue::Str(exc_msg)),
    ];
    if capture_content {
        event_attrs.push((
            "exception.stacktrace".to_string(),
            AttrValue::Str(FakeDataGenerator::generate_stacktrace(&exc_type)),
        ));
    }
    nodes[target].events.push(RelEvent {
        offset_frac: 0.95,
        name: "exception".to_string(),
        attributes: event_attrs,
    });

    if propagate {
        // Bubble the failure up the call chain only. Descendants stay as-is: a generic
        // wrapper's own I/O/timeout failure (e.g. a `task` span) does not imply its child LLM
        // call also errored — it may have completed earlier or never run — so that child chat
        // legitimately keeps its `Unset` status.
        let mut ancestor = nodes[target].parent;
        while let Some(p) = ancestor {
            nodes[p].status_code = SpanStatusCode::Error;
            ancestor = nodes[p].parent;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    /// A fresh conversation cursor for profile tests.
    fn cursor() -> Arc<ConversationCursor> {
        ConversationCursor::shared()
    }

    #[test]
    fn llm_profile_builds_rooted_tree_with_gen_ai_attrs() {
        let profile = LlmSpanProfile {
            max_tool_calls: 2,
            capture_content: false,
            weights: ProfileWeights::default(),
            conversations: cursor(),
            budget: None,
        };
        let mut rng = StdRng::seed_from_u64(1);
        let nodes = profile.build_tree(&mut rng);
        // root has no parent
        assert!(nodes[0].parent.is_none());
        // every child references an existing parent index
        for node in &nodes[1..] {
            assert!(node.parent.is_some());
        }
        // root carries gen_ai.operation.name
        assert!(nodes[0]
            .attributes
            .iter()
            .any(|(k, _)| k == "gen_ai.operation.name"));
        // a chat span with model and tokens exists
        assert!(nodes.iter().any(|n| n
            .attributes
            .iter()
            .any(|(k, _)| k == "gen_ai.usage.input_tokens")));
    }

    #[test]
    fn capture_content_off_omits_messages() {
        let profile = LlmSpanProfile {
            max_tool_calls: 0,
            capture_content: false,
            weights: ProfileWeights::default(),
            conversations: cursor(),
            budget: None,
        };
        let mut rng = StdRng::seed_from_u64(2);
        let nodes = profile.build_tree(&mut rng);
        assert!(!nodes.iter().any(|n| n
            .attributes
            .iter()
            .any(|(k, _)| k == "gen_ai.input.messages")));
    }

    /// The same seed must reproduce the same tree *structure* (node count and parent links),
    /// even though fake values (durations, tokens) differ run to run.
    #[test]
    fn seeded_rng_reproduces_tree_structure() {
        let profile = LlmSpanProfile {
            max_tool_calls: 5,
            capture_content: false,
            weights: ProfileWeights::default(),
            conversations: cursor(),
            budget: None,
        };
        let shape = |seed| {
            let mut rng = StdRng::seed_from_u64(seed);
            profile
                .build_tree(&mut rng)
                .iter()
                .map(|n| n.parent)
                .collect::<Vec<_>>()
        };
        assert_eq!(shape(42), shape(42));
    }

    fn build(seed: u64, max_tool_calls: u32, weights: ProfileWeights) -> Vec<SpanNode> {
        let profile = LlmSpanProfile {
            max_tool_calls,
            capture_content: false,
            weights,
            conversations: cursor(),
            budget: None,
        };
        let mut rng = StdRng::seed_from_u64(seed);
        profile.build_tree(&mut rng)
    }

    /// Weights that force a single call form.
    fn only(form: CallForm) -> ProfileWeights {
        let mut w = ProfileWeights {
            simple_chat: 0,
            tool_loop: 0,
            plan_execute_reflect: 0,
            rag: 0,
        };
        match form {
            CallForm::SimpleChat => w.simple_chat = 1,
            CallForm::ToolLoop => w.tool_loop = 1,
            CallForm::PlanExecuteReflect => w.plan_execute_reflect = 1,
            CallForm::Rag => w.rag = 1,
        }
        w
    }

    fn has_exception_event(node: &SpanNode) -> bool {
        node.events.iter().any(|e| e.name == "exception")
    }

    /// An llm error lands on the `chat` span (with an exception event) and bubbles up to the root.
    /// The simple_chat form has only a chat below the agent, so any error is an llm one.
    #[test]
    fn llm_error_marks_chat_and_bubbles_to_root() {
        for seed in 0..2000 {
            let nodes = build(seed, 0, only(CallForm::SimpleChat));
            if nodes.iter().any(|n| n.status_code == SpanStatusCode::Error) {
                let chat = nodes
                    .iter()
                    .find(|n| operation_name(n) == Some("chat"))
                    .expect("chat span present");
                assert_eq!(chat.status_code, SpanStatusCode::Error);
                assert!(has_exception_event(chat));
                assert_eq!(
                    nodes[0].status_code,
                    SpanStatusCode::Error,
                    "llm error bubbles to root"
                );
                return;
            }
        }
        panic!("no llm error trace found in seed range");
    }

    /// A tool error stays isolated: the `execute_tool` span fails but the root is not marked Error.
    /// Only one error is injected per trace, so a failing tool implies no other failing span.
    #[test]
    fn tool_error_is_isolated_from_root() {
        for seed in 0..5000 {
            let nodes = build(seed, 3, only(CallForm::ToolLoop));
            if let Some(tool) = nodes
                .iter()
                .find(|n| operation_name(n) == Some("execute_tool"))
                .filter(|n| n.status_code == SpanStatusCode::Error)
            {
                assert!(has_exception_event(tool));
                assert_eq!(
                    nodes[0].status_code,
                    SpanStatusCode::Unset,
                    "tool error must not bubble to root"
                );
                return;
            }
        }
        panic!("no tool error trace found in seed range");
    }

    /// Across many seeds the share of error traces is close to `ERROR_TRACE_PROB`.
    #[test]
    fn error_rate_is_approximately_one_in_ten() {
        let n = 4000;
        let errors = (0..n)
            .filter(|&seed| {
                build(seed, 3, ProfileWeights::default())
                    .iter()
                    .any(|node| node.status_code == SpanStatusCode::Error)
            })
            .count();
        let rate = errors as f64 / n as f64;
        assert!(
            (rate - ERROR_TRACE_PROB).abs() < 0.03,
            "rate {rate} not ~0.1"
        );
    }

    /// Forcing each form yields its expected backbone of operations / generic markers.
    #[test]
    fn each_form_builds_its_expected_shape() {
        let mut rng = StdRng::seed_from_u64(7);
        let profile = |w| LlmSpanProfile {
            max_tool_calls: 2,
            capture_content: false,
            weights: w,
            conversations: cursor(),
            budget: None,
        };

        let simple = profile(only(CallForm::SimpleChat)).build_tree(&mut rng);
        assert!(operation_name(&simple[0]) == Some("invoke_agent"));
        assert!(
            simple
                .iter()
                .filter(|n| operation_name(n) == Some("chat"))
                .count()
                == 1
        );

        let rag = profile(only(CallForm::Rag)).build_tree(&mut rng);
        assert!(rag.iter().any(|n| operation_name(n) == Some("embeddings")));

        let plan = profile(only(CallForm::PlanExecuteReflect)).build_tree(&mut rng);
        assert!(plan.iter().any(is_generic_wrapper));
        assert!(
            plan.iter()
                .filter(|n| operation_name(n) == Some("chat"))
                .count()
                == 3
        );

        let tools = profile(only(CallForm::ToolLoop)).build_tree(&mut rng);
        // turn 1 chat fans its tools out in parallel
        assert!(tools
            .iter()
            .any(|n| operation_name(n) == Some("chat") && n.parallel_children));
    }

    fn pairs(items: &[(&str, u32)]) -> HashMap<String, u32> {
        items.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn weights_from_pairs_maps_forms_and_rejects_domain_errors() {
        // String tokenisation (malformed/non-numeric) is the config layer's concern; here we test
        // only the domain rules: known form names and a positive total.
        let w = pairs(&[
            ("simple_chat", 1),
            ("tool_loop", 3),
            ("plan_execute_reflect", 2),
            ("rag", 1),
        ]);
        assert_eq!(
            ProfileWeights::from_pairs(&w).unwrap(),
            ProfileWeights::default()
        );
        // unknown form name
        assert!(ProfileWeights::from_pairs(&pairs(&[("bogus", 1)])).is_err());
        // zero total (single zero weight and the empty map both)
        assert!(ProfileWeights::from_pairs(&pairs(&[("simple_chat", 0)])).is_err());
        assert!(ProfileWeights::from_pairs(&HashMap::new()).is_err());
    }

    /// Every span of the built tree carries the same pooled `gen_ai.conversation.id` — the
    /// profile, not the generator, owns conversation tagging.
    #[test]
    fn build_tree_stamps_one_conversation_id_on_every_span() {
        let profile = LlmSpanProfile {
            max_tool_calls: 3,
            capture_content: false,
            weights: ProfileWeights::default(),
            conversations: cursor(),
            budget: None,
        };
        let mut rng = StdRng::seed_from_u64(9);
        let nodes = profile.build_tree(&mut rng);
        let id_of = |n: &SpanNode| {
            n.attributes
                .iter()
                .find_map(|(k, v)| match (k.as_str(), v) {
                    ("gen_ai.conversation.id", AttrValue::Str(s)) => Some(s.clone()),
                    _ => None,
                })
        };
        let first = id_of(&nodes[0]).expect("conversation.id present on root");
        assert!(
            nodes
                .iter()
                .all(|n| id_of(n).as_deref() == Some(first.as_str())),
            "all spans share one conversation id"
        );
    }

    /// Weighted selection is reproducible under a seed and honours zero weights.
    #[test]
    fn weighted_form_selection_is_seed_deterministic() {
        let profile = LlmSpanProfile {
            max_tool_calls: 2,
            capture_content: false,
            weights: only(CallForm::Rag),
            conversations: cursor(),
            budget: None,
        };
        // RAG is the only weighted form, so every draw must produce an embeddings span.
        for seed in 0..50 {
            let mut rng = StdRng::seed_from_u64(seed);
            let nodes = profile.build_tree(&mut rng);
            assert!(nodes
                .iter()
                .any(|n| operation_name(n) == Some("embeddings")));
        }
    }

    /// Build a budgeted tree with the given bounds under a seed.
    fn budgeted(min: u32, max: u32, seed: u64) -> Vec<SpanNode> {
        let profile = LlmSpanProfile {
            max_tool_calls: 3,
            capture_content: false,
            weights: ProfileWeights::default(),
            conversations: cursor(),
            budget: Some(SpanBudget { min, max }),
        };
        let mut rng = StdRng::seed_from_u64(seed);
        profile.build_tree(&mut rng)
    }

    #[test]
    fn budget_fixed_size_yields_exact_span_count() {
        // min == max: every trace has exactly that many spans (edge-case mode).
        for seed in 0..100 {
            assert_eq!(budgeted(20, 20, seed).len(), 20, "seed {seed}");
        }
    }

    #[test]
    fn budget_range_stays_within_bounds() {
        for seed in 0..500 {
            let n = budgeted(5, 40, seed).len();
            assert!((5..=40).contains(&n), "seed {seed}: span count {n} outside [5,40]");
        }
    }

    #[test]
    fn budget_of_one_is_root_only() {
        let nodes = budgeted(1, 1, 7);
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].parent.is_none());
        assert_eq!(operation_name(&nodes[0]), Some("invoke_agent"));
    }

    #[test]
    fn budgeted_tree_is_a_valid_single_rooted_tree() {
        // Exactly one root; every child references an in-range, non-self parent.
        let nodes = budgeted(500, 500, 3);
        assert_eq!(nodes.len(), 500);
        assert_eq!(nodes.iter().filter(|n| n.parent.is_none()).count(), 1);
        for (i, n) in nodes.iter().enumerate() {
            if let Some(p) = n.parent {
                assert!(p < nodes.len() && p != i, "node {i} parent {p} out of range");
            }
        }
    }

    #[test]
    fn budgeted_tree_lays_out_without_panic() {
        // Feed a large budgeted tree through the real time layout: nesting holds and 500 spans do
        // not overflow the recursive layout (turns are siblings, depth stays <= 3).
        let nodes = budgeted(500, 500, 11);
        let parents: Vec<Option<usize>> = nodes.iter().map(|n| n.parent).collect();
        let durations: Vec<i64> = nodes.iter().map(|n| n.duration_ns).collect();
        let parallel: Vec<bool> = nodes.iter().map(|n| n.parallel_children).collect();
        let windows = crate::message::traces::trace_time::layout_span_tree(
            0, &parents, &durations, 1_000_000, &parallel,
        )
        .unwrap();
        assert_eq!(windows.len(), 500);
    }

    #[test]
    fn budget_stamps_one_conversation_id_on_every_span() {
        let nodes = budgeted(30, 30, 5);
        let id_of = |n: &SpanNode| {
            n.attributes.iter().find_map(|(k, v)| match (k.as_str(), v) {
                ("gen_ai.conversation.id", AttrValue::Str(s)) => Some(s.clone()),
                _ => None,
            })
        };
        let first = id_of(&nodes[0]).expect("conversation.id present on root");
        assert!(nodes.iter().all(|n| id_of(n).as_deref() == Some(first.as_str())));
    }

    #[test]
    fn no_budget_keeps_natural_form_shapes() {
        // Sanity: with budget None, simple_chat still yields its 2-span shape.
        let profile = LlmSpanProfile {
            max_tool_calls: 0,
            capture_content: false,
            weights: only(CallForm::SimpleChat),
            conversations: cursor(),
            budget: None,
        };
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(profile.build_tree(&mut rng).len(), 2);
    }
}
