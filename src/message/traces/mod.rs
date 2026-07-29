pub mod conversation;
pub mod span_profile;
pub mod trace_encoder;
pub mod trace_generator;
pub mod trace_plan;
pub mod trace_time;

pub use conversation::ConversationCursor;
pub use trace_encoder::{TraceEncoder, TraceJsonEncoder, TraceProtobufEncoder};
pub use trace_generator::TraceMessageGenerator;
pub use trace_plan::{
    AttrValue, PlannedEvent, PlannedResourceSpans, PlannedScopeSpans, PlannedSpan, PlannedTraces,
    SpanAttrs, SpanKind, SpanStatusCode,
};
// Orchestration handles for the planners only; not part of the public library API.
pub(crate) use trace_plan::{SpanAnchor, TraceCorrelation};
