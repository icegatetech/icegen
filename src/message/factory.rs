//! Signal-specific message factory. The orchestrator (`OtelGenerator`) selects tenant/cloud/
//! shards and delegates message construction to the factory. Logs and traces are separate
//! implementations.

use crate::error::Result;
use crate::message::log_generator::OTLPLogMessageGenerator;
use crate::message::traces::trace_generator::TraceMessageGenerator;
use crate::message::types::OTLPMessage;
use crate::message::ServiceShard;

/// Context of a single message, selected by the orchestrator.
pub struct GenContext {
    pub tenant_id: Option<String>,
    pub cloud_account_id: Option<String>,
    pub shards: Vec<ServiceShard>,
    pub invalid: bool,
}

/// Builds an OTLP message for a specific signal.
pub trait MessageFactory: Send + Sync {
    #[allow(clippy::result_large_err)]
    fn build(&self, ctx: GenContext) -> Result<OTLPMessage>;
}

/// Logs: proxies to the existing `OTLPLogMessageGenerator`.
pub struct LogMessageFactory {
    pub generator: OTLPLogMessageGenerator,
}

impl MessageFactory for LogMessageFactory {
    fn build(&self, ctx: GenContext) -> Result<OTLPMessage> {
        if ctx.invalid {
            self.generator.generate_invalid_message(ctx.tenant_id)
        } else {
            self.generator
                .generate_message(ctx.tenant_id, ctx.cloud_account_id, ctx.shards)
        }
    }
}

/// Traces: proxies to `TraceMessageGenerator`. Invalid traces are a non-goal for v1; the config
/// rejects `invalid_record_percent > 0` when signal=traces (see `OtelConfig::validate`),
/// so `ctx.invalid` is always `false` here.
pub struct TraceMessageFactory {
    pub generator: TraceMessageGenerator,
}

impl MessageFactory for TraceMessageFactory {
    fn build(&self, ctx: GenContext) -> Result<OTLPMessage> {
        self.generator
            .generate_message(ctx.tenant_id, ctx.cloud_account_id, ctx.shards)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::traces::conversation::ConversationPool;
    use crate::message::traces::span_profile::{LlmSpanProfile, ProfileWeights};
    use crate::message::traces::trace_encoder::TraceJsonEncoder;
    use crate::message::types::Signal;
    use std::sync::Arc;

    #[test]
    fn trace_factory_builds_trace_signal() {
        let factory = TraceMessageFactory {
            generator: TraceMessageGenerator::new(
                "src".to_string(),
                Arc::new(TraceJsonEncoder),
                Arc::new(LlmSpanProfile {
                    max_tool_calls: 1,
                    capture_content: false,
                    weights: ProfileWeights::default(),
                    conversations: ConversationPool::shared_default(&mut rand::thread_rng()),
                }),
            ),
        };
        let msg = factory
            .build(GenContext {
                tenant_id: Some("t1".to_string()),
                cloud_account_id: None,
                shards: vec![ServiceShard {
                    service_name: Some("svc".to_string()),
                    num_records: 1,
                }],
                invalid: false,
            })
            .unwrap();
        assert_eq!(msg.signal, Signal::Traces);
    }
}
