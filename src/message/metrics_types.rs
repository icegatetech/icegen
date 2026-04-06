use crate::transport::types::{MessagePayload, MessageType, OTLPMessage, SignalPath};

#[derive(Debug, Clone)]
pub struct OTLPMetricsMessage {
    pub message: OTLPMessage,
}

impl OTLPMetricsMessage {
    pub fn new(
        payload: MessagePayload,
        tenant_id: String,
        project_id: String,
        source: String,
        message_type: MessageType,
    ) -> Self {
        Self {
            message: OTLPMessage {
                payload,
                tenant_id,
                project_id,
                source,
                message_type,
                signal_path: SignalPath::Metrics,
            },
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.message.tenant_id
    }

    pub fn project_id(&self) -> &str {
        &self.message.project_id
    }

    pub fn source(&self) -> &str {
        &self.message.source
    }

    pub fn message_type(&self) -> MessageType {
        self.message.message_type
    }

    pub fn payload(&self) -> &MessagePayload {
        &self.message.payload
    }

    pub fn payload_size_bytes(&self) -> usize {
        self.message.payload_size_bytes()
    }

    pub fn as_otlp_message(&self) -> &OTLPMessage {
        &self.message
    }
}

/// Metric names pool following OpenTelemetry semantic conventions.
pub const GAUGE_METRIC_NAMES: &[&str] = &[
    "system.cpu.utilization",
    "system.memory.utilization",
    "system.network.io.queue_depth",
    "process.runtime.jvm.memory.usage",
    "system.disk.utilization",
];

pub const SUM_METRIC_NAMES: &[&str] = &[
    "http.server.request.count",
    "network.bytes_transferred",
    "http.server.error.count",
    "system.network.packets",
    "process.runtime.jvm.gc.count",
];

pub const HISTOGRAM_METRIC_NAMES: &[&str] = &[
    "http.server.request.duration",
    "http.server.response.body.size",
    "rpc.server.duration",
    "db.client.operation.duration",
];

pub const EXPONENTIAL_HISTOGRAM_METRIC_NAMES: &[&str] = &[
    "http.client.request.duration",
    "messaging.process.duration",
];

pub const SUMMARY_METRIC_NAMES: &[&str] = &[
    "http.server.request.duration.summary",
    "rpc.client.duration.summary",
];

pub const HISTOGRAM_BOUNDARIES: &[f64] =
    &[0.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

pub const SUMMARY_QUANTILES: &[f64] = &[0.5, 0.9, 0.95, 0.99];
