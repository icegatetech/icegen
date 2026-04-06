pub mod attributes;
pub mod fake_data;
pub mod generator;
pub mod metrics_generator;
pub mod metrics_types;
pub mod types;

pub use attributes::AttributeGenerator;
pub use fake_data::FakeDataGenerator;
pub use generator::OTLPLogMessageGenerator;
pub use metrics_generator::OTLPMetricsMessageGenerator;
pub use metrics_types::OTLPMetricsMessage;
pub use types::{OTLPLogMessage, OTLPLogMessageType};

// Re-export MessagePayload from its new canonical location for convenience
pub use crate::transport::types::MessagePayload;
