pub mod cli;
pub mod config;
pub mod error;
pub mod generator;
pub mod message;
pub mod pb;
pub mod transport;

pub use cli::{Cli, GeneratorType, OtelArgs};
pub use config::{BatchResult, OtelConfig, RetryConfig};
pub use error::{GeneratorError, Result};
pub use generator::{LogGenerator, OtelLogGenerator, OtelMetricsGenerator};
pub use message::{MessagePayload, OTLPLogMessage, OTLPLogMessageGenerator, OTLPLogMessageType};
pub use transport::types::{MessageType, OTLPMessage, SignalPath};
pub use transport::{GrpcTransport, HttpTransport, Transport};
