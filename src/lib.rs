
pub mod cli;
pub mod config;
pub mod error;
pub mod generator;
pub mod message;
pub mod pb;
pub mod transport;

pub use cli::{Cli, GeneratorType, OtelArgs};
pub use config::{BatchResult, OtelConfig, RetryConfig, TimestampJitterConfig};
pub use error::{GeneratorError, Result};
pub use generator::{LogGenerator, OtelLogGenerator};
pub use message::{
    JsonEncoder, MessagePayload, OTLPLogMessage, OTLPLogMessageGenerator, OTLPLogMessageType,
    OtlpEncoder, ProtobufEncoder, ServiceShard,
};
