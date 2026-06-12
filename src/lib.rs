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
pub use generator::{OtelGenerator, SignalGenerator};
pub use message::traces::{
    TraceEncoder, TraceJsonEncoder, TraceMessageGenerator, TraceProtobufEncoder,
};
pub use message::{
    JsonEncoder, MessagePayload, OTLPLogMessageGenerator, OTLPMessage, OTLPMessageType,
    OtlpEncoder, ProtobufEncoder, ServiceShard, Signal,
};
