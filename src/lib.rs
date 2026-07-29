pub mod cli;
pub mod config;
pub mod error;
pub mod generator;
pub mod message;
pub mod pb;
pub mod report;
pub mod transport;

pub use cli::{reject_retired_env_vars, Cli, GeneratorType, OtelArgs};
pub use config::{GenerationStats, OtelConfig, RetryConfig, SignalCounters, TimestampJitterConfig};
pub use error::{GeneratorError, Result};
pub use generator::{OtelGenerator, SignalGenerator};
pub use message::traces::{
    TraceEncoder, TraceJsonEncoder, TraceMessageGenerator, TraceProtobufEncoder,
};
pub use message::{
    LogEncoder, LogJsonEncoder, LogProtobufEncoder, MessagePayload, OTLPLogMessageGenerator,
    OTLPMessage, OTLPMessageType, ServiceShard, Signal,
};
