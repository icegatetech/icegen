//! Log-signal message generation. Mirrors the `traces` submodule layout: `log_generator` owns
//! orchestration (plan → encode), while the signal-specific pieces live beside it — `log_time`
//! (timestamp layout), `log_attrs` (record content), `log_encoder` (wire format), and `log_plan`
//! (format-neutral plan types).

pub mod log_attrs;
pub mod log_encoder;
pub mod log_generator;
pub mod log_plan;
pub mod log_time;

pub use log_encoder::{LogEncoder, LogJsonEncoder, LogProtobufEncoder};
pub use log_generator::OTLPLogMessageGenerator;
pub use log_plan::{PlannedRequest, PlannedShard};
