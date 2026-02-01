pub mod grpc;
pub mod http;
pub mod protobuf;

use crate::error::Result;
use crate::message::OTLPLogMessage;
use async_trait::async_trait;

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, message: &OTLPLogMessage) -> Result<()>;
}

pub use grpc::GrpcTransport;
pub use http::HttpTransport;
