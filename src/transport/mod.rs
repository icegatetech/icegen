pub mod grpc;
pub mod http;
pub mod protobuf;

use crate::error::Result;
use crate::message::OTLPLogMessage;
use async_trait::async_trait;
use tokio::sync::watch;

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<()>;
}

pub use grpc::GrpcTransport;
pub use http::HttpTransport;
