pub mod grpc;
pub mod http;
pub mod protobuf;
pub mod types;

use crate::error::Result;
use async_trait::async_trait;
use tokio::sync::watch;
use types::OTLPMessage;

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(
        &self,
        message: &OTLPMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<()>;
}

pub use grpc::GrpcTransport;
pub use http::HttpTransport;
