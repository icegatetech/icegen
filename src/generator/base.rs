use crate::config::BatchResult;
use crate::error::Result;
use crate::message::OTLPMessage;
use crate::transport::SendOutcome;
use async_trait::async_trait;
use tokio::sync::watch;

#[async_trait]
pub trait SignalGenerator: Send + Sync {
    #[allow(clippy::result_large_err)]
    fn generate_message(&self) -> Result<OTLPMessage>;
    async fn send_message(
        &self,
        message: &OTLPMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<SendOutcome>;
    async fn send_messages_batch(
        &self,
        count: usize,
        message_interval_ms: u64,
    ) -> Result<BatchResult>;
    async fn close(&self) -> Result<()>;
}
