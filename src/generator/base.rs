use crate::config::GenerationStats;
use crate::error::Result;
use crate::message::OTLPMessage;
use async_trait::async_trait;

/// A generator that produces and sends telemetry for one or more signals.
///
/// One *generation cycle* yields one [`OTLPMessage`] per configured signal (see
/// [`Self::generate_messages`]); the batch/continuous operations schedule cycles and send each
/// cycle's messages, tracking results in [`GenerationStats`].
#[async_trait]
pub trait SignalGenerator: Send + Sync {
    /// Build one [`OTLPMessage`] per configured signal for a single generation cycle, in signal
    /// order (the order returned drives diagnostics and per-signal statistics).
    ///
    /// # Errors
    ///
    /// Returns any planning/encoding error from the underlying generators; all messages are built
    /// before any is returned, so a failure yields no partial cycle.
    #[allow(clippy::result_large_err)]
    fn generate_messages(&self) -> Result<Vec<OTLPMessage>>;

    /// Run `count` generation cycles distributed across workers, applying `message_interval_ms` as
    /// the minimum spacing between cycle starts (`0` disables pacing).
    ///
    /// # Returns
    ///
    /// The merged [`GenerationStats`] across all workers for the run.
    ///
    /// # Errors
    ///
    /// Returns the first error propagated by a worker (e.g. a planning/encoding failure); transport
    /// send failures are recorded in the returned stats rather than surfaced as an error.
    async fn send_messages_batch(
        &self,
        count: usize,
        message_interval_ms: u64,
    ) -> Result<GenerationStats>;

    /// Release any resources held by the generator once sending is complete.
    ///
    /// # Errors
    ///
    /// Returns an error if a resource fails to shut down cleanly.
    async fn close(&self) -> Result<()>;
}
