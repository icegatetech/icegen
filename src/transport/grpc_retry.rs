//! Shared gRPC retry with exponential backoff and cooperative shutdown, used by the log and
//! trace transports (DRY: one attempt-loop implementation for both signals).

use crate::config::RetryConfig;
use crate::error::GeneratorError;
use crate::transport::SendOutcome;
use std::future::Future;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

/// Run a gRPC call with exponential backoff and cooperative shutdown.
///
/// `attempt` is invoked on every try and returns `Result<(), tonic::Status>`.
/// Only ResourceExhausted/Unavailable/Aborted/DeadlineExceeded codes are retried.
///
/// # Parameters
///
/// * `retry_config` — attempt limit and backoff.
/// * `shutdown_rx` — cooperative-stop channel; the backoff wait is interrupted on signal.
/// * `attempt` — closure producing the future of a single export attempt.
pub async fn run_with_retry<F, Fut>(
    retry_config: &RetryConfig,
    shutdown_rx: &watch::Receiver<bool>,
    mut attempt: F,
) -> SendOutcome
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), tonic::Status>>,
{
    let max_retries = retry_config.max_retries;
    let mut shutdown_rx = shutdown_rx.clone();

    for attempt_idx in 0..=max_retries {
        match attempt().await {
            Ok(()) => {
                if attempt_idx > 0 {
                    eprintln!("  \u{2713} Request succeeded after {} retries", attempt_idx);
                }
                return SendOutcome::Success {
                    retries: attempt_idx as usize,
                };
            }
            Err(status) if is_retryable_grpc_code(status.code()) => {
                if attempt_idx == max_retries {
                    // Normalize a terminal deadline into the dedicated Timeout variant so
                    // SendOutcome::is_timeout() and the timeout counter recognize it.
                    let error = if status.code() == tonic::Code::DeadlineExceeded {
                        GeneratorError::Timeout
                    } else {
                        GeneratorError::GrpcError(status)
                    };
                    return SendOutcome::Failure {
                        retries: attempt_idx as usize,
                        error,
                    };
                }
                let delay = retry_config.compute_delay(attempt_idx, None);
                eprintln!(
                    " \u{26a0} Retry[grpc]: {}, attempt {}/{}, waiting {}ms, error: {}",
                    status.code(),
                    attempt_idx + 1,
                    max_retries + 1,
                    delay,
                    status.message()
                );
                if *shutdown_rx.borrow() {
                    return SendOutcome::Failure {
                        retries: attempt_idx as usize,
                        error: GeneratorError::Interrupted,
                    };
                }
                let sleep_fut = sleep(Duration::from_millis(delay));
                tokio::pin!(sleep_fut);
                loop {
                    tokio::select! {
                        _ = &mut sleep_fut => break,
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() {
                                // Sender dropped: no shutdown signal will arrive.
                                // Preserve the remaining backoff delay instead of
                                // spinning hot and hammering the collector.
                                (&mut sleep_fut).await;
                                break;
                            }
                            if *shutdown_rx.borrow() {
                                return SendOutcome::Failure {
                                    retries: attempt_idx as usize,
                                    error: GeneratorError::Interrupted,
                                };
                            }
                        }
                    }
                }
            }
            Err(status) => {
                return SendOutcome::Failure {
                    retries: attempt_idx as usize,
                    error: GeneratorError::GrpcError(status),
                };
            }
        }
    }
    unreachable!("all loop paths return explicitly")
}

/// gRPC codes worth retrying.
pub fn is_retryable_grpc_code(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::ResourceExhausted
            | tonic::Code::Unavailable
            | tonic::Code::Aborted
            | tonic::Code::DeadlineExceeded
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn succeeds_first_attempt() {
        let cfg = RetryConfig::new(3, 1000, 2000).unwrap();
        let (_tx, rx) = watch::channel(false);
        let calls = AtomicUsize::new(0);
        let outcome = run_with_retry(&cfg, &rx, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<(), tonic::Status>(()) }
        })
        .await;
        assert!(outcome.is_success());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn non_retryable_fails_immediately() {
        let cfg = RetryConfig::new(3, 1000, 2000).unwrap();
        let (_tx, rx) = watch::channel(false);
        let calls = AtomicUsize::new(0);
        let outcome = run_with_retry(&cfg, &rx, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), tonic::Status>(tonic::Status::invalid_argument("bad")) }
        })
        .await;
        assert!(matches!(
            outcome,
            SendOutcome::Failure {
                error: GeneratorError::GrpcError(_),
                ..
            }
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "non-retryable must not retry"
        );
    }

    #[tokio::test]
    async fn retries_until_max_then_fails() {
        let cfg = RetryConfig::new(2, 100, 200).unwrap();
        let (_tx, rx) = watch::channel(false);
        let calls = AtomicUsize::new(0);
        let outcome = run_with_retry(&cfg, &rx, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), tonic::Status>(tonic::Status::unavailable("down")) }
        })
        .await;
        assert!(matches!(outcome, SendOutcome::Failure { .. }));
        // 1 initial + 2 retries = 3 attempts.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
