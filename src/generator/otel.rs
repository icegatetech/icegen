use crate::config::{BatchResult, OtelConfig};
use crate::error::Result;
use crate::generator::base::LogGenerator;
use crate::message::{MessagePayload, OTLPLogMessage, OTLPLogMessageGenerator};
use crate::transport::{GrpcTransport, HttpTransport, Transport};
use async_trait::async_trait;
use rand::Rng;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, Instant};

#[derive(Clone)]
pub struct OtelLogGenerator {
    config: OtelConfig,
    message_generator: OTLPLogMessageGenerator,
    transport: Arc<dyn Transport>,
}

struct ProgressTracker {
    total_target: Option<usize>,
    started_at: Instant,
    last_progress_at: Mutex<Instant>,
    last_reported_step: AtomicUsize,
    processed: AtomicUsize,
    total_payload_bytes: AtomicUsize,
    window_payload_bytes: AtomicUsize,
}

impl ProgressTracker {
    fn new(total_target: Option<usize>) -> Self {
        let now = Instant::now();
        Self {
            total_target,
            started_at: now,
            last_progress_at: Mutex::new(now),
            last_reported_step: AtomicUsize::new(0),
            processed: AtomicUsize::new(0),
            total_payload_bytes: AtomicUsize::new(0),
            window_payload_bytes: AtomicUsize::new(0),
        }
    }

    fn record(&self, payload_size_bytes: usize) -> usize {
        if payload_size_bytes > 0 {
            self.total_payload_bytes
                .fetch_add(payload_size_bytes, Ordering::Relaxed);
            self.window_payload_bytes
                .fetch_add(payload_size_bytes, Ordering::Relaxed);
        }

        self.processed.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn maybe_log(&self, processed: usize) {
        if processed % 10 != 0 {
            return;
        }

        let step = processed / 10;
        if self
            .last_reported_step
            .compare_exchange(step - 1, step, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let total_elapsed_secs = self.started_at.elapsed().as_secs_f64().max(f64::EPSILON);
        let total_payload_bytes = self.total_payload_bytes.load(Ordering::Relaxed);
        let total_sent_mib = total_payload_bytes as f64 / 1024.0 / 1024.0;
        let avg_speed_mib_s = total_sent_mib / total_elapsed_secs;

        let window_payload_bytes = self.window_payload_bytes.swap(0, Ordering::Relaxed);
        let mut last_progress_at = self
            .last_progress_at
            .lock()
            .expect("progress mutex poisoned");
        let window_elapsed_secs = last_progress_at.elapsed().as_secs_f64().max(f64::EPSILON);
        *last_progress_at = Instant::now();
        let current_speed_mib_s =
            (window_payload_bytes as f64 / 1024.0 / 1024.0) / window_elapsed_secs;

        match self.total_target {
            Some(total_target) => println!(
                "Progress: {}/{} messages processed, payload sent: {:.4} MiB, throughput: avg {:.4} MiB/s, current {:.4} MiB/s",
                processed,
                total_target,
                total_sent_mib,
                avg_speed_mib_s,
                current_speed_mib_s
            ),
            None => println!(
                "Progress: {} messages processed, payload sent: {:.4} MiB, throughput: avg {:.4} MiB/s, current {:.4} MiB/s",
                processed,
                total_sent_mib,
                avg_speed_mib_s,
                current_speed_mib_s
            ),
        }
    }
}

struct BatchPacer {
    delay: Duration,
    next_slot: tokio::sync::Mutex<Instant>,
}

impl BatchPacer {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            next_slot: tokio::sync::Mutex::new(Instant::now()),
        }
    }

    async fn wait_turn(&self, shutdown_rx: &mut watch::Receiver<bool>) -> bool {
        let now = Instant::now();
        let wait_until = {
            let mut next_slot = self.next_slot.lock().await;
            let wait_until = (*next_slot).max(now);
            *next_slot = wait_until + self.delay;
            wait_until
        };

        if wait_until <= now {
            return true;
        }

        tokio::select! {
            _ = tokio::time::sleep_until(wait_until) => true,
            changed = shutdown_rx.changed() => !(changed.is_ok() && *shutdown_rx.borrow()),
        }
    }
}

type BatchFuture<'a> = Pin<Box<dyn Future<Output = Result<BatchResult>> + Send + 'a>>;

impl OtelLogGenerator {
    fn batch_counts_per_worker(total: usize, concurrency: usize) -> Vec<usize> {
        let workers = concurrency.max(1);
        let base = total / workers;
        let remainder = total % workers;

        (0..workers)
            .map(|idx| base + usize::from(idx < remainder))
            .collect()
    }

    pub async fn new(config: OtelConfig) -> Result<Self> {
        config.validate()?;

        println!("Initializing OTEL Log Generator...");
        println!("  Endpoint: {}", config.ingest_endpoint);
        println!("  Transport: {}", config.transport);
        println!("  Use Protobuf: {}", config.use_protobuf);
        println!("  Records per message: {}", config.records_per_message);
        println!("  Invalid record %: {}", config.invalid_record_percent);
        println!("  Concurrency: {}", config.concurrency);

        let retry_config = config.retry_config()?;
        println!(
            "  Retry: max_retries={}, base_delay={}ms, max_delay={}ms",
            retry_config.max_retries, retry_config.base_delay_ms, retry_config.max_delay_ms
        );
        println!(
            "  Label cardinality limiting: {}",
            config.label_cardinality_enabled
        );

        let transport: Arc<dyn Transport> = match config.transport.as_str() {
            "http" => {
                let http_transport = HttpTransport::new(
                    config.ingest_endpoint.clone(),
                    config.use_protobuf,
                    retry_config,
                    config.org_id.clone(),
                )?;

                if let Some(ref health_endpoint) = config.healthcheck_endpoint {
                    println!("Performing health check: {}", health_endpoint);
                    match http_transport.health_check(health_endpoint).await {
                        Ok(_) => println!("✓ Health check passed"),
                        Err(e) => {
                            eprintln!("✗ Health check failed: {}", e);
                            return Err(e);
                        }
                    }
                }

                Arc::new(http_transport)
            }
            "grpc" => {
                let grpc_transport =
                    GrpcTransport::new(config.ingest_endpoint.clone(), retry_config).await?;
                Arc::new(grpc_transport)
            }
            _ => unreachable!(),
        };

        Self::with_transport(config, transport)
    }

    pub(crate) fn with_transport(
        config: OtelConfig,
        transport: Arc<dyn Transport>,
    ) -> Result<Self> {
        config.validate()?;

        let cardinality_config = config.label_cardinality_config()?;
        let message_generator = OTLPLogMessageGenerator::new_with_cardinality(
            "rust-generator".to_string(),
            cardinality_config,
        );
        println!("✓ Generator initialized successfully\n");

        Ok(Self {
            config,
            message_generator,
            transport,
        })
    }

    pub async fn run_continuous(&self, shutdown_rx: watch::Receiver<bool>) -> Result<BatchResult> {
        let progress = (!self.config.print_logs).then(|| Arc::new(ProgressTracker::new(None)));
        let mut workers = JoinSet::new();

        for _ in 0..self.config.concurrency {
            let worker = self.clone();
            let shutdown_rx = shutdown_rx.clone();
            let progress = progress.clone();
            let message_interval_ms = self.config.message_interval_ms;
            workers.spawn(async move {
                worker
                    .run_continuous_worker(message_interval_ms, shutdown_rx, progress)
                    .await
            });
        }

        let mut result = BatchResult::new();
        while let Some(worker_result) = workers.join_next().await {
            result.merge(worker_result??);
        }

        Ok(result)
    }

    async fn run_continuous_worker(
        &self,
        message_interval_ms: u64,
        shutdown_rx: watch::Receiver<bool>,
        progress: Option<Arc<ProgressTracker>>,
    ) -> Result<BatchResult> {
        self.run_continuous_worker_with_runner(
            message_interval_ms,
            shutdown_rx,
            progress,
            |generator, message_interval_ms, shutdown_rx, progress| {
                let _ = message_interval_ms;
                Box::pin(generator.run_worker_batch(1, shutdown_rx, progress, None))
            },
        )
        .await
    }

    async fn run_continuous_worker_with_runner<F>(
        &self,
        message_interval_ms: u64,
        mut shutdown_rx: watch::Receiver<bool>,
        progress: Option<Arc<ProgressTracker>>,
        mut run_batch: F,
    ) -> Result<BatchResult>
    where
        F: for<'a> FnMut(
            &'a Self,
            u64,
            &'a mut watch::Receiver<bool>,
            Option<Arc<ProgressTracker>>,
        ) -> BatchFuture<'a>,
    {
        let mut result = BatchResult::new();

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            let batch =
                match run_batch(self, message_interval_ms, &mut shutdown_rx, progress.clone())
                    .await
                {
                Ok(batch) => batch,
                Err(error) => {
                    eprintln!("Continuous worker iteration failed, continuing: {}", error);

                    if *shutdown_rx.borrow() {
                        break;
                    }

                    continue;
                }
            };
            let processed = batch.total;
            result.merge(batch);

            if processed == 0 && *shutdown_rx.borrow() {
                break;
            }

            if message_interval_ms > 0 {
                tokio::select! {
                    _ = sleep(Duration::from_millis(message_interval_ms)) => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    async fn run_worker_batch(
        &self,
        count: usize,
        shutdown_rx: &mut watch::Receiver<bool>,
        progress: Option<Arc<ProgressTracker>>,
        batch_pacer: Option<&BatchPacer>,
    ) -> Result<BatchResult> {
        let mut result = BatchResult::new();

        for _ in 0..count {
            if *shutdown_rx.borrow() {
                break;
            }

            if let Some(batch_pacer) = batch_pacer {
                if !batch_pacer.wait_turn(shutdown_rx).await {
                    break;
                }
            }

            let message = self.generate_message()?;
            let payload_size_bytes = message.payload_size_bytes();
            let success = self.send_message(&message, shutdown_rx).await?;

            let sent_payload_bytes = if success {
                result.add_success();
                payload_size_bytes
            } else {
                result.add_failure();
                0
            };

            if let Some(progress) = &progress {
                let processed = progress.record(sent_payload_bytes);
                progress.maybe_log(processed);
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl LogGenerator for OtelLogGenerator {
    fn generate_message(&self) -> Result<OTLPLogMessage> {
        let mut rng = rand::thread_rng();

        let should_be_invalid = rng.gen::<f32>() * 100.0 < self.config.invalid_record_percent;

        let message = if should_be_invalid {
            self.message_generator.generate_invalid_message()?
        } else if self.config.transport == "grpc" || self.config.use_protobuf {
            self.message_generator
                .generate_protobuf_message(self.config.records_per_message)?
        } else {
            self.message_generator
                .generate_aggregated_message(self.config.records_per_message)?
        };

        Ok(message)
    }

    async fn send_message(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<bool> {
        if self.config.print_logs {
            println!("Sending message:");
            println!("  Project ID: {}", message.project_id);
            println!("  Source: {}", message.source);
            println!("  Type: {:?}", message.message_type);
            match &message.message {
                MessagePayload::Json(json) => {
                    println!(
                        "  Payload: {}",
                        serde_json::to_string_pretty(json).unwrap_or_default()
                    );
                }
                MessagePayload::Protobuf(bytes) => {
                    println!("  Payload: <protobuf {} bytes>", bytes.len());
                }
                MessagePayload::MalformedJson(s) => {
                    println!("  Payload: {}", s);
                }
            }
        }

        match self.transport.send(message, shutdown_rx).await {
            Ok(_) => {
                if self.config.print_logs {
                    println!("✓ Message sent successfully\n");
                }
                Ok(true)
            }
            Err(e) => {
                eprintln!("✗ Failed to send message: {}", e);
                Ok(false)
            }
        }
    }

    async fn send_messages_batch(
        &self,
        count: usize,
        message_interval_ms: u64,
    ) -> Result<BatchResult> {
        let total_target = count;
        let progress =
            (!self.config.print_logs).then(|| Arc::new(ProgressTracker::new(Some(total_target))));
        let mut workers = JoinSet::new();
        let (shutdown_tx, _) = watch::channel(false);
        let batch_pacer = (message_interval_ms > 0)
            .then(|| Arc::new(BatchPacer::new(Duration::from_millis(message_interval_ms))));

        for worker_count in Self::batch_counts_per_worker(count, self.config.concurrency) {
            let worker = self.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            let progress = progress.clone();
            let batch_pacer = batch_pacer.clone();
            workers.spawn(async move {
                worker
                    .run_worker_batch(
                        worker_count,
                        &mut shutdown_rx,
                        progress,
                        batch_pacer.as_deref(),
                    )
                    .await
            });
        }

        let mut result = BatchResult::new();
        while let Some(worker_result) = workers.join_next().await {
            result.merge(worker_result??);
        }

        Ok(result)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::GeneratorError;
    use crate::transport::Transport;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct NoopTransport;

    struct CountingTransport {
        started: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        send_delay: Duration,
    }

    struct RetryAwareTransport {
        started: AtomicUsize,
        retry_wait: Duration,
    }

    struct TimestampTransport {
        started: AtomicUsize,
        timestamps: Mutex<Vec<std::time::Instant>>,
    }

    impl CountingTransport {
        fn new(send_delay: Duration) -> Self {
            Self {
                started: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                send_delay,
            }
        }

        fn started(&self) -> usize {
            self.started.load(Ordering::SeqCst)
        }

        fn max_active(&self) -> usize {
            self.max_active.load(Ordering::SeqCst)
        }
    }

    impl RetryAwareTransport {
        fn new(retry_wait: Duration) -> Self {
            Self {
                started: AtomicUsize::new(0),
                retry_wait,
            }
        }

        fn started(&self) -> usize {
            self.started.load(Ordering::SeqCst)
        }
    }

    impl TimestampTransport {
        fn new() -> Self {
            Self {
                started: AtomicUsize::new(0),
                timestamps: Mutex::new(Vec::new()),
            }
        }

        fn timestamps(&self) -> Vec<std::time::Instant> {
            self.timestamps
                .lock()
                .expect("timestamps mutex poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl Transport for NoopTransport {
        async fn send(
            &self,
            _message: &OTLPLogMessage,
            _shutdown_rx: &watch::Receiver<bool>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl Transport for CountingTransport {
        async fn send(
            &self,
            _message: &OTLPLogMessage,
            _shutdown_rx: &watch::Receiver<bool>,
        ) -> Result<()> {
            self.started.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;

            loop {
                let current_max = self.max_active.load(Ordering::SeqCst);
                if active <= current_max {
                    break;
                }
                if self
                    .max_active
                    .compare_exchange(current_max, active, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }

            tokio::time::sleep(self.send_delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl Transport for RetryAwareTransport {
        async fn send(
            &self,
            _message: &OTLPLogMessage,
            shutdown_rx: &watch::Receiver<bool>,
        ) -> Result<()> {
            self.started.fetch_add(1, Ordering::SeqCst);

            let mut shutdown_rx = shutdown_rx.clone();
            tokio::select! {
                _ = tokio::time::sleep(self.retry_wait) => {
                    self.started.fetch_add(1, Ordering::SeqCst);
                    Err(GeneratorError::ConnectionError("retry started after backoff".to_string()))
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        Err(GeneratorError::ConnectionError("retry cancelled by shutdown".to_string()))
                    } else {
                        Err(GeneratorError::ConnectionError("retry wait ended unexpectedly".to_string()))
                    }
                }
            }
        }
    }

    #[async_trait]
    impl Transport for TimestampTransport {
        async fn send(
            &self,
            _message: &OTLPLogMessage,
            _shutdown_rx: &watch::Receiver<bool>,
        ) -> Result<()> {
            self.started.fetch_add(1, Ordering::SeqCst);
            self.timestamps
                .lock()
                .expect("timestamps mutex poisoned")
                .push(std::time::Instant::now());
            Ok(())
        }
    }

    fn test_config(count: usize, message_interval_ms: u64, concurrency: usize) -> OtelConfig {
        OtelConfig {
            ingest_endpoint: "http://localhost:4318/v1/logs".to_string(),
            healthcheck_endpoint: None,
            use_protobuf: false,
            transport: "http".to_string(),
            invalid_record_percent: 0.0,
            records_per_message: 1,
            print_logs: false,
            count,
            message_interval_ms,
            concurrency,
            continuous: false,
            retry_max_retries: 3,
            retry_base_delay_ms: 1000,
            retry_max_delay_ms: 32000,
            org_id: "tenant1".to_string(),
            label_cardinality_enabled: true,
            label_cardinality_default_limit: None,
            label_cardinality_limits: String::new(),
        }
    }

    async fn wait_for_started(transport: &CountingTransport, expected: usize) {
        for _ in 0..50 {
            if transport.started() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!(
            "timed out waiting for {} started sends, got {}",
            expected,
            transport.started()
        );
    }

    #[tokio::test]
    async fn continuous_worker_continues_after_single_iteration_error() {
        let config = OtelConfig {
            ingest_endpoint: "http://localhost:4318/v1/logs".to_string(),
            healthcheck_endpoint: None,
            use_protobuf: false,
            transport: "http".to_string(),
            invalid_record_percent: 0.0,
            records_per_message: 1,
            print_logs: false,
            count: 1,
            message_interval_ms: 0,
            concurrency: 1,
            continuous: true,
            retry_max_retries: 3,
            retry_base_delay_ms: 1000,
            retry_max_delay_ms: 32000,
            org_id: "tenant1".to_string(),
            label_cardinality_enabled: true,
            label_cardinality_default_limit: None,
            label_cardinality_limits: String::new(),
        };

        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let attempts = AtomicUsize::new(0);

        let result = generator
            .run_continuous_worker_with_runner(0, shutdown_rx, None, |generator, _, _, _| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let shutdown_tx = shutdown_tx.clone();

                Box::pin(async move {
                    if attempt == 0 {
                        return Err(GeneratorError::InvalidConfiguration(
                            "synthetic worker failure".to_string(),
                        ));
                    }

                    let _ = shutdown_tx.send(true);

                    let mut batch = BatchResult::new();
                    batch.add_success();
                    let _ = generator;
                    Ok(batch)
                })
            })
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(result.total, 1);
        assert_eq!(result.success, 1);
        assert_eq!(result.failed, 0);
    }

    #[tokio::test]
    async fn batch_result_scales_with_concurrency() {
        let config = test_config(3, 0, 4);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(20)));
        let generator = OtelLogGenerator::with_transport(config.clone(), transport).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.total, 3);
        assert_eq!(result.success, 3);
        assert_eq!(result.failed, 0);
    }

    #[tokio::test]
    async fn parallelism_does_not_exceed_configured_concurrency() {
        let config = test_config(3, 0, 4);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(50)));
        let generator =
            OtelLogGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.total, 3);
        assert!(transport.max_active() <= config.concurrency);
    }

    #[tokio::test]
    async fn batch_result_preserves_total_count_with_remainder_distribution() {
        let config = test_config(5, 0, 3);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(20)));
        let generator =
            OtelLogGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.total, 5);
        assert_eq!(result.success, 5);
        assert_eq!(result.failed, 0);
        assert_eq!(transport.started(), 5);
    }

    #[tokio::test]
    async fn batch_interval_is_global_across_workers() {
        let config = test_config(4, 40, 2);
        let transport = Arc::new(TimestampTransport::new());
        let generator =
            OtelLogGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.total, 4);
        assert_eq!(result.success, 4);

        let timestamps = transport.timestamps();
        assert_eq!(timestamps.len(), 4);

        for window in timestamps.windows(2) {
            let elapsed = window[1].duration_since(window[0]);
            assert!(
                elapsed >= Duration::from_millis(30),
                "expected global batch interval, got only {:?} between sends",
                elapsed
            );
        }
    }

    #[tokio::test]
    async fn continuous_mode_stops_without_starting_new_requests_after_shutdown() {
        let mut config = test_config(100, 100, 3);
        config.continuous = true;

        let transport = Arc::new(CountingTransport::new(Duration::from_millis(50)));
        let generator =
            Arc::new(OtelLogGenerator::with_transport(config, transport.clone()).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = {
            let generator = Arc::clone(&generator);
            tokio::spawn(async move { generator.run_continuous(shutdown_rx).await })
        };

        wait_for_started(transport.as_ref(), 3).await;
        let _ = shutdown_tx.send(true);

        let started_after_shutdown = transport.started();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(transport.started(), started_after_shutdown);

        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("continuous run did not stop in time")
            .unwrap()
            .unwrap();

        assert_eq!(result.total, started_after_shutdown);
    }

    #[tokio::test]
    async fn continuous_mode_cancels_retry_backoff_after_shutdown() {
        let mut config = test_config(100, 0, 1);
        config.continuous = true;

        let transport = Arc::new(RetryAwareTransport::new(Duration::from_millis(200)));
        let generator =
            Arc::new(OtelLogGenerator::with_transport(config, transport.clone()).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = {
            let generator = Arc::clone(&generator);
            tokio::spawn(async move { generator.run_continuous(shutdown_rx).await })
        };

        for _ in 0..50 {
            if transport.started() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(transport.started(), 1);
        let _ = shutdown_tx.send(true);

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(transport.started(), 1);

        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("continuous run did not stop in time")
            .unwrap()
            .unwrap();

        assert_eq!(result.total, 1);
        assert_eq!(result.success, 0);
        assert_eq!(result.failed, 1);
    }

    #[tokio::test]
    async fn generator_can_run_batch_after_continuous_shutdown() {
        let mut config = test_config(100, 100, 2);
        config.continuous = true;

        let transport = Arc::new(CountingTransport::new(Duration::from_millis(30)));
        let generator =
            Arc::new(OtelLogGenerator::with_transport(config.clone(), transport.clone()).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = {
            let generator = Arc::clone(&generator);
            tokio::spawn(async move { generator.run_continuous(shutdown_rx).await })
        };

        wait_for_started(transport.as_ref(), config.concurrency).await;
        let _ = shutdown_tx.send(true);

        let continuous_result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("continuous run did not stop in time")
            .unwrap()
            .unwrap();

        assert!(continuous_result.total >= config.concurrency);

        let batch_result = generator.send_messages_batch(2, 0).await.unwrap();

        assert_eq!(batch_result.total, 2);
        assert_eq!(batch_result.success, 2);
        assert_eq!(batch_result.failed, 0);
    }

    #[tokio::test]
    async fn concurrency_one_preserves_sequential_behavior() {
        let config = test_config(3, 0, 1);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(20)));
        let generator =
            OtelLogGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.total, 3);
        assert_eq!(result.success, 3);
        assert_eq!(transport.max_active(), 1);
    }
}
