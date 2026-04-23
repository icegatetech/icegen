use crate::config::{BatchResult, OtelConfig};
use crate::error::Result;
use crate::generator::base::LogGenerator;
use crate::message::{MessagePayload, OTLPLogMessage, OTLPLogMessageGenerator};
use crate::transport::{GrpcTransport, HttpTransport, SendOutcome, Transport};
use async_trait::async_trait;
use rand::Rng;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, Instant};

#[derive(Clone, Debug)]
struct TenantProfile {
    tenant_id: Option<String>,
    cloud_account_ids: Option<Arc<[String]>>,
    service_names: Option<Arc<[String]>>,
}

impl TenantProfile {
    fn select_cloud_account_id(&self) -> Option<String> {
        let pool = self.cloud_account_ids.as_ref()?;
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..pool.len());
        Some(pool[index].clone())
    }

    fn select_service_name(&self) -> Option<String> {
        let pool = self.service_names.as_ref()?;
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..pool.len());
        Some(pool[index].clone())
    }
}

#[derive(Clone)]
pub struct OtelLogGenerator {
    config: OtelConfig,
    message_generator: OTLPLogMessageGenerator,
    transport: Arc<dyn Transport>,
    tenant_profiles: Arc<[TenantProfile]>,
}

struct ProgressTracker {
    total_target: Option<usize>,
    started_at: Instant,
    last_progress_at: Mutex<Instant>,
    last_reported_step: AtomicUsize,
    processed: AtomicUsize,
    sent: AtomicUsize,
    total_payload_bytes: AtomicUsize,
    window_payload_bytes: AtomicUsize,
    total_response_time_micros: AtomicU64,
    total_responses: AtomicUsize,
    total_retries: AtomicUsize,
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
            sent: AtomicUsize::new(0),
            total_payload_bytes: AtomicUsize::new(0),
            window_payload_bytes: AtomicUsize::new(0),
            total_response_time_micros: AtomicU64::new(0),
            total_responses: AtomicUsize::new(0),
            total_retries: AtomicUsize::new(0),
        }
    }

    fn record(
        &self,
        sent: bool,
        retries: usize,
        payload_size_bytes: usize,
        response_time: Duration,
    ) -> usize {
        if sent {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        if retries > 0 {
            self.total_retries.fetch_add(retries, Ordering::Relaxed);
        }

        if sent && payload_size_bytes > 0 {
            self.total_payload_bytes
                .fetch_add(payload_size_bytes, Ordering::Relaxed);
            self.window_payload_bytes
                .fetch_add(payload_size_bytes, Ordering::Relaxed);
        }

        if sent {
            let response_micros = response_time.as_micros().min(u64::MAX as u128) as u64;
            self.total_response_time_micros
                .fetch_add(response_micros, Ordering::Relaxed);
            self.total_responses.fetch_add(1, Ordering::Relaxed);
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
        let sent = self.sent.load(Ordering::Relaxed);
        let total_payload_bytes = self.total_payload_bytes.load(Ordering::Relaxed);
        let total_sent_mib = total_payload_bytes as f64 / 1024.0 / 1024.0;
        let avg_speed_mib_s = total_sent_mib / total_elapsed_secs;
        let avg_rps = sent as f64 / total_elapsed_secs;
        let total_retries = self.total_retries.load(Ordering::Relaxed);
        let retries_per_processed_message = total_retries as f64 / processed as f64;

        let total_response_time_micros = self.total_response_time_micros.load(Ordering::Relaxed);
        let total_responses = self.total_responses.load(Ordering::Relaxed);
        let avg_response_time_ms = if total_responses > 0 {
            (total_response_time_micros as f64 / total_responses as f64) / 1000.0
        } else {
            0.0
        };

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
                "Progress: {}/{} messages processed; payload sent: {:.4} MiB; throughput: avg {:.4} MiB/s, current {:.4} MiB/s; avg rps: {:.2}; avg response time: {:.2} ms; retries total: {}; retries/processed: {:.3}",
                processed,
                total_target,
                total_sent_mib,
                avg_speed_mib_s,
                current_speed_mib_s,
                avg_rps,
                avg_response_time_ms,
                total_retries,
                retries_per_processed_message
            ),
            None => println!(
                "Progress: {} messages processed; payload sent: {:.4} MiB; throughput: avg {:.4} MiB/s, current {:.4} MiB/s; avg rps: {:.2}; avg response time: {:.2} ms; retries total: {}; retries/processed: {:.3}",
                processed,
                total_sent_mib,
                avg_speed_mib_s,
                current_speed_mib_s,
                avg_rps,
                avg_response_time_ms,
                total_retries,
                retries_per_processed_message
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
    fn build_tenant_profiles(config: &OtelConfig) -> Arc<[TenantProfile]> {
        let tenant_ids: Vec<Option<String>> = match config.tenant_count {
            0 => vec![None],
            1 => vec![Some(config.tenant_id.clone())],
            n => (1..=n).map(|i| Some(format!("tenant{i}"))).collect(),
        };

        Arc::from(
            tenant_ids
                .into_iter()
                .map(|tenant_id| {
                    let key = tenant_id.as_deref().unwrap_or("notenant").to_string();
                    TenantProfile {
                        cloud_account_ids: (config.cloud_account_count_per_tenant > 0).then(|| {
                            build_tenant_value_pool(
                                &key,
                                "acc",
                                config.cloud_account_count_per_tenant,
                            )
                        }),
                        service_names: (config.service_count_per_tenant > 0).then(|| {
                            build_tenant_value_pool(&key, "svc", config.service_count_per_tenant)
                        }),
                        tenant_id,
                    }
                })
                .collect::<Vec<_>>(),
        )
    }

    fn select_tenant_profile(&self) -> &TenantProfile {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.tenant_profiles.len());
        &self.tenant_profiles[index]
    }

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
        if config.tenant_count == 0 {
            println!("  Tenant routing: no tenant header");
        } else if config.tenant_count == 1 {
            println!("  Tenant routing: single tenant '{}'", config.tenant_id);
        } else {
            println!(
                "  Tenant routing: {} tenants, random tenant1..tenant{}",
                config.tenant_count, config.tenant_count
            );
        }
        let cloud_pool_str = if config.cloud_account_count_per_tenant == 0 {
            "omitted".to_string()
        } else {
            config.cloud_account_count_per_tenant.to_string()
        };
        let service_pool_str = if config.service_count_per_tenant == 0 {
            "omitted".to_string()
        } else {
            config.service_count_per_tenant.to_string()
        };
        if config.tenant_count == 0 {
            println!(
                "  Tenant profile pools: {} cloud accounts, {} services (tenantless)",
                cloud_pool_str, service_pool_str
            );
        } else {
            println!(
                "  Tenant profile pools: {} cloud accounts/tenant, {} services/tenant",
                cloud_pool_str, service_pool_str
            );
        }

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
            config.timestamp_jitter_config(),
        );
        let tenant_profiles = Self::build_tenant_profiles(&config);
        println!("✓ Generator initialized successfully\n");

        Ok(Self {
            config,
            message_generator,
            transport,
            tenant_profiles,
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

            let batch = match run_batch(
                self,
                message_interval_ms,
                &mut shutdown_rx,
                progress.clone(),
            )
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
            let send_started = Instant::now();
            let send_report = self.send_message(&message, shutdown_rx).await?;
            let response_time = send_started.elapsed();

            let sent_payload_bytes = match &send_report {
                SendOutcome::Success { .. } => { result.add_success(); payload_size_bytes }
                SendOutcome::Failure { .. } => { result.add_failure(); 0 }
            };

            if let Some(progress) = &progress {
                let processed = progress.record(
                    send_report.is_success(),
                    send_report.retries(),
                    sent_payload_bytes,
                    response_time,
                );
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
        let tenant_profile = self.select_tenant_profile();
        let tenant_id = tenant_profile.tenant_id.clone();
        let cloud_account_id = tenant_profile.select_cloud_account_id();
        let service_name = tenant_profile.select_service_name();
        let should_be_invalid = rng.gen::<f32>() * 100.0 < self.config.invalid_record_percent;

        if should_be_invalid {
            self.message_generator.generate_invalid_message(tenant_id)
        } else if self.config.transport == "grpc" || self.config.use_protobuf {
            self.message_generator.generate_protobuf_message(
                tenant_id,
                cloud_account_id,
                service_name,
                self.config.records_per_message,
            )
        } else {
            self.message_generator.generate_aggregated_message(
                tenant_id,
                cloud_account_id,
                service_name,
                self.config.records_per_message,
            )
        }
    }

    async fn send_message(
        &self,
        message: &OTLPLogMessage,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<SendOutcome> {
        if self.config.print_logs {
            println!("Sending message:");
            println!("  Tenant ID: {}", message.tenant_id.as_deref().unwrap_or("(none)"));
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

        let report = self.transport.send(message, shutdown_rx).await;
        match &report {
            SendOutcome::Success { .. } => {
                if self.config.print_logs {
                    println!("✓ Message sent successfully\n");
                }
            }
            SendOutcome::Failure { error, .. } => {
                eprintln!("✗ Failed to send message: {}", error);
            }
        }

        Ok(report)
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

fn build_tenant_value_pool(tenant_id: &str, suffix: &str, count: usize) -> Arc<[String]> {
    let width = count.to_string().len().max(2);
    Arc::from(
        (1..=count)
            .map(|index| format!("{tenant_id}-{suffix}-{index:0width$}"))
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::GeneratorError;
    use crate::transport::{SendOutcome, Transport};
    use serde_json::Value;
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
        ) -> SendOutcome {
            SendOutcome::Success { retries: 0 }
        }
    }

    #[async_trait]
    impl Transport for CountingTransport {
        async fn send(
            &self,
            _message: &OTLPLogMessage,
            _shutdown_rx: &watch::Receiver<bool>,
        ) -> SendOutcome {
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
            SendOutcome::Success { retries: 0 }
        }
    }

    #[async_trait]
    impl Transport for RetryAwareTransport {
        async fn send(
            &self,
            _message: &OTLPLogMessage,
            shutdown_rx: &watch::Receiver<bool>,
        ) -> SendOutcome {
            self.started.fetch_add(1, Ordering::SeqCst);

            let mut shutdown_rx = shutdown_rx.clone();
            tokio::select! {
                _ = tokio::time::sleep(self.retry_wait) => {
                    self.started.fetch_add(1, Ordering::SeqCst);
                    SendOutcome::Failure { retries: 1, error: GeneratorError::ConnectionError("retry started after backoff".to_string()) }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        SendOutcome::Failure { retries: 0, error: GeneratorError::Interrupted }
                    } else {
                        SendOutcome::Failure { retries: 0, error: GeneratorError::ConnectionError("retry wait ended unexpectedly".to_string()) }
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
        ) -> SendOutcome {
            self.started.fetch_add(1, Ordering::SeqCst);
            self.timestamps
                .lock()
                .expect("timestamps mutex poisoned")
                .push(std::time::Instant::now());
            SendOutcome::Success { retries: 0 }
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
            tenant_id: "tenant1".to_string(),
            tenant_count: 1,
            cloud_account_count_per_tenant: 4,
            service_count_per_tenant: 6,
            label_cardinality_enabled: true,
            label_cardinality_default_limit: None,
            label_cardinality_limits: String::new(),
            record_timestamp_jitter_ms: 1_000,
            record_intra_batch_jitter_ms: 5,
            record_intra_batch_overlap_probability: 0.05,
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

    fn resource_attribute(message: &OTLPLogMessage, key: &str) -> Option<String> {
        let MessagePayload::Json(json) = &message.message else {
            return None;
        };

        json.get("resourceLogs")
            .and_then(Value::as_array)
            .and_then(|resource_logs| resource_logs.first())
            .and_then(|resource_log| resource_log.get("resource"))
            .and_then(|resource| resource.get("attributes"))
            .and_then(Value::as_array)
            .and_then(|attributes| {
                attributes.iter().find_map(|attribute| {
                    (attribute.get("key").and_then(Value::as_str) == Some(key)).then(|| {
                        attribute
                            .get("value")
                            .and_then(|value| value.get("stringValue"))
                            .and_then(Value::as_str)
                            .map(ToString::to_string)
                    })?
                })
            })
    }

    fn profile_for<'a>(generator: &'a OtelLogGenerator, tenant_id: &str) -> &'a TenantProfile {
        generator
            .tenant_profiles
            .iter()
            .find(|profile| profile.tenant_id.as_deref() == Some(tenant_id))
            .expect("tenant profile must exist")
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
            tenant_id: "tenant1".to_string(),
            tenant_count: 1,
            cloud_account_count_per_tenant: 4,
            service_count_per_tenant: 6,
            label_cardinality_enabled: true,
            label_cardinality_default_limit: None,
            label_cardinality_limits: String::new(),
            record_timestamp_jitter_ms: 1_000,
            record_intra_batch_jitter_ms: 5,
            record_intra_batch_overlap_probability: 0.05,
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

    #[test]
    fn generate_message_uses_single_tenant_config() {
        let config = test_config(3, 0, 1);
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        for _ in 0..5 {
            let message = generator.generate_message().unwrap();
            assert_eq!(message.tenant_id, Some("tenant1".to_string()));
        }
    }

    #[test]
    fn single_tenant_builds_one_profile_for_explicit_tenant_id() {
        let mut config = test_config(3, 0, 1);
        config.tenant_id = "tenant_custom".to_string();
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        assert_eq!(generator.tenant_profiles.len(), 1);
        let profile = &generator.tenant_profiles[0];
        assert_eq!(profile.tenant_id, Some("tenant_custom".to_string()));
        let acc_ids = profile.cloud_account_ids.as_ref().unwrap();
        assert_eq!(
            acc_ids.as_ref(),
            [
                "tenant_custom-acc-01",
                "tenant_custom-acc-02",
                "tenant_custom-acc-03",
                "tenant_custom-acc-04"
            ]
        );
        let svc_names = profile.service_names.as_ref().unwrap();
        assert_eq!(svc_names.len(), 6);
        assert_eq!(svc_names[0], "tenant_custom-svc-01");
        assert_eq!(svc_names[5], "tenant_custom-svc-06");
    }

    #[test]
    fn multi_tenant_builds_profiles_for_tenant_range() {
        let mut config = test_config(3, 0, 1);
        config.tenant_count = 3;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let tenant_ids = generator
            .tenant_profiles
            .iter()
            .map(|profile| profile.tenant_id.as_deref().unwrap_or(""))
            .collect::<Vec<_>>();
        assert_eq!(tenant_ids, vec!["tenant1", "tenant2", "tenant3"]);
    }

    #[test]
    fn generated_message_uses_only_values_from_selected_tenant_profile() {
        let mut config = test_config(3, 0, 1);
        config.tenant_count = 3;
        config.cloud_account_count_per_tenant = 2;
        config.service_count_per_tenant = 3;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        for _ in 0..50 {
            let message = generator.generate_message().unwrap();
            let tenant_id = message.tenant_id.clone().expect("tenant_id");
            let profile = profile_for(&generator, &tenant_id);

            let cloud_account_id =
                resource_attribute(&message, "cloud.account.id").expect("cloud.account.id");
            let service_name = resource_attribute(&message, "service.name").expect("service.name");

            assert!(profile
                .cloud_account_ids
                .as_ref()
                .unwrap()
                .iter()
                .any(|value| value == &cloud_account_id));
            assert!(profile
                .service_names
                .as_ref()
                .unwrap()
                .iter()
                .any(|value| value == &service_name));
        }
    }

    #[test]
    fn multi_tenant_generation_never_crosses_profile_boundaries() {
        let mut config = test_config(100, 0, 1);
        config.tenant_count = 4;
        config.cloud_account_count_per_tenant = 2;
        config.service_count_per_tenant = 2;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let mut seen_tenants = std::collections::BTreeSet::new();

        for _ in 0..200 {
            let message = generator.generate_message().unwrap();
            let tenant_id = message.tenant_id.clone().expect("tenant_id");
            seen_tenants.insert(tenant_id.clone());

            let cloud_account_id =
                resource_attribute(&message, "cloud.account.id").expect("cloud.account.id");
            let service_name = resource_attribute(&message, "service.name").expect("service.name");

            assert!(cloud_account_id.starts_with(&format!("{tenant_id}-acc-")));
            assert!(service_name.starts_with(&format!("{tenant_id}-svc-")));
        }

        assert!(seen_tenants.len() > 1);
    }

    #[test]
    fn generate_message_uses_only_multi_tenant_pool() {
        let mut config = test_config(3, 0, 1);
        config.tenant_count = 3;
        config.tenant_id = "legacy_tenant".to_string();
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        for _ in 0..50 {
            let message = generator.generate_message().unwrap();
            assert!(matches!(
                message.tenant_id.as_deref(),
                Some("tenant1") | Some("tenant2") | Some("tenant3")
            ));
        }
    }

    #[test]
    fn zero_tenant_count_builds_single_profile_without_tenant_id() {
        let mut config = test_config(1, 0, 1);
        config.tenant_count = 0;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        assert_eq!(generator.tenant_profiles.len(), 1);
        assert!(generator.tenant_profiles[0].tenant_id.is_none());
    }

    #[test]
    fn zero_cloud_account_count_builds_profile_without_pool() {
        let mut config = test_config(1, 0, 1);
        config.cloud_account_count_per_tenant = 0;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        assert!(generator.tenant_profiles[0].cloud_account_ids.is_none());
    }

    #[test]
    fn zero_service_count_builds_profile_without_pool() {
        let mut config = test_config(1, 0, 1);
        config.service_count_per_tenant = 0;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        assert!(generator.tenant_profiles[0].service_names.is_none());
    }

    #[test]
    fn message_omits_service_name_when_disabled() {
        let mut config = test_config(1, 0, 1);
        config.service_count_per_tenant = 0;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let message = generator.generate_message().unwrap();
        assert!(resource_attribute(&message, "service.name").is_none());
        let MessagePayload::Json(json) = &message.message else { panic!("expected JSON") };
        let scope = json
            .get("resourceLogs").and_then(|v| v.as_array()).and_then(|a| a.first())
            .and_then(|rl| rl.get("scopeLogs")).and_then(|v| v.as_array()).and_then(|a| a.first())
            .and_then(|sl| sl.get("scope"))
            .expect("scope must be present");
        let scope_name = scope.get("name").and_then(|n| n.as_str());
        assert_eq!(scope_name, Some("io.trihub.generator"));
        let scope_attrs = scope.get("attributes").and_then(|a| a.as_array());
        let attr_keys: Vec<&str> = scope_attrs
            .map(|attrs| {
                attrs.iter()
                    .filter_map(|a| a.get("key").and_then(|k| k.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        assert!(!attr_keys.contains(&"library.name"), "library.name must be absent when service_count_per_tenant=0");
        assert!(attr_keys.contains(&"library.version"), "library.version must be present");
    }

    #[test]
    fn message_omits_cloud_account_id_when_disabled() {
        let mut config = test_config(1, 0, 1);
        config.cloud_account_count_per_tenant = 0;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let message = generator.generate_message().unwrap();
        assert!(resource_attribute(&message, "cloud.account.id").is_none());
    }

    #[test]
    fn message_has_no_tenant_id_when_disabled() {
        let mut config = test_config(1, 0, 1);
        config.tenant_count = 0;
        let generator = OtelLogGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let message = generator.generate_message().unwrap();
        assert!(message.tenant_id.is_none());
    }
}
