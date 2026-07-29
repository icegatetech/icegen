use crate::config::{GenerationStats, OtelConfig, RetryConfig};
use crate::error::Result;
use crate::generator::base::SignalGenerator;
use crate::message::factory::{GenContext, SignalFactory};
use crate::message::{FakeDataGenerator, OTLPMessage, ServiceShard};
use crate::report::{self, ProgressSnapshot};
use crate::transport::{
    AuthHeaders, Destination, GrpcTransport, HttpTransport, NoopTransport, SendOutcome, Transport,
};
use async_trait::async_trait;
use chrono::Utc;
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

    /// Split one message's `records` and `traces` across at most `requested` service shards.
    ///
    /// `shard_limit` caps the shard count so no shard of a configured signal comes out empty (see
    /// [`OtelConfig::service_shard_limit`]); without a service pool the whole message stays a single
    /// unnamed shard.
    fn select_service_shards(
        &self,
        requested: usize,
        records: usize,
        traces: usize,
        shard_limit: usize,
    ) -> Vec<ServiceShard> {
        let records = records.max(1);
        let traces = traces.max(1);
        match self.service_names.as_deref() {
            None => vec![ServiceShard {
                service_name: None,
                num_logs: records,
                num_traces: traces,
            }],
            Some(pool) => {
                let k = requested.min(shard_limit.max(1)).max(1);
                let mut rng = rand::thread_rng();
                let records_per_shard = split_evenly(records, k);
                let traces_per_shard = split_evenly(traces, k);
                (0..k)
                    .map(|i| {
                        let idx = rng.gen_range(0..pool.len());
                        ServiceShard {
                            service_name: Some(pool[idx].clone()),
                            num_logs: records_per_shard[i],
                            num_traces: traces_per_shard[i],
                        }
                    })
                    .collect()
            }
        }
    }
}

/// Split `total` into `parts` counts that sum back to `total`: every part gets the same base share
/// and the remainder is handed out one-by-one to the first parts. `parts` must be `>= 1`.
fn split_evenly(total: usize, parts: usize) -> Vec<usize> {
    let base = total / parts;
    let remainder = total % parts;
    (0..parts)
        .map(|index| base + usize::from(index < remainder))
        .collect()
}

#[derive(Clone)]
pub struct OtelGenerator {
    config: OtelConfig,
    factory: Arc<SignalFactory>,
    transport: Arc<dyn Transport>,
    tenant_profiles: Arc<[TenantProfile]>,
}

/// Live run metrics: a lock-light accumulator of throughput, payload, response-time, retry, and
/// timeout counters, shared across the concurrent workers. It only records and computes; emitting a
/// tick is [`crate::report`]'s job — [`Self::progress_snapshot_if_due`] returns the pure
/// [`ProgressSnapshot`] the report layer formats.
struct RunMetrics {
    total_target: Option<usize>,
    started_at: Instant,
    last_progress_at: Mutex<Instant>,
    last_reported_step: AtomicUsize,
    processed: AtomicUsize,
    sent: AtomicUsize,
    total_payload_bytes: AtomicUsize,
    window_payload_bytes: AtomicUsize,
    total_response_time_micros: AtomicU64,
    max_response_time_micros: AtomicU64,
    total_responses: AtomicUsize,
    total_retries: AtomicUsize,
    total_timeouts: AtomicUsize,
}

impl RunMetrics {
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
            max_response_time_micros: AtomicU64::new(0),
            total_responses: AtomicUsize::new(0),
            total_retries: AtomicUsize::new(0),
            total_timeouts: AtomicUsize::new(0),
        }
    }

    /// Record the outcome of one signal message (one network request). Throughput, payload, and
    /// response-time metrics are measured per request; `processed` (generation cycles) is advanced
    /// separately by [`Self::record_cycle`].
    fn record_signal(
        &self,
        sent: bool,
        is_timeout: bool,
        retries: usize,
        payload_size_bytes: usize,
        response_time: Duration,
    ) {
        if sent {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        if is_timeout {
            self.total_timeouts.fetch_add(1, Ordering::Relaxed);
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

            // CAS loop to update the running maximum without a mutex.
            let mut current = self.max_response_time_micros.load(Ordering::Relaxed);
            while response_micros > current {
                match self.max_response_time_micros.compare_exchange_weak(
                    current,
                    response_micros,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// Record the completion of one generation cycle, returning the new processed-cycle count.
    fn record_cycle(&self) -> usize {
        self.processed.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Return a [`ProgressSnapshot`] when this `processed` count crosses a reporting step (every 10
    /// cycles), or `None` otherwise. Advancing `last_reported_step` dedups concurrent callers so
    /// exactly one worker produces the snapshot for a given step — that is concurrency control, not
    /// output, so it stays here; formatting the snapshot is [`crate::report`]'s job.
    ///
    /// `processed` values arrive out of order across workers (each is a distinct `fetch_add`
    /// result), so the advance is a `fetch_max`-style CAS loop rather than an exact `step - 1`
    /// swap: a worker that races ahead to a higher step still reports it, instead of the strict
    /// predecessor check dropping that tick and wedging every later one.
    // `is_multiple_of` was stabilised in Rust 1.87; use `%` to stay within the declared MSRV.
    #[allow(clippy::manual_is_multiple_of)]
    fn progress_snapshot_if_due(&self, processed: usize) -> Option<ProgressSnapshot> {
        if processed % 10 != 0 {
            return None;
        }

        let step = processed / 10;
        let mut last = self.last_reported_step.load(Ordering::SeqCst);
        loop {
            if step <= last {
                return None;
            }
            match self.last_reported_step.compare_exchange_weak(
                last,
                step,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => last = actual,
            }
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
        let max_response_time_micros = self.max_response_time_micros.load(Ordering::Relaxed);
        let total_responses = self.total_responses.load(Ordering::Relaxed);
        let total_timeouts = self.total_timeouts.load(Ordering::Relaxed);
        let avg_response_time_ms = if total_responses > 0 {
            (total_response_time_micros as f64 / total_responses as f64) / 1000.0
        } else {
            0.0
        };
        let max_response_time_ms = max_response_time_micros as f64 / 1000.0;

        let window_payload_bytes = self.window_payload_bytes.swap(0, Ordering::Relaxed);
        let mut last_progress_at = self
            .last_progress_at
            .lock()
            .expect("progress mutex poisoned");
        let window_elapsed_secs = last_progress_at.elapsed().as_secs_f64().max(f64::EPSILON);
        *last_progress_at = Instant::now();
        let current_speed_mib_s =
            (window_payload_bytes as f64 / 1024.0 / 1024.0) / window_elapsed_secs;

        Some(ProgressSnapshot {
            total_target: self.total_target,
            processed,
            sent,
            total_sent_mib,
            avg_speed_mib_s,
            current_speed_mib_s,
            avg_rps,
            avg_response_time_ms,
            max_response_time_ms,
            total_timeouts,
            total_retries,
            retries_per_cycle: retries_per_processed_message,
        })
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

type BatchFuture<'a> = Pin<Box<dyn Future<Output = Result<GenerationStats>> + Send + 'a>>;

impl OtelGenerator {
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
        split_evenly(total, concurrency.max(1))
    }

    pub async fn new(config: OtelConfig) -> Result<Self> {
        config.validate()?;

        // The startup banner is built by config (data) and emitted by report (output); generation
        // code carries no printing of its own.
        report::report_startup_summary(&config.startup_summary()?);

        let transport = Self::open_transport(&config).await?;
        Self::with_transport(config, transport)
    }

    /// Open the transport this run's flow calls for. Every variant is handled, so no branch can be
    /// reached that has no transport — the match is what replaced the old transport-string
    /// dispatch.
    async fn open_transport(config: &OtelConfig) -> Result<Arc<dyn Transport>> {
        match &config.destination {
            Destination::DryRun { .. } => Ok(Arc::new(NoopTransport)),
            // HTTP: one URL per configured signal, already resolved.
            Destination::Http {
                endpoints,
                protobuf,
            } => {
                let (retry_config, auth) = Self::build_network_settings(config)?;
                Ok(Arc::new(HttpTransport::new(
                    endpoints.clone(),
                    *protobuf,
                    retry_config,
                    auth,
                )?))
            }
            // gRPC: one channel, distinct OTLP service clients per configured signal.
            Destination::Grpc { endpoint } => {
                let (retry_config, auth) = Self::build_network_settings(config)?;
                Ok(Arc::new(
                    GrpcTransport::new(endpoint.clone(), &config.signals, retry_config, auth)
                        .await?,
                ))
            }
        }
    }

    /// Retry policy plus the vendor auth headers applied to every request, regardless of transport
    /// or tenant. Parsed only for a flow that opens a network transport, so a dry run never fails on
    /// headers it will never send.
    #[allow(clippy::result_large_err)]
    fn build_network_settings(config: &OtelConfig) -> Result<(RetryConfig, AuthHeaders)> {
        Ok((config.retry_config()?, config.auth_headers()?))
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn with_transport(
        config: OtelConfig,
        transport: Arc<dyn Transport>,
    ) -> Result<Self> {
        // Which signals get generated, and how they are encoded, is the factory's concern; the
        // generator stays signal-neutral and only resolves the spec from config.
        let factory = Arc::new(SignalFactory::from_spec(config.signal_factory_spec()?)?);
        let tenant_profiles = Self::build_tenant_profiles(&config);
        tracing::info!("[ok] Generator initialized successfully");

        Ok(Self {
            config,
            factory,
            transport,
            tenant_profiles,
        })
    }

    pub async fn run_continuous(
        &self,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<GenerationStats> {
        let progress = (!self.config.print_logs).then(|| Arc::new(RunMetrics::new(None)));
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

        let mut result = GenerationStats::new();
        while let Some(worker_result) = workers.join_next().await {
            result.merge(worker_result??);
        }

        Ok(result)
    }

    async fn run_continuous_worker(
        &self,
        message_interval_ms: u64,
        shutdown_rx: watch::Receiver<bool>,
        progress: Option<Arc<RunMetrics>>,
    ) -> Result<GenerationStats> {
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
        progress: Option<Arc<RunMetrics>>,
        mut run_batch: F,
    ) -> Result<GenerationStats>
    where
        F: for<'a> FnMut(
            &'a Self,
            u64,
            &'a mut watch::Receiver<bool>,
            Option<Arc<RunMetrics>>,
        ) -> BatchFuture<'a>,
    {
        let mut result = GenerationStats::new();

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
                    tracing::error!("Continuous worker iteration failed, continuing: {}", error);

                    if *shutdown_rx.borrow() {
                        break;
                    }

                    // Back off before retrying: a persistent deterministic failure (planning or
                    // encoding) would otherwise bypass the success-path pacing below and spin the
                    // worker at full CPU. Honour the configured interval, with a floor so a zero
                    // interval still throttles the error loop.
                    let backoff = Duration::from_millis(message_interval_ms.max(100));
                    tokio::select! {
                        _ = sleep(backoff) => {}
                        changed = shutdown_rx.changed() => {
                            if changed.is_ok() && *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }

                    continue;
                }
            };
            let processed = batch.cycles.total;
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
        progress: Option<Arc<RunMetrics>>,
        batch_pacer: Option<&BatchPacer>,
    ) -> Result<GenerationStats> {
        let mut result = GenerationStats::new();

        for _ in 0..count {
            if *shutdown_rx.borrow() {
                break;
            }

            // One pacing slot per generation cycle; the cycle's signal messages then start
            // together.
            if let Some(batch_pacer) = batch_pacer {
                if !batch_pacer.wait_turn(shutdown_rx).await {
                    break;
                }
            }

            // Build every signal message before any network send, so a planning/encoding failure
            // never yields a partially sent cycle.
            let messages = self.generate_messages()?;

            let print_logs = self.config.print_logs;
            let dry_run = self.config.is_dry_run();

            // Diagnostics: emit each message's full request block up front, in signal (build)
            // order, one atomic event per message. Doing this before spawning keeps the multi-line
            // PRINT_LOGS blocks from interleaving with each other or with the concurrent send
            // output below; network completion order is deliberately not reflected here.
            if print_logs {
                for message in &messages {
                    report::report_request(message);
                }
            }

            // Send all signal messages of this cycle concurrently. Each task borrows only an
            // `Arc<dyn Transport>` clone (not the whole generator), so no `OtelConfig` is deep-
            // cloned per signal. Retries are independent: a successful signal is never resent
            // because another signal failed.
            let mut sends = JoinSet::new();
            for message in messages {
                let transport = Arc::clone(&self.transport);
                let shutdown_rx = shutdown_rx.clone();
                sends.spawn(async move {
                    let signal = message.signal;
                    let payload_size_bytes = message.payload_size_bytes();
                    let send_started = Instant::now();
                    let outcome = transport.send(&message, &shutdown_rx).await;
                    let response_time = send_started.elapsed();
                    // Emit this signal's result as a single atomic event: success/dry-run only under
                    // PRINT_LOGS, a failure always (at error level).
                    match &outcome {
                        SendOutcome::Success { .. } => {
                            if print_logs {
                                report::report_send_outcome(signal, &outcome, dry_run);
                            }
                        }
                        SendOutcome::Failure { .. } => {
                            report::report_send_outcome(signal, &outcome, dry_run);
                        }
                    }
                    (signal, outcome, payload_size_bytes, response_time)
                });
            }

            // Await every send even if one already failed; the cycle succeeds only if all did.
            let mut cycle_success = true;
            while let Some(joined) = sends.join_next().await {
                let (signal, outcome, payload_size_bytes, response_time) = joined?;
                let sent = outcome.is_success();
                if !sent {
                    cycle_success = false;
                }
                result.record_signal(signal, sent);

                if let Some(progress) = &progress {
                    progress.record_signal(
                        sent,
                        outcome.is_timeout(),
                        outcome.retries(),
                        if sent { payload_size_bytes } else { 0 },
                        response_time,
                    );
                }
            }

            result.record_cycle(cycle_success);
            if let Some(progress) = &progress {
                let processed = progress.record_cycle();
                if let Some(snapshot) = progress.progress_snapshot_if_due(processed) {
                    report::report_progress(&snapshot);
                }
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl SignalGenerator for OtelGenerator {
    fn generate_messages(&self) -> Result<Vec<OTLPMessage>> {
        let mut rng = rand::thread_rng();
        let tenant_profile = self.select_tenant_profile();
        let tenant_id = tenant_profile.tenant_id.clone();
        let cloud_account_id = tenant_profile.select_cloud_account_id();
        let shards = tenant_profile.select_service_shards(
            self.config.service_shards_per_message,
            self.config.logs_per_message,
            self.config.traces_per_message,
            self.config.service_shard_limit(),
        );
        let should_be_invalid = rng.gen::<f32>() * 100.0 < self.config.invalid_record_percent;
        // Shared identity/time for every signal of this cycle, drawn once so logs and traces agree.
        let project_id = FakeDataGenerator::generate_project_id();
        let now_ns = Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            crate::error::GeneratorError::InvalidConfiguration(
                "current time is outside the representable nanosecond range".to_string(),
            )
        })?;

        self.factory.build(&GenContext {
            tenant_id,
            cloud_account_id,
            project_id,
            now_ns,
            shards,
            invalid: should_be_invalid,
        })
    }

    async fn send_messages_batch(
        &self,
        count: usize,
        message_interval_ms: u64,
    ) -> Result<GenerationStats> {
        let total_target = count;
        let progress =
            (!self.config.print_logs).then(|| Arc::new(RunMetrics::new(Some(total_target))));
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

        let mut result = GenerationStats::new();
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
    use crate::message::{MessagePayload, Signal};
    use crate::transport::{NoopTransport, SendOutcome, Transport};
    use serde_json::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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
    impl Transport for CountingTransport {
        async fn send(
            &self,
            _message: &OTLPMessage,
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
            _message: &OTLPMessage,
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
            _message: &OTLPMessage,
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

    /// Succeeds for logs, fails for traces. Counts how many log sends occurred so a test can prove
    /// the successful log signal is not resent when the trace signal of the same cycle fails.
    struct SignalSelectiveTransport {
        logs_sent: AtomicUsize,
    }

    impl SignalSelectiveTransport {
        fn new() -> Self {
            Self {
                logs_sent: AtomicUsize::new(0),
            }
        }

        fn logs_sent(&self) -> usize {
            self.logs_sent.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Transport for SignalSelectiveTransport {
        async fn send(
            &self,
            message: &OTLPMessage,
            _shutdown_rx: &watch::Receiver<bool>,
        ) -> SendOutcome {
            match message.signal {
                Signal::Logs => {
                    self.logs_sent.fetch_add(1, Ordering::SeqCst);
                    SendOutcome::Success { retries: 0 }
                }
                Signal::Traces => SendOutcome::Failure {
                    retries: 0,
                    error: GeneratorError::ConnectionError("traces down".to_string()),
                },
            }
        }
    }

    /// Blocks every send until either `send_delay` elapses (success) or shutdown is signalled
    /// (`Interrupted` failure), the way the real transports treat a cooperative stop. With a delay
    /// far longer than a test's completion timeout, only the shutdown path can finish a send, which
    /// is what makes cancellation of a cycle's in-flight sends observable.
    struct ShutdownAwareTransport {
        started: AtomicUsize,
        send_delay: Duration,
    }

    impl ShutdownAwareTransport {
        fn new(send_delay: Duration) -> Self {
            Self {
                started: AtomicUsize::new(0),
                send_delay,
            }
        }

        fn started(&self) -> usize {
            self.started.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Transport for ShutdownAwareTransport {
        async fn send(
            &self,
            _message: &OTLPMessage,
            shutdown_rx: &watch::Receiver<bool>,
        ) -> SendOutcome {
            self.started.fetch_add(1, Ordering::SeqCst);

            let mut shutdown_rx = shutdown_rx.clone();
            if *shutdown_rx.borrow() {
                return SendOutcome::Failure {
                    retries: 0,
                    error: GeneratorError::Interrupted,
                };
            }

            tokio::select! {
                _ = tokio::time::sleep(self.send_delay) => SendOutcome::Success { retries: 0 },
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        SendOutcome::Failure { retries: 0, error: GeneratorError::Interrupted }
                    } else {
                        SendOutcome::Success { retries: 0 }
                    }
                }
            }
        }
    }

    fn http_logs_destination(protobuf: bool) -> Destination {
        Destination::Http {
            endpoints: std::collections::HashMap::from([(
                crate::message::Signal::Logs,
                "http://localhost:4318/v1/logs".to_string(),
            )]),
            protobuf,
        }
    }

    fn http_traces_destination(protobuf: bool) -> Destination {
        Destination::Http {
            endpoints: std::collections::HashMap::from([(
                crate::message::Signal::Traces,
                "http://localhost:4318/v1/traces".to_string(),
            )]),
            protobuf,
        }
    }

    /// The HTTP flow carrying both signals, each on its own URL.
    fn http_logs_and_traces_destination() -> Destination {
        Destination::Http {
            endpoints: std::collections::HashMap::from([
                (
                    crate::message::Signal::Logs,
                    "http://localhost:4318/v1/logs".to_string(),
                ),
                (
                    crate::message::Signal::Traces,
                    "http://localhost:4318/v1/traces".to_string(),
                ),
            ]),
            protobuf: false,
        }
    }

    fn test_config(count: usize, message_interval_ms: u64, concurrency: usize) -> OtelConfig {
        OtelConfig {
            destination: http_logs_destination(false),
            invalid_record_percent: 0.0,
            logs_per_message: 1,
            traces_per_message: 1,
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
            record_across_batch_timestamp_jitter_ms: 1_000,
            record_intra_batch_timestamp_jitter_ns: 5,
            record_intra_batch_overlap_probability: 0.05,
            service_shards_per_message: 1,
            signals: vec![crate::message::Signal::Logs],
            llm_max_tool_calls: 3,
            llm_capture_content: false,
            llm_profile_weights: "simple_chat:1,tool_loop:3,plan_execute_reflect:2,rag:1"
                .to_string(),
            trace_min_spans: 0,
            trace_max_spans: 0,
            auth_headers: String::new(),
            auth_bearer: None,
            auth_basic: None,
        }
    }

    /// Poll `current` until it reaches `expected`, bounded so a run that never gets there fails the
    /// test instead of hanging; the panic reports the last observed value as diagnostic state.
    async fn wait_for_count(what: &str, expected: usize, mut current: impl FnMut() -> usize) {
        for _ in 0..50 {
            if current() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!(
            "timed out waiting for {} {}, got {}",
            expected,
            what,
            current()
        );
    }

    async fn wait_for_started(transport: &CountingTransport, expected: usize) {
        wait_for_count("started sends", expected, || transport.started()).await;
    }

    fn resource_attribute(message: &OTLPMessage, key: &str) -> Option<String> {
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

    fn profile_for<'a>(generator: &'a OtelGenerator, tenant_id: &str) -> &'a TenantProfile {
        generator
            .tenant_profiles
            .iter()
            .find(|profile| profile.tenant_id.as_deref() == Some(tenant_id))
            .expect("tenant profile must exist")
    }

    #[tokio::test]
    async fn continuous_worker_continues_after_single_iteration_error() {
        let config = OtelConfig {
            destination: http_logs_destination(false),
            invalid_record_percent: 0.0,
            logs_per_message: 1,
            traces_per_message: 1,
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
            record_across_batch_timestamp_jitter_ms: 1_000,
            record_intra_batch_timestamp_jitter_ns: 5,
            record_intra_batch_overlap_probability: 0.05,
            service_shards_per_message: 1,
            signals: vec![crate::message::Signal::Logs],
            llm_max_tool_calls: 3,
            llm_capture_content: false,
            llm_profile_weights: "simple_chat:1,tool_loop:3,plan_execute_reflect:2,rag:1"
                .to_string(),
            trace_min_spans: 0,
            trace_max_spans: 0,
            auth_headers: String::new(),
            auth_bearer: None,
            auth_basic: None,
        };

        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
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

                    let mut batch = GenerationStats::new();
                    batch.record_signal(crate::message::Signal::Logs, true);
                    batch.record_cycle(true);
                    let _ = generator;
                    Ok(batch)
                })
            })
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(result.cycles.total, 1);
        assert_eq!(result.cycles.success, 1);
        assert_eq!(result.cycles.failed, 0);
    }

    #[tokio::test]
    async fn batch_result_scales_with_concurrency() {
        let config = test_config(3, 0, 4);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(20)));
        let generator = OtelGenerator::with_transport(config.clone(), transport).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.cycles.total, 3);
        assert_eq!(result.cycles.success, 3);
        assert_eq!(result.cycles.failed, 0);
    }

    #[tokio::test]
    async fn parallelism_does_not_exceed_configured_concurrency() {
        let config = test_config(3, 0, 4);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(50)));
        let generator = OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.cycles.total, 3);
        // Upper bound on concurrent requests is CONCURRENCY * signals.len().
        assert!(transport.max_active() <= config.concurrency * config.signals.len());
    }

    #[tokio::test]
    async fn two_signals_produce_two_requests_per_cycle() {
        // MESSAGE_COUNT=3 with two signals => 3 generation cycles and 6 transport calls.
        let mut config = test_config(3, 0, 2);
        config.signals = vec![Signal::Logs, Signal::Traces];
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(5)));
        let generator = OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.cycles.total, 3);
        assert_eq!(result.cycles.success, 3);
        assert_eq!(transport.started(), 6);
        assert_eq!(result.per_signal[&Signal::Logs].total, 3);
        assert_eq!(result.per_signal[&Signal::Traces].total, 3);
    }

    #[tokio::test]
    async fn concurrency_one_two_signals_overlap_within_a_cycle() {
        // CONCURRENCY=1 with two signals: the two signal sends of one cycle run in parallel, so
        // max concurrent requests is exactly signals.len() = 2 (never more, and it does reach 2).
        let mut config = test_config(2, 0, 1);
        config.signals = vec![Signal::Logs, Signal::Traces];
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(50)));
        let generator = OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.cycles.total, 2);
        assert_eq!(
            transport.max_active(),
            2,
            "the two signals of a cycle must overlap"
        );
    }

    #[tokio::test]
    async fn failed_signal_marks_cycle_failed_without_resending_successful_signal() {
        let mut config = test_config(1, 0, 1);
        config.signals = vec![Signal::Logs, Signal::Traces];
        let transport = Arc::new(SignalSelectiveTransport::new());
        let generator = OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        // The cycle failed because traces failed, but logs succeeded exactly once (no resend).
        assert_eq!(result.cycles.total, 1);
        assert_eq!(result.cycles.failed, 1);
        assert_eq!(result.cycles.success, 0);
        assert_eq!(result.per_signal[&Signal::Logs].success, 1);
        assert_eq!(result.per_signal[&Signal::Logs].failed, 0);
        assert_eq!(result.per_signal[&Signal::Traces].success, 0);
        assert_eq!(result.per_signal[&Signal::Traces].failed, 1);
        assert_eq!(transport.logs_sent(), 1, "logs must be sent exactly once");
    }

    #[tokio::test]
    async fn batch_result_preserves_total_count_with_remainder_distribution() {
        let config = test_config(5, 0, 3);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(20)));
        let generator = OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.cycles.total, 5);
        assert_eq!(result.cycles.success, 5);
        assert_eq!(result.cycles.failed, 0);
        assert_eq!(transport.started(), 5);
    }

    #[tokio::test]
    async fn batch_interval_is_global_across_workers() {
        let config = test_config(4, 40, 2);
        let transport = Arc::new(TimestampTransport::new());
        let generator = OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.cycles.total, 4);
        assert_eq!(result.cycles.success, 4);

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
        let generator = Arc::new(OtelGenerator::with_transport(config, transport.clone()).unwrap());
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

        assert_eq!(result.cycles.total, started_after_shutdown);
    }

    #[tokio::test]
    async fn continuous_mode_with_two_signals_cancels_both_in_flight_sends_on_shutdown() {
        // The central multi-signal path under a stop: every worker is blocked on both of its signal
        // sends when shutdown arrives. Each send task watches the shutdown channel itself, so the
        // run must end long before the transport's own delay elapses, both signals must end up with
        // the same number of recorded requests, and an interrupted signal must fail its cycle.
        let mut config = test_config(100, 0, 2);
        config.continuous = true;
        config.signals = vec![Signal::Logs, Signal::Traces];

        // Far longer than the completion timeout below: only cancellation can finish these sends.
        let transport = Arc::new(ShutdownAwareTransport::new(Duration::from_secs(30)));
        let generator =
            Arc::new(OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = {
            let generator = Arc::clone(&generator);
            tokio::spawn(async move { generator.run_continuous(shutdown_rx).await })
        };

        // A cycle starts all of its signal sends together, so concurrency * signals are in flight.
        let in_flight = config.concurrency * config.signals.len();
        wait_for_count("started sends", in_flight, || transport.started()).await;
        let _ = shutdown_tx.send(true);

        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("continuous run did not stop in time")
            .unwrap()
            .unwrap();

        assert_eq!(
            transport.started(),
            in_flight,
            "no send may start after shutdown"
        );
        assert_eq!(result.cycles.total, config.concurrency);
        assert_eq!(
            result.cycles.failed, config.concurrency,
            "an interrupted signal fails its whole cycle"
        );
        assert_eq!(result.cycles.success, 0);
        assert_eq!(
            result.per_signal[&Signal::Logs].total,
            result.per_signal[&Signal::Traces].total,
            "both signals of a cycle are accounted for"
        );
        assert_eq!(result.per_signal[&Signal::Logs].failed, config.concurrency);
        assert_eq!(
            result.per_signal[&Signal::Traces].failed,
            config.concurrency
        );
    }

    #[tokio::test]
    async fn continuous_mode_partial_signal_failure_fails_cycles_without_resending_logs() {
        // Traces are down for the whole continuous run: every cycle must be counted failed while
        // its log message is sent exactly once, so a failing signal never triggers a resend of the
        // signal that already succeeded in the same cycle.
        let mut config = test_config(100, 1, 2);
        config.continuous = true;
        config.signals = vec![Signal::Logs, Signal::Traces];

        let transport = Arc::new(SignalSelectiveTransport::new());
        let generator =
            Arc::new(OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = {
            let generator = Arc::clone(&generator);
            tokio::spawn(async move { generator.run_continuous(shutdown_rx).await })
        };

        wait_for_count("log sends", config.concurrency, || transport.logs_sent()).await;
        let _ = shutdown_tx.send(true);

        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("continuous run did not stop in time")
            .unwrap()
            .unwrap();

        let cycles = result.cycles.total;
        assert!(cycles >= config.concurrency);
        assert_eq!(result.cycles.failed, cycles);
        assert_eq!(result.cycles.success, 0);
        assert_eq!(result.per_signal[&Signal::Logs].success, cycles);
        assert_eq!(result.per_signal[&Signal::Logs].failed, 0);
        assert_eq!(result.per_signal[&Signal::Traces].failed, cycles);
        assert_eq!(result.per_signal[&Signal::Traces].success, 0);
        assert_eq!(
            transport.logs_sent(),
            cycles,
            "each cycle sends its log message exactly once"
        );
    }

    #[tokio::test]
    async fn continuous_mode_cancels_retry_backoff_after_shutdown() {
        let mut config = test_config(100, 0, 1);
        config.continuous = true;

        let transport = Arc::new(RetryAwareTransport::new(Duration::from_millis(200)));
        let generator = Arc::new(OtelGenerator::with_transport(config, transport.clone()).unwrap());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = {
            let generator = Arc::clone(&generator);
            tokio::spawn(async move { generator.run_continuous(shutdown_rx).await })
        };

        wait_for_count("started sends", 1, || transport.started()).await;
        assert_eq!(transport.started(), 1);
        let _ = shutdown_tx.send(true);

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(transport.started(), 1);

        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("continuous run did not stop in time")
            .unwrap()
            .unwrap();

        assert_eq!(result.cycles.total, 1);
        assert_eq!(result.cycles.success, 0);
        assert_eq!(result.cycles.failed, 1);
    }

    #[tokio::test]
    async fn generator_can_run_batch_after_continuous_shutdown() {
        let mut config = test_config(100, 100, 2);
        config.continuous = true;

        let transport = Arc::new(CountingTransport::new(Duration::from_millis(30)));
        let generator =
            Arc::new(OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap());
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

        assert!(continuous_result.cycles.total >= config.concurrency);

        let batch_result = generator.send_messages_batch(2, 0).await.unwrap();

        assert_eq!(batch_result.cycles.total, 2);
        assert_eq!(batch_result.cycles.success, 2);
        assert_eq!(batch_result.cycles.failed, 0);
    }

    #[tokio::test]
    async fn concurrency_one_preserves_sequential_behavior() {
        let config = test_config(3, 0, 1);
        let transport = Arc::new(CountingTransport::new(Duration::from_millis(20)));
        let generator = OtelGenerator::with_transport(config.clone(), transport.clone()).unwrap();

        let result = generator
            .send_messages_batch(config.count, config.message_interval_ms)
            .await
            .unwrap();

        assert_eq!(result.cycles.total, 3);
        assert_eq!(result.cycles.success, 3);
        assert_eq!(transport.max_active(), 1);
    }

    #[test]
    fn generate_message_uses_single_tenant_config() {
        let config = test_config(3, 0, 1);
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        for _ in 0..5 {
            let message = generator.generate_messages().unwrap().remove(0);
            assert_eq!(message.tenant_id, Some("tenant1".to_string()));
        }
    }

    #[test]
    fn single_tenant_builds_one_profile_for_explicit_tenant_id() {
        let mut config = test_config(3, 0, 1);
        config.tenant_id = "tenant_custom".to_string();
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

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
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

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
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        for _ in 0..50 {
            let message = generator.generate_messages().unwrap().remove(0);
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
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let mut seen_tenants = std::collections::BTreeSet::new();

        for _ in 0..200 {
            let message = generator.generate_messages().unwrap().remove(0);
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
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        for _ in 0..50 {
            let message = generator.generate_messages().unwrap().remove(0);
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
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        assert_eq!(generator.tenant_profiles.len(), 1);
        assert!(generator.tenant_profiles[0].tenant_id.is_none());
    }

    #[test]
    fn zero_cloud_account_count_builds_profile_without_pool() {
        let mut config = test_config(1, 0, 1);
        config.cloud_account_count_per_tenant = 0;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        assert!(generator.tenant_profiles[0].cloud_account_ids.is_none());
    }

    #[test]
    fn zero_service_count_builds_profile_without_pool() {
        let mut config = test_config(1, 0, 1);
        config.service_count_per_tenant = 0;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        assert!(generator.tenant_profiles[0].service_names.is_none());
    }

    #[test]
    fn message_omits_service_name_when_disabled() {
        let mut config = test_config(1, 0, 1);
        config.service_count_per_tenant = 0;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let message = generator.generate_messages().unwrap().remove(0);
        assert!(resource_attribute(&message, "service.name").is_none());
        let MessagePayload::Json(json) = &message.message else {
            panic!("expected JSON")
        };
        let scope = json
            .get("resourceLogs")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|rl| rl.get("scopeLogs"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|sl| sl.get("scope"))
            .expect("scope must be present");
        let scope_name = scope.get("name").and_then(|n| n.as_str());
        assert_eq!(scope_name, Some("io.trihub.icegen"));
        let scope_attrs = scope.get("attributes").and_then(|a| a.as_array());
        let attr_keys: Vec<&str> = scope_attrs
            .map(|attrs| {
                attrs
                    .iter()
                    .filter_map(|a| a.get("key").and_then(|k| k.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            !attr_keys.contains(&"library.name"),
            "library.name must be absent when service_count_per_tenant=0"
        );
        assert!(
            attr_keys.contains(&"library.version"),
            "library.version must be present"
        );
    }

    #[test]
    fn message_omits_cloud_account_id_when_disabled() {
        let mut config = test_config(1, 0, 1);
        config.cloud_account_count_per_tenant = 0;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let message = generator.generate_messages().unwrap().remove(0);
        assert!(resource_attribute(&message, "cloud.account.id").is_none());
    }

    #[tokio::test]
    async fn dry_run_initializes_without_endpoint() {
        let mut config = test_config(1, 0, 1);
        config.destination = Destination::DryRun {
            flow: crate::transport::TransportFlow::Http,
            protobuf: false,
        };

        let generator = OtelGenerator::new(config).await.unwrap();
        let result = generator.send_messages_batch(1, 0).await.unwrap();
        assert_eq!(result.cycles.total, 1);
        assert_eq!(result.cycles.success, 1);
        assert_eq!(result.cycles.failed, 0);
    }

    #[test]
    fn message_has_no_tenant_id_when_disabled() {
        let mut config = test_config(1, 0, 1);
        config.tenant_count = 0;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let message = generator.generate_messages().unwrap().remove(0);
        assert!(message.tenant_id.is_none());
    }

    fn shard_count_json(message: &OTLPMessage) -> usize {
        let MessagePayload::Json(json) = &message.message else {
            panic!("expected JSON payload");
        };
        json["resourceLogs"]
            .as_array()
            .expect("resourceLogs array")
            .len()
    }

    fn shard_service_names_json(message: &OTLPMessage) -> Vec<String> {
        let MessagePayload::Json(json) = &message.message else {
            panic!("expected JSON payload");
        };
        json["resourceLogs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|rl| {
                rl["resource"]["attributes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find_map(|a| {
                        (a["key"].as_str() == Some("service.name"))
                            .then(|| a["value"]["stringValue"].as_str().unwrap().to_string())
                    })
                    .expect("service.name attribute missing")
            })
            .collect()
    }

    fn shard_record_counts_json(message: &OTLPMessage) -> Vec<usize> {
        let MessagePayload::Json(json) = &message.message else {
            panic!("expected JSON payload");
        };
        json["resourceLogs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|rl| rl["scopeLogs"][0]["logRecords"].as_array().unwrap().len())
            .collect()
    }

    #[test]
    fn generate_message_with_service_shards_per_message_emits_one_resource_logs_per_shard_json() {
        // End-to-end through OtelGenerator::generate_message: select_service_shards
        // distributes records across shards, LogJsonEncoder emits one ResourceLogs per shard.
        let mut config = test_config(1, 0, 1);
        config.logs_per_message = 9;
        config.service_shards_per_message = 3;
        config.service_count_per_tenant = 3;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();
        let pool: Vec<String> = generator.tenant_profiles[0]
            .service_names
            .as_ref()
            .unwrap()
            .iter()
            .cloned()
            .collect();

        let message = generator.generate_messages().unwrap().remove(0);
        assert_eq!(shard_count_json(&message), 3);
        assert_eq!(shard_record_counts_json(&message).iter().sum::<usize>(), 9);
        for name in shard_service_names_json(&message) {
            assert!(
                pool.contains(&name),
                "service name {name} must come from tenant pool"
            );
        }
    }

    #[test]
    fn generate_message_with_use_protobuf_selects_protobuf_encoder() {
        // Guards the encoder selection branch in with_transport: the HTTP flow's protobuf choice
        // must produce MessagePayload::Protobuf. Also re-asserts intra-shard monotonicity on the
        // protobuf path so we keep that signal after deleting the protobuf-only single-purpose
        // test.
        let mut config = test_config(1, 0, 1);
        config.destination = http_logs_destination(true);
        config.logs_per_message = 8;
        config.service_shards_per_message = 2;
        config.service_count_per_tenant = 2;
        // Disable overlap so monotonicity is a hard invariant.
        config.record_intra_batch_overlap_probability = 0.0;
        config.record_intra_batch_timestamp_jitter_ns = 5_000_000;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let message = generator.generate_messages().unwrap().remove(0);
        let MessagePayload::Protobuf(bytes) = &message.message else {
            panic!("expected Protobuf payload from the protobuf HTTP flow");
        };
        use prost::Message;
        let decoded =
            crate::pb::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest::decode(
                bytes.as_slice(),
            )
            .unwrap();
        assert_eq!(decoded.resource_logs.len(), 2);
        let total: usize = decoded
            .resource_logs
            .iter()
            .map(|rl| rl.scope_logs[0].log_records.len())
            .sum();
        assert_eq!(total, 8);

        for (shard_idx, rl) in decoded.resource_logs.iter().enumerate() {
            let ts: Vec<u64> = rl.scope_logs[0]
                .log_records
                .iter()
                .map(|r| r.time_unix_nano)
                .collect();
            for i in 1..ts.len() {
                assert!(
                    ts[i - 1] <= ts[i],
                    "shard {shard_idx}: non-monotonic at i={i}: {} > {}",
                    ts[i - 1],
                    ts[i]
                );
            }
        }
    }

    #[test]
    fn generate_message_with_traces_signal_emits_resource_spans() {
        // signal=traces routes through TraceMessageFactory: payload must carry resourceSpans
        // with gen_ai.* span attributes, and the message must be tagged Signal::Traces.
        let mut config = test_config(1, 0, 1);
        config.signals = vec![crate::message::Signal::Traces];
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let message = generator.generate_messages().unwrap().remove(0);
        assert_eq!(message.signal, crate::message::Signal::Traces);
        let MessagePayload::Json(json) = &message.message else {
            panic!("expected JSON trace payload");
        };
        let resource_spans = json["resourceSpans"]
            .as_array()
            .expect("resourceSpans array");
        assert!(!resource_spans.is_empty());
        let spans = resource_spans[0]["scopeSpans"][0]["spans"]
            .as_array()
            .expect("spans array");
        // Spans are emitted in random order; the root is the single parent-less span.
        let root = spans
            .iter()
            .find(|s| s["parentSpanId"].as_str().is_none())
            .expect("exactly one root span");
        assert!(root["name"].as_str().unwrap().starts_with("invoke_agent"));
        let has_gen_ai = spans.iter().any(|s| {
            s["attributes"]
                .as_array()
                .map(|attrs| {
                    attrs
                        .iter()
                        .any(|a| a["key"].as_str().unwrap_or("").starts_with("gen_ai."))
                })
                .unwrap_or(false)
        });
        assert!(has_gen_ai, "trace spans must carry gen_ai.* attributes");
    }

    #[test]
    fn generate_message_with_traces_signal_and_protobuf_uses_protobuf_encoder() {
        let mut config = test_config(1, 0, 1);
        config.signals = vec![crate::message::Signal::Traces];
        config.destination = http_traces_destination(true);
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let message = generator.generate_messages().unwrap().remove(0);
        assert!(
            matches!(message.message, MessagePayload::Protobuf(_)),
            "the protobuf HTTP flow must select TraceProtobufEncoder"
        );
    }

    #[test]
    fn generate_message_with_grpc_transport_forces_protobuf_encoder() {
        // The gRPC flow must pick LogProtobufEncoder regardless of the HTTP flow's protobuf flag
        // (gRPC wire is always protobuf). This is the second leg of the encoder-selection
        // conditional in with_transport; without this test, dropping the gRPC leg would go silent.
        let mut config = test_config(1, 0, 1);
        config.destination = Destination::Grpc {
            endpoint: "http://localhost:4317".to_string(),
        };
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let message = generator.generate_messages().unwrap().remove(0);
        assert!(
            matches!(message.message, MessagePayload::Protobuf(_)),
            "the gRPC flow must force LogProtobufEncoder"
        );
    }

    #[test]
    fn generate_message_without_service_pool_emits_single_shard_without_service_name() {
        // Covers the None branch of select_service_shards: with no pool, service_shards_per_message is
        // ignored and we get exactly one shard with no service.name attribute, regardless of the
        // requested fan-out.
        let mut config = test_config(1, 0, 1);
        config.logs_per_message = 5;
        config.service_shards_per_message = 5;
        config.service_count_per_tenant = 0;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let message = generator.generate_messages().unwrap().remove(0);
        assert_eq!(shard_count_json(&message), 1);
        assert_eq!(shard_record_counts_json(&message), vec![5]);
        assert!(
            resource_attribute(&message, "service.name").is_none(),
            "service.name must be absent when service_count_per_tenant=0"
        );
    }

    fn make_profile(service_names: Option<Vec<&str>>) -> TenantProfile {
        TenantProfile {
            tenant_id: Some("t1".to_string()),
            cloud_account_ids: None,
            service_names: service_names
                .map(|names| Arc::from(names.iter().map(|s| s.to_string()).collect::<Vec<_>>())),
        }
    }

    #[test]
    fn select_service_shards_clamps_to_records() {
        let profile = make_profile(Some(vec!["svc-a", "svc-b", "svc-c"]));
        let shards = profile.select_service_shards(10, 3, 1, 3);
        assert_eq!(shards.len(), 3, "clamped to records count");
        assert_eq!(shards.iter().map(|s| s.num_logs).sum::<usize>(), 3);
    }

    #[test]
    fn select_service_shards_falls_back_to_single_shard_without_pool() {
        let profile = make_profile(None);
        let shards = profile.select_service_shards(5, 7, 4, 7);
        assert_eq!(shards.len(), 1);
        assert!(shards[0].service_name.is_none());
        assert_eq!(shards[0].num_logs, 7);
        // Without a pool the whole message stays one shard, so it carries every trace too.
        assert_eq!(shards[0].num_traces, 4);
    }

    #[test]
    fn select_service_shards_picks_with_replacement_when_requested_exceeds_pool() {
        // Single-element pool makes "with replacement" deterministic: every shard must hold "a".
        // This locks down the documented behaviour where requested > pool.len() simulates
        // multiple pods of the same service.
        let profile = make_profile(Some(vec!["a"]));
        let shards = profile.select_service_shards(5, 5, 5, 5);
        assert_eq!(shards.len(), 5, "5 shards even with pool_size=1");
        assert_eq!(shards.iter().map(|s| s.num_logs).sum::<usize>(), 5);
        assert!(
            shards
                .iter()
                .all(|s| s.service_name.as_deref() == Some("a")),
            "every shard must reuse the only pool entry"
        );
    }

    #[test]
    fn select_service_shards_returns_single_shard_when_requested_is_one() {
        let profile = make_profile(Some(vec!["svc-a", "svc-b", "svc-c"]));
        let shards = profile.select_service_shards(1, 7, 1, 7);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].num_logs, 7);
        let name = shards[0].service_name.as_deref().expect("name from pool");
        assert!(["svc-a", "svc-b", "svc-c"].contains(&name));
    }

    #[test]
    fn select_service_shards_collapses_to_one_shard_when_records_is_one() {
        // k = min(requested, shard_limit).max(1) = min(5, 1) = 1; the records.max(1) floor on the
        // input must not break the k clamp.
        let profile = make_profile(Some(vec!["svc-a", "svc-b"]));
        let shards = profile.select_service_shards(5, 1, 1, 1);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].num_logs, 1);
        assert!(shards[0].service_name.is_some());
    }

    #[test]
    fn select_service_shards_distributes_records_evenly() {
        let profile = make_profile(Some(vec!["svc-a", "svc-b", "svc-c"]));
        let shards = profile.select_service_shards(3, 10, 1, 3);
        assert_eq!(shards.len(), 3);
        let counts: Vec<usize> = shards.iter().map(|s| s.num_logs).collect();
        assert_eq!(counts.iter().sum::<usize>(), 10);
        // base=3, remainder=1 → first shard (i=0 < remainder=1) gets base+1, rest get base.
        // The distribution contract is positional: index order determines which shard receives +1.
        assert_eq!(counts[0], 4);
        assert_eq!(counts[1], 3);
        assert_eq!(counts[2], 3);
    }

    #[test]
    fn select_service_shards_distributes_traces_evenly() {
        // Traces are split by the same positional contract as records and independently of them:
        // 10 traces over 3 shards → 4/3/3, while the 3 records go one per shard.
        let profile = make_profile(Some(vec!["svc-a", "svc-b", "svc-c"]));
        let shards = profile.select_service_shards(3, 3, 10, 3);
        assert_eq!(shards.len(), 3);
        let traces: Vec<usize> = shards.iter().map(|s| s.num_traces).collect();
        assert_eq!(traces, vec![4, 3, 3]);
        assert_eq!(shards.iter().map(|s| s.num_logs).sum::<usize>(), 3);
    }

    #[test]
    fn select_service_shards_clamps_to_the_traces_budget_on_a_traces_only_run() {
        // Traces-only: the shard limit comes from traces_per_message alone, so records must not
        // cap the shard count.
        let profile = make_profile(Some(vec!["svc-a", "svc-b", "svc-c"]));
        let shards = profile.select_service_shards(3, 1, 3, 3);
        assert_eq!(shards.len(), 3);
        assert!(shards.iter().all(|s| s.num_traces == 1));
    }

    /// `resourceSpans` groups of a JSON trace message, one per service shard.
    fn resource_spans_count_json(message: &OTLPMessage) -> usize {
        let MessagePayload::Json(json) = &message.message else {
            panic!("expected JSON trace payload");
        };
        json["resourceSpans"]
            .as_array()
            .expect("resourceSpans array")
            .len()
    }

    /// Distinct `traceId` values across every span of a JSON trace message.
    fn distinct_trace_ids_json(message: &OTLPMessage) -> std::collections::HashSet<String> {
        let MessagePayload::Json(json) = &message.message else {
            panic!("expected JSON trace payload");
        };
        json["resourceSpans"]
            .as_array()
            .expect("resourceSpans array")
            .iter()
            .flat_map(|rs| {
                rs["scopeSpans"][0]["spans"]
                    .as_array()
                    .expect("spans array")
                    .iter()
                    .map(|span| span["traceId"].as_str().expect("traceId").to_string())
            })
            .collect()
    }

    #[test]
    fn traces_per_message_drives_the_trace_count_end_to_end() {
        // The knob is independent of records: 6 traces are emitted even though a single shard is
        // requested, and each is its own `trace_id` — all inside that shard's single group.
        let mut config = test_config(1, 0, 1);
        config.signals = vec![crate::message::Signal::Traces];
        config.traces_per_message = 6;
        config.logs_per_message = 1;
        config.service_shards_per_message = 1;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let message = generator.generate_messages().unwrap().remove(0);
        assert_eq!(resource_spans_count_json(&message), 1);
        assert_eq!(distinct_trace_ids_json(&message).len(), 6);
    }

    #[test]
    fn traces_per_message_is_split_across_service_shards() {
        // 6 traces over 3 shards → 2 each, still one `trace_id` per trace, and one group per shard.
        let mut config = test_config(1, 0, 1);
        config.signals = vec![crate::message::Signal::Traces];
        config.traces_per_message = 6;
        config.service_shards_per_message = 3;
        config.service_count_per_tenant = 3;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let message = generator.generate_messages().unwrap().remove(0);
        assert_eq!(resource_spans_count_json(&message), 3);
        assert_eq!(distinct_trace_ids_json(&message).len(), 6);
    }

    #[test]
    fn logs_and_traces_scale_independently() {
        // 10 records and 4 traces in one cycle: the log message keeps all 10 records, the trace
        // message carries 4 traces in the single shard's group, and every record points at one of
        // those traces.
        let mut config = test_config(1, 0, 1);
        config.signals = vec![crate::message::Signal::Logs, crate::message::Signal::Traces];
        config.destination = http_logs_and_traces_destination();
        config.logs_per_message = 10;
        config.traces_per_message = 4;
        config.service_shards_per_message = 1;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        let messages = generator.generate_messages().unwrap();
        let logs = &messages[0];
        let traces = &messages[1];
        assert_eq!(shard_record_counts_json(logs), vec![10]);
        assert_eq!(resource_spans_count_json(traces), 1);

        let trace_ids = distinct_trace_ids_json(traces);
        assert_eq!(trace_ids.len(), 4);

        let MessagePayload::Json(logs_json) = &logs.message else {
            panic!("expected JSON log payload");
        };
        let record_trace_ids: std::collections::HashSet<String> = logs_json["resourceLogs"][0]
            ["scopeLogs"][0]["logRecords"]
            .as_array()
            .expect("logRecords array")
            .iter()
            .map(|record| record["traceId"].as_str().expect("traceId").to_string())
            .collect();
        assert!(
            record_trace_ids.is_subset(&trace_ids),
            "every log record must point at one of the shard's traces"
        );
        // 10 records over 4 traces leaves no trace empty, so all four ids must show up in the logs.
        assert_eq!(record_trace_ids.len(), 4);
    }

    #[test]
    fn budgeted_traces_emit_exact_span_count_end_to_end() {
        // signal=traces + fixed budget (min == max) => every trace carries exactly N spans through
        // the real factory/encoder path.
        let mut config = test_config(1, 0, 1);
        config.signals = vec![crate::message::Signal::Traces];
        config.trace_min_spans = 20;
        config.trace_max_spans = 20;
        let generator = OtelGenerator::with_transport(config, Arc::new(NoopTransport)).unwrap();

        for _ in 0..20 {
            let message = generator.generate_messages().unwrap().remove(0);
            let MessagePayload::Json(json) = &message.message else {
                panic!("expected JSON trace payload");
            };
            let spans = json["resourceSpans"][0]["scopeSpans"][0]["spans"]
                .as_array()
                .expect("spans array");
            assert_eq!(spans.len(), 20);
        }
    }

    #[test]
    fn progress_snapshot_is_absent_between_reporting_steps() {
        // Progress is due only on cycle counts that are multiples of 10; every other count yields
        // nothing to report.
        let metrics = RunMetrics::new(Some(100));
        for processed in [1usize, 7, 9, 11, 19] {
            assert!(
                metrics.progress_snapshot_if_due(processed).is_none(),
                "cycle {processed} is not a reporting step"
            );
        }
    }

    #[test]
    fn progress_snapshot_ticks_on_the_tenth_cycle_then_dedups_the_step() {
        let metrics = RunMetrics::new(Some(100));

        let snapshot = metrics
            .progress_snapshot_if_due(10)
            .expect("a snapshot is due on the tenth cycle");
        assert_eq!(snapshot.processed, 10);
        assert_eq!(snapshot.total_target, Some(100));

        // Re-asking for the same step returns nothing: the CAS on `last_reported_step` already
        // advanced, so a second worker landing on this step never double-reports it.
        assert!(
            metrics.progress_snapshot_if_due(10).is_none(),
            "the same step must not report twice"
        );
    }

    #[test]
    fn concurrent_callers_produce_exactly_one_snapshot_per_step() {
        use std::sync::Barrier;
        use std::thread;

        // Many workers can cross the same reporting step at once; the step-dedup CAS must let
        // exactly one of them produce the snapshot.
        const WORKERS: usize = 16;
        let metrics = Arc::new(RunMetrics::new(Some(1000)));
        let snapshots = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(WORKERS));

        let handles: Vec<_> = (0..WORKERS)
            .map(|_| {
                let metrics = Arc::clone(&metrics);
                let snapshots = Arc::clone(&snapshots);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    // Release all workers together so they genuinely race on the same step.
                    barrier.wait();
                    if metrics.progress_snapshot_if_due(10).is_some() {
                        snapshots.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker thread panicked");
        }

        assert_eq!(
            snapshots.load(Ordering::SeqCst),
            1,
            "exactly one worker may snapshot a given step"
        );
    }
}
