use clap::Parser;
use otel_log_generator::{
    BatchResult, Cli, GeneratorType, GrpcTransport, HttpTransport, LogGenerator, OtelConfig,
    OtelLogGenerator, OtelMetricsGenerator, Transport,
};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables from .env file if present
    dotenvy::dotenv().ok();

    // Parse CLI arguments
    let cli = Cli::parse();

    match cli.generator {
        GeneratorType::Otel(args) => {
            let config: OtelConfig = args.into();
            config.validate()?;

            // Print initialization info
            println!("Initializing OTEL Generator...");
            println!("  Endpoint: {}", config.ingest_endpoint);
            println!("  Transport: {}", config.transport);
            println!("  Use Protobuf: {}", config.use_protobuf);
            println!(
                "  Signals: logs={}, metrics={}",
                config.enable_logs, config.enable_metrics
            );
            println!("  Records per message: {}", config.records_per_message);
            println!("  Invalid record %: {}", config.invalid_record_percent);
            println!("  Concurrency: {}", config.concurrency);
            if config.tenant_count == 1 {
                println!("  Tenant routing: single tenant '{}'", config.tenant_id);
            } else {
                println!(
                    "  Tenant routing: {} tenants, random tenant1..tenant{}",
                    config.tenant_count, config.tenant_count
                );
            }
            println!(
                "  Tenant profile pools: {} cloud accounts/tenant, {} services/tenant",
                config.cloud_account_count_per_tenant, config.service_count_per_tenant
            );

            let retry_config = config.retry_config()?;
            println!(
                "  Retry: max_retries={}, base_delay={}ms, max_delay={}ms",
                retry_config.max_retries, retry_config.base_delay_ms, retry_config.max_delay_ms
            );
            println!(
                "  Label cardinality limiting: {}",
                config.label_cardinality_enabled
            );

            // Create shared transport
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
                                return Err(e.into());
                            }
                        }
                    }

                    Arc::new(http_transport)
                }
                "grpc" => Arc::new(
                    GrpcTransport::new(config.ingest_endpoint.clone(), retry_config).await?,
                ),
                _ => unreachable!(),
            };

            // Create generators based on enabled signals
            let log_generator = if config.enable_logs {
                Some(Arc::new(OtelLogGenerator::with_transport(
                    config.clone(),
                    transport.clone(),
                )?))
            } else {
                None
            };

            let metrics_generator = if config.enable_metrics {
                Some(Arc::new(OtelMetricsGenerator::with_transport(
                    config.clone(),
                    transport,
                )?))
            } else {
                None
            };

            println!("✓ Generator initialized successfully\n");

            if config.continuous {
                println!("Running in continuous mode. Press Ctrl+C or send SIGTERM to stop.");

                // Setup signal handlers for graceful shutdown
                let ctrl_c = signal::ctrl_c();
                #[cfg(unix)]
                let mut sigterm =
                    signal::unix::signal(signal::unix::SignalKind::terminate())?;

                #[cfg(unix)]
                let shutdown = async {
                    tokio::select! {
                        _ = ctrl_c => {
                            println!("\nReceived Ctrl+C, shutting down gracefully...");
                        }
                        _ = sigterm.recv() => {
                            println!("\nReceived SIGTERM, shutting down gracefully...");
                        }
                    }
                };

                #[cfg(not(unix))]
                let shutdown = async {
                    ctrl_c.await.ok();
                    println!("\nReceived Ctrl+C, shutting down gracefully...");
                };

                let (shutdown_tx, shutdown_rx) = watch::channel(false);

                let log_task = log_generator.map(|gen| {
                    let rx = shutdown_rx.clone();
                    tokio::spawn(async move { gen.run_continuous(rx).await })
                });

                let metrics_task = metrics_generator.map(|gen| {
                    let rx = shutdown_rx.clone();
                    tokio::spawn(async move { gen.send_messages_batch_continuous(rx).await })
                });

                // Wait for shutdown signal
                shutdown.await;
                let _ = shutdown_tx.send(true);

                // Collect results
                let mut log_result = BatchResult::new();
                let mut metrics_result = BatchResult::new();

                if let Some(task) = log_task {
                    log_result = task.await??;
                }
                if let Some(task) = metrics_task {
                    metrics_result = task.await??;
                }

                print_summary(&config, &log_result, &metrics_result);
            } else {
                let mut log_result = BatchResult::new();
                let mut metrics_result = BatchResult::new();

                if let Some(gen) = &log_generator {
                    log_result = gen
                        .send_messages_batch(config.count, config.message_interval_ms)
                        .await?;
                }
                if let Some(gen) = &metrics_generator {
                    metrics_result = gen
                        .send_messages_batch(config.count, config.message_interval_ms)
                        .await?;
                }

                print_summary(&config, &log_result, &metrics_result);
            }
        }
    }

    Ok(())
}

fn print_summary(config: &OtelConfig, log_result: &BatchResult, metrics_result: &BatchResult) {
    if config.enable_logs && config.enable_metrics {
        println!(
            "\nSummary: logs {}/{} sent, metrics {}/{} sent",
            log_result.success, log_result.total, metrics_result.success, metrics_result.total
        );
        let failed = log_result.failed + metrics_result.failed;
        if failed > 0 {
            println!("Failed: {} messages", failed);
        }
    } else if config.enable_logs {
        println!(
            "\nSummary: {}/{} messages sent successfully",
            log_result.success, log_result.total
        );
        if log_result.failed > 0 {
            println!("Failed: {} messages", log_result.failed);
        }
    } else {
        println!(
            "\nSummary: {}/{} metrics sent successfully",
            metrics_result.success, metrics_result.total
        );
        if metrics_result.failed > 0 {
            println!("Failed: {} messages", metrics_result.failed);
        }
    }
}
