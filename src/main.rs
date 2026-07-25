use clap::Parser;
use otel_log_generator::{
    reject_retired_env_vars, report, Cli, GeneratorType, OtelConfig, OtelGenerator, SignalGenerator,
};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;
use tracing::Level;
use tracing_subscriber::filter::{FilterExt, FilterFn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_output();

    // Load environment variables from .env file if present
    dotenvy::dotenv().ok();

    // A retired variable left in the shell or in `.env` would be ignored by clap and silently
    // change the run, so reject it before anything reads the configuration.
    reject_retired_env_vars()?;

    // Parse CLI arguments
    let cli = Cli::parse();

    match cli.generator {
        GeneratorType::Otel(args) => {
            let config: OtelConfig = args.try_into()?;
            let generator = Arc::new(OtelGenerator::new(config.clone()).await?);

            if config.continuous {
                tracing::info!("Running in continuous mode. Press Ctrl+C or send SIGTERM to stop.");

                // Setup signal handlers for graceful shutdown
                let ctrl_c = signal::ctrl_c();
                #[cfg(unix)]
                let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;

                #[cfg(unix)]
                let shutdown = async {
                    tokio::select! {
                        _ = ctrl_c => {
                            tracing::info!("Received Ctrl+C, shutting down gracefully...");
                        }
                        _ = sigterm.recv() => {
                            tracing::info!("Received SIGTERM, shutting down gracefully...");
                        }
                    }
                };

                #[cfg(not(unix))]
                let shutdown = async {
                    ctrl_c.await.ok();
                    tracing::info!("Received Ctrl+C, shutting down gracefully...");
                };

                let mut generator_task = {
                    let generator = Arc::clone(&generator);
                    let (shutdown_tx, shutdown_rx) = watch::channel(false);
                    let generator_task =
                        tokio::spawn(async move { generator.run_continuous(shutdown_rx).await });
                    (shutdown_tx, generator_task)
                };

                tokio::select! {
                    result = &mut generator_task.1 => {
                        let result = result??;
                        if !config.print_logs {
                            report::report_run_summary(&result, &config, "Summary: ");
                        }
                    }
                    _ = shutdown => {
                        let _ = generator_task.0.send(true);
                        let result = generator_task.1.await??;
                        generator.close().await?;
                        report::report_run_summary(&result, &config, "Shutdown complete. Final summary: ");
                    }
                }
            } else {
                let result = generator
                    .send_messages_batch(config.count, config.message_interval_ms)
                    .await?;
                report::report_run_summary(&result, &config, "Summary: ");
            }
        }
    }

    Ok(())
}

/// Install the two-stream output subscriber.
///
/// Product output — banner, progress ticks, run summary, `PRINT_LOGS` dumps, and per-signal `[ok]`
/// lines — is emitted through `report` at info level with the [`report::TARGET`] target; it goes to
/// **stdout** and is deliberately *not* wired to `RUST_LOG`, so a run whose output is piped or saved
/// still receives its results and muting the diagnostic channel never silences them.
///
/// Diagnostics — transport retries, send failures (`report`'s error-level line), and lifecycle
/// notes — go to **stderr** under `RUST_LOG` (default `info`). The stderr filter excludes the
/// product events so nothing is duplicated across streams. Both layers drop the timestamp/target/
/// level decoration so each stream reads as plain CLI text rather than structured logs.
fn init_output() {
    let is_product = |meta: &tracing::Metadata<'_>| {
        meta.target() == report::TARGET && *meta.level() == Level::INFO
    };

    let product = fmt::layer()
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_writer(std::io::stdout)
        .with_filter(FilterFn::new(is_product));

    let diagnostics = fmt::layer()
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_writer(std::io::stderr)
        .with_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
                .and(FilterFn::new(move |meta| !is_product(meta))),
        );

    tracing_subscriber::registry()
        .with(product)
        .with(diagnostics)
        .init();
}
