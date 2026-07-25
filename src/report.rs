//! User-facing output layer. Every banner, progress tick, run summary, and per-message diagnostic
//! is formatted here and emitted through `tracing`, so generation and transport code carry no
//! `println!`/`eprintln!` of their own. Formatting is deliberately plain (see the subscriber set up
//! in `main`) so the output reads as CLI text rather than structured logs; the exact shape is not a
//! stability contract.

use crate::config::{GenerationStats, OtelConfig};
use crate::error::GeneratorError;
use crate::message::{MessagePayload, OTLPMessage, Signal};
use crate::transport::SendOutcome;
use std::fmt::Write as _;

/// Tracing target carried by every event this module emits. `main` routes events with this target
/// to stdout as product output, keeping the run's own results (banner, progress, summary,
/// `PRINT_LOGS` dumps, per-signal `[ok]` lines) on a channel separate from — and not silenced by —
/// the `RUST_LOG`-governed stderr diagnostic stream. Emitting with an explicit target rather than
/// relying on the module path keeps that contract intact even if a `report_*` helper is later moved
/// into a submodule.
pub const TARGET: &str = module_path!();

/// One computed progress tick. Pure data: [`crate::generator::RunMetrics`] fills every field from
/// its atomics, and [`report_progress`] only formats it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProgressSnapshot {
    /// Configured total generation cycles, or `None` in continuous mode.
    pub total_target: Option<usize>,
    pub processed: usize,
    pub sent: usize,
    pub total_sent_mib: f64,
    pub avg_speed_mib_s: f64,
    pub current_speed_mib_s: f64,
    pub avg_rps: f64,
    pub avg_response_time_ms: f64,
    pub max_response_time_ms: f64,
    pub total_timeouts: usize,
    pub total_retries: usize,
    pub retries_per_cycle: f64,
}

/// Emit the startup banner: an intro line followed by one `label: value` row per entry, in the
/// order [`OtelConfig::startup_summary`] produced them.
pub fn report_startup_summary(rows: &[(String, String)]) {
    let mut banner = String::from("Initializing OTEL Generator...");
    for (label, value) in rows {
        let _ = write!(banner, "\n  {label}: {value}");
    }
    tracing::info!(target: TARGET, "{banner}");
}

/// Emit one progress tick.
pub(crate) fn report_progress(snapshot: &ProgressSnapshot) {
    tracing::info!(target: TARGET, "{}", format_progress(snapshot));
}

fn format_progress(s: &ProgressSnapshot) -> String {
    match s.total_target {
        Some(total_target) => format!(
            "Progress: {}/{} generation cycles processed; requests sent: {}; payload sent: {:.4} MiB; throughput: avg {:.4} MiB/s, current {:.4} MiB/s; avg rps: {:.2}; avg response time: {:.2} ms, max: {:.2} ms; timeouts: {}; retries total: {}; retries/cycle: {:.3}",
            s.processed,
            total_target,
            s.sent,
            s.total_sent_mib,
            s.avg_speed_mib_s,
            s.current_speed_mib_s,
            s.avg_rps,
            s.avg_response_time_ms,
            s.max_response_time_ms,
            s.total_timeouts,
            s.total_retries,
            s.retries_per_cycle
        ),
        None => format!(
            "Progress: {} generation cycles processed; requests sent: {}; payload sent: {:.4} MiB; throughput: avg {:.4} MiB/s, current {:.4} MiB/s; avg rps: {:.2}; avg response time: {:.2} ms, max: {:.2} ms; timeouts: {}; retries total: {}; retries/cycle: {:.3}",
            s.processed,
            s.sent,
            s.total_sent_mib,
            s.avg_speed_mib_s,
            s.current_speed_mib_s,
            s.avg_rps,
            s.avg_response_time_ms,
            s.max_response_time_ms,
            s.total_timeouts,
            s.total_retries,
            s.retries_per_cycle
        ),
    }
}

/// Emit the run summary: generation cycles plus a per-signal request breakdown, in the order the
/// signals were configured. Used by batch, normal continuous completion, and shutdown completion.
pub fn report_run_summary(stats: &GenerationStats, config: &OtelConfig, prefix: &str) {
    tracing::info!(target: TARGET, "{}", format_run_summary(stats, config, prefix));
}

fn format_run_summary(stats: &GenerationStats, config: &OtelConfig, prefix: &str) -> String {
    let mut out = String::new();
    if config.is_dry_run() {
        let _ = write!(
            out,
            "{prefix}{} generation cycles generated (dry-run, nothing sent)",
            stats.cycles.total
        );
        for signal in &config.signals {
            let counters = stats.per_signal.get(signal).cloned().unwrap_or_default();
            let _ = write!(
                out,
                "\n  {}: {} messages generated",
                signal.as_str(),
                counters.total
            );
        }
        return out;
    }

    let _ = write!(
        out,
        "{prefix}{}/{} generation cycles delivered successfully",
        stats.cycles.success, stats.cycles.total
    );
    for signal in &config.signals {
        let counters = stats.per_signal.get(signal).cloned().unwrap_or_default();
        let _ = write!(
            out,
            "\n  {}: {}/{} requests successful",
            signal.as_str(),
            counters.success,
            counters.total
        );
    }
    if stats.cycles.failed > 0 {
        let _ = write!(out, "\nFailed cycles: {}", stats.cycles.failed);
    }
    out
}

/// Emit the full `PRINT_LOGS` request block for one message (built before the network send, in
/// signal order). One event = one atomic multi-line write.
pub(crate) fn report_request(message: &OTLPMessage) {
    tracing::info!(target: TARGET, "{}", format_request_block(message));
}

/// Emit the outcome of one signal's send as a single line: success/dry-run at info level, failure
/// at error level. The signal is named because a cycle's outcomes come from concurrent send tasks.
pub(crate) fn report_send_outcome(signal: Signal, outcome: &SendOutcome, dry_run: bool) {
    match outcome {
        // Success lines are product output (stdout at info); the failure line stays a diagnostic
        // (error level), so `main` routes it to stderr — preserving the original channel split.
        SendOutcome::Success { .. } => {
            tracing::info!(target: TARGET, "{}", format_success_line(signal, dry_run))
        }
        SendOutcome::Failure { error, .. } => {
            tracing::error!(target: TARGET, "{}", format_failure_line(signal, error))
        }
    }
}

/// Format the full `PRINT_LOGS` request block for one message as a single string. Pure: no I/O.
fn format_request_block(message: &OTLPMessage) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Sending message ({}):", message.signal.as_str());
    let _ = writeln!(
        out,
        "  Tenant ID: {}",
        message.tenant_id.as_deref().unwrap_or("(none)")
    );
    let _ = writeln!(out, "  Project ID: {}", message.project_id);
    let _ = writeln!(out, "  Source: {}", message.source);
    let _ = writeln!(out, "  Type: {:?}", message.message_type);
    match &message.message {
        MessagePayload::Json(json) => {
            let _ = writeln!(
                out,
                "  Payload: {}",
                serde_json::to_string_pretty(json).unwrap_or_default()
            );
        }
        MessagePayload::Protobuf(bytes) => {
            let _ = writeln!(out, "  Payload: <protobuf {} bytes>", bytes.len());
        }
        MessagePayload::MalformedJson(s) => {
            let _ = writeln!(out, "  Payload: {}", s);
        }
    }
    out
}

/// Format the `PRINT_LOGS` success line for one signal. The signal is named because a cycle's lines
/// are emitted from concurrent send tasks, in completion order.
fn format_success_line(signal: Signal, dry_run: bool) -> String {
    if dry_run {
        format!("[ok] Message generated (dry-run) ({})", signal.as_str())
    } else {
        format!("[ok] Message sent successfully ({})", signal.as_str())
    }
}

/// Format the failure line for one signal.
fn format_failure_line(signal: Signal, error: &GeneratorError) -> String {
    format!(
        "[error] Failed to send {} message: {}",
        signal.as_str(),
        error
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::OTLPMessageType;
    use serde_json::json;

    #[test]
    fn request_block_is_one_atomic_multiline_string() {
        let message = OTLPMessage::new(
            MessagePayload::Json(json!({"resourceLogs": []})),
            Signal::Logs,
            Some("tenant1".to_string()),
            "proj-1".to_string(),
            "src".to_string(),
            OTLPMessageType::Valid,
        );
        let block = format_request_block(&message);
        // One string carries the whole block (so a single event is atomic) and ends with a
        // newline, so the next block starts on its own line.
        assert!(block.starts_with("Sending message (logs):\n"));
        assert!(block.contains("  Tenant ID: tenant1\n"));
        assert!(block.contains("  Project ID: proj-1\n"));
        assert!(block.contains("  Source: src\n"));
        assert!(block.ends_with('\n'));
    }

    #[test]
    fn request_block_renders_protobuf_payload_size() {
        let message = OTLPMessage::new(
            MessagePayload::Protobuf(vec![0u8; 7]),
            Signal::Traces,
            None,
            "proj-1".to_string(),
            "src".to_string(),
            OTLPMessageType::Valid,
        );
        let block = format_request_block(&message);
        assert!(block.starts_with("Sending message (traces):\n"));
        assert!(block.contains("  Tenant ID: (none)\n"));
        assert!(block.contains("  Payload: <protobuf 7 bytes>\n"));
    }

    #[test]
    fn outcome_lines_name_their_signal() {
        // Every outcome line names its signal: a cycle's lines are written by concurrent send
        // tasks, so without it a `--signals logs,traces` run cannot be attributed.
        assert_eq!(
            format_success_line(Signal::Logs, false),
            "[ok] Message sent successfully (logs)"
        );
        assert_eq!(
            format_success_line(Signal::Traces, true),
            "[ok] Message generated (dry-run) (traces)"
        );
        let failure = format_failure_line(Signal::Traces, &GeneratorError::Timeout);
        assert!(failure.starts_with("[error] Failed to send traces message: "));
    }
}
