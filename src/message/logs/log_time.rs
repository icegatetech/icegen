//! Timestamp planner for log records. Pure functions parameterised by a
//! [`TimestampJitterConfig`] and an explicit `rng`, mirroring the trace-side
//! [`crate::message::traces::trace_time`]: the [`crate::message::logs::OTLPLogMessageGenerator`]
//! owns configuration and identity, while time layout lives here.

use crate::config::TimestampJitterConfig;
use crate::message::traces::{SpanAnchor, TraceCorrelation};
use rand::Rng;

/// A single planned record's time and, on the correlated path, the trace and span it is attached
/// to. The whole [`SpanAnchor`] is kept (not just its id) because the post-sort overlap nudge has
/// to clamp a moved record back into its own span's window. `trace_id`/`anchor` are `None` on the
/// log-only path, where each record gets freshly generated ids and is bounded by the batch window
/// instead. `trace_id` is carried per slot (not per shard) because a shard may hold several traces.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordSlot {
    pub timestamp_ns: i64,
    pub trace_id: Option<[u8; 16]>,
    pub anchor: Option<SpanAnchor>,
}

/// Sample the per-batch backward offset applied to a whole request, in `0..across_batch` ns.
pub(crate) fn sample_batch_offset_ns(jitter: &TimestampJitterConfig, rng: &mut impl Rng) -> i64 {
    if jitter.across_batch_timestamp_jitter_ns > 0 {
        rng.gen_range(0..jitter.across_batch_timestamp_jitter_ns)
    } else {
        0
    }
}

/// Plan `num_records` monotonic-by-default timestamps anchored at `request_now_ns - batch_offset_ns`.
///
/// Each record advances the base plan by a random step in `0..intra_batch_timestamp_jitter_ns`;
/// with probability `intra_batch_overlap_probability` a record (i > 0) is instead moved backwards
/// relative to its predecessor, producing the out-of-order shape used on the log-only path.
pub(crate) fn plan_timestamps_with_offset(
    jitter: &TimestampJitterConfig,
    request_now_ns: i64,
    batch_offset_ns: i64,
    num_records: usize,
    rng: &mut impl Rng,
) -> Vec<i64> {
    if num_records == 0 {
        return vec![];
    }

    let intra = jitter.intra_batch_timestamp_jitter_ns;
    let overlap_prob = jitter.intra_batch_overlap_probability;

    let mut result: Vec<i64> = Vec::with_capacity(num_records);
    let mut total_span_ns: i64 = 0;
    for _ in 0..num_records {
        let step = if intra > 0 {
            rng.gen_range(0..intra)
        } else {
            0
        };
        total_span_ns += step;
        result.push(step);
    }

    let mut prev_ns = request_now_ns - batch_offset_ns - total_span_ns;
    for (i, step_slot) in result.iter_mut().enumerate() {
        let step = *step_slot;
        let candidate = prev_ns + step;
        *step_slot = if i > 0 && intra > 0 && rng.gen::<f32>() < overlap_prob {
            prev_ns - rng.gen_range(0..intra)
        } else {
            candidate
        };
        prev_ns = candidate;
    }

    result
}

/// Plan timestamps for the records attached to a single span, so every one lands inside that
/// span's window `[start_ns, end_ns]`.
///
/// The normal intra-batch jitter shape from [`plan_timestamps_with_offset`] is reused (anchored at
/// `end_ns` with the across-batch offset clamped to the window length), then every value is clamped
/// into the window so correlated logs never fall outside the span they point at. Unlike the
/// log-only path this ignores `Utc::now()`; the trace planner owns time.
pub(crate) fn plan_timestamps_within_span(
    jitter: &TimestampJitterConfig,
    start_ns: i64,
    end_ns: i64,
    num_records: usize,
    rng: &mut impl Rng,
) -> Vec<i64> {
    if num_records == 0 {
        return vec![];
    }

    let window = (end_ns - start_ns).max(0);
    let batch_offset_ns = if jitter.across_batch_timestamp_jitter_ns > 0 && window > 0 {
        let bound = jitter.across_batch_timestamp_jitter_ns.min(window);
        rng.gen_range(0..bound)
    } else {
        0
    };

    let mut timestamps =
        plan_timestamps_with_offset(jitter, end_ns, batch_offset_ns, num_records, rng);
    for ts in timestamps.iter_mut() {
        *ts = (*ts).clamp(start_ns, end_ns);
    }
    timestamps
}

/// Split `num_records` across the trace's spans, returning one record count per anchor (in
/// anchor order).
pub(crate) fn distribute_records_across_spans(
    anchors: &[SpanAnchor],
    num_records: usize,
    rng: &mut impl Rng,
) -> Vec<usize> {
    let mut counts = vec![num_records / anchors.len(); anchors.len()];
    let remainder = num_records % anchors.len();
    if remainder == 0 {
        return counts;
    }

    let weights: Vec<i64> = anchors
        .iter()
        .map(|anchor| anchor.duration_ns().saturating_add(1))
        .collect();
    let total: i64 = weights.iter().copied().fold(0i64, i64::saturating_add);
    for _ in 0..remainder {
        let mut pick = rng.gen_range(0..total);
        let index = weights
            .iter()
            .position(|&weight| {
                pick -= weight;
                pick < 0
            })
            // `total` is the exact sum of the weights, so the cumulative walk always lands
            // inside it; the last anchor is a defensive fallback against rounding.
            .unwrap_or(anchors.len() - 1);
        counts[index] += 1;
    }
    counts
}

/// Plan the [`RecordSlot`]s a single trace of a correlated shard carries: which span each record
/// belongs to and when it happened. Unsorted; the caller merges the slots of every trace of the
/// shard before ordering them.
pub(crate) fn plan_slots_within_trace(
    jitter: &TimestampJitterConfig,
    correlation: &TraceCorrelation,
    num_records: usize,
    rng: &mut impl Rng,
) -> Vec<RecordSlot> {
    let anchors = &correlation.anchors;
    let counts = distribute_records_across_spans(anchors, num_records, rng);
    let mut slots: Vec<RecordSlot> = Vec::with_capacity(num_records);
    for (&anchor, count) in anchors.iter().zip(counts) {
        slots.extend(
            plan_timestamps_within_span(jitter, anchor.start_ns, anchor.end_ns, count, rng)
                .into_iter()
                .map(|timestamp_ns| RecordSlot {
                    timestamp_ns,
                    trace_id: Some(correlation.trace_id),
                    anchor: Some(anchor),
                }),
        );
    }
    slots
}

/// Plan one [`RecordSlot`] per record of a correlated shard: which trace and span it belongs to
/// and when it happened.
pub(crate) fn plan_correlated_slots(
    jitter: &TimestampJitterConfig,
    correlations: &[TraceCorrelation],
    num_records: usize,
    rng: &mut impl Rng,
) -> Vec<RecordSlot> {
    let base = num_records / correlations.len();
    let remainder = num_records % correlations.len();
    let mut slots: Vec<RecordSlot> = Vec::with_capacity(num_records);
    for (index, correlation) in correlations.iter().enumerate() {
        let count = base + usize::from(index < remainder);
        slots.extend(plan_slots_within_trace(jitter, correlation, count, rng));
    }
    slots.sort_by_key(|slot| slot.timestamp_ns);
    apply_overlap_nudge(jitter, &mut slots, rng);
    slots
}

/// Move records backwards relative to their predecessor with probability
/// `intra_batch_overlap_probability`, the out-of-order shape [`plan_timestamps_with_offset`]
/// produces on the log-only path.
fn apply_overlap_nudge(
    jitter: &TimestampJitterConfig,
    slots: &mut [RecordSlot],
    rng: &mut impl Rng,
) {
    let intra = jitter.intra_batch_timestamp_jitter_ns;
    if intra <= 0 || jitter.intra_batch_overlap_probability <= 0.0 {
        return;
    }

    for slot in slots.iter_mut().skip(1) {
        if rng.gen::<f32>() >= jitter.intra_batch_overlap_probability {
            continue;
        }
        let Some(anchor) = slot.anchor else {
            continue;
        };
        slot.timestamp_ns =
            (slot.timestamp_ns - rng.gen_range(0..intra)).clamp(anchor.start_ns, anchor.end_ns);
    }
}
