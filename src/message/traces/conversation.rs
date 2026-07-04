//! Sequential, budgeted source of `gen_ai.conversation.id` values. Each id is handed out a bounded
//! number of times (1–3 traces) and then retired, so a conversation holds a small, fixed set of
//! traces instead of accumulating them without end. A single shared cursor advances under a lock,
//! so conversations are emitted sequentially and no id ever exceeds its budget — regardless of how
//! many workers draw from it.

use crate::message::fake_data::FakeDataGenerator;
use rand::{Rng, RngCore};
use std::sync::{Arc, Mutex};

/// Minimum number of traces stamped with a single conversation id.
pub const MIN_TRACES_PER_CONVERSATION: u32 = 1;
/// Maximum number of traces stamped with a single conversation id. Each conversation's budget is
/// drawn uniformly from `[MIN_TRACES_PER_CONVERSATION, MAX_TRACES_PER_CONVERSATION]`.
pub const MAX_TRACES_PER_CONVERSATION: u32 = 3;

/// The in-flight conversation and how many more traces may still carry it.
#[derive(Debug)]
struct CursorState {
    /// Current conversation id, or `None` before the first id is minted.
    current: Option<String>,
    /// Traces still to be stamped with `current` before it is retired.
    remaining: u32,
}

/// Hands out conversation ids one trace at a time, minting a fresh id with a new 1–3 budget
/// whenever the current one is exhausted.
#[derive(Debug)]
pub struct ConversationCursor {
    // The cursor is shared by every worker (it lives behind an `Arc`), so the handout must be
    // guarded; the lock also makes each conversation's trace count exact under concurrency.
    state: Mutex<CursorState>,
}

impl ConversationCursor {
    /// Create an empty cursor; the first [`ConversationCursor::next_id`] call mints the first id.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CursorState {
                current: None,
                remaining: 0,
            }),
        }
    }

    /// Create an empty cursor wrapped in an [`Arc`] for sharing across workers.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Return the conversation id for the next trace, advancing the cursor.
    ///
    /// The current id is returned until its budget is spent; the call that spends the last unit
    /// leaves the cursor empty, so the *next* call mints a fresh id. Each id is therefore returned
    /// exactly `budget` times, where `budget` is uniform in
    /// `[MIN_TRACES_PER_CONVERSATION, MAX_TRACES_PER_CONVERSATION]`.
    ///
    /// # Arguments
    ///
    /// * `rng` - randomness source for minting ids and drawing budgets.
    pub fn next_id(&self, rng: &mut dyn RngCore) -> String {
        // The `Mutex` is only poisoned if a holder panicked; recover the guard so one panicked
        // trace does not wedge every worker.
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.remaining == 0 {
            state.current = Some(FakeDataGenerator::generate_conversation_id(rng));
            state.remaining =
                rng.gen_range(MIN_TRACES_PER_CONVERSATION..=MAX_TRACES_PER_CONVERSATION);
        }
        state.remaining -= 1;
        // `current` is always `Some` here: it was just set above, or a previous call set it while
        // leaving `remaining > 0`.
        state
            .current
            .clone()
            .expect("current conversation id is set whenever remaining was > 0")
    }
}

impl Default for ConversationCursor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::{HashMap, HashSet};

    /// Draw `n` ids in sequence from a freshly seeded cursor.
    fn draw(seed: u64, n: usize) -> Vec<String> {
        let mut rng = StdRng::seed_from_u64(seed);
        let cursor = ConversationCursor::new();
        (0..n).map(|_| cursor.next_id(&mut rng)).collect()
    }

    /// Total number of times each id appears across the whole sequence.
    fn counts(ids: &[String]) -> HashMap<String, usize> {
        let mut m = HashMap::new();
        for id in ids {
            *m.entry(id.clone()).or_insert(0) += 1;
        }
        m
    }

    const MIN: usize = MIN_TRACES_PER_CONVERSATION as usize;
    const MAX: usize = MAX_TRACES_PER_CONVERSATION as usize;

    #[test]
    fn ids_carry_the_conv_prefix() {
        assert!(draw(1, 20).iter().all(|id| id.starts_with("conv-")));
    }

    #[test]
    fn first_call_mints_an_id() {
        let mut rng = StdRng::seed_from_u64(5);
        let cursor = ConversationCursor::new();
        assert!(cursor.next_id(&mut rng).starts_with("conv-"));
    }

    /// The core guarantee that replaces the old unbounded pool: a conversation id is never stamped
    /// on more than `MAX` traces (nor fewer than `MIN`).
    #[test]
    fn no_conversation_exceeds_its_budget() {
        for (id, count) in counts(&draw(7, 5_000)) {
            assert!(
                (MIN..=MAX).contains(&count),
                "id {id} used {count} times, outside {MIN}..={MAX}",
            );
        }
    }

    /// Over many conversations every budget in 1..=3 occurs at least once (uniform distribution).
    #[test]
    fn budgets_span_the_full_range() {
        let observed: HashSet<usize> = counts(&draw(42, 5_000)).into_values().collect();
        for b in MIN..=MAX {
            assert!(observed.contains(&b), "budget {b} never occurred");
        }
    }

    /// Ids are handed out in contiguous runs — a conversation's traces are consecutive — and each
    /// run is 1..=3 long.
    #[test]
    fn ids_are_handed_out_in_bounded_contiguous_runs() {
        let ids = draw(3, 300);
        let mut i = 0;
        while i < ids.len() {
            let mut run = 1;
            while i + run < ids.len() && ids[i + run] == ids[i] {
                run += 1;
            }
            assert!((MIN..=MAX).contains(&run), "run of {run} for {}", ids[i]);
            i += run;
        }
    }

    #[test]
    fn seeded_runs_are_reproducible() {
        assert_eq!(draw(99, 200), draw(99, 200));
    }
}
