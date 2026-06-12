//! Cross-trace conversation pool. A fixed set of `gen_ai.conversation.id` values is created once
//! and shared by the trace generator, so conversation ids recur across traces — modelling
//! multi-trace agent sessions rather than a unique id per trace.

use crate::message::fake_data::FakeDataGenerator;
use rand::{Rng, RngCore};
use std::sync::Arc;

/// Default number of distinct conversation ids in the pool. A constant for now; promote to config
/// if a tunable cardinality is ever needed.
pub const DEFAULT_CONVERSATION_POOL_SIZE: usize = 256;

/// A bounded set of reusable conversation identifiers.
#[derive(Debug, Clone)]
pub struct ConversationPool {
    ids: Vec<String>,
}

impl ConversationPool {
    /// Build a pool of `size` conversation ids (clamped to at least 1) drawn from `rng`.
    ///
    /// # Arguments
    ///
    /// * `size` - number of distinct ids to pre-generate.
    /// * `rng` - randomness source for the identifiers.
    pub fn new(size: usize, rng: &mut dyn RngCore) -> Self {
        let size = size.max(1);
        let ids = (0..size)
            .map(|_| FakeDataGenerator::generate_conversation_id(rng))
            .collect();
        Self { ids }
    }

    /// Build a pool of [`DEFAULT_CONVERSATION_POOL_SIZE`] ids, wrapped in an [`Arc`] for sharing.
    ///
    /// # Arguments
    ///
    /// * `rng` - randomness source for the identifiers.
    pub fn shared_default(rng: &mut dyn RngCore) -> Arc<Self> {
        Arc::new(Self::new(DEFAULT_CONVERSATION_POOL_SIZE, rng))
    }

    /// Pick one conversation id uniformly at random.
    ///
    /// # Arguments
    ///
    /// * `rng` - randomness source for the selection.
    pub fn pick(&self, rng: &mut dyn RngCore) -> &str {
        // `ids` is never empty (see `new`), so the index is always valid.
        &self.ids[rng.gen_range(0..self.ids.len())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn pool_has_requested_size_and_conv_prefix() {
        let mut rng = StdRng::seed_from_u64(1);
        let pool = ConversationPool::new(8, &mut rng);
        assert_eq!(pool.ids.len(), 8);
        assert!(pool.ids.iter().all(|id| id.starts_with("conv-")));
    }

    #[test]
    fn zero_size_is_clamped_to_one() {
        let mut rng = StdRng::seed_from_u64(2);
        let pool = ConversationPool::new(0, &mut rng);
        assert_eq!(pool.ids.len(), 1);
    }

    #[test]
    fn pick_returns_a_pool_member() {
        let mut rng = StdRng::seed_from_u64(3);
        let pool = ConversationPool::new(4, &mut rng);
        let picked = pool.pick(&mut rng).to_string();
        assert!(pool.ids.contains(&picked));
    }
}
