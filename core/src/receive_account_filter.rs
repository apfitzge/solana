//! Rough filter on write account locks to prevent too many transactions locking the same
//! accounts enter the buffered queue.
//!

use {
    ahash::AHasher,
    rand::thread_rng,
    solana_runtime::block_cost_limits::MAX_WRITABLE_ACCOUNT_UNITS,
    solana_sdk::{pubkey::Pubkey, saturating_add_assign},
    std::{hash::Hasher, time::Instant},
};

const HASH_BUFFER_SIZE: usize = 1_000_000;
const RECEIVE_FILTER_ACCOUNT_MAX_CU: u64 = 4 * MAX_WRITABLE_ACCOUNT_UNITS;

/// Filters out transactions exceeding a write-account compute-unit threshold.
pub struct ReceiveAccountFilter {
    /// Last clearing time - should be cleared incrementally
    last_clear_time: Instant,
    /// Total compute units for write-account hash
    compute_units: [u64; HASH_BUFFER_SIZE],
    /// Seed for hashing write-accounts
    seed: (u128, u128),
}

impl ReceiveAccountFilter {
    /// Creates a new filter with random seed
    pub fn new() -> Self {
        let seed = thread_rng().gen();
        Self {
            last_clear_time: Instant::now(),
            compute_units: [0; HASH_BUFFER_SIZE],
            seed,
        }
    }

    /// Reset the filter and send metrics
    pub fn reset(&mut self) {
        self.last_clear_time = Instant::now();
        self.compute_units = [0; HASH_BUFFER_SIZE];
    }

    /// Iterates over accounts and accumulates CUs for each write account
    /// Returns true if the transaction should be filtered out
    pub fn should_filter(&mut self, write_accounts: &[Pubkey], compute_units: u64) -> bool {
        let mut should_filter = false;
        for account in write_accounts {
            let bin = self.get_account_bin(write_account);
            saturating_add_assign!(self.compute_units[bin], compute_units);

            // Add to all bins - return true if any exceed
            should_filter |= self.compute_units[bin] > RECEIVE_FILTER_ACCOUNT_MAX_CU;
        }

        should_filter
    }

    /// Compute hash for write-account. Returns (hash, bin_pos)
    fn get_account_bin(&self, write_account: &Pubkey) -> (u64, usize) {
        let mut hasher = AHasher::new_with_keys(self.seed.0, self.seed.1);
        hasher.write(write_account.as_ref()).unwrap_or_default();
        let h = hasher.finish();
        (usize::try_from(h).unwrap()).wrapping_rem(HASH_BUFFER_SIZE)
    }
}
