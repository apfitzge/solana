//! Rough filter on write account locks to prevent too many transactions locking the same
//! accounts enter the buffered queue.
//!

use {
    ahash::AHasher,
    rand::{thread_rng, Rng},
    solana_runtime::block_cost_limits::MAX_WRITABLE_ACCOUNT_UNITS,
    solana_sdk::{pubkey::Pubkey, saturating_add_assign},
    std::{
        hash::Hasher,
        time::{Duration, Instant},
    },
};

const HASH_BUFFER_SIZE: usize = 1_000_000;
const RECEIVE_FILTER_ACCOUNT_MAX_CU: u64 = 4 * MAX_WRITABLE_ACCOUNT_UNITS;

/// Filters out transactions exceeding a write-account compute-unit threshold.
pub struct ReceiveAccountFilter {
    /// Seed for hashing write-accounts
    seed: (u128, u128),
    /// Total compute units for write-account hash
    compute_units: Vec<u64>,
    /// Banking stage thread id - used to distringuish metrics
    id: u32,
    /// Last clearing time - should be cleared incrementally
    last_clear_time: Instant,
    /// Count the number of filtered transactions
    num_filtered: u64,
}

impl ReceiveAccountFilter {
    /// Creates a new filter with random seed
    pub fn new(id: u32) -> Self {
        let seed = thread_rng().gen();

        Self {
            seed,
            compute_units: vec![0; HASH_BUFFER_SIZE],
            id,
            last_clear_time: Instant::now(),
            num_filtered: 0,
        }
    }

    /// Reset the filter and send metrics
    pub fn reset(&mut self) {
        const CLEAR_INTERVAL: Duration = Duration::from_secs(1);
        if self.last_clear_time.elapsed() >= CLEAR_INTERVAL {
            self.report_metrics();
            self.last_clear_time = Instant::now();
            self.compute_units.clear();
            self.compute_units.resize(HASH_BUFFER_SIZE, 0);
            self.num_filtered = 0;
        }
    }

    /// Iterates over accounts and accumulates CUs for each write account
    /// Returns true if the transaction should be filtered out
    pub fn should_filter(&mut self, write_accounts: &[&Pubkey], compute_units: u64) -> bool {
        let mut should_filter = false;
        for account in write_accounts {
            let bin = self.get_account_bin(account);
            saturating_add_assign!(self.compute_units[bin], compute_units);

            // Add to all bins - return true if any exceed
            should_filter |= self.compute_units[bin] > RECEIVE_FILTER_ACCOUNT_MAX_CU;
        }

        if should_filter {
            saturating_add_assign!(self.num_filtered, 1);
        }

        should_filter
    }

    /// Compute hash for write-account. Returns (hash, bin_pos)
    fn get_account_bin(&self, write_account: &Pubkey) -> usize {
        let mut hasher = AHasher::new_with_keys(self.seed.0, self.seed.1);
        hasher.write(write_account.as_ref());
        let h = hasher.finish();
        (usize::try_from(h).unwrap()).wrapping_rem(HASH_BUFFER_SIZE)
    }

    /// Report metrics on how many transactions were filtered out
    fn report_metrics(&self) {
        datapoint_info!(
            "receive_account_filter",
            ("id", self.id, i64),
            ("num_filtered", self.num_filtered, i64),
        );
    }
}
