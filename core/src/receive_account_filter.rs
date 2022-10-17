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
    /// Count the number of passed transactions
    num_passed: u64,
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
            num_passed: 0,
        }
    }

    /// Reset the filter and send metrics
    pub fn reset_on_interval(&mut self) {
        const CLEAR_INTERVAL: Duration = Duration::from_millis(2000);
        if self.last_clear_time.elapsed() >= CLEAR_INTERVAL {
            self.report_metrics();
            self.reset();
        }
    }

    /// Iterates over accounts and accumulates CUs for each write account
    /// Returns true if the transaction should be filtered out
    ///
    /// Note: If two or more keys in the same transaction hash to the same bin, the CU count check only considers them once.
    ///       but the value is incremented for all.
    pub fn should_filter<'a>(
        &mut self,
        write_accounts: impl Iterator<Item = &'a Pubkey>,
        compute_units: u64,
    ) -> bool {
        let bins = write_accounts
            .map(|a| self.get_account_bin(a))
            .collect::<Vec<_>>();

        for bin in bins.iter() {
            if self.compute_units[*bin] + compute_units > RECEIVE_FILTER_ACCOUNT_MAX_CU {
                saturating_add_assign!(self.num_filtered, 1);
                return true;
            }
        }

        for bin in bins {
            saturating_add_assign!(self.compute_units[bin], compute_units);
        }

        saturating_add_assign!(self.num_passed, 1);
        false
    }

    /// Compute hash for write-account.
    fn get_account_bin(&self, write_account: &Pubkey) -> usize {
        let mut hasher = AHasher::new_with_keys(self.seed.0, self.seed.1);
        hasher.write(write_account.as_ref());
        let h = hasher.finish();
        (usize::try_from(h).unwrap()).wrapping_rem(HASH_BUFFER_SIZE)
    }

    /// Reset state
    fn reset(&mut self) {
        self.last_clear_time = Instant::now();
        self.compute_units.clear();
        self.compute_units.resize(HASH_BUFFER_SIZE, 0);
        self.num_filtered = 0;
        self.num_passed = 0;
    }

    /// Report metrics on how many transactions were filtered out
    fn report_metrics(&self) {
        datapoint_info!(
            "receive_account_filter",
            ("id", self.id, i64),
            ("num_filtered", self.num_filtered, i64),
            ("num_passed", self.num_passed, i64),
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::receive_account_filter::RECEIVE_FILTER_ACCOUNT_MAX_CU, solana_sdk::pubkey::Pubkey,
    };

    #[test]
    fn test_should_filter() {
        // TODO: this test is flaky - we're relying on hash function binning w/ random keys
        const TEST_TX_COST: u64 = RECEIVE_FILTER_ACCOUNT_MAX_CU / 2;
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let pk3 = Pubkey::new_unique();
        let pk4 = Pubkey::new_unique();

        let mut filter = super::ReceiveAccountFilter::new(0);

        assert!(!filter.should_filter([pk1, pk2].iter(), TEST_TX_COST)); // under limit shouldn't filter
        assert!(!filter.should_filter([pk1, pk3].iter(), TEST_TX_COST)); // at limit for pk1, shouldn't filter
        assert!(filter.should_filter([pk1, pk4].iter(), TEST_TX_COST)); // above limit for pk1, should filter - pk4 is not incremented
        assert!(!filter.should_filter([pk2, pk4].iter(), TEST_TX_COST)); // at limit for pk2, shoulnd't filter
        assert!(!filter.should_filter([pk3, pk4].iter(), TEST_TX_COST)); // at limit for pk3 and pk4, shoulnd't filter
        filter.reset(); // force reset - no interval check
        assert!(!filter.should_filter([pk1, pk2].iter(), TEST_TX_COST)); // limits reset - we can do this again
    }
}
