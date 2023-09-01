//! Manages a set of recent invalid fee payers.
//! This is used to filter out transactions that are unlikely to be included
//! in any block, much earlier in the transaction pipeline.
//!

use {
    solana_bloom::bloom::{AtomicBloom, Bloom},
    solana_sdk::{pubkey::Pubkey, timing::AtomicInterval},
    std::sync::atomic::{AtomicUsize, Ordering},
};

pub struct InvalidFeePayerFilter {
    filter_index: AtomicUsize,
    filters: [AtomicBloom<Pubkey>; 2],

    stats: InvalidFeePayerFilterStats,
}

impl Default for InvalidFeePayerFilter {
    fn default() -> Self {
        const NUM_INVALID_FEE_PAYERS: usize = 4096;
        const FALSE_POSITIVE_RATE: f64 = 0.000001;
        const MAX_BITS: usize = 16 * 1024 * 8;

        let bloom0 = Bloom::random(NUM_INVALID_FEE_PAYERS, FALSE_POSITIVE_RATE, MAX_BITS);
        let bloom1 = Bloom::random(NUM_INVALID_FEE_PAYERS, FALSE_POSITIVE_RATE, MAX_BITS);

        Self {
            filter_index: AtomicUsize::new(0),
            filters: [bloom0.into(), bloom1.into()],
            stats: InvalidFeePayerFilterStats::default(),
        }
    }
}

impl InvalidFeePayerFilter {
    /// Add a pubkey to the filter.
    pub fn add(&self, pubkey: &Pubkey) {
        self.stats.num_added.fetch_add(1, Ordering::Relaxed);
        let filter_index = self.filter_index.load(Ordering::Acquire);
        self.filters[filter_index].add(pubkey);
    }

    /// Check if a pubkey is in the filter.
    pub fn should_reject(&self, pubkey: &Pubkey) -> bool {
        let filter_index = self.filter_index.load(Ordering::Acquire);
        let should_reject = self.filters[filter_index].contains(pubkey);
        if should_reject {
            self.stats.num_rejects.fetch_add(1, Ordering::Relaxed);
        }
        should_reject
    }

    /// Reset the filter if enough time has passed since last reset.
    pub fn reset_on_interval(&self) {
        if self.stats.try_report() {
            self.reset();
        }
    }

    /// Atomically toggle `filter_index` between 0 and 1, and clear the filter at the old index.
    /// This will not block any other threads, however could clear bits for an in-progress
    /// `should_reject` or `add`.
    fn reset(&self) {
        let old_filter_index = self.filter_index.fetch_xor(1, Ordering::AcqRel);
        self.filters[old_filter_index].clear();
    }
}

#[derive(Default)]
struct InvalidFeePayerFilterStats {
    interval: AtomicInterval,
    num_added: AtomicUsize,
    num_rejects: AtomicUsize,
}

impl InvalidFeePayerFilterStats {
    fn try_report(&self) -> bool {
        const REPORT_INTERVAL_MS: u64 = 2000;

        if self.interval.should_update(REPORT_INTERVAL_MS) {
            let num_added = self.num_added.swap(0, Ordering::Relaxed);
            if num_added > 0 {
                let num_rejects = self.num_rejects.swap(0, Ordering::Relaxed);
                datapoint_info!(
                    "invalid_fee_payer_filter_stats",
                    ("added", num_added, i64),
                    ("rejects", num_rejects, i64),
                );
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_sdk::pubkey::Pubkey, std::sync::Arc};

    #[test]
    fn test_invalid_fee_payer_filter() {
        let invalid_fee_payer_filter = Arc::new(InvalidFeePayerFilter::default());
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        assert!(!invalid_fee_payer_filter.should_reject(&pubkey1));
        assert!(!invalid_fee_payer_filter.should_reject(&pubkey2));
        invalid_fee_payer_filter.add(&pubkey1);
        assert!(invalid_fee_payer_filter.should_reject(&pubkey1));
        assert!(!invalid_fee_payer_filter.should_reject(&pubkey2));

        invalid_fee_payer_filter.reset();
        assert!(!invalid_fee_payer_filter.should_reject(&pubkey1));
        assert!(!invalid_fee_payer_filter.should_reject(&pubkey2));
    }
}
