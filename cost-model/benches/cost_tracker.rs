#![feature(test)]
extern crate test;
use {
    itertools::Itertools,
    solana_cost_model::{
        cost_tracker::CostTracker,
        transaction_cost::{TransactionCost, UsageCostDetails},
    },
    solana_sdk::pubkey::Pubkey,
    test::Bencher,
};

struct BenchSetup {
    cost_tracker: CostTracker,
    writable_accounts: Vec<Vec<Pubkey>>,
    tx_costs: Vec<TransactionCost>,
}

fn setup(num_transactions: usize, contentious_transactions: bool) -> BenchSetup {
    let mut cost_tracker = CostTracker::default();
    // set cost_tracker with max limits to stretch testing
    cost_tracker.set_limits(u64::MAX, u64::MAX, u64::MAX);

    let max_accounts_per_tx = 128;
    let pubkey = Pubkey::new_unique();
    let mut writable_accounts = Vec::with_capacity(num_transactions);
    let tx_costs = (0..num_transactions)
        .map(|_| {
            let mut usage_cost_details = UsageCostDetails::default();
            writable_accounts.push(
                (0..max_accounts_per_tx)
                    .map(|_| {
                        if contentious_transactions {
                            pubkey
                        } else {
                            Pubkey::new_unique()
                        }
                    })
                    .collect(),
            );
            usage_cost_details.programs_execution_cost = 9999;
            TransactionCost::Transaction(usage_cost_details)
        })
        .collect_vec();

    BenchSetup {
        cost_tracker,
        writable_accounts,
        tx_costs,
    }
}

#[bench]
fn bench_cost_tracker_non_contentious_transaction(bencher: &mut Bencher) {
    let BenchSetup {
        mut cost_tracker,
        writable_accounts,
        tx_costs,
    } = setup(1024, false);

    bencher.iter(|| {
        for (writable_accounts, tx_cost) in writable_accounts.iter().zip(tx_costs.iter()) {
            if cost_tracker
                .try_add(writable_accounts.iter(), tx_cost)
                .is_err()
            {
                break;
            } // stop when hit limits
            cost_tracker.update_execution_cost(writable_accounts.iter(), tx_cost, 0, 0);
            // update execution cost down to zero
        }
    });
}

#[bench]
fn bench_cost_tracker_contentious_transaction(bencher: &mut Bencher) {
    let BenchSetup {
        mut cost_tracker,
        writable_accounts,
        tx_costs,
    } = setup(1024, true);

    bencher.iter(|| {
        for (writable_accounts, tx_cost) in writable_accounts.iter().zip(tx_costs.iter()) {
            if cost_tracker
                .try_add(writable_accounts.iter(), tx_cost)
                .is_err()
            {
                break;
            } // stop when hit limits
            cost_tracker.update_execution_cost(writable_accounts.iter(), tx_cost, 0, 0);
            // update execution cost down to zero
        }
    });
}
