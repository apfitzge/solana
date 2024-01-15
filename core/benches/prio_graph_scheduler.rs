#![allow(clippy::arithmetic_side_effects)]
#![feature(test)]

use {
    crossbeam_channel::{unbounded, Receiver, Sender},
    itertools::Itertools,
    rand::{thread_rng, Rng},
    rayon::{
        iter::IndexedParallelIterator,
        prelude::{IntoParallelIterator, ParallelIterator},
    },
    solana_core::banking_stage::{
        scheduler_messages::TransactionId,
        transaction_scheduler::{
            prio_graph_scheduler::PrioGraphScheduler, transaction_state::SanitizedTransactionTTL,
            transaction_state_container::TransactionStateContainer,
        },
    },
    solana_runtime::transaction_priority_details::TransactionPriorityDetails,
    solana_sdk::{
        clock::Slot, hash::Hash, signature::Keypair, signer::Signer, system_transaction,
        transaction::SanitizedTransaction,
    },
    test::Bencher,
};

extern crate test;

fn create_accounts(num: usize) -> Vec<Keypair> {
    (0..num).into_par_iter().map(|_| Keypair::new()).collect()
}

fn create_transactions(num: usize) -> Vec<SanitizedTransaction> {
    let accounts = create_accounts(2 * num);
    accounts
        .into_par_iter()
        .chunks(2)
        .map(|chunk| {
            let from = &chunk[0];
            let to = &chunk[1];
            system_transaction::transfer(from, &to.pubkey(), 1, Hash::default())
        })
        .map(SanitizedTransaction::from_transaction_for_tests)
        .collect()
}

fn bench_schedule(bencher: &mut Bencher, batch_size: usize) {
    const NUM_WORKERS: usize = 4;
    const TRANSACTIONS_PER_ITERATION: usize = 10240;
    assert_eq!(
        TRANSACTIONS_PER_ITERATION % batch_size,
        0,
        "batch_size must be a factor of \
        `TRANSACTIONS_PER_ITERATION` ({TRANSACTIONS_PER_ITERATION}) \
        so that bench results are easily comparable"
    );
    let batches_per_iteration = TRANSACTIONS_PER_ITERATION / batch_size;

    let (work_senders, _work_receivers): (Vec<Sender<_>>, Vec<Receiver<_>>) =
        (0..NUM_WORKERS).map(|_| unbounded()).unzip();
    let (_finished_work_sender, finished_work_receiver) = unbounded();
    let mut scheduler = PrioGraphScheduler::new(work_senders, finished_work_receiver);

    // Each iteration has a new container. This is so we can avoid the cost of filling the container
    // in the benchmark time.
    let transactions = create_transactions(2_usize.pow(24));
    let containers: Vec<_> = transactions
        .into_iter()
        .chunks(batch_size)
        .into_iter()
        .map(|chunk| {
            let mut container = TransactionStateContainer::with_capacity(batch_size);
            for (index, transaction) in chunk.enumerate() {
                let id = TransactionId::new(index as u64);
                container.insert_new_transaction(
                    id,
                    SanitizedTransactionTTL {
                        transaction,
                        max_age_slot: Slot::MAX,
                    },
                    TransactionPriorityDetails {
                        priority: thread_rng().gen_range(0..10000),
                        compute_unit_limit: 1500,
                    },
                );
            }
            container
        })
        .collect();
    let mut container_iter = containers.into_iter();

    bencher.iter(move || {
        for _ in 0..batches_per_iteration {
            let mut container = container_iter.next().unwrap();
            let summary = test::black_box(scheduler.schedule(
                &mut container,
                |_, results| results.fill(true),
                |_| true,
            ));
            assert!(summary.is_ok());
        }
    })
}

#[bench]
fn bench_schedule_1024(bencher: &mut Bencher) {
    bench_schedule(bencher, 1024);
}
