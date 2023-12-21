#![allow(clippy::integer_arithmetic)]
#![feature(test)]

use {
    crossbeam_channel::{unbounded, Receiver},
    itertools::Itertools,
    min_max_heap::MinMaxHeap,
    rayon::{
        iter::IndexedParallelIterator,
        prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator},
    },
    solana_core::{
        banking_stage::{committer::Committer, consumer::Consumer},
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        multi_iterator_scanner::{MultiIteratorScanner, ProcessingDecision},
        qos_service::QosService,
        unprocessed_transaction_storage::{
            ThreadLocalUnprocessedPackets, UnprocessedTransactionStorage,
        },
    },
    solana_entry::entry::Entry,
    solana_ledger::{
        blockstore::Blockstore,
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
    },
    solana_poh::{
        poh_recorder::{create_test_recorder, PohRecorder},
        poh_service::PohService,
    },
    solana_runtime::bank::Bank,
    solana_sdk::{
        account::Account,
        feature_set::apply_cost_tracker_during_replay,
        hash::Hash,
        packet::Packet,
        signature::Keypair,
        signer::Signer,
        stake_history::Epoch,
        system_program, system_transaction,
        transaction::{self, SanitizedTransaction},
    },
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    tempfile::TempDir,
    test::Bencher,
};

extern crate test;

fn create_accounts(num: usize) -> Vec<Keypair> {
    (0..num).into_par_iter().map(|_| Keypair::new()).collect()
}

fn create_transactions(num: usize) -> Vec<Arc<ImmutableDeserializedPacket>> {
    let accounts = create_accounts(2 * num);
    accounts
        .into_par_iter()
        .chunks(2)
        .map(|chunk| {
            let from = &chunk[0];
            let to = &chunk[1];
            system_transaction::transfer(from, &to.pubkey(), 1, Hash::default())
        })
        .map(|tx| {
            let mut packet = Packet::default();
            Packet::populate_packet(&mut packet, None, &tx).unwrap();
            packet
        })
        .map(|packet| ImmutableDeserializedPacket::new(packet).unwrap())
        .map(Arc::new)
        .collect()
}

fn bench_unprocessed_transaction_storage(bencher: &mut Bencher, capacity: usize) {
    let transactions = create_transactions(capacity);
    let mut heap = MinMaxHeap::with_capacity(capacity);
    for transaction in transactions {
        heap.push(transaction);
    }

    bencher.iter(move || {
        let mut retryable_packets = core::mem::take(&mut heap);
        assert_eq!(retryable_packets.len(), capacity, "fucked something up");
        let original_capacity = retryable_packets.capacity();
        let all_packets_to_process = retryable_packets.drain_desc().collect_vec();
        let mut new_retryable_packets = MinMaxHeap::with_capacity(original_capacity);

        let mut scanner = MultiIteratorScanner::new(&all_packets_to_process, 64, (), |_, _| {
            ProcessingDecision::Now
        });

        while let Some((chunk, _)) = scanner.iterate() {
            let packets_to_process = chunk.into_iter().map(|p| (*p).clone()).collect_vec();
            new_retryable_packets.extend(packets_to_process);
        }

        // swap so next iter works
        core::mem::swap(&mut heap, &mut new_retryable_packets);
    })
}

#[bench]
fn bench_unprocessed_transaction_storage_1000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 1000);
}

#[bench]
fn bench_unprocessed_transaction_storage_10000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 10000);
}

#[bench]
fn bench_unprocessed_transaction_storage_50000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 50000);
}

#[bench]
fn bench_unprocessed_transaction_storage_75000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 75000);
}

#[bench]
fn bench_unprocessed_transaction_storage_100000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 100000);
}

#[bench]
fn bench_unprocessed_transaction_storage_125000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 125000);
}

#[bench]
fn bench_unprocessed_transaction_storage_150000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 150000);
}

#[bench]
fn bench_unprocessed_transaction_storage_175000(bencher: &mut Bencher) {
    bench_unprocessed_transaction_storage(bencher, 175000);
}
