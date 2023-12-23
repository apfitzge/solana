#![allow(clippy::integer_arithmetic)]
#![feature(test)]

use {
    crossbeam_channel::unbounded,
    itertools::Itertools,
    rayon::iter::{IntoParallelIterator, ParallelIterator},
    solana_core::{
        banking_stage::{packet_receiver::PacketReceiver, BankingStageStats},
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        tracer_packet_stats::TracerPacketStats,
        unprocessed_packet_batches::UnprocessedPacketBatches,
        unprocessed_transaction_storage::{ThreadType, UnprocessedTransactionStorage},
    },
    solana_ledger::genesis_utils::create_genesis_config,
    solana_perf::packet::PacketBatch,
    solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::GenesisConfigInfo},
    solana_sdk::{
        hash::Hash, packet::Packet, signature::Keypair, signer::Signer, system_transaction,
    },
    std::sync::{Arc, RwLock},
    test::Bencher,
};

extern crate test;

fn create_accounts(num: usize) -> Vec<Keypair> {
    (0..num).into_iter().map(|_| Keypair::new()).collect()
}

fn create_transactions(num: usize) -> PacketBatch {
    let accounts = create_accounts(2 * num);
    PacketBatch::new(
        accounts
            .chunks(2)
            .into_iter()
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
            .collect(),
    )
}

fn bench_packet_receiver(bencher: &mut Bencher, batch_size: usize) {
    let mint_total = u64::MAX;
    let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(mint_total);
    let bank_forks = Arc::new(RwLock::new(BankForks::new(Bank::new_for_benches(
        &genesis_config,
    ))));

    let (sender, receiver) = unbounded();
    let mut packet_receiver = PacketReceiver::new(0, receiver, bank_forks);

    let mut unprocessed_transaction_storage =
        UnprocessedTransactionStorage::new_transaction_storage(
            UnprocessedPacketBatches::with_capacity(175_000),
            ThreadType::Transactions,
        );

    let banking_stage_stats = &mut BankingStageStats::default();
    let tracer_packet_stats = &mut TracerPacketStats::default();
    let slot_metrics_tracker = &mut LeaderSlotMetricsTracker::new(0);

    const NUM_BATCHES: usize = 2048;
    const NUM_INNER_BATCHES: usize = 4;
    let batches: Vec<_> = (0..NUM_BATCHES)
        .into_par_iter()
        .map(|_| {
            let packet_batches = (0..NUM_INNER_BATCHES)
                .map(|_| create_transactions(batch_size))
                .collect_vec();
            Arc::new((packet_batches, None))
        })
        .collect();
    let mut batches_iter = batches.into_iter().cycle();
    sender.send(batches_iter.next().unwrap()).unwrap();
    sender.send(batches_iter.next().unwrap()).unwrap();

    eprintln!("beginning bench for {batch_size}");
    bencher.iter(move || {
        let _ = packet_receiver.receive_and_buffer_packets(
            &mut unprocessed_transaction_storage,
            banking_stage_stats,
            tracer_packet_stats,
            slot_metrics_tracker,
        );
        unprocessed_transaction_storage.clear();
        sender.send(batches_iter.next().unwrap()).unwrap();
        sender.send(batches_iter.next().unwrap()).unwrap();
    });
}

#[bench]
fn bench_packet_receiver_1(bencher: &mut Bencher) {
    bench_packet_receiver(bencher, 1);
}

#[bench]
fn bench_packet_receiver_128(bencher: &mut Bencher) {
    bench_packet_receiver(bencher, 128);
}

#[bench]
fn bench_packet_receiver_1024(bencher: &mut Bencher) {
    bench_packet_receiver(bencher, 1024);
}

#[bench]
fn bench_packet_receiver_2500(bencher: &mut Bencher) {
    bench_packet_receiver(bencher, 2500);
}
