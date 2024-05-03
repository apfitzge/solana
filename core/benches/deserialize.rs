#![allow(clippy::arithmetic_side_effects)]
#![feature(test)]

use {
    rand::{thread_rng, Rng},
    rayon::prelude::*,
    solana_core::transaction_view::TransactionView,
    solana_perf::packet::{to_packet_batches, Packet},
    solana_sdk::{
        hash::Hash,
        message::{Message, SimpleAddressLoader},
        pubkey,
        short_vec::decode_shortu16_len,
        signature::{Keypair, Signature, Signer},
        system_instruction, system_transaction,
        transaction::{
            SanitizedTransaction, SanitizedVersionedTransaction, Transaction, TransactionError,
            VersionedTransaction,
        },
    },
    std::{collections::HashSet, iter::repeat_with},
    test::Bencher,
};

extern crate test;

fn make_accounts_txs(txes: usize) -> Vec<Transaction> {
    let keypair = Keypair::new();
    let to_pubkey = pubkey::new_rand();
    let hash = Hash::new_unique();
    let dummy = system_transaction::transfer(&keypair, &to_pubkey, 1, hash);
    (0..txes)
        .into_par_iter()
        .map(|_| {
            let mut new = dummy.clone();
            let sig: [u8; 64] = std::array::from_fn(|_| thread_rng().gen::<u8>());
            new.message.account_keys[0] = pubkey::new_rand();
            new.message.account_keys[1] = pubkey::new_rand();
            new.signatures = vec![Signature::from(sig)];
            new
        })
        .collect()
}

fn make_programs_txs(txes: usize) -> Vec<Transaction> {
    let progs = 4;
    (0..txes)
        .map(|_| {
            let from_key = Keypair::new();
            let instructions: Vec<_> = repeat_with(|| {
                let to_key = pubkey::new_rand();
                system_instruction::transfer(&from_key.pubkey(), &to_key, 1)
            })
            .take(progs)
            .collect();
            let message = Message::new(&instructions, Some(&from_key.pubkey()));
            let hash = Hash::new_unique();
            Transaction::new(&[&from_key], message, hash)
        })
        .collect()
}

enum TransactionType {
    Accounts,
    Programs,
}

fn bench_deserialize(bencher: &mut Bencher, tx_type: TransactionType) {
    solana_logger::setup();

    const CHUNKS: usize = 8;
    const PACKETS_PER_BATCH: usize = 192;
    let txes = PACKETS_PER_BATCH * CHUNKS;

    let transactions = match tx_type {
        TransactionType::Accounts => make_accounts_txs(txes),
        TransactionType::Programs => make_programs_txs(txes),
    };

    let verified: Vec<_> = to_packet_batches(&transactions, PACKETS_PER_BATCH);

    let mut packet_batch_iterator = verified.iter().cycle();
    let mut transaction_view = TransactionView::default();
    let mut transaction_views = core::iter::repeat_with(TransactionView::default)
        .take(PACKETS_PER_BATCH)
        .collect::<Vec<_>>();
    bencher.iter(|| {
        let packet_batch = packet_batch_iterator.next().unwrap();
        for (idx, packet) in packet_batch.iter().enumerate() {
            // let _ = SanitizedVersionedTransaction::try_new(
            //     test::black_box(packet.clone())
            //         .deserialize_slice::<VersionedTransaction, _>(..)
            //         .unwrap(),
            // )
            // .unwrap();

            // let mut transaction_view = TransactionView::try_new(test::black_box(&packet)).unwrap();
            // transaction_view.sanitize().unwrap();

            transaction_views[idx]
                .populate_from(test::black_box(packet))
                .unwrap();
            transaction_views[idx].sanitize().unwrap();
        }
    });
}

fn packet_message(packet: &Packet) -> Result<&[u8], TransactionError> {
    let (sig_len, sig_size) = packet
        .data(..)
        .and_then(|bytes| decode_shortu16_len(bytes).ok())
        .ok_or(TransactionError::SanitizeFailure)?;
    sig_len
        .checked_mul(core::mem::size_of::<Signature>())
        .and_then(|v| v.checked_add(sig_size))
        .and_then(|msg_start| packet.data(msg_start..))
        .ok_or(TransactionError::SanitizeFailure)
}

#[bench]
fn bench_deserialize_multi_accounts(bencher: &mut Bencher) {
    bench_deserialize(bencher, TransactionType::Accounts);
}

#[bench]
fn bench_deserialize_multi_programs(bencher: &mut Bencher) {
    bench_deserialize(bencher, TransactionType::Programs);
}
