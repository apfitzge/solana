#![feature(test)]

use {
    agave_transaction_view::transaction_view::TransactionView,
    solana_sdk::{
        hash::Hash,
        message::Message,
        packet::{Packet, PACKET_DATA_SIZE},
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        system_instruction, system_transaction,
        transaction::{Transaction, VersionedTransaction},
    },
    test::Bencher,
};
extern crate test;

const NUM_PACKETS: usize = 1024;

fn bench_versioned_transaction_deserialize_packets(bencher: &mut Bencher, packets: &[Packet]) {
    bencher.iter(|| {
        for packet in packets.iter() {
            let _: VersionedTransaction = packet.deserialize_slice(..).unwrap();
        }
    });
}

fn bench_transaction_view_try_new_from_slice_packets(bencher: &mut Bencher, packets: &[Packet]) {
    bencher.iter(|| {
        for packet in packets.iter() {
            let _ = TransactionView::try_new_from_slice(packet.data(..).unwrap()).unwrap();
        }
    })
}

fn bench_transaction_view_copy_from_slice_packets(bencher: &mut Bencher, packets: &[Packet]) {
    let mut transaction_views = (0..NUM_PACKETS)
        .map(|_| TransactionView::default())
        .collect::<Vec<_>>();
    bencher.iter(|| {
        for idx in 0..NUM_PACKETS {
            TransactionView::copy_from_slice(
                &mut transaction_views[idx],
                packets[idx].data(..).unwrap(),
            )
            .unwrap();
        }
    })
}

fn bench_transaction_view_try_new_from_boxed_data_packets(
    bencher: &mut Bencher,
    packets: &[Packet],
) {
    let mut boxed_data_and_lens = packets
        .iter()
        .map(|packet| {
            let packet_data = packet.data(..).unwrap();
            let mut boxed_data = Box::new([0u8; PACKET_DATA_SIZE]);
            boxed_data[..packet_data.len()].copy_from_slice(packet_data);
            Some((boxed_data, packet_data.len()))
        })
        .collect::<Vec<_>>();

    bencher.iter(|| {
        for boxed_data_and_len in boxed_data_and_lens.iter_mut() {
            let (boxed_data, len) = boxed_data_and_len.take().unwrap();
            let transaction_view =
                TransactionView::try_new_from_boxed_data(boxed_data, len).unwrap();
            // Take back ownership so that the boxed data is not dropped, and we can re-use.
            // Makes sure we are not freeing or allocating memory in the benchmark.
            let _ = boxed_data_and_len.insert(transaction_view.take_data());
        }
    });
}

// Create a simple transfer transaction, convert to a `Packet`.
fn create_simple_transfer_packet() -> Packet {
    let transaction =
        system_transaction::transfer(&Keypair::new(), &Pubkey::new_unique(), 1, Hash::default());
    let versioned_transaction = VersionedTransaction::from(transaction);
    let mut packet = Packet::default();
    packet
        .populate_packet(None, &versioned_transaction)
        .unwrap();
    packet
}

// Create a transaction that does as many transfer ixs as possible.
// Convert to Packet.
fn create_packed_transfer_packet() -> Packet {
    let from_keypair = Keypair::new();
    let to_pubkey = Pubkey::new_unique();
    // 60 transfer instructions.
    let ixs = system_instruction::transfer_many(&from_keypair.pubkey(), &[(to_pubkey, 1); 60]);
    let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
    let transaction = Transaction::new(&[from_keypair], message, Hash::default());
    let versioned_transaction = VersionedTransaction::from(transaction);
    let mut packet = Packet::default();
    packet
        .populate_packet(None, &versioned_transaction)
        .unwrap();
    packet
}

fn create_simple_transfer_packets() -> Vec<Packet> {
    (0..NUM_PACKETS)
        .map(|_| create_simple_transfer_packet())
        .collect::<Vec<_>>()
}

fn create_many_transfer_packets() -> Vec<Packet> {
    (0..NUM_PACKETS)
        .map(|_| create_packed_transfer_packet())
        .collect::<Vec<_>>()
}

#[bench]
fn bench_versioned_transaction_deserialization_simple_transfer(bencher: &mut Bencher) {
    let packets = create_simple_transfer_packets();
    bench_versioned_transaction_deserialize_packets(bencher, &packets);
}

#[bench]
fn bench_transaction_view_try_new_from_slice_simple_transfer(bencher: &mut Bencher) {
    let packets = create_simple_transfer_packets();
    bench_transaction_view_try_new_from_slice_packets(bencher, &packets);
}

#[bench]
fn bench_transaction_view_copy_from_slice_simple_transfer(bencher: &mut Bencher) {
    let packets = create_simple_transfer_packets();
    bench_transaction_view_copy_from_slice_packets(bencher, &packets);
}

#[bench]
fn bench_transaction_view_try_new_from_boxed_data_simple_transfer(bencher: &mut Bencher) {
    let packets = create_simple_transfer_packets();
    bench_transaction_view_try_new_from_boxed_data_packets(bencher, &packets);
}

#[bench]
fn bench_versioned_transaction_deserialization_packed_transfer(bencher: &mut Bencher) {
    let packets = create_many_transfer_packets();
    bench_versioned_transaction_deserialize_packets(bencher, &packets);
}

#[bench]
fn bench_transaction_view_try_new_from_slice_packed_transfer(bencher: &mut Bencher) {
    let packets = create_many_transfer_packets();
    bench_transaction_view_try_new_from_slice_packets(bencher, &packets);
}

#[bench]
fn bench_transaction_view_copy_from_slice_packed_transfer(bencher: &mut Bencher) {
    let packets = create_many_transfer_packets();
    bench_transaction_view_copy_from_slice_packets(bencher, &packets);
}

#[bench]
fn bench_transaction_view_try_new_from_boxed_data_packed_transfer(bencher: &mut Bencher) {
    let packets = create_many_transfer_packets();
    bench_transaction_view_try_new_from_boxed_data_packets(bencher, &packets);
}
