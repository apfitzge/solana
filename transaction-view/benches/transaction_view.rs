#![feature(test)]

use {
    agave_transaction_view::transaction_view::TransactionView,
    solana_sdk::{
        hash::Hash,
        packet::{self, Packet, PACKET_DATA_SIZE},
        pubkey::Pubkey,
        signature::Keypair,
        system_transaction,
        transaction::VersionedTransaction,
    },
    test::Bencher,
};
extern crate test;

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

const NUM_PACKETS: usize = 1024;

#[bench]
fn bench_versioned_transaction_deserialization(bencher: &mut Bencher) {
    let packets = (0..NUM_PACKETS)
        .map(|_| create_simple_transfer_packet())
        .collect::<Vec<_>>();

    bencher.iter(|| {
        for idx in 0..NUM_PACKETS {
            let _: VersionedTransaction = packets[idx].deserialize_slice(..).unwrap();
        }
    });
}

#[bench]
fn bench_transaction_view_try_new_from_slice(bencher: &mut Bencher) {
    let packets = (0..NUM_PACKETS)
        .map(|_| create_simple_transfer_packet())
        .collect::<Vec<_>>();
    bencher.iter(|| {
        for idx in 0..NUM_PACKETS {
            let _ = TransactionView::try_new_from_slice(packets[idx].data(..).unwrap()).unwrap();
        }
    })
}

#[bench]
fn bench_transaction_view_copy_from_slice(bencher: &mut Bencher) {
    let packets = (0..NUM_PACKETS)
        .map(|_| create_simple_transfer_packet())
        .collect::<Vec<_>>();
    let mut transaction_views = (0..NUM_PACKETS)
        .map(|_| TransactionView::default())
        .collect::<Vec<_>>();
    bencher.iter(|| {
        for idx in 0..NUM_PACKETS {
            let _ = TransactionView::copy_from_slice(
                &mut transaction_views[idx],
                packets[idx].data(..).unwrap(),
            )
            .unwrap();
        }
    })
}

#[bench]
fn bench_transaction_view_try_new_from_boxed_data(bencher: &mut Bencher) {
    let packets = (0..NUM_PACKETS)
        .map(|_| create_simple_transfer_packet())
        .collect::<Vec<_>>();
    let mut boxed_data_and_len = packets
        .into_iter()
        .map(|packet| {
            let packet_data = packet.data(..).unwrap();
            let mut boxed_data = Box::new([0u8; PACKET_DATA_SIZE]);
            boxed_data[..packet_data.len()].copy_from_slice(packet_data);
            Some((boxed_data, packet_data.len()))
        })
        .collect::<Vec<_>>();

    bencher.iter(|| {
        for idx in 0..NUM_PACKETS {
            let (boxed_data, len) = boxed_data_and_len[idx].take().unwrap();
            let transaction_view =
                TransactionView::try_new_from_boxed_data(boxed_data, len).unwrap();
            // Take back ownership so that the boxed data is not dropped, and we can re-use.
            // Makes sure we are not freeing or allocating memory in the benchmark.
            boxed_data_and_len[idx] = Some(transaction_view.take_data());
        }
    });
}
