//! The `sigverify` module provides digital signature verification functions.
//! By default, signatures are verified in parallel using all available CPU
//! cores.  When perf-libs are available signature verification is offloaded
//! to the GPU.
//!
use {
    crate::{
        cuda_runtime::PinnedVec,
        packet::{Packet, PacketBatch, PacketFlags, PACKET_DATA_SIZE},
        perf_libs,
        recycler::Recycler,
    },
    agave_transaction_view::{
        message_header_meta::TransactionVersion, transaction_meta::TransactionMeta,
    },
    rayon::{prelude::*, ThreadPool},
    solana_metrics::inc_new_counter_debug,
    solana_rayon_threadlimit::get_thread_count,
    solana_sdk::pubkey::Pubkey,
    std::mem::size_of,
};

// Representing key tKeYE4wtowRb8yRroZShTipE18YVnqwXjsSAoNsFU6g
const TRACER_KEY_BYTES: [u8; 32] = [
    13, 37, 180, 170, 252, 137, 36, 194, 183, 143, 161, 193, 201, 207, 211, 23, 189, 93, 33, 110,
    155, 90, 30, 39, 116, 115, 238, 38, 126, 21, 232, 133,
];
const TRACER_KEY: Pubkey = Pubkey::new_from_array(TRACER_KEY_BYTES);
const TRACER_KEY_OFFSET_IN_TRANSACTION: usize = 69;
// Empirically derived to constrain max verify latency to ~8ms at lower packet counts
pub const VERIFY_PACKET_CHUNK_SIZE: usize = 128;

lazy_static! {
    static ref PAR_THREAD_POOL: ThreadPool = rayon::ThreadPoolBuilder::new()
        .num_threads(get_thread_count())
        .thread_name(|i| format!("solSigVerify{i:02}"))
        .build()
        .unwrap();
}

pub type TxOffset = PinnedVec<u32>;

type TxOffsets = (TxOffset, TxOffset, TxOffset, TxOffset, Vec<Vec<u32>>);

#[derive(Debug, PartialEq, Eq)]
pub enum PacketError {
    InvalidLen,
    InvalidPubkeyLen,
    InvalidShortVec,
    InvalidSignatureLen,
    MismatchSignatureLen,
    PayerNotWritable,
    InvalidProgramIdIndex,
    InvalidProgramLen,
    UnsupportedVersion,
}

impl std::convert::From<std::boxed::Box<bincode::ErrorKind>> for PacketError {
    fn from(_e: std::boxed::Box<bincode::ErrorKind>) -> PacketError {
        PacketError::InvalidShortVec
    }
}

impl std::convert::From<std::num::TryFromIntError> for PacketError {
    fn from(_e: std::num::TryFromIntError) -> Self {
        Self::InvalidLen
    }
}

pub fn init() {
    if let Some(api) = perf_libs::api() {
        unsafe {
            (api.ed25519_set_verbose)(true);
            assert!((api.ed25519_init)(), "ed25519_init() failed");
            (api.ed25519_set_verbose)(false);
        }
    }
}

/// Returns true if the signatrue on the packet verifies.
/// Caller must do packet.set_discard(true) if this returns false.
#[must_use]
fn verify_packet(packet: &mut Packet, reject_non_vote: bool) -> bool {
    // If this packet was already marked as discard, drop it
    if packet.meta().discard() {
        return false;
    }

    let Some(packet_bytes) = packet.data(..) else {
        return false;
    };
    // This performs some work that is not **strictly** necessary at this point
    // in the processing pipeline. However, this work to validate packet
    // structure is eventually necessary, and it is generally better to do it
    // before we verify the signatures on invalid packets.
    let Ok(transaction_meta) = TransactionMeta::try_new(packet_bytes) else {
        return false;
    };

    // We need to additionally perform some basic checks on the number of
    // signatures and the number of static account keys.
    if transaction_meta.num_required_signatures() != transaction_meta.num_signatures()
        || transaction_meta.num_static_account_keys() < transaction_meta.num_signatures()
    {
        return false;
    }

    if is_simple_vote_tx(packet_bytes, &transaction_meta) {
        packet.meta_mut().flags |= PacketFlags::SIMPLE_VOTE_TX;
    }
    if reject_non_vote && !packet.meta().is_simple_vote_tx() {
        return false;
    }

    // This should never fail - but need to appease borrow-checker since we cannot
    // mutate `Packet` in above `if` block.
    let Some(packet_bytes) = packet.data(..) else {
        return false;
    };

    // SAFETY: `packet_bytes` was used to populate `transaction_meta`.
    let signatures = unsafe { transaction_meta.signatures(packet_bytes) };
    let static_account_keys = unsafe { transaction_meta.static_account_keys(packet_bytes) };
    let message_bytes = unsafe { transaction_meta.message_bytes(packet_bytes) };

    for (signature, pubkey) in signatures.iter().zip(static_account_keys.iter()) {
        if !signature.verify(pubkey.as_ref(), message_bytes) {
            return false;
        }
    }

    true
}

fn is_simple_vote_tx(packet_bytes: &[u8], transaction_meta: &TransactionMeta) -> bool {
    // Vote could have 1 or 2 sigs; zero sig has already been excluded
    if transaction_meta.num_signatures() > 2 {
        return false;
    }

    // Simple vote should only be legacy message
    if !matches!(transaction_meta.version(), TransactionVersion::Legacy) {
        return false;
    }

    // skip if has more than 1 instruction
    if transaction_meta.num_instructions() != 1 {
        return false;
    }

    // SAFETY: `packet_bytes` was used to populate `transaction_meta`.
    let static_account_keys = unsafe { transaction_meta.static_account_keys(packet_bytes) };
    let mut instructions_iter = unsafe { transaction_meta.instructions_iter(packet_bytes) };

    // SAFETY: we just checked that there is exactly one instruction
    let instruction = instructions_iter.next().unwrap();

    // This is actually an error, and we should discard the packet anyway.
    let Some(program_id) = static_account_keys.get(usize::from(instruction.program_id_index))
    else {
        return false;
    };

    program_id == &solana_sdk::vote::program::id()
}

pub fn count_packets_in_batches(batches: &[PacketBatch]) -> usize {
    batches.iter().map(|batch| batch.len()).sum()
}

pub fn count_valid_packets(
    batches: &[PacketBatch],
    mut process_valid_packet: impl FnMut(&Packet),
) -> usize {
    batches
        .iter()
        .map(|batch| {
            batch
                .iter()
                .filter(|p| {
                    let should_keep = !p.meta().discard();
                    if should_keep {
                        process_valid_packet(p);
                    }
                    should_keep
                })
                .count()
        })
        .sum()
}

pub fn count_discarded_packets(batches: &[PacketBatch]) -> usize {
    batches
        .iter()
        .map(|batch| batch.iter().filter(|p| p.meta().discard()).count())
        .sum()
}

pub fn check_for_tracer_packet(packet: &mut Packet) -> bool {
    let first_pubkey_start: usize = TRACER_KEY_OFFSET_IN_TRANSACTION;
    let Some(first_pubkey_end) = first_pubkey_start.checked_add(size_of::<Pubkey>()) else {
        return false;
    };
    // Check for tracer pubkey
    match packet.data(first_pubkey_start..first_pubkey_end) {
        Some(pubkey) if pubkey == TRACER_KEY.as_ref() => {
            packet.meta_mut().set_tracer(true);
            true
        }
        _ => false,
    }
}

pub fn generate_offsets(
    batches: &mut [PacketBatch],
    recycler: &Recycler<TxOffset>,
    reject_non_vote: bool,
) -> TxOffsets {
    debug!("allocating..");
    let mut signature_offsets: PinnedVec<_> = recycler.allocate("sig_offsets");
    signature_offsets.set_pinnable();
    let mut pubkey_offsets: PinnedVec<_> = recycler.allocate("pubkey_offsets");
    pubkey_offsets.set_pinnable();
    let mut msg_start_offsets: PinnedVec<_> = recycler.allocate("msg_start_offsets");
    msg_start_offsets.set_pinnable();
    let mut msg_sizes: PinnedVec<_> = recycler.allocate("msg_size_offsets");
    msg_sizes.set_pinnable();
    let mut current_offset: u32 = 0;
    let offsets = batches
        .iter_mut()
        .map(|batch| {
            batch
                .iter_mut()
                .map(|packet| {
                    // Make sure that offsets will not overflow the u32 limit
                    if current_offset > u32::MAX.wrapping_sub(core::mem::size_of::<Packet>() as u32)
                    {
                        return 0;
                    }

                    let Some(packet_bytes) = packet.data(..) else {
                        return 0;
                    };
                    let Ok(transaction_meta) = TransactionMeta::try_new(packet_bytes) else {
                        return 0;
                    };

                    if is_simple_vote_tx(packet_bytes, &transaction_meta) {
                        packet.meta_mut().flags |= PacketFlags::SIMPLE_VOTE_TX;
                    }
                    if reject_non_vote && !packet.meta().is_simple_vote_tx() {
                        packet.meta_mut().set_discard(true);
                        return 0;
                    }

                    let Some(packet_bytes) = packet.data(..) else {
                        return 0;
                    };

                    let message_start =
                        current_offset.wrapping_add(transaction_meta.message_offset());
                    // SAFETY: `packet_bytes` was used to populate `transaction_meta`.
                    let message_length = unsafe { transaction_meta.message_length(packet_bytes) };

                    let num_signatures = u32::from(transaction_meta.num_signatures());
                    for index in 0..num_signatures {
                        // Offsets are relative to the start of the packet. We must offset them
                        // from the start of the batch.
                        signature_offsets.push(
                            current_offset.wrapping_add(transaction_meta.signature_offset(index)),
                        );
                        pubkey_offsets.push(
                            current_offset
                                .wrapping_add(transaction_meta.static_account_key_offset(index)),
                        );
                        msg_start_offsets.push(message_start);
                        msg_sizes.push(message_length);
                    }

                    current_offset =
                        current_offset.saturating_add(core::mem::size_of::<Packet>() as u32);

                    num_signatures
                })
                .collect()
        })
        .collect();
    (
        signature_offsets,
        pubkey_offsets,
        msg_start_offsets,
        msg_sizes,
        offsets,
    )
}

//inplace shrink a batch of packets
pub fn shrink_batches(batches: &mut Vec<PacketBatch>) {
    let mut valid_batch_ix = 0;
    let mut valid_packet_ix = 0;
    let mut last_valid_batch = 0;
    for batch_ix in 0..batches.len() {
        for packet_ix in 0..batches[batch_ix].len() {
            if batches[batch_ix][packet_ix].meta().discard() {
                continue;
            }
            last_valid_batch = batch_ix.saturating_add(1);
            let mut found_spot = false;
            while valid_batch_ix < batch_ix && !found_spot {
                while valid_packet_ix < batches[valid_batch_ix].len() {
                    if batches[valid_batch_ix][valid_packet_ix].meta().discard() {
                        batches[valid_batch_ix][valid_packet_ix] =
                            batches[batch_ix][packet_ix].clone();
                        batches[batch_ix][packet_ix].meta_mut().set_discard(true);
                        last_valid_batch = valid_batch_ix.saturating_add(1);
                        found_spot = true;
                        break;
                    }
                    valid_packet_ix = valid_packet_ix.saturating_add(1);
                }
                if valid_packet_ix >= batches[valid_batch_ix].len() {
                    valid_packet_ix = 0;
                    valid_batch_ix = valid_batch_ix.saturating_add(1);
                }
            }
        }
    }
    batches.truncate(last_valid_batch);
}

pub fn ed25519_verify_cpu(batches: &mut [PacketBatch], reject_non_vote: bool, packet_count: usize) {
    debug!("CPU ECDSA for {}", packet_count);
    PAR_THREAD_POOL.install(|| {
        batches
            .par_iter_mut()
            .flatten()
            .collect::<Vec<&mut Packet>>()
            .par_chunks_mut(VERIFY_PACKET_CHUNK_SIZE)
            .for_each(|packets| {
                for packet in packets.iter_mut() {
                    if !packet.meta().discard() && !verify_packet(packet, reject_non_vote) {
                        packet.meta_mut().set_discard(true);
                    }
                }
            });
    });
    inc_new_counter_debug!("ed25519_verify_cpu", packet_count);
}

pub fn ed25519_verify_disabled(batches: &mut [PacketBatch]) {
    let packet_count = count_packets_in_batches(batches);
    debug!("disabled ECDSA for {}", packet_count);
    batches.into_par_iter().for_each(|batch| {
        batch
            .par_iter_mut()
            .for_each(|p| p.meta_mut().set_discard(false))
    });
    inc_new_counter_debug!("ed25519_verify_disabled", packet_count);
}

pub fn copy_return_values<I, T>(sig_lens: I, out: &PinnedVec<u8>, rvs: &mut [Vec<u8>])
where
    I: IntoIterator<Item = T>,
    T: IntoIterator<Item = u32>,
{
    debug_assert!(rvs.iter().flatten().all(|&rv| rv == 0u8));
    let mut offset = 0usize;
    let rvs = rvs.iter_mut().flatten();
    for (k, rv) in sig_lens.into_iter().flatten().zip(rvs) {
        let out = out[offset..].iter().take(k as usize).all(|&x| x == 1u8);
        *rv = u8::from(k != 0u32 && out);
        offset = offset.saturating_add(k as usize);
    }
}

// return true for success, i.e ge unpacks and !ge.is_small_order()
pub fn check_packed_ge_small_order(ge: &[u8; 32]) -> bool {
    if let Some(api) = perf_libs::api() {
        unsafe {
            // Returns 1 == fail, 0 == success
            let res = (api.ed25519_check_packed_ge_small_order)(ge.as_ptr());

            return res == 0;
        }
    }
    false
}

pub fn get_checked_scalar(scalar: &[u8; 32]) -> Result<[u8; 32], PacketError> {
    let mut out = [0u8; 32];
    if let Some(api) = perf_libs::api() {
        unsafe {
            let res = (api.ed25519_get_checked_scalar)(out.as_mut_ptr(), scalar.as_ptr());
            if res == 0 {
                return Ok(out);
            } else {
                return Err(PacketError::InvalidLen);
            }
        }
    }
    Ok(out)
}

pub fn mark_disabled(batches: &mut [PacketBatch], r: &[Vec<u8>]) {
    for (batch, v) in batches.iter_mut().zip(r) {
        for (pkt, f) in batch.iter_mut().zip(v) {
            if !pkt.meta().discard() {
                pkt.meta_mut().set_discard(*f == 0);
            }
        }
    }
}

pub fn ed25519_verify(
    batches: &mut [PacketBatch],
    recycler: &Recycler<TxOffset>,
    recycler_out: &Recycler<PinnedVec<u8>>,
    reject_non_vote: bool,
    valid_packet_count: usize,
) {
    let Some(api) = perf_libs::api() else {
        return ed25519_verify_cpu(batches, reject_non_vote, valid_packet_count);
    };
    let total_packet_count = count_packets_in_batches(batches);
    // micro-benchmarks show GPU time for smallest batch around 15-20ms
    // and CPU speed for 64-128 sigverifies around 10-20ms. 64 is a nice
    // power-of-two number around that accounting for the fact that the CPU
    // may be busy doing other things while being a real validator
    // TODO: dynamically adjust this crossover
    let maybe_valid_percentage = 100usize
        .wrapping_mul(valid_packet_count)
        .checked_div(total_packet_count);
    let Some(valid_percentage) = maybe_valid_percentage else {
        return;
    };
    if valid_percentage < 90 || valid_packet_count < 64 {
        ed25519_verify_cpu(batches, reject_non_vote, valid_packet_count);
        return;
    }

    let (signature_offsets, pubkey_offsets, msg_start_offsets, msg_sizes, sig_lens) =
        generate_offsets(batches, recycler, reject_non_vote);

    debug!("CUDA ECDSA for {}", valid_packet_count);
    debug!("allocating out..");
    let mut out = recycler_out.allocate("out_buffer");
    out.set_pinnable();
    let mut elems = Vec::new();
    let mut rvs = Vec::new();

    let mut num_packets: usize = 0;
    for batch in batches.iter() {
        elems.push(perf_libs::Elems {
            elems: batch.as_ptr().cast::<u8>(),
            num: batch.len() as u32,
        });
        let v = vec![0u8; batch.len()];
        rvs.push(v);
        num_packets = num_packets.saturating_add(batch.len());
    }
    out.resize(signature_offsets.len(), 0);
    trace!("Starting verify num packets: {}", num_packets);
    trace!("elem len: {}", elems.len() as u32);
    trace!("packet sizeof: {}", size_of::<Packet>() as u32);
    trace!("len offset: {}", PACKET_DATA_SIZE as u32);
    const USE_NON_DEFAULT_STREAM: u8 = 1;
    unsafe {
        let res = (api.ed25519_verify_many)(
            elems.as_ptr(),
            elems.len() as u32,
            size_of::<Packet>() as u32,
            num_packets as u32,
            signature_offsets.len() as u32,
            msg_sizes.as_ptr(),
            pubkey_offsets.as_ptr(),
            signature_offsets.as_ptr(),
            msg_start_offsets.as_ptr(),
            out.as_mut_ptr(),
            USE_NON_DEFAULT_STREAM,
        );
        if res != 0 {
            trace!("RETURN!!!: {}", res);
        }
    }
    trace!("done verify");
    copy_return_values(sig_lens, &out, &mut rvs);
    mark_disabled(batches, &rvs);
    inc_new_counter_debug!("ed25519_verify_gpu", valid_packet_count);
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use {
        super::*,
        crate::{
            packet::{to_packet_batches, Packet, PacketBatch, PACKETS_PER_BATCH},
            sigverify::{self},
            test_tx::{new_test_vote_tx, test_multisig_tx, test_tx},
        },
        curve25519_dalek::{edwards::CompressedEdwardsY, scalar::Scalar},
        rand::{thread_rng, Rng},
        solana_sdk::{
            hash::Hash,
            instruction::CompiledInstruction,
            message::{Message, MESSAGE_VERSION_PREFIX},
            signature::{Keypair, Signature, Signer},
            transaction::Transaction,
        },
        std::{
            iter::repeat_with,
            sync::atomic::{AtomicU64, Ordering},
        },
    };

    #[test]
    fn test_copy_return_values() {
        let mut rng = rand::thread_rng();
        let sig_lens: Vec<Vec<u32>> = {
            let size = rng.gen_range(0..64);
            repeat_with(|| {
                let size = rng.gen_range(0..16);
                repeat_with(|| rng.gen_range(0..5)).take(size).collect()
            })
            .take(size)
            .collect()
        };
        let out: Vec<Vec<Vec<bool>>> = sig_lens
            .iter()
            .map(|sig_lens| {
                sig_lens
                    .iter()
                    .map(|&size| repeat_with(|| rng.gen()).take(size as usize).collect())
                    .collect()
            })
            .collect();
        let expected: Vec<Vec<u8>> = out
            .iter()
            .map(|out| {
                out.iter()
                    .map(|out| u8::from(!out.is_empty() && out.iter().all(|&k| k)))
                    .collect()
            })
            .collect();
        let out =
            PinnedVec::<u8>::from_vec(out.into_iter().flatten().flatten().map(u8::from).collect());
        let mut rvs: Vec<Vec<u8>> = sig_lens
            .iter()
            .map(|sig_lens| vec![0u8; sig_lens.len()])
            .collect();
        copy_return_values(sig_lens, &out, &mut rvs);
        assert_eq!(rvs, expected);
    }

    #[test]
    fn test_mark_disabled() {
        let batch_size = 1;
        let mut batch = PacketBatch::with_capacity(batch_size);
        batch.resize(batch_size, Packet::default());
        let mut batches: Vec<PacketBatch> = vec![batch];
        mark_disabled(&mut batches, &[vec![0]]);
        assert!(batches[0][0].meta().discard());
        batches[0][0].meta_mut().set_discard(false);
        mark_disabled(&mut batches, &[vec![1]]);
        assert!(!batches[0][0].meta().discard());
    }

    #[test]
    fn test_pubkey_too_small() {
        solana_logger::setup();
        let mut tx = test_tx();
        let sig = tx.signatures[0];
        const NUM_SIG: usize = 18;
        tx.signatures = vec![sig; NUM_SIG];
        tx.message.account_keys = vec![];
        tx.message.header.num_required_signatures = NUM_SIG as u8;
        let mut packet = Packet::from_data(None, tx).unwrap();

        assert!(!verify_packet(&mut packet, false));

        packet.meta_mut().set_discard(false);
        let mut batches = generate_packet_batches(&packet, 1, 1);
        ed25519_verify(&mut batches);
        assert!(batches[0][0].meta().discard());
    }

    #[test]
    fn test_pubkey_len() {
        // See that the verify cannot walk off the end of the packet
        // trying to index into the account_keys to access pubkey.
        solana_logger::setup();

        const NUM_SIG: usize = 17;
        let keypair1 = Keypair::new();
        let pubkey1 = keypair1.pubkey();
        let mut message = Message::new(&[], Some(&pubkey1));
        message.account_keys.push(pubkey1);
        message.account_keys.push(pubkey1);
        message.header.num_required_signatures = NUM_SIG as u8;
        message.recent_blockhash = Hash::new_from_array(pubkey1.to_bytes());
        let mut tx = Transaction::new_unsigned(message);

        info!("message: {:?}", tx.message_data());
        info!("tx: {:?}", tx);
        let sig = keypair1.try_sign_message(&tx.message_data()).unwrap();
        tx.signatures = vec![sig; NUM_SIG];

        let mut packet = Packet::from_data(None, tx).unwrap();

        assert!(!verify_packet(&mut packet, false));

        packet.meta_mut().set_discard(false);
        let mut batches = generate_packet_batches(&packet, 1, 1);
        ed25519_verify(&mut batches);
        assert!(batches[0][0].meta().discard());
    }

    fn generate_packet_batches_random_size(
        packet: &Packet,
        max_packets_per_batch: usize,
        num_batches: usize,
    ) -> Vec<PacketBatch> {
        // generate packet vector
        let batches: Vec<_> = (0..num_batches)
            .map(|_| {
                let num_packets_per_batch = thread_rng().gen_range(1..max_packets_per_batch);
                let mut packet_batch = PacketBatch::with_capacity(num_packets_per_batch);
                for _ in 0..num_packets_per_batch {
                    packet_batch.push(packet.clone());
                }
                assert_eq!(packet_batch.len(), num_packets_per_batch);
                packet_batch
            })
            .collect();
        assert_eq!(batches.len(), num_batches);

        batches
    }

    fn generate_packet_batches(
        packet: &Packet,
        num_packets_per_batch: usize,
        num_batches: usize,
    ) -> Vec<PacketBatch> {
        // generate packet vector
        let batches: Vec<_> = (0..num_batches)
            .map(|_| {
                let mut packet_batch = PacketBatch::with_capacity(num_packets_per_batch);
                for _ in 0..num_packets_per_batch {
                    packet_batch.push(packet.clone());
                }
                assert_eq!(packet_batch.len(), num_packets_per_batch);
                packet_batch
            })
            .collect();
        assert_eq!(batches.len(), num_batches);

        batches
    }

    fn test_verify_n(n: usize, modify_data: bool) {
        let tx = test_tx();
        let mut packet = Packet::from_data(None, tx).unwrap();

        // jumble some data to test failure
        if modify_data {
            packet.buffer_mut()[20] = packet.data(20).unwrap().wrapping_add(10);
        }

        let mut batches = generate_packet_batches(&packet, n, 2);

        // verify packets
        ed25519_verify(&mut batches);

        // check result
        let should_discard = modify_data;
        assert!(batches
            .iter()
            .flat_map(|batch| batch.iter())
            .all(|p| p.meta().discard() == should_discard));
    }

    fn ed25519_verify(batches: &mut [PacketBatch]) {
        let recycler = Recycler::default();
        let recycler_out = Recycler::default();
        let packet_count = sigverify::count_packets_in_batches(batches);
        sigverify::ed25519_verify(batches, &recycler, &recycler_out, false, packet_count);
    }

    #[test]
    fn test_verify_tampered_sig_len() {
        let mut tx = test_tx();
        // pretend malicious leader dropped a signature...
        tx.signatures.pop();
        let packet = Packet::from_data(None, tx).unwrap();

        let mut batches = generate_packet_batches(&packet, 1, 1);

        // verify packets
        ed25519_verify(&mut batches);
        assert!(batches
            .iter()
            .flat_map(|batch| batch.iter())
            .all(|p| p.meta().discard()));
    }

    #[test]
    fn test_verify_zero() {
        test_verify_n(0, false);
    }

    #[test]
    fn test_verify_one() {
        test_verify_n(1, false);
    }

    #[test]
    fn test_verify_seventy_one() {
        test_verify_n(71, false);
    }

    #[test]
    fn test_verify_medium_pass() {
        test_verify_n(VERIFY_PACKET_CHUNK_SIZE, false);
    }

    #[test]
    fn test_verify_large_pass() {
        test_verify_n(VERIFY_PACKET_CHUNK_SIZE * get_thread_count(), false);
    }

    #[test]
    fn test_verify_medium_fail() {
        test_verify_n(VERIFY_PACKET_CHUNK_SIZE, true);
    }

    #[test]
    fn test_verify_large_fail() {
        test_verify_n(VERIFY_PACKET_CHUNK_SIZE * get_thread_count(), true);
    }

    #[test]
    fn test_verify_multisig() {
        solana_logger::setup();

        let tx = test_multisig_tx();
        let mut packet = Packet::from_data(None, tx).unwrap();

        let n = 4;
        let num_batches = 3;
        let mut batches = generate_packet_batches(&packet, n, num_batches);

        packet.buffer_mut()[40] = packet.data(40).unwrap().wrapping_add(8);

        batches[0].push(packet);

        // verify packets
        ed25519_verify(&mut batches);

        // check result
        let ref_ans = 1u8;
        let mut ref_vec = vec![vec![ref_ans; n]; num_batches];
        ref_vec[0].push(0u8);
        assert!(batches
            .iter()
            .flat_map(|batch| batch.iter())
            .zip(ref_vec.into_iter().flatten())
            .all(|(p, discard)| {
                if discard == 0 {
                    p.meta().discard()
                } else {
                    !p.meta().discard()
                }
            }));
    }

    #[test]
    fn test_verify_fuzz() {
        solana_logger::setup();

        let tx = test_multisig_tx();
        let packet = Packet::from_data(None, tx).unwrap();

        let recycler = Recycler::default();
        let recycler_out = Recycler::default();
        for _ in 0..50 {
            let num_batches = thread_rng().gen_range(2..30);
            let mut batches = generate_packet_batches_random_size(&packet, 128, num_batches);

            let num_modifications = thread_rng().gen_range(0..5);
            for _ in 0..num_modifications {
                let batch = thread_rng().gen_range(0..batches.len());
                let packet = thread_rng().gen_range(0..batches[batch].len());
                let offset = thread_rng().gen_range(0..batches[batch][packet].meta().size);
                let add = thread_rng().gen_range(0..255);
                batches[batch][packet].buffer_mut()[offset] = batches[batch][packet]
                    .data(offset)
                    .unwrap()
                    .wrapping_add(add);
            }

            let batch_to_disable = thread_rng().gen_range(0..batches.len());
            for p in batches[batch_to_disable].iter_mut() {
                p.meta_mut().set_discard(true);
            }

            // verify from GPU verification pipeline (when GPU verification is enabled) are
            // equivalent to the CPU verification pipeline.
            let mut batches_cpu = batches.clone();
            let packet_count = sigverify::count_packets_in_batches(&batches);
            sigverify::ed25519_verify(&mut batches, &recycler, &recycler_out, false, packet_count);
            ed25519_verify_cpu(&mut batches_cpu, false, packet_count);

            // check result
            batches
                .iter()
                .flat_map(|batch| batch.iter())
                .zip(batches_cpu.iter().flat_map(|batch| batch.iter()))
                .for_each(|(p1, p2)| assert_eq!(p1, p2));
        }
    }

    #[test]
    fn test_verify_fail() {
        test_verify_n(5, true);
    }

    #[test]
    fn test_get_checked_scalar() {
        solana_logger::setup();
        if perf_libs::api().is_none() {
            return;
        }

        let passed_g = AtomicU64::new(0);
        let failed_g = AtomicU64::new(0);
        (0..4).into_par_iter().for_each(|_| {
            let mut input = [0u8; 32];
            let mut passed = 0;
            let mut failed = 0;
            for _ in 0..1_000_000 {
                thread_rng().fill(&mut input);
                let ans = get_checked_scalar(&input);
                let ref_ans = Scalar::from_canonical_bytes(input);
                if let Some(ref_ans) = ref_ans {
                    passed += 1;
                    assert_eq!(ans.unwrap(), ref_ans.to_bytes());
                } else {
                    failed += 1;
                    assert!(ans.is_err());
                }
            }
            passed_g.fetch_add(passed, Ordering::Relaxed);
            failed_g.fetch_add(failed, Ordering::Relaxed);
        });
        info!(
            "passed: {} failed: {}",
            passed_g.load(Ordering::Relaxed),
            failed_g.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn test_ge_small_order() {
        solana_logger::setup();
        if perf_libs::api().is_none() {
            return;
        }

        let passed_g = AtomicU64::new(0);
        let failed_g = AtomicU64::new(0);
        (0..4).into_par_iter().for_each(|_| {
            let mut input = [0u8; 32];
            let mut passed = 0;
            let mut failed = 0;
            for _ in 0..1_000_000 {
                thread_rng().fill(&mut input);
                let ans = check_packed_ge_small_order(&input);
                let ref_ge = CompressedEdwardsY::from_slice(&input);
                if let Some(ref_element) = ref_ge.decompress() {
                    if ref_element.is_small_order() {
                        assert!(!ans);
                    } else {
                        assert!(ans);
                    }
                } else {
                    assert!(!ans);
                }
                if ans {
                    passed += 1;
                } else {
                    failed += 1;
                }
            }
            passed_g.fetch_add(passed, Ordering::Relaxed);
            failed_g.fetch_add(failed, Ordering::Relaxed);
        });
        info!(
            "passed: {} failed: {}",
            passed_g.load(Ordering::Relaxed),
            failed_g.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn test_is_simple_vote_transaction() {
        solana_logger::setup();
        let mut rng = rand::thread_rng();

        // tansfer tx is not
        {
            let mut tx = test_tx();
            tx.message.instructions[0].data = vec![1, 2, 3];
            let packet = Packet::from_data(None, tx).unwrap();
            let packet_bytes = packet.data(..).unwrap();
            let transaction_meta = TransactionMeta::try_new(packet_bytes).unwrap();
            assert!(!is_simple_vote_tx(packet_bytes, &transaction_meta));
        }

        // single legacy vote tx is
        {
            let mut tx = new_test_vote_tx(&mut rng);
            tx.message.instructions[0].data = vec![1, 2, 3];
            let packet = Packet::from_data(None, tx).unwrap();
            let packet_bytes = packet.data(..).unwrap();
            let transaction_meta = TransactionMeta::try_new(packet_bytes).unwrap();
            assert!(is_simple_vote_tx(packet_bytes, &transaction_meta));
        }

        // single versioned vote tx is not
        {
            let mut tx = new_test_vote_tx(&mut rng);
            tx.message.instructions[0].data = vec![1, 2, 3];

            let msg_start = {
                1 + // num signatures
                tx.signatures.len() * core::mem::size_of::<Signature>() // signatures
            };

            let mut packet = Packet::from_data(None, tx).unwrap();

            // set message version to v0
            let msg_bytes = packet.data(msg_start..).unwrap().to_vec();
            packet.buffer_mut()[msg_start] = MESSAGE_VERSION_PREFIX;
            let msg_end = packet.meta().size + 1;
            packet.buffer_mut()[msg_start + 1..msg_end].copy_from_slice(&msg_bytes);
            packet.meta_mut().size += 2; // 1 byte for version, 1 byte for num ATLs
            packet.buffer_mut()[msg_end] = 0; // num ATLs

            let packet_bytes = packet.data(..).unwrap();
            let transaction_meta = TransactionMeta::try_new(packet_bytes).unwrap();
            assert!(!is_simple_vote_tx(packet_bytes, &transaction_meta));
        }

        // multiple mixed tx is not
        {
            let key = Keypair::new();
            let key1 = Pubkey::new_unique();
            let key2 = Pubkey::new_unique();
            let tx = Transaction::new_with_compiled_instructions(
                &[&key],
                &[key1, key2],
                Hash::default(),
                vec![solana_vote_program::id(), Pubkey::new_unique()],
                vec![
                    CompiledInstruction::new(3, &(), vec![0, 1]),
                    CompiledInstruction::new(4, &(), vec![0, 2]),
                ],
            );
            let packet = Packet::from_data(None, tx).unwrap();
            let packet_bytes = packet.data(..).unwrap();
            let transaction_meta = TransactionMeta::try_new(packet_bytes).unwrap();
            assert!(!is_simple_vote_tx(packet_bytes, &transaction_meta));
        }

        // single legacy vote tx with extra (invalid) signature is not
        {
            let mut tx = new_test_vote_tx(&mut rng);
            tx.signatures.push(Signature::default());
            tx.message.header.num_required_signatures = 3;
            tx.message.instructions[0].data = vec![1, 2, 3];
            let packet = Packet::from_data(None, tx).unwrap();
            let packet_bytes = packet.data(..).unwrap();
            let transaction_meta = TransactionMeta::try_new(packet_bytes).unwrap();
            assert!(!is_simple_vote_tx(packet_bytes, &transaction_meta));
        }
    }

    #[test]
    fn test_is_simple_vote_transaction_with_offsets() {
        solana_logger::setup();
        let mut rng = rand::thread_rng();

        // batch of legacy messages
        {
            let mut current_offset = 0usize;
            let mut batch = PacketBatch::default();
            batch.push(Packet::from_data(None, test_tx()).unwrap());
            let tx = new_test_vote_tx(&mut rng);
            batch.push(Packet::from_data(None, tx).unwrap());
            batch.iter_mut().enumerate().for_each(|(index, packet)| {
                let packet_data = packet.data(..).unwrap();
                let transaction_meta = TransactionMeta::try_new(packet_data).unwrap();
                assert_eq!(
                    is_simple_vote_tx(packet_data, &transaction_meta),
                    index == 1
                );

                current_offset = current_offset.saturating_add(size_of::<Packet>());
            });
        }

        // batch of mixed legacy messages and versioned vote tx, which won't be flagged as
        // simple_vote_tx
        {
            let mut current_offset = 0usize;
            let mut batch = PacketBatch::default();
            batch.push(Packet::from_data(None, test_tx()).unwrap());
            // versioned vote tx
            let tx = new_test_vote_tx(&mut rng);
            let msg_start = {
                1 + // num signatures
                core::mem::size_of::<Signature>() * tx.signatures.len() // signatures
            };
            let mut packet = Packet::from_data(None, tx).unwrap();

            let msg_bytes = packet.data(msg_start..).unwrap().to_vec();
            packet.buffer_mut()[msg_start] = MESSAGE_VERSION_PREFIX;
            let msg_end = packet.meta().size + 1;
            packet.meta_mut().size += 2; // 1 byte for version, 1 byte for num ATLs
            packet.buffer_mut()[msg_start + 1..msg_end].copy_from_slice(&msg_bytes);
            packet.buffer_mut()[msg_end] = 0; // num ATLs
            batch.push(packet);

            batch.iter_mut().for_each(|packet| {
                let packet_bytes = packet.data(..).unwrap();
                let transaction_meta = TransactionMeta::try_new(packet_bytes).unwrap();
                assert!(!is_simple_vote_tx(packet_bytes, &transaction_meta));

                current_offset = current_offset.saturating_add(size_of::<Packet>());
            });
        }
    }

    #[test]
    fn test_shrink_fuzz() {
        for _ in 0..5 {
            let mut batches = to_packet_batches(
                &(0..PACKETS_PER_BATCH * 3)
                    .map(|_| test_tx())
                    .collect::<Vec<_>>(),
                PACKETS_PER_BATCH,
            );
            batches.iter_mut().for_each(|b| {
                b.iter_mut()
                    .for_each(|p| p.meta_mut().set_discard(thread_rng().gen()))
            });
            //find all the non discarded packets
            let mut start = vec![];
            batches.iter_mut().for_each(|b| {
                b.iter_mut()
                    .filter(|p| !p.meta().discard())
                    .for_each(|p| start.push(p.clone()))
            });
            start.sort_by(|a, b| a.data(..).cmp(&b.data(..)));

            let packet_count = count_valid_packets(&batches, |_| ());
            shrink_batches(&mut batches);

            //make sure all the non discarded packets are the same
            let mut end = vec![];
            batches.iter_mut().for_each(|b| {
                b.iter_mut()
                    .filter(|p| !p.meta().discard())
                    .for_each(|p| end.push(p.clone()))
            });
            end.sort_by(|a, b| a.data(..).cmp(&b.data(..)));
            let packet_count2 = count_valid_packets(&batches, |_| ());
            assert_eq!(packet_count, packet_count2);
            assert_eq!(start, end);
        }
    }

    #[test]
    fn test_shrink_empty() {
        const PACKET_COUNT: usize = 1024;
        const BATCH_COUNT: usize = PACKET_COUNT / PACKETS_PER_BATCH;

        // No batches
        // truncate of 1 on len 0 is a noop
        shrink_batches(&mut Vec::new());
        // One empty batch
        {
            let mut batches = vec![PacketBatch::with_capacity(0)];
            shrink_batches(&mut batches);
            assert_eq!(batches.len(), 0);
        }
        // Many empty batches
        {
            let mut batches = (0..BATCH_COUNT)
                .map(|_| PacketBatch::with_capacity(0))
                .collect::<Vec<_>>();
            shrink_batches(&mut batches);
            assert_eq!(batches.len(), 0);
        }
    }

    #[test]
    fn test_shrink_vectors() {
        const PACKET_COUNT: usize = 1024;
        const BATCH_COUNT: usize = PACKET_COUNT / PACKETS_PER_BATCH;

        let set_discards = [
            // contiguous
            // 0
            // No discards
            |_, _| false,
            // All discards
            |_, _| true,
            // single partitions
            // discard last half of packets
            |b, p| ((b * PACKETS_PER_BATCH) + p) >= (PACKET_COUNT / 2),
            // discard first half of packets
            |b, p| ((b * PACKETS_PER_BATCH) + p) < (PACKET_COUNT / 2),
            // discard last half of each batch
            |_, p| p >= (PACKETS_PER_BATCH / 2),
            // 5
            // discard first half of each batch
            |_, p| p < (PACKETS_PER_BATCH / 2),
            // uniform sparse
            // discard even packets
            |b, p| ((b * PACKETS_PER_BATCH) + p) % 2 == 0,
            // discard odd packets
            |b, p| ((b * PACKETS_PER_BATCH) + p) % 2 == 1,
            // discard even batches
            |b, _| b % 2 == 0,
            // discard odd batches
            |b, _| b % 2 == 1,
            // edges
            // 10
            // discard first batch
            |b, _| b == 0,
            // discard last batch
            |b, _| b == BATCH_COUNT - 1,
            // discard first and last batches
            |b, _| b == 0 || b == BATCH_COUNT - 1,
            // discard all but first and last batches
            |b, _| b != 0 && b != BATCH_COUNT - 1,
            // discard first packet
            |b, p| ((b * PACKETS_PER_BATCH) + p) == 0,
            // 15
            // discard all but first packet
            |b, p| ((b * PACKETS_PER_BATCH) + p) != 0,
            // discard last packet
            |b, p| ((b * PACKETS_PER_BATCH) + p) == PACKET_COUNT - 1,
            // discard all but last packet
            |b, p| ((b * PACKETS_PER_BATCH) + p) != PACKET_COUNT - 1,
            // discard first packet of each batch
            |_, p| p == 0,
            // discard all but first packet of each batch
            |_, p| p != 0,
            // 20
            // discard last packet of each batch
            |_, p| p == PACKETS_PER_BATCH - 1,
            // discard all but last packet of each batch
            |_, p| p != PACKETS_PER_BATCH - 1,
            // discard first and last packet of each batch
            |_, p| p == 0 || p == PACKETS_PER_BATCH - 1,
            // discard all but first and last packet of each batch
            |_, p| p != 0 && p != PACKETS_PER_BATCH - 1,
            // discard all after first packet in second to last batch
            |b, p| (b == BATCH_COUNT - 2 && p > 0) || b == BATCH_COUNT - 1,
            // 25
        ];

        let expect_valids = [
            // (expected_batches, expected_valid_packets)
            //
            // contiguous
            // 0
            (BATCH_COUNT, PACKET_COUNT),
            (0, 0),
            // single partitions
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            // 5
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            // uniform sparse
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            (BATCH_COUNT / 2, PACKET_COUNT / 2),
            // edges
            // 10
            (BATCH_COUNT - 1, PACKET_COUNT - PACKETS_PER_BATCH),
            (BATCH_COUNT - 1, PACKET_COUNT - PACKETS_PER_BATCH),
            (BATCH_COUNT - 2, PACKET_COUNT - 2 * PACKETS_PER_BATCH),
            (2, 2 * PACKETS_PER_BATCH),
            (BATCH_COUNT, PACKET_COUNT - 1),
            // 15
            (1, 1),
            (BATCH_COUNT, PACKET_COUNT - 1),
            (1, 1),
            (
                (BATCH_COUNT * (PACKETS_PER_BATCH - 1) + PACKETS_PER_BATCH) / PACKETS_PER_BATCH,
                (PACKETS_PER_BATCH - 1) * BATCH_COUNT,
            ),
            (
                (BATCH_COUNT + PACKETS_PER_BATCH) / PACKETS_PER_BATCH,
                BATCH_COUNT,
            ),
            // 20
            (
                (BATCH_COUNT * (PACKETS_PER_BATCH - 1) + PACKETS_PER_BATCH) / PACKETS_PER_BATCH,
                (PACKETS_PER_BATCH - 1) * BATCH_COUNT,
            ),
            (
                (BATCH_COUNT + PACKETS_PER_BATCH) / PACKETS_PER_BATCH,
                BATCH_COUNT,
            ),
            (
                (BATCH_COUNT * (PACKETS_PER_BATCH - 2) + PACKETS_PER_BATCH) / PACKETS_PER_BATCH,
                (PACKETS_PER_BATCH - 2) * BATCH_COUNT,
            ),
            (
                (2 * BATCH_COUNT + PACKETS_PER_BATCH) / PACKETS_PER_BATCH,
                PACKET_COUNT - (PACKETS_PER_BATCH - 2) * BATCH_COUNT,
            ),
            (BATCH_COUNT - 1, PACKET_COUNT - 2 * PACKETS_PER_BATCH + 1),
            // 25
        ];

        let test_cases = set_discards.iter().zip(&expect_valids).enumerate();
        for (i, (set_discard, (expect_batch_count, expect_valid_packets))) in test_cases {
            debug!("test_shrink case: {}", i);
            let mut batches = to_packet_batches(
                &(0..PACKET_COUNT).map(|_| test_tx()).collect::<Vec<_>>(),
                PACKETS_PER_BATCH,
            );
            assert_eq!(batches.len(), BATCH_COUNT);
            assert_eq!(count_valid_packets(&batches, |_| ()), PACKET_COUNT);
            batches.iter_mut().enumerate().for_each(|(i, b)| {
                b.iter_mut()
                    .enumerate()
                    .for_each(|(j, p)| p.meta_mut().set_discard(set_discard(i, j)))
            });
            assert_eq!(count_valid_packets(&batches, |_| ()), *expect_valid_packets);
            debug!("show valid packets for case {}", i);
            batches.iter_mut().enumerate().for_each(|(i, b)| {
                b.iter_mut().enumerate().for_each(|(j, p)| {
                    if !p.meta().discard() {
                        trace!("{} {}", i, j)
                    }
                })
            });
            debug!("done show valid packets for case {}", i);
            shrink_batches(&mut batches);
            let shrunken_batch_count = batches.len();
            debug!("shrunk batch test {} count: {}", i, shrunken_batch_count);
            assert_eq!(shrunken_batch_count, *expect_batch_count);
            assert_eq!(count_valid_packets(&batches, |_| ()), *expect_valid_packets);
        }
    }
}
