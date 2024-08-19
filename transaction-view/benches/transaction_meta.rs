use {
    agave_transaction_view::{
        bytes::{advance_offset_for_array, advance_offset_for_type, read_byte},
        instructions_meta::InstructionsMeta,
        message_header_meta::{MessageHeaderMeta, TransactionVersion},
        result::TransactionParsingError,
        sanitize::sanitize_transaction_meta,
        signature_meta::SignatureMeta,
        static_account_keys_meta::StaticAccountKeysMeta,
        transaction_meta::TransactionMeta,
    },
    criterion::{
        black_box, criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup,
        Criterion, Throughput,
    },
    solana_sdk::{
        hash::Hash,
        instruction::{CompiledInstruction, Instruction},
        message::{
            v0::{self, MessageAddressTableLookup},
            Message, MessageHeader, VersionedMessage,
        },
        pubkey::Pubkey,
        short_vec::ShortVec,
        signature::{Keypair, Signature},
        signer::Signer,
        system_instruction,
        transaction::VersionedTransaction,
    },
};

#[repr(C)]
struct CompiledInstructionOffset {
    // This can be stored inline because the size is fixed
    program_id_index: u8,
    _padding: u8,
    /// Offset to the start of the accounts array.
    /// The offset is relative to the start of the transaction packet.
    /// This value is in bytes.
    /// Serialization order is accounts, then data.
    accounts_and_data_offset: u16,
    // The number of accounts in the accounts array.
    // Array starts at `accounts_and_data_offset`.
    accounts_len: u16,
    // The number of bytes in the data array.
    // Array starts at `accounts_data_data_offset` + `accounts_len`.
    data_len: u16,
}

pub fn to_compiled_instruction_offset_bytes(ixs: &[CompiledInstruction]) -> Vec<u8> {
    let slice_size = core::mem::size_of::<CompiledInstructionOffset>() * ixs.len();
    let buffer_size = 2
        + slice_size
        + ixs
            .iter()
            .map(|ix| ix.accounts.len() + ix.data.len())
            .sum::<usize>();
    let mut buffer = Vec::from_iter((0..buffer_size).map(|_| 0u8));

    let mut offset = 0;
    buffer[offset..offset + 2].copy_from_slice(&(ixs.len() as u16).to_le_bytes());
    offset += 2;

    let mut trailing_bytes_offset = 2 + slice_size;

    for ix in ixs {
        let accounts_and_data_offset = trailing_bytes_offset as u16;
        let accounts_len = ix.accounts.len() as u16;
        let data_len = ix.data.len() as u16;

        buffer[offset] = ix.program_id_index;
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&accounts_and_data_offset.to_le_bytes());
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&accounts_len.to_le_bytes());
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&data_len.to_le_bytes());
        offset += 2;

        buffer[trailing_bytes_offset..trailing_bytes_offset + ix.accounts.len()]
            .copy_from_slice(&ix.accounts);
        trailing_bytes_offset += ix.accounts.len();

        buffer[trailing_bytes_offset..trailing_bytes_offset + ix.data.len()]
            .copy_from_slice(&ix.data);
        trailing_bytes_offset += ix.data.len();
    }

    buffer
}

fn bench_parse_instructions(
    group: &mut BenchmarkGroup<impl Measurement>,
    ixs: ShortVec<CompiledInstruction>,
) {
    group.bench_function("CompiledInstruction Slice", |c| {
        c.iter(|| {
            let ixs = black_box(&ixs);
            let mut sum_of_data: u64 = 0;
            for ix in ixs.0.iter() {
                sum_of_data += ix.data.iter().copied().map(u64::from).sum::<u64>();
            }
            black_box(sum_of_data);
        });
    });

    let serialized_ixs = bincode::serialize(&ixs).unwrap();
    group.bench_function("CompiledInstruction Slice with Deserialization", |c| {
        c.iter(|| {
            let bytes = black_box(&serialized_ixs);
            let ixs = bincode::deserialize::<ShortVec<CompiledInstruction>>(bytes).unwrap();
            let mut sum_of_data: u64 = 0;
            for ix in ixs.0.iter() {
                sum_of_data += ix.data.iter().copied().map(u64::from).sum::<u64>();
            }
            black_box(sum_of_data);
        });
    });

    group.bench_function("inline serialization", |c| {
        c.iter(|| {
            let bytes = black_box(&serialized_ixs);

            // Parse using inline deserialization - this is equivalent to InstructionsMeta.
            let mut offset = 0;
            let meta = InstructionsMeta::try_new::<false>(
                bytes,
                &mut offset,
                TransactionVersion::Legacy,
                u8::MAX,
            )
            .unwrap();

            let iterator = unsafe { meta.instructions_iter(bytes) };
            let mut sum_of_data: u64 = 0;
            for ix in iterator {
                sum_of_data += ix.data.iter().copied().map(u64::from).sum::<u64>();
            }
            black_box(sum_of_data);
        });
    });

    let serialized_ixs = to_compiled_instruction_offset_bytes(&ixs.0);
    group.bench_function("oob serialization", |c| {
        c.iter(|| {
            let bytes = black_box(&serialized_ixs);

            let num_ixs = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
            let ixs: &[CompiledInstructionOffset] = unsafe {
                core::slice::from_raw_parts(
                    serialized_ixs.as_ptr().add(2) as *const CompiledInstructionOffset,
                    num_ixs,
                )
            };

            let mut sum_of_data: u64 = 0;
            for ix in ixs {
                let accounts_offset = ix.accounts_and_data_offset as usize;
                let data_offset = accounts_offset + ix.accounts_len as usize;

                let _accounts = &bytes[accounts_offset..data_offset];
                let data = &bytes[data_offset..data_offset + ix.data_len as usize];

                sum_of_data += data.iter().copied().map(u64::from).sum::<u64>();
            }

            black_box(sum_of_data);
        });
    });
}

// OOB Full transaction parsing

#[repr(C)]
struct CompiledInstructionCounts {
    num_accounts: u16,
    data_len: u16,
}

#[repr(C)]
struct ATLCounts {
    account_key: Pubkey,
    writable_indexes_len: u8,
    readonly_indexes_len: u8,
}

fn oob_serialize_transaction(transaction: &VersionedTransaction) -> Vec<u8> {
    let signatures_size = 1 + transaction.signatures.len() * core::mem::size_of::<Signature>();
    let version_len = match transaction.message {
        VersionedMessage::Legacy(_) => 0,
        VersionedMessage::V0(_) => 1,
    };
    let message_header_size = core::mem::size_of::<MessageHeader>();
    let account_keys_size =
        1 + transaction.message.static_account_keys().len() * core::mem::size_of::<Pubkey>();
    let recent_blockhash_size = core::mem::size_of::<Hash>();
    let instruction_counts_slice_size = 1 + transaction.message.instructions().len()
        * core::mem::size_of::<CompiledInstructionCounts>();
    let atl_counts_slice_size = version_len
        + transaction
            .message
            .address_table_lookups()
            .map(|atl| atl.len())
            .unwrap_or(0)
            * core::mem::size_of::<ATLCounts>();

    let serialized_instructions_size = transaction
        .message
        .instructions()
        .iter()
        .map(|ix| ix.accounts.len() + ix.data.len())
        .sum::<usize>();
    let serialized_atls_size = transaction
        .message
        .address_table_lookups()
        .map(|atls| {
            atls.iter()
                .map(|atl| atl.writable_indexes.len() + atl.readonly_indexes.len())
                .sum::<usize>()
        })
        .unwrap_or(0);

    // Instructions offset
    let instructions_count_offset = signatures_size
        + version_len
        + message_header_size
        + account_keys_size
        + recent_blockhash_size
        + 1;
    let padded_instructions_count_offset =
        instructions_count_offset + instructions_count_offset % 2;
    assert!(padded_instructions_count_offset % 2 == 0, "alignment check");
    let atl_count_offset = if version_len != 0 {
        padded_instructions_count_offset + instruction_counts_slice_size
    } else {
        0
    };
    let padded_atl_count_offset = atl_count_offset + atl_count_offset % 2;
    assert!(padded_atl_count_offset % 2 == 0, "alignment check");

    let packet_size = signatures_size
        + version_len
        + message_header_size
        + account_keys_size
        + recent_blockhash_size
        + instruction_counts_slice_size
        + atl_counts_slice_size
        + serialized_instructions_size
        + serialized_atls_size
        + (padded_instructions_count_offset - instructions_count_offset)
        + (padded_atl_count_offset - atl_count_offset);
    let mut buffer = Vec::from_iter((0..packet_size).map(|_| 0u8));

    let mut offset = 0;
    let mut trailing_offset = signatures_size
        + version_len
        + message_header_size
        + account_keys_size
        + recent_blockhash_size
        + instruction_counts_slice_size
        + atl_counts_slice_size
        + (padded_instructions_count_offset - instructions_count_offset)
        + (padded_atl_count_offset - atl_count_offset);
    let trailing_offset_start = trailing_offset;

    // Signatures
    buffer[offset] = transaction.signatures.len() as u8;
    offset += 1;

    for signature in &transaction.signatures {
        buffer[offset..offset + core::mem::size_of::<Signature>()]
            .copy_from_slice(signature.as_ref());
        offset += core::mem::size_of::<Signature>();
    }
    assert_eq!(offset, signatures_size, "signature offset check");

    // Version
    match transaction.message {
        VersionedMessage::Legacy(_) => {}
        VersionedMessage::V0(_) => {
            buffer[offset] = 128;
            offset += 1;
        }
    }

    assert_eq!(
        offset,
        signatures_size + version_len,
        "version offset check"
    );

    // Message Header
    let message_header = transaction.message.header();
    buffer[offset] = message_header.num_required_signatures;
    offset += 1;
    buffer[offset] = message_header.num_readonly_signed_accounts;
    offset += 1;
    buffer[offset] = message_header.num_readonly_unsigned_accounts;
    offset += 1;

    assert_eq!(
        offset,
        signatures_size + version_len + message_header_size,
        "message header offset check"
    );

    // Account Keys
    let account_keys = transaction.message.static_account_keys();
    buffer[offset] = account_keys.len() as u8; // can only have up to 34 accounts, u8 is better!
    offset += 1;

    for account_key in account_keys {
        buffer[offset..offset + core::mem::size_of::<Pubkey>()]
            .copy_from_slice(account_key.as_ref());
        offset += core::mem::size_of::<Pubkey>();
    }

    assert_eq!(
        offset,
        signatures_size + version_len + message_header_size + account_keys_size,
        "account keys offset check"
    );

    // Recent Blockhash
    buffer[offset..offset + core::mem::size_of::<Hash>()]
        .copy_from_slice(transaction.message.recent_blockhash().as_ref());
    offset += core::mem::size_of::<Hash>();

    assert_eq!(
        offset,
        signatures_size
            + version_len
            + message_header_size
            + account_keys_size
            + recent_blockhash_size,
        "recent blockhash offset check"
    );

    // Instructions
    let instructions = transaction.message.instructions();
    buffer[offset] = instructions.len() as u8; // assume SIMD-0160 is in place
    offset += 1;

    // Padding
    if offset % 2 != 0 {
        offset += 1;
    }

    assert_eq!(
        offset, padded_instructions_count_offset,
        "instruction count offset check"
    );
    for ix in instructions {
        // Inline serialization of counts.
        let accounts_len = ix.accounts.len() as u16;
        let data_len = ix.data.len() as u16;
        buffer[offset..offset + 2].copy_from_slice(&accounts_len.to_le_bytes());
        offset += 2;
        buffer[offset..offset + 2].copy_from_slice(&data_len.to_le_bytes());
        offset += 2;

        // Trailing serialization of variable length data.
        buffer[trailing_offset..trailing_offset + ix.accounts.len()].copy_from_slice(&ix.accounts);
        trailing_offset += ix.accounts.len();
        buffer[trailing_offset..trailing_offset + ix.data.len()].copy_from_slice(&ix.data);
        trailing_offset += ix.data.len();
    }

    assert_eq!(
        offset,
        padded_instructions_count_offset
            + instructions.len() * core::mem::size_of::<CompiledInstructionCounts>(),
        "instruction count offset check"
    );
    assert_eq!(
        offset,
        signatures_size
            + version_len
            + message_header_size
            + account_keys_size
            + recent_blockhash_size
            + instruction_counts_slice_size
            + (padded_instructions_count_offset - instructions_count_offset),
    );

    // ATLs
    if version_len == 0 {
        return buffer;
    }

    let atls = transaction.message.address_table_lookups().unwrap();
    buffer[offset] = atls.len() as u8;
    offset += 1;

    // Padding
    if offset % 2 != 0 {
        offset += 1;
    }

    assert_eq!(offset, padded_atl_count_offset, "atl count offset check");

    for atl in atls {
        // Inline serialization of counts and Pubkey
        buffer[offset..offset + core::mem::size_of::<Pubkey>()]
            .copy_from_slice(atl.account_key.as_ref());
        offset += core::mem::size_of::<Pubkey>();
        buffer[offset] = atl.writable_indexes.len() as u8;
        offset += 1;
        buffer[offset] = atl.readonly_indexes.len() as u8;
        offset += 1;

        // Trailing serialization of variable length data.
        buffer[trailing_offset..trailing_offset + atl.writable_indexes.len()]
            .copy_from_slice(&atl.writable_indexes);
        trailing_offset += atl.writable_indexes.len();
        buffer[trailing_offset..trailing_offset + atl.readonly_indexes.len()]
            .copy_from_slice(&atl.readonly_indexes);
        trailing_offset += atl.readonly_indexes.len();
    }

    assert_eq!(
        offset,
        padded_atl_count_offset + atls.len() * core::mem::size_of::<ATLCounts>(),
        "atl count offset check"
    );
    assert_eq!(offset, trailing_offset_start, "offset check");

    buffer
}

fn serialize_transactions_oob(transactions: &[VersionedTransaction]) -> Vec<Vec<u8>> {
    transactions
        .into_iter()
        .map(|transaction| oob_serialize_transaction(transaction))
        .collect()
}

struct OOBTransactionMeta {
    signature: SignatureMeta,
    header: MessageHeaderMeta,
    account_keys: StaticAccountKeysMeta,
    blockhash_offset: u16,

    instructions_len: u8,
    instruction_counts_offset: u16,

    atls_len: u8,
    atls_offset: u16,
}

fn parse_oob_transaction(bytes: &[u8]) -> Result<OOBTransactionMeta, TransactionParsingError> {
    let mut offset = 0;
    let signature_meta = SignatureMeta::try_new(bytes, &mut offset)?;
    let header_meta = MessageHeaderMeta::try_new(bytes, &mut offset)?;
    let account_keys_meta = StaticAccountKeysMeta::try_new(bytes, &mut offset)?;
    let recent_block_hash_offset = offset as u16;
    advance_offset_for_type::<Hash>(bytes, &mut offset)?;

    // Instructions
    let instructions_len = read_byte(bytes, &mut offset)?;
    if offset % 2 != 0 {
        offset += 1; // padding byte
    }
    assert_eq!(offset % 2, 0, "instruction alignment check");
    let instructions_offset = offset as u16;
    advance_offset_for_array::<CompiledInstructionCounts>(
        bytes,
        &mut offset,
        u16::from(instructions_len),
    )?;

    // ATLs
    let (atls_len, atls_offset) = match header_meta.version {
        TransactionVersion::Legacy => (0, 0),
        TransactionVersion::V0 => {
            let atls_len = read_byte(bytes, &mut offset)?;
            if offset % 2 != 0 {
                offset += 1; // padding byte
            }
            let atls_offset = offset as u16;
            assert_eq!(offset % 2, 0, "atl alignment check");
            advance_offset_for_array::<ATLCounts>(bytes, &mut offset, u16::from(atls_len))?;
            (atls_len, atls_offset)
        }
    };

    // Trailing offset data
    let instruction_counts_slice = unsafe {
        core::slice::from_raw_parts::<CompiledInstructionCounts>(
            bytes.as_ptr().add(usize::from(instructions_offset))
                as *const CompiledInstructionCounts,
            instructions_len as usize,
        )
    };
    for ix in instruction_counts_slice {
        advance_offset_for_array::<u8>(bytes, &mut offset, u16::from(ix.num_accounts))?;
        advance_offset_for_array::<u8>(bytes, &mut offset, u16::from(ix.data_len))?;
    }

    match header_meta.version {
        TransactionVersion::Legacy => {}
        TransactionVersion::V0 => {
            let atl_counts_slice = unsafe {
                core::slice::from_raw_parts::<ATLCounts>(
                    bytes.as_ptr().add(usize::from(atls_offset)) as *const ATLCounts,
                    atls_len as usize,
                )
            };
            let mut count = 0;
            for atl in atl_counts_slice {
                count += 1;
                advance_offset_for_array::<u8>(
                    bytes,
                    &mut offset,
                    u16::from(atl.writable_indexes_len),
                )?;
                advance_offset_for_array::<u8>(
                    bytes,
                    &mut offset,
                    u16::from(atl.readonly_indexes_len),
                )?;
            }
        }
    }

    assert_eq!(offset, bytes.len(), "offset check");

    Ok(OOBTransactionMeta {
        signature: signature_meta,
        header: header_meta,
        account_keys: account_keys_meta,
        blockhash_offset: recent_block_hash_offset,
        instructions_len,
        instruction_counts_offset: instructions_offset,
        atls_len,
        atls_offset,
    })
}

const NUM_TRANSACTIONS: usize = 1024;

fn serialize_transactions(transactions: &[VersionedTransaction]) -> Vec<Vec<u8>> {
    transactions
        .into_iter()
        .map(|transaction| bincode::serialize(transaction).unwrap())
        .collect()
}

fn bench_transactions_parsing(
    group: &mut BenchmarkGroup<impl Measurement>,
    transactions: &[VersionedTransaction],
) {
    let serialized_transactions = serialize_transactions(transactions);

    // // Legacy Transaction Parsing
    // group.bench_function("VersionedTransaction", |c| {
    //     c.iter(|| {
    //         for bytes in serialized_transactions.iter() {
    //             let _ = bincode::deserialize::<VersionedTransaction>(black_box(bytes)).unwrap();
    //         }
    //     });
    // });

    // // New Transaction Parsing
    // group.bench_function("TransactionMeta", |c| {
    //     c.iter(|| {
    //         for bytes in serialized_transactions.iter() {
    //             let _ = TransactionMeta::try_new(black_box(bytes)).unwrap();
    //         }
    //     });
    // });

    // // New Transaction Parsing - separated sanitization
    // group.bench_function("TransactionMeta (separate sanitize)", |c| {
    //     c.iter(|| {
    //         for bytes in serialized_transactions.iter() {
    //             let transaction_meta = TransactionMeta::try_new(black_box(bytes)).unwrap();
    //             unsafe { sanitize_transaction_meta(bytes, &transaction_meta).unwrap() }
    //         }
    //     });
    // });

    // // New Transaction Parsing - inline sanitization
    // group.bench_function("TransactionMeta (inline sanitize)", |c| {
    //     c.iter(|| {
    //         for bytes in serialized_transactions.iter() {
    //             let _ = TransactionMeta::try_new_sanitized(black_box(bytes)).unwrap();
    //         }
    //     });
    // });

    // New Transaction Parsing - OOB serialization
    let oob_serialized_transactions = serialize_transactions_oob(transactions);
    group.bench_function("OOBTransactionMeta", |c| {
        c.iter(|| {
            for bytes in oob_serialized_transactions.iter() {
                let _ = parse_oob_transaction(black_box(bytes)).unwrap();
            }
        });
    });
}

fn minimum_sized_transactions() -> Vec<VersionedTransaction> {
    (0..NUM_TRANSACTIONS)
        .map(|_| {
            let keypair = Keypair::new();
            VersionedTransaction::try_new(
                VersionedMessage::Legacy(Message::new_with_blockhash(
                    &[],
                    Some(&keypair.pubkey()),
                    &Hash::default(),
                )),
                &[&keypair],
            )
            .unwrap()
        })
        .collect()
}

fn simple_transfers() -> Vec<VersionedTransaction> {
    (0..NUM_TRANSACTIONS)
        .map(|_| {
            let keypair = Keypair::new();
            VersionedTransaction::try_new(
                VersionedMessage::Legacy(Message::new_with_blockhash(
                    &[system_instruction::transfer(
                        &keypair.pubkey(),
                        &Pubkey::new_unique(),
                        1,
                    )],
                    Some(&keypair.pubkey()),
                    &Hash::default(),
                )),
                &[&keypair],
            )
            .unwrap()
        })
        .collect()
}

fn packed_transfers() -> Vec<VersionedTransaction> {
    // Creating transfer instructions between same keys to maximize the number
    // of transfers per transaction. We can fit up to 60 transfers.
    const MAX_TRANSFERS_PER_TX: usize = 60;

    (0..NUM_TRANSACTIONS)
        .map(|_| {
            let keypair = Keypair::new();
            let to_pubkey = Pubkey::new_unique();
            let ixs = system_instruction::transfer_many(
                &keypair.pubkey(),
                &vec![(to_pubkey, 1); MAX_TRANSFERS_PER_TX],
            );
            VersionedTransaction::try_new(
                VersionedMessage::Legacy(Message::new(&ixs, Some(&keypair.pubkey()))),
                &[&keypair],
            )
            .unwrap()
        })
        .collect()
}

fn packed_noops() -> Vec<VersionedTransaction> {
    // Creating noop instructions to maximize the number of instructions per
    // transaction. We can fit up to 355 noops.
    const MAX_INSTRUCTIONS_PER_TRANSACTION: usize = 355;

    (0..NUM_TRANSACTIONS)
        .map(|_| {
            let keypair = Keypair::new();
            let program_id = Pubkey::new_unique();
            let ixs = (0..MAX_INSTRUCTIONS_PER_TRANSACTION)
                .map(|_| Instruction::new_with_bytes(program_id, &[], vec![]));
            VersionedTransaction::try_new(
                VersionedMessage::Legacy(Message::new(
                    &ixs.collect::<Vec<_>>(),
                    Some(&keypair.pubkey()),
                )),
                &[&keypair],
            )
            .unwrap()
        })
        .collect()
}

fn packed_atls() -> Vec<VersionedTransaction> {
    // Creating ATLs to maximize the number of ATLS per transaction. We can fit
    // up to 31.
    const MAX_ATLS_PER_TRANSACTION: usize = 31;

    (0..NUM_TRANSACTIONS)
        .map(|_| {
            let keypair = Keypair::new();
            VersionedTransaction::try_new(
                VersionedMessage::V0(v0::Message {
                    header: MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![keypair.pubkey()],
                    recent_blockhash: Hash::default(),
                    instructions: vec![],
                    address_table_lookups: Vec::from_iter((0..MAX_ATLS_PER_TRANSACTION).map(
                        |_| MessageAddressTableLookup {
                            account_key: Pubkey::new_unique(),
                            writable_indexes: vec![0],
                            readonly_indexes: vec![],
                        },
                    )),
                }),
                &[&keypair],
            )
            .unwrap()
        })
        .collect()
}

fn bench_parse_min_sized_transactions(c: &mut Criterion) {
    let transactions = minimum_sized_transactions();
    let mut group = c.benchmark_group("min sized transactions");
    group.throughput(Throughput::Elements(transactions.len() as u64));
    bench_transactions_parsing(&mut group, &transactions);
}

fn bench_parse_simple_transfers(c: &mut Criterion) {
    let transactions = simple_transfers();
    let mut group = c.benchmark_group("simple transfers");
    group.throughput(Throughput::Elements(transactions.len() as u64));
    bench_transactions_parsing(&mut group, &transactions);
}

fn bench_parse_packed_transfers(c: &mut Criterion) {
    let transactions = packed_transfers();
    let mut group = c.benchmark_group("packed transfers");
    group.throughput(Throughput::Elements(transactions.len() as u64));
    bench_transactions_parsing(&mut group, &transactions);
}

// fn bench_parse_packed_noops(c: &mut Criterion) {
//     let transactions = packed_noops();
//     let mut group = c.benchmark_group("packed noops");
//     group.throughput(Throughput::Elements(transactions.len() as u64));
//     bench_transactions_parsing(&mut group, &transactions);
// }

fn bench_parse_packed_atls(c: &mut Criterion) {
    let transactions = packed_atls();
    let mut group = c.benchmark_group("packed atls");
    group.throughput(Throughput::Elements(transactions.len() as u64));
    bench_transactions_parsing(&mut group, &transactions);
}

fn bench_stuff(c: &mut Criterion) {
    let packed_transfers = packed_transfers();
    let instruction_array = packed_transfers[0].message.instructions();
    let instruction_short_vec = ShortVec(instruction_array.to_vec());
    let mut group = c.benchmark_group("serialization format");
    group.throughput(Throughput::Elements(1));
    bench_parse_instructions(&mut group, instruction_short_vec);
}

criterion_group!(
    benches,
    bench_parse_min_sized_transactions,
    bench_parse_simple_transfers,
    bench_parse_packed_transfers,
    bench_parse_packed_atls,
    // bench_stuff
);
criterion_main!(benches);
