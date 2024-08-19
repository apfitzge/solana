use {
    agave_transaction_view::{
        instructions_meta::InstructionsMeta, message_header_meta::TransactionVersion,
        sanitize::sanitize_transaction_meta, transaction_meta::TransactionMeta,
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
        signature::Keypair,
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

const NUM_TRANSACTIONS: usize = 1024;

fn serialize_transactions(transactions: Vec<VersionedTransaction>) -> Vec<Vec<u8>> {
    transactions
        .into_iter()
        .map(|transaction| bincode::serialize(&transaction).unwrap())
        .collect()
}

fn bench_transactions_parsing(
    group: &mut BenchmarkGroup<impl Measurement>,
    serialized_transactions: Vec<Vec<u8>>,
) {
    // Legacy Transaction Parsing
    group.bench_function("VersionedTransaction", |c| {
        c.iter(|| {
            for bytes in serialized_transactions.iter() {
                let _ = bincode::deserialize::<VersionedTransaction>(black_box(bytes)).unwrap();
            }
        });
    });

    // New Transaction Parsing
    group.bench_function("TransactionMeta", |c| {
        c.iter(|| {
            for bytes in serialized_transactions.iter() {
                let _ = TransactionMeta::try_new(black_box(bytes)).unwrap();
            }
        });
    });

    // New Transaction Parsing - separated sanitization
    group.bench_function("TransactionMeta (separate sanitize)", |c| {
        c.iter(|| {
            for bytes in serialized_transactions.iter() {
                let transaction_meta = TransactionMeta::try_new(black_box(bytes)).unwrap();
                unsafe { sanitize_transaction_meta(bytes, &transaction_meta).unwrap() }
            }
        });
    });

    // New Transaction Parsing - inline sanitization
    group.bench_function("TransactionMeta (inline sanitize)", |c| {
        c.iter(|| {
            for bytes in serialized_transactions.iter() {
                let _ = TransactionMeta::try_new_sanitized(black_box(bytes)).unwrap();
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
    let serialized_transactions = serialize_transactions(minimum_sized_transactions());
    let mut group = c.benchmark_group("min sized transactions");
    group.throughput(Throughput::Elements(serialized_transactions.len() as u64));
    bench_transactions_parsing(&mut group, serialized_transactions);
}

fn bench_parse_simple_transfers(c: &mut Criterion) {
    let serialized_transactions = serialize_transactions(simple_transfers());
    let mut group = c.benchmark_group("simple transfers");
    group.throughput(Throughput::Elements(serialized_transactions.len() as u64));
    bench_transactions_parsing(&mut group, serialized_transactions);
}

fn bench_parse_packed_transfers(c: &mut Criterion) {
    let serialized_transactions = serialize_transactions(packed_transfers());
    let mut group = c.benchmark_group("packed transfers");
    group.throughput(Throughput::Elements(serialized_transactions.len() as u64));
    bench_transactions_parsing(&mut group, serialized_transactions);
}

fn bench_parse_packed_noops(c: &mut Criterion) {
    let serialized_transactions = serialize_transactions(packed_noops());
    let mut group = c.benchmark_group("packed noops");
    group.throughput(Throughput::Elements(serialized_transactions.len() as u64));
    bench_transactions_parsing(&mut group, serialized_transactions);
}

fn bench_parse_packed_atls(c: &mut Criterion) {
    let serialized_transactions = serialize_transactions(packed_atls());
    let mut group = c.benchmark_group("packed atls");
    group.throughput(Throughput::Elements(serialized_transactions.len() as u64));
    bench_transactions_parsing(&mut group, serialized_transactions);
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
    bench_parse_packed_noops,
    bench_parse_packed_atls,
    bench_stuff
);
criterion_main!(benches);
