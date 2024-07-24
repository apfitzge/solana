use {
    criterion::{
        black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup,
        BenchmarkId, Criterion, Throughput,
    },
    solana_sdk::{
        fee::{FeeBudgetLimits, FeeStructure},
        hash::Hash,
        instruction::Instruction,
        message::{Message, TransactionSignatureDetails},
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        system_transaction,
        transaction::{SanitizedTransaction, Transaction},
    },
    solana_svm_transaction::svm_message::SVMMessage,
    std::fmt::Debug,
};

fn bench_calculate_fee_details(c: &mut Criterion) {
    let mut group = c.benchmark_group("calculate_fee_details");
    group.throughput(Throughput::Elements(1));
    benchmark_simple_transfer(&mut group);
    benchmark_packed_noop(&mut group);
}

fn benchmark_simple_transfer(group: &mut BenchmarkGroup<WallTime>) {
    const TRANSACTION_TYPE: &str = "simple_transfer";
    let transaction = SanitizedTransaction::from_transaction_for_tests(
        system_transaction::transfer(&Keypair::new(), &Pubkey::new_unique(), 1, Hash::default()),
    );
    let fee_structure = FeeStructure::default();
    let lamports_per_signature = fee_structure.lamports_per_signature;
    let fee_budget_limits = FeeBudgetLimits {
        loaded_accounts_data_size_limit: 0,
        heap_cost: 0,
        compute_unit_limit: 10_000,
        prioritization_fee: 0,
    };
    let cached_num_total_signatures = transaction.num_total_signatures();

    group.bench_function(
        BenchmarkId::new("solana_sdk::calculate_fee_details", TRANSACTION_TYPE),
        |c| {
            c.iter(|| {
                benchmark_sdk_calculate_fee_details(
                    &fee_structure,
                    &transaction,
                    &fee_budget_limits,
                )
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("solana_fee::calculate_fee_details", TRANSACTION_TYPE),
        |c| {
            c.iter(|| {
                benchmark_solana_fee_calculate_fee_details(
                    &transaction,
                    lamports_per_signature,
                    &fee_budget_limits,
                )
            });
        },
    );

    group.bench_function(
        BenchmarkId::new(
            "solana_fee::calculate_fee_details_with_cached_signature_count",
            TRANSACTION_TYPE,
        ),
        |c| {
            c.iter(|| {
                benchmark_solana_fee_calculate_details_with_cached_signature_count(
                    &transaction,
                    lamports_per_signature,
                    &fee_budget_limits,
                    cached_num_total_signatures,
                )
            });
        },
    );
}

fn benchmark_packed_noop(group: &mut BenchmarkGroup<WallTime>) {
    const TRANSACTION_TYPE: &str = "packed_noop";

    let from_keypair = Keypair::new();
    let program_id = Pubkey::new_unique();
    let ixs: Vec<_> = (0..355)
        .map(|_| Instruction {
            program_id,
            accounts: vec![],
            data: vec![],
        })
        .collect();
    let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
    let transaction = Transaction::new(&[from_keypair], message, Hash::default());
    let transaction = SanitizedTransaction::from_transaction_for_tests(transaction);
    let fee_structure = FeeStructure::default();
    let lamports_per_signature = fee_structure.lamports_per_signature;
    let fee_budget_limits = FeeBudgetLimits {
        loaded_accounts_data_size_limit: 0,
        heap_cost: 0,
        compute_unit_limit: 10_000,
        prioritization_fee: 0,
    };
    let cached_num_total_signatures = transaction.num_total_signatures();

    group.bench_function(
        BenchmarkId::new("solana_sdk::calculate_fee_details", TRANSACTION_TYPE),
        |c| {
            c.iter(|| {
                benchmark_sdk_calculate_fee_details(
                    &fee_structure,
                    &transaction,
                    &fee_budget_limits,
                )
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("solana_fee::calculate_fee_details", TRANSACTION_TYPE),
        |c| {
            c.iter(|| {
                benchmark_solana_fee_calculate_fee_details(
                    &transaction,
                    lamports_per_signature,
                    &fee_budget_limits,
                )
            });
        },
    );

    group.bench_function(
        BenchmarkId::new(
            "solana_fee::calculate_fee_details_with_cached_signature_count",
            TRANSACTION_TYPE,
        ),
        |c| {
            c.iter(|| {
                benchmark_solana_fee_calculate_details_with_cached_signature_count(
                    &transaction,
                    lamports_per_signature,
                    &fee_budget_limits,
                    cached_num_total_signatures,
                )
            });
        },
    );
}

fn benchmark_sdk_calculate_fee_details(
    fee_structure: &FeeStructure,
    transaction: &SanitizedTransaction,
    fee_budget_limits: &FeeBudgetLimits,
) {
    #[allow(deprecated)]
    let _ = fee_structure.calculate_fee_details(
        black_box(transaction.message()),
        black_box(fee_structure.lamports_per_signature),
        black_box(fee_budget_limits),
        true,
        true,
    );
}

fn benchmark_solana_fee_calculate_fee_details(
    transaction: &impl SVMMessage,
    lamports_per_signature: u64,
    fee_budget_limits: &FeeBudgetLimits,
) {
    let _ = solana_fee::calculate_fee_details(
        black_box(transaction),
        black_box(lamports_per_signature),
        black_box(fee_budget_limits),
        true,
    );
}

fn benchmark_solana_fee_calculate_details_with_cached_signature_count(
    transaction: &impl SVMMessage,
    lamports_per_signature: u64,
    fee_budget_limits: &FeeBudgetLimits,
    cached_num_total_signatures: u64,
) {
    #[derive(Debug)]
    struct TxWithCachedSignatureDetails<'a, T: SVMMessage> {
        transaction: &'a T,
        cached_num_total_signatures: u64,
    }

    impl<T: SVMMessage> SVMMessage for TxWithCachedSignatureDetails<'_, T> {
        fn num_total_signatures(&self) -> u64 {
            self.cached_num_total_signatures
        }

        fn num_write_locks(&self) -> u64 {
            unimplemented!()
        }

        fn recent_blockhash(&self) -> &Hash {
            unimplemented!()
        }

        fn num_instructions(&self) -> usize {
            unimplemented!()
        }

        fn instructions_iter(
            &self,
        ) -> impl Iterator<Item = solana_svm_transaction::instruction::SVMInstruction> {
            core::iter::empty()
        }

        fn program_instructions_iter(
            &self,
        ) -> impl Iterator<Item = (&Pubkey, solana_svm_transaction::instruction::SVMInstruction)>
        {
            core::iter::empty()
        }

        fn account_keys(&self) -> solana_sdk::message::AccountKeys {
            unimplemented!()
        }

        fn fee_payer(&self) -> &Pubkey {
            unimplemented!()
        }

        fn is_writable(&self, _index: usize) -> bool {
            unimplemented!()
        }

        fn is_signer(&self, _index: usize) -> bool {
            unimplemented!()
        }

        fn is_invoked(&self, _key_index: usize) -> bool {
            unimplemented!()
        }

        fn num_lookup_tables(&self) -> usize {
            unimplemented!()
        }

        fn message_address_table_lookups(&self) -> impl Iterator<Item = solana_svm_transaction::message_address_table_lookup::SVMMessageAddressTableLookup>{
            core::iter::empty()
        }
    }

    let tx_with_cached_signature_details = TxWithCachedSignatureDetails {
        transaction,
        cached_num_total_signatures,
    };

    let _ = solana_fee::calculate_fee_details(
        black_box(&tx_with_cached_signature_details),
        black_box(lamports_per_signature),
        black_box(fee_budget_limits),
        true,
    );
}

criterion_group!(benches, bench_calculate_fee_details);
criterion_main!(benches);
