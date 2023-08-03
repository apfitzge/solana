use {
    crate::bench_tps_client::*,
    itertools::Itertools,
    log::*,
    solana_address_lookup_table_program::instruction::{create_lookup_table, extend_lookup_table},
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        slot_history::Slot,
        system_transaction,
        timing::AtomicInterval,
        transaction::Transaction,
    },
    std::{
        collections::HashSet,
        sync::Arc,
        time::{Duration, Instant},
    },
};

// Number of pubkeys to be included in single extend_lookup_table transaction that not exceeds MTU
const NUMBER_OF_ADDRESSES_PER_EXTEND: usize = 16;

pub fn create_address_lookup_table_accounts<T: 'static + BenchTpsClient + Send + Sync + ?Sized>(
    client: &Arc<T>,
    funding_key: &Keypair,
    num_addresses: usize,
    account_size: usize,
    num_tables: usize,
    program_id: &Pubkey,
) -> Result<Vec<Pubkey>> {
    const CHUNK_SIZE: usize = 128;

    let minimum_balance = client.get_minimum_balance_for_rent_exemption(account_size)?;

    let authorities: Vec<_> = (0..num_tables).map(|_| Keypair::new()).collect();
    let mut lookup_table_addresses = Vec::with_capacity(num_tables);
    authorities
        .iter()
        .chunks(CHUNK_SIZE)
        .into_iter()
        .for_each(|chunk| {
            let (chunk_lookup_table_addresses, tx_sigs): (Vec<_>, Vec<_>) = chunk
                .map(|authority| {
                    let (transaction, lookup_table_address) = build_create_lookup_table_tx(
                        funding_key,
                        authority,
                        client.get_slot().unwrap_or(0),
                        client.get_latest_blockhash().unwrap(),
                    );
                    let tx_sig = client.send_transaction(transaction).unwrap();
                    (lookup_table_address, tx_sig)
                })
                .unzip();

            confirm_sigs(client, tx_sigs, "create lookup table");
            lookup_table_addresses.extend(chunk_lookup_table_addresses);
        });

    let mut num_accounts_per_table = 0;
    let mut sized_account_keys = Vec::with_capacity(num_tables * num_addresses);
    while num_accounts_per_table < num_addresses {
        let extend_num_addresses =
            NUMBER_OF_ADDRESSES_PER_EXTEND.min(num_addresses - num_accounts_per_table);
        num_accounts_per_table += extend_num_addresses;

        lookup_table_addresses
            .iter()
            .zip(authorities.iter())
            .chunks(CHUNK_SIZE)
            .into_iter()
            .for_each(|chunk| {
                let mut tx_sigs = Vec::with_capacity(num_tables);
                for (lookup_table_address, authority) in chunk {
                    let pubkeys = (0..extend_num_addresses)
                        .map(|_| {
                            let keypair = Keypair::new();
                            let pubkey = keypair.pubkey();
                            sized_account_keys.push(keypair);
                            pubkey
                        })
                        .collect::<Vec<_>>();
                    let transaction = build_extend_lookup_table_tx(
                        lookup_table_address,
                        funding_key,
                        authority,
                        pubkeys,
                        client.get_latest_blockhash().unwrap(),
                    );
                    let tx_sig = client.send_transaction(transaction).unwrap();
                    tx_sigs.push(tx_sig);
                }

                confirm_sigs(client, tx_sigs, "extend lookup table");
            });
    }

    // Create accounts that will be used for funding the creation of these accounts.
    // First, need to determine how many accounts will be created. Minimum balance for each.
    let funders = (0..CHUNK_SIZE).map(|_| Keypair::new()).collect_vec();
    let lamports = sized_account_keys.len() as u64 * minimum_balance / CHUNK_SIZE as u64;
    let initial_funding_sigs = funders
        .iter()
        .map(|key| {
            let transaction = system_transaction::transfer(
                funding_key,
                &key.pubkey(),
                lamports,
                client.get_latest_blockhash().unwrap(),
            );
            client.send_transaction(transaction).unwrap()
        })
        .collect_vec();
    confirm_sigs(client, initial_funding_sigs, "initial funding sigs");

    // Allocate accounts
    sized_account_keys
        .into_iter()
        .chunks(CHUNK_SIZE)
        .into_iter()
        .for_each(|chunk| {
            let tx_sigs = chunk
                .into_iter()
                .zip(funders.iter())
                .map(|(keypair, funder)| {
                    let transaction = build_allocate_account_tx(
                        funder,
                        &keypair,
                        account_size,
                        minimum_balance,
                        client.get_latest_blockhash().unwrap(),
                        program_id,
                    );

                    client.send_transaction(transaction).unwrap()
                })
                .collect_vec();

            confirm_sigs(client, tx_sigs, "create account");
        });

    Ok(lookup_table_addresses)
}

fn confirm_sigs<T: 'static + BenchTpsClient + Send + Sync + ?Sized>(
    client: &Arc<T>,
    mut tx_sigs: Vec<Signature>,
    tx_type: &'static str,
) {
    let num_txs = tx_sigs.len();
    let now = Instant::now();
    let interval = AtomicInterval::default();
    while !tx_sigs.is_empty() && now.elapsed().as_secs() < 30 {
        let current_len = tx_sigs.len();
        trace!("confirming {tx_type} signatures: remaining={current_len}");
        let mut idx = 0;
        tx_sigs.retain(|sig| {
            if interval.should_update(1_000) {
                info!("confirming {tx_type} txs... {idx}/{num_txs}");
            }
            let result = client.transaction_confirmation(sig).unwrap();
            trace!("{sig} confirmation result: {result:?}");
            idx += 1;
            result.is_none()
        });
    }

    assert!(
        tx_sigs.is_empty(),
        "took too long to confirm: remaining={}",
        tx_sigs.len()
    );
}

fn build_allocate_account_tx(
    funding_key: &Keypair,
    account_key: &Keypair,
    account_size: usize,
    minimum_balance: u64,
    recent_blockhash: Hash,
    program_id: &Pubkey,
) -> Transaction {
    system_transaction::create_account(
        funding_key,
        account_key,
        recent_blockhash,
        minimum_balance,
        account_size as u64,
        program_id,
    )
}

fn build_create_lookup_table_tx(
    funding_key: &Keypair,
    authority_key: &Keypair,
    recent_slot: Slot,
    recent_blockhash: Hash,
) -> (Transaction, Pubkey) {
    let (create_lookup_table_ix, lookup_table_address) = create_lookup_table(
        authority_key.pubkey(), // authority
        funding_key.pubkey(),   // payer
        recent_slot,            // slot
    );

    let instructions = &[
        // ComputeBudgetInstruction::set_compute_unit_limit(50_000),
        // ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(64000),
        create_lookup_table_ix,
    ];

    (
        Transaction::new_signed_with_payer(
            instructions,
            Some(&funding_key.pubkey()),
            &[funding_key],
            recent_blockhash,
        ),
        lookup_table_address,
    )
}

fn build_extend_lookup_table_tx(
    lookup_table_address: &Pubkey,
    funding_key: &Keypair,
    authority_key: &Keypair,
    pubkeys: Vec<Pubkey>,
    recent_blockhash: Hash,
) -> Transaction {
    let extend_lookup_table_ix = extend_lookup_table(
        *lookup_table_address,
        authority_key.pubkey(),     // authority
        Some(funding_key.pubkey()), // payer
        pubkeys,
    );

    let instructions = &[
        ComputeBudgetInstruction::set_compute_unit_limit(50_000),
        ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(64000),
        extend_lookup_table_ix,
    ];

    Transaction::new_signed_with_payer(
        instructions,
        Some(&funding_key.pubkey()),
        &[funding_key, authority_key],
        recent_blockhash,
    )
}
