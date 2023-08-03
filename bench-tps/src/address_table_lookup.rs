use {
    crate::bench_tps_client::*,
    itertools::Itertools,
    log::*,
    solana_address_lookup_table_program::{
        instruction::{create_lookup_table, extend_lookup_table},
        state::AddressLookupTable,
    },
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        pubkey::Pubkey,
        rent::Rent,
        signature::{Keypair, Signature, Signer},
        slot_history::Slot,
        system_transaction,
        timing::AtomicInterval,
        transaction::Transaction,
    },
    std::{
        collections::HashSet,
        sync::Arc,
        thread::sleep,
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
    let minimum_balance = client.get_minimum_balance_for_rent_exemption(account_size)?;

    let authorities: Vec<_> = (0..num_tables).map(|_| Keypair::new()).collect();
    let interval = AtomicInterval::default();
    let mut lookup_table_addresses = Vec::with_capacity(num_tables);
    authorities
        .iter()
        .enumerate()
        .chunks(1024)
        .into_iter()
        .for_each(|chunk| {
            let (chunk_lookup_table_addresses, tx_sigs): (Vec<_>, Vec<_>) = chunk
                .map(|(idx, authority)| {
                    if interval.should_update(1_000) {
                        info!("Creating address lookup tables... {idx}/{num_tables}");
                    }
                    let (transaction, lookup_table_address) = build_create_lookup_table_tx(
                        funding_key,
                        authority,
                        client.get_slot().unwrap_or(0),
                        client.get_latest_blockhash().unwrap(),
                    );
                    let tx_sig = client.send_transaction(transaction).unwrap();
                    trace!(
                "address_table_lookup create sent transaction, {lookup_table_address} sig {tx_sig}",
            );
                    (lookup_table_address, tx_sig)
                })
                .unzip();

            confirm_sigs(client, tx_sigs);
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
            .chunks(1024)
            .into_iter()
            .for_each(|chunk| {
                let mut tx_sigs = Vec::with_capacity(num_tables);
                for (lookup_table_address, authority) in chunk {
                    if interval.should_update(1_000) {
                        info!(
                            "extending address lookup tables... {}/{}",
                            tx_sigs.len(),
                            tx_sigs.capacity()
                        );
                    }

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
                    trace!(
                "address_table_lookup extend sent transaction, {lookup_table_address} sig {tx_sig}",
            );
                    tx_sigs.push(tx_sig);
                }

                confirm_sigs(client, tx_sigs);
            });
    }

    // Allocate accounts
    sized_account_keys
        .into_iter()
        .chunks(1024)
        .into_iter()
        .for_each(|chunk| {
            let tx_sigs = chunk
                .map(|keypair| {
                    let transaction = build_allocate_account_tx(
                        funding_key,
                        &keypair,
                        account_size,
                        minimum_balance,
                        client.get_latest_blockhash().unwrap(),
                        program_id,
                    );

                    let tx_sig = client.send_transaction(transaction).unwrap();
                    trace!(
                        "address_table_lookup allocate sent transaction, {} sig {}",
                        keypair.pubkey(),
                        tx_sig,
                    );
                    tx_sig
                })
                .collect_vec();

            confirm_sigs(client, tx_sigs);
        });

    Ok(lookup_table_addresses)
}

fn confirm_sigs<T: 'static + BenchTpsClient + Send + Sync + ?Sized>(
    client: &Arc<T>,
    mut tx_sigs: Vec<Signature>,
) {
    let now = Instant::now();
    let interval = AtomicInterval::default();
    while !tx_sigs.is_empty() && now.elapsed().as_secs() < 120 {
        let current_len = tx_sigs.len();
        info!("confirming signatures: remaining={current_len}");
        let mut idx = 0;
        tx_sigs.retain(|sig| {
            if interval.should_update(1_000) {
                info!("confirming txs... {idx}/{current_len}");
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

fn confirm_lookup_table_accounts<T: 'static + BenchTpsClient + Send + Sync + ?Sized>(
    client: &Arc<T>,
    lookup_table_addresses: &[Pubkey],
) {
    // Sleep a few slots to allow transactions to process
    sleep(Duration::from_secs(10));

    for lookup_table_address in lookup_table_addresses {
        // confirm tx
        sleep(Duration::from_millis(10));
        let lookup_table_account = client
            .get_account_with_commitment(lookup_table_address, CommitmentConfig::processed())
            .unwrap();
        let lookup_table = AddressLookupTable::deserialize(&lookup_table_account.data).unwrap();
        debug!("lookup table: {:?}", lookup_table);
    }
}
