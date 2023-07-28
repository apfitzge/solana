use std::{collections::HashSet, time::Instant};

use solana_sdk::timing::AtomicInterval;

use {
    crate::bench_tps_client::*,
    log::*,
    solana_address_lookup_table_program::{
        instruction::{create_lookup_table, extend_lookup_table},
        state::AddressLookupTable,
    },
    solana_sdk::compute_budget::ComputeBudgetInstruction,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        slot_history::Slot,
        transaction::Transaction,
    },
    std::{sync::Arc, thread::sleep, time::Duration},
};

// Number of pubkeys to be included in single extend_lookup_table transaction that not exceeds MTU
const NUMBER_OF_ADDRESSES_PER_EXTEND: usize = 16;

pub fn create_address_lookup_table_accounts<T: 'static + BenchTpsClient + Send + Sync + ?Sized>(
    client: &Arc<T>,
    funding_key: &Keypair,
    num_addresses: usize,
    num_tables: usize,
) -> Result<Vec<Pubkey>> {
    let authorities: Vec<_> = (0..num_tables).map(|_| Keypair::new()).collect();
    let mut interval = AtomicInterval::default();
    let (lookup_table_addresses, mut tx_sigs): (Vec<_>, Vec<_>) = authorities
        .iter()
        .enumerate()
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

    // Wait until all transactions have been processed
    {
        let now = Instant::now();
        while !tx_sigs.is_empty() && now.elapsed().as_secs() < 120 {
            let current_len = tx_sigs.len();
            info!("confirming create signatures: remaining={current_len}");
            let mut idx = 0;
            tx_sigs.retain(|sig| {
                if interval.should_update(1_000) {
                    info!("confirming create address lookup tables... {idx}/{current_len}");
                }
                let result = client.transaction_confirmation(sig).unwrap();
                trace!("{sig} confirmation result: {result:?}");
                idx += 1;
                result.is_none()
            });
        }

        assert!(
            tx_sigs.is_empty(),
            "took too long to create address lookup tables: remaining={}",
            tx_sigs.len()
        );
    }

    let mut num_accounts_per_table = 0;
    let mut tx_sigs = Vec::with_capacity(num_tables);
    while num_accounts_per_table < num_addresses {
        let extend_num_addresses =
            NUMBER_OF_ADDRESSES_PER_EXTEND.min(num_addresses - num_accounts_per_table);
        num_accounts_per_table += extend_num_addresses;

        for (lookup_table_address, authority) in
            lookup_table_addresses.iter().zip(authorities.iter())
        {
            if interval.should_update(1_000) {
                info!(
                    "extending address lookup tables... {}/{}",
                    tx_sigs.len(),
                    tx_sigs.capacity()
                );
            }
            let transaction = build_extend_lookup_table_tx(
                lookup_table_address,
                funding_key,
                authority,
                extend_num_addresses,
                client.get_latest_blockhash().unwrap(),
            );
            let tx_sig = client.send_transaction(transaction).unwrap();
            trace!(
                "address_table_lookup extend sent transaction, {lookup_table_address} sig {tx_sig}",
            );
            tx_sigs.push(tx_sig);
        }

        // Wait until all transactions have been processed
        {
            let now = Instant::now();
            while !tx_sigs.is_empty() && now.elapsed().as_secs() < 120 {
                let current_len = tx_sigs.len();
                info!("confirming extend signatures: remaining={current_len}");
                let mut idx = 0;
                tx_sigs.retain(|sig| {
                    if interval.should_update(1_000) {
                        info!("confirming extend address lookup tables... {idx}/{current_len}");
                    }
                    let result = client.transaction_confirmation(sig).unwrap();
                    trace!("{sig} confirmation result: {result:?}");
                    idx += 1;
                    result.is_none()
                });
            }

            assert!(
                tx_sigs.is_empty(),
                "took too long to extend address lookup tables: remaining={}",
                tx_sigs.len()
            );
        }
        tx_sigs.clear();
    }

    Ok(lookup_table_addresses)
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
    num_addresses: usize,
    recent_blockhash: Hash,
) -> Transaction {
    let mut addresses = Vec::with_capacity(num_addresses);
    // NOTE - generated bunch of random addresses for sbf program (eg noop.so) to use,
    //        if real accounts are required, can use funded keypairs in lookup-table.
    addresses.resize_with(num_addresses, Pubkey::new_unique);
    let extend_lookup_table_ix = extend_lookup_table(
        *lookup_table_address,
        authority_key.pubkey(),     // authority
        Some(funding_key.pubkey()), // payer
        addresses,
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
