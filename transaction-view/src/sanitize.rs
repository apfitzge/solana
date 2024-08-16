use {
    crate::{
        address_table_lookup_meta::AddressTableLookupMeta,
        instructions_meta::InstructionsMeta,
        message_header_meta::{MessageHeaderMeta, TransactionVersion},
        result::{Result, TransactionParsingError},
        signature_meta::SignatureMeta,
        static_account_keys_meta::StaticAccountKeysMeta,
        transaction_meta::TransactionMeta,
    },
    solana_svm_transaction::{
        instruction::SVMInstruction, message_address_table_lookup::SVMMessageAddressTableLookup,
    },
};

/// Perform sanitization checks on the transaction.
/// These checks should be consistent with those performed by
/// `solana_sdk::transaction::versioned::sanitized::SanitizedVersionedTransaction`.
///
/// # Safety
/// - This function must be called with the same `bytes` slice that was
///   used to create the `TransactionMeta` instance.
pub unsafe fn sanitize_transaction_meta(
    bytes: &[u8],
    transaction_meta: &TransactionMeta,
) -> Result<()> {
    sanitize_pre_instruction_meta(
        &transaction_meta.signature,
        &transaction_meta.message_header,
        &transaction_meta.static_account_keys,
    )?;

    // Loop over address table lookups to do the initial pass.
    for address_table_lookup in transaction_meta.address_table_lookup_iter(bytes) {
        sanitize_address_table_lookup_initial_pass(address_table_lookup)?;
    }
    sanitize_address_table_lookup_meta_final(
        &transaction_meta.static_account_keys,
        &transaction_meta.address_table_lookup,
    )?;

    // Do final instructions pass.
    sanitize_instruction_final_pass::<true>(
        bytes,
        &transaction_meta.message_header,
        &transaction_meta.static_account_keys,
        &transaction_meta.instructions,
        &transaction_meta.address_table_lookup,
    )?;

    Ok(())
}

/// Simple sanitization checks that can be done with the meta information
/// collected before instruction parsing.
/// Checks include:
/// 1. The number of signatures matches the `num_required_signatures` in the
///    message header.
/// 2. Each signature has a corresponding static account key.
/// 3. The signing area and read-only non-signing area do not overlap.
/// 4. There should be at least one RW fee-payer account.
#[inline(always)]
pub(crate) fn sanitize_pre_instruction_meta(
    signature_meta: &SignatureMeta,
    message_header_meta: &MessageHeaderMeta,
    static_account_keys_meta: &StaticAccountKeysMeta,
) -> Result<()> {
    // Check that the number of signatures matches the number of signatures
    // in the message header.
    if signature_meta.num_signatures != message_header_meta.num_required_signatures {
        return Err(TransactionParsingError);
    }

    // Each signature must have a corresponding static account key.
    if signature_meta.num_signatures > static_account_keys_meta.num_static_accounts {
        return Err(TransactionParsingError);
    }

    // Signing area and read-only non-signing area should not overlap
    if message_header_meta.num_readonly_unsigned_accounts
        > static_account_keys_meta
            .num_static_accounts
            .wrapping_sub(message_header_meta.num_required_signatures)
    {
        return Err(TransactionParsingError);
    }

    // There should be at least one RW fee-payer account.
    if message_header_meta.num_readonly_signed_accounts
        >= message_header_meta.num_required_signatures
    {
        return Err(TransactionParsingError);
    }

    Ok(())
}

/// Perform sanitization checks on the instruction during the initial pass.
/// Checks:
/// 1. The program_id_index is within the bounds of the static accounts.
/// 2. Legacy transaction account indexes are within the bounds of the static accounts.
///    For V0 transactions, this check must be done after parsing the ATLs.
#[inline(always)]
pub(crate) fn sanitize_instruction_initial_pass(
    instruction: SVMInstruction,
    version: TransactionVersion,
    num_static_accounts: u8,
) -> Result<()> {
    if instruction.program_id_index >= num_static_accounts {
        return Err(TransactionParsingError);
    }

    if matches!(version, TransactionVersion::Legacy) {
        if instruction
            .accounts
            .iter()
            .any(|account_index| *account_index >= num_static_accounts)
        {
            return Err(TransactionParsingError);
        }
    }

    Ok(())
}

#[inline(always)]
pub(crate) fn sanitize_address_table_lookup_initial_pass(
    address_lookup_table: SVMMessageAddressTableLookup,
) -> Result<()> {
    // Each address table lookup must have at least on account index.
    if address_lookup_table.writable_indexes.len() == 0
        && address_lookup_table.readonly_indexes.len() == 0
    {
        return Err(TransactionParsingError);
    }

    Ok(())
}

#[inline(always)]
pub(crate) fn sanitize_address_table_lookup_meta_final(
    static_account_keys_meta: &StaticAccountKeysMeta,
    address_table_lookup_meta: &AddressTableLookupMeta,
) -> Result<()> {
    // We checked during initial parsing that the number of lookup accounts is less than 255.
    // We need to check that the total number of read-only accounts is less than 256.
    if address_table_lookup_meta
        .total_num_writable_accounts
        .wrapping_add(address_table_lookup_meta.total_num_readonly_accounts)
        > u8::MAX.wrapping_sub(static_account_keys_meta.num_static_accounts)
    {
        return Err(TransactionParsingError);
    }

    Ok(())
}

/// Perform sanitization checks on the instruction during the final pass.
///
/// # Safety
/// - This function must be called with the same `bytes` slice that was
///  used to create the meta instances.
#[inline(always)]
pub(crate) unsafe fn sanitize_instruction_final_pass<const DO_FIRST_PASS: bool>(
    bytes: &[u8],
    message_header_meta: &MessageHeaderMeta,
    static_account_keys_meta: &StaticAccountKeysMeta,
    instructions_meta: &InstructionsMeta,
    address_table_lookup_meta: &AddressTableLookupMeta,
) -> Result<()> {
    // If this is a legacy transaction, everything has already been verified.
    if matches!(message_header_meta.version, TransactionVersion::Legacy) {
        return Ok(());
    }

    // Loop over each instruction and check that the account indexes are within bounds.
    let total_num_accounts = static_account_keys_meta
        .num_static_accounts
        .wrapping_add(address_table_lookup_meta.total_num_writable_accounts)
        .wrapping_add(address_table_lookup_meta.total_num_readonly_accounts);

    // Check that the account indexes are within bounds.
    for instruction in instructions_meta.instructions_iter(bytes) {
        if DO_FIRST_PASS {
            sanitize_instruction_initial_pass(
                instruction.clone(),
                message_header_meta.version,
                static_account_keys_meta.num_static_accounts,
            )?;
        }
        if instruction
            .accounts
            .iter()
            .any(|account_index| *account_index >= total_num_accounts)
        {
            return Err(TransactionParsingError);
        }
    }
    Ok(())
}
