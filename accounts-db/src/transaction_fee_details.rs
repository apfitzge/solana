use {
    crate::{
        blockhash_queue::BlockhashQueue,
        nonce_info::{NonceInfo, NoncePartial},
    },
    solana_program_runtime::compute_budget_processor::process_compute_budget_instructions,
    solana_sdk::{
        feature_set::{
            include_loaded_accounts_data_size_in_fee_calculation,
            remove_congestion_multiplier_from_fee_calculation, FeatureSet,
        },
        fee::FeeStructure,
        message::SanitizedMessage,
        transaction::TransactionError,
    },
};

/// Details of total fee and fee from prioritization.
pub struct TransactionFeeDetails {
    pub total_fee: u64,
    pub priority_fee: u64,
}

/// Calculate fee for a sanitized message and optional nonce.
pub fn calculate_transaction_fee_details(
    message: &SanitizedMessage,
    nonce: &Option<NoncePartial>,
    hash_queue: &BlockhashQueue,
    feature_set: &FeatureSet,
    fee_structure: &FeeStructure,
) -> Result<TransactionFeeDetails, TransactionError> {
    let Some(lamports_per_signature) = nonce
        .as_ref()
        .map(|nonce| nonce.lamports_per_signature())
        .unwrap_or_else(|| hash_queue.get_lamports_per_signature(message.recent_blockhash()))
    else {
        return Err(TransactionError::BlockhashNotFound);
    };

    let fee_budget_limits =
        process_compute_budget_instructions(message.program_instructions_iter(), feature_set)
            .unwrap_or_default()
            .into();
    let total_fee = fee_structure.calculate_fee(
        message,
        lamports_per_signature,
        &fee_budget_limits,
        feature_set.is_active(&remove_congestion_multiplier_from_fee_calculation::id()),
        feature_set.is_active(&include_loaded_accounts_data_size_in_fee_calculation::id()),
    );

    Ok(TransactionFeeDetails {
        total_fee,
        priority_fee: fee_budget_limits.prioritization_fee,
    })
}
