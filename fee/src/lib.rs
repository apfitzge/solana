use {
    solana_sdk::fee::{FeeBudgetLimits, FeeDetails, FeeStructure},
    solana_svm_transaction::svm_message::SVMMessage,
};

/// Calculate fee for `SanitizedMessage`
pub fn calculate_fee(
    fee_structure: &FeeStructure,
    message: &impl SVMMessage,
    lamports_per_signature: u64,
    budget_limits: &FeeBudgetLimits,
    include_loaded_account_data_size_in_fee: bool,
    remove_rounding_in_fee_calculation: bool,
) -> u64 {
    calculate_fee_details(
        fee_structure,
        message,
        lamports_per_signature,
        budget_limits,
        include_loaded_account_data_size_in_fee,
        remove_rounding_in_fee_calculation,
    )
    .total_fee()
}

/// Calculate fee details for `SanitizedMessage`
pub fn calculate_fee_details(
    fee_structure: &FeeStructure,
    message: &impl SVMMessage,
    lamports_per_signature: u64,
    budget_limits: &FeeBudgetLimits,
    include_loaded_account_data_size_in_fee: bool,
    remove_rounding_in_fee_calculation: bool,
) -> FeeDetails {
    // Backward compatibility - lamports_per_signature == 0 means to clear
    // transaction fee to zero
    if lamports_per_signature == 0 {
        return FeeDetails::default();
    }

    let signature_fee = message
        .num_signatures()
        .saturating_mul(fee_structure.lamports_per_signature);
    let write_lock_fee = message
        .num_write_locks()
        .saturating_mul(fee_structure.lamports_per_write_lock);

    // `compute_fee` covers costs for both requested_compute_units and
    // requested_loaded_account_data_size
    let loaded_accounts_data_size_cost = if include_loaded_account_data_size_in_fee {
        FeeStructure::calculate_memory_usage_cost(
            budget_limits.loaded_accounts_data_size_limit,
            budget_limits.heap_cost,
        )
    } else {
        0_u64
    };
    let total_compute_units =
        loaded_accounts_data_size_cost.saturating_add(budget_limits.compute_unit_limit);
    let compute_fee = fee_structure
        .compute_fee_bins
        .iter()
        .find(|bin| total_compute_units <= bin.limit)
        .map(|bin| bin.fee)
        .unwrap_or_else(|| {
            fee_structure
                .compute_fee_bins
                .last()
                .map(|bin| bin.fee)
                .unwrap_or_default()
        });

    FeeDetails {
        transaction_fee: signature_fee
            .saturating_add(write_lock_fee)
            .saturating_add(compute_fee),
        prioritization_fee: budget_limits.prioritization_fee,
        remove_rounding_in_fee_calculation,
    }
}
