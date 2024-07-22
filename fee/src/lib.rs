use {
    solana_sdk::fee::{FeeBudgetLimits, FeeDetails},
    solana_svm_transaction::svm_message::SVMMessage,
};

/// Calculate fee for `SanitizedMessage`
pub fn calculate_fee(
    message: &impl SVMMessage,
    lamports_per_signature: u64,
    budget_limits: &FeeBudgetLimits,
    remove_rounding_in_fee_calculation: bool,
) -> u64 {
    calculate_fee_details(
        message,
        lamports_per_signature,
        budget_limits,
        remove_rounding_in_fee_calculation,
    )
    .total_fee()
}

pub fn calculate_fee_details(
    message: &impl SVMMessage,
    lamports_per_signature: u64,
    budget_limits: &FeeBudgetLimits,
    remove_rounding_in_fee_calculation: bool,
) -> FeeDetails {
    // Backward compatibility - lamports_per_signature == 0 means to clear
    // transaction fee to zero
    if lamports_per_signature == 0 {
        return FeeDetails::default();
    }

    let signature_fee = message
        .num_signatures()
        .saturating_mul(lamports_per_signature);

    FeeDetails {
        transaction_fee: signature_fee,
        prioritization_fee: budget_limits.prioritization_fee,
        remove_rounding_in_fee_calculation,
    }
}
