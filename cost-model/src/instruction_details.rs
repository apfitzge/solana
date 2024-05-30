use {
    crate::block_cost_limits::BUILT_IN_INSTRUCTION_COSTS,
    solana_program_runtime::compute_budget_processor::{
        DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT, MAX_COMPUTE_UNIT_LIMIT, MAX_HEAP_FRAME_BYTES,
        MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES,
    },
    solana_sdk::{
        borsh1::try_from_slice_unchecked,
        compute_budget::{self, ComputeBudgetInstruction},
        entrypoint::HEAP_LENGTH as MIN_HEAP_FRAME_BYTES,
        feature_set::FeatureSet,
        instruction::InstructionError,
        transaction::TransactionError,
    },
    solana_signed_message::Message,
};

/// A catch all for instruction checks.
/// Used to calculate compute_budget_limits, fee_budget_limits, etc.
/// We want to only loop our instructions a single time during the initial processing if possible!
pub struct InstructionDetails {
    // Signature details
    pub num_secp256k1_instruction_signatures: u64,
    pub num_ed25519_instruction_signatures: u64,

    // Compute budget details
    pub updated_heap_bytes: u32,
    pub compute_unit_limit: u32,
    pub compute_unit_price: u64,
    pub loaded_accounts_bytes: u32,

    // Cost details
    pub programs_execution_cost: u64,
    pub instruction_data_bytes: u64,
}

impl InstructionDetails {
    pub fn new(message: &impl Message) -> Result<Self, TransactionError> {
        // Signatures
        let mut num_secp256k1_instruction_signatures: u64 = 0;
        let mut num_ed25519_instruction_signatures: u64 = 0;

        // Compute budget
        let mut num_non_compute_budget_instructions: u32 = 0;
        let mut updated_compute_unit_limit = None;
        let mut updated_compute_unit_price = None;
        let mut requested_heap_size = None;
        let mut updated_loaded_accounts_data_size_limit = None;

        // Cost details
        let mut programs_execution_costs = 0u64;
        let mut data_bytes_len_total = 0u64;
        let mut has_user_space_instructions = false;

        for (i, (program_id, instruction)) in message.program_instructions_iter().enumerate() {
            // Check if the program is a signature
            if solana_sdk::secp256k1_program::check_id(program_id) {
                if let Some(num_verifies) = instruction.data.first() {
                    num_secp256k1_instruction_signatures = num_secp256k1_instruction_signatures
                        .saturating_add(u64::from(*num_verifies));
                }
            }
            if solana_sdk::ed25519_program::check_id(program_id) {
                if let Some(num_verifies) = instruction.data.first() {
                    num_ed25519_instruction_signatures =
                        num_ed25519_instruction_signatures.saturating_add(u64::from(*num_verifies));
                }
            }

            if compute_budget::check_id(program_id) {
                let invalid_instruction_data_error = TransactionError::InstructionError(
                    i as u8,
                    InstructionError::InvalidInstructionData,
                );
                let duplicate_instruction_error = TransactionError::DuplicateInstruction(i as u8);
                match try_from_slice_unchecked(instruction.data) {
                    Ok(ComputeBudgetInstruction::RequestHeapFrame(bytes)) => {
                        if requested_heap_size.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        if sanitize_requested_heap_size(bytes) {
                            requested_heap_size = Some(bytes);
                        } else {
                            return Err(invalid_instruction_data_error);
                        }
                    }
                    Ok(ComputeBudgetInstruction::SetComputeUnitLimit(compute_unit_limit)) => {
                        if updated_compute_unit_limit.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        updated_compute_unit_limit = Some(compute_unit_limit);
                    }
                    Ok(ComputeBudgetInstruction::SetComputeUnitPrice(micro_lamports)) => {
                        if updated_compute_unit_price.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        updated_compute_unit_price = Some(micro_lamports);
                    }
                    Ok(ComputeBudgetInstruction::SetLoadedAccountsDataSizeLimit(bytes)) => {
                        if updated_loaded_accounts_data_size_limit.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        updated_loaded_accounts_data_size_limit = Some(bytes);
                    }
                    _ => return Err(invalid_instruction_data_error),
                }
            } else {
                // only include non-request instructions in default max calc
                num_non_compute_budget_instructions =
                    num_non_compute_budget_instructions.saturating_add(1);
            }

            // Cost details
            let ix_execution_cost =
                if let Some(builtin_cost) = BUILT_IN_INSTRUCTION_COSTS.get(program_id) {
                    *builtin_cost
                } else {
                    has_user_space_instructions = true;
                    u64::from(DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT)
                };

            programs_execution_costs = programs_execution_costs
                .saturating_add(ix_execution_cost)
                .min(u64::from(MAX_COMPUTE_UNIT_LIMIT));

            data_bytes_len_total =
                data_bytes_len_total.saturating_add(instruction.data.len() as u64);
        }

        // sanitize limits
        let updated_heap_bytes = requested_heap_size
            .unwrap_or(u32::try_from(MIN_HEAP_FRAME_BYTES).unwrap()) // loader's default heap_size
            .min(MAX_HEAP_FRAME_BYTES);
        let compute_unit_limit = updated_compute_unit_limit
            .unwrap_or_else(|| {
                num_non_compute_budget_instructions
                    .saturating_mul(DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT)
            })
            .min(MAX_COMPUTE_UNIT_LIMIT);
        let compute_unit_price = updated_compute_unit_price.unwrap_or(0);
        let loaded_accounts_bytes = updated_loaded_accounts_data_size_limit
            .unwrap_or(MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES)
            .min(MAX_LOADED_ACCOUNTS_DATA_SIZE_BYTES);

        Ok(Self {
            num_secp256k1_instruction_signatures,
            num_ed25519_instruction_signatures,
            updated_heap_bytes,
            compute_unit_limit,
            compute_unit_price,
            loaded_accounts_bytes,
            programs_execution_cost: programs_execution_costs,
            instruction_data_bytes: data_bytes_len_total,
        })
    }
}

fn sanitize_requested_heap_size(bytes: u32) -> bool {
    (u32::try_from(MIN_HEAP_FRAME_BYTES).unwrap()..=MAX_HEAP_FRAME_BYTES).contains(&bytes)
        && bytes % 1024 == 0
}
