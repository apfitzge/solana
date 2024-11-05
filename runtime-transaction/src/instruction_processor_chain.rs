use {
    crate::{
        compute_budget_instruction_details::ComputeBudgetInstructionDetails,
        signature_details::PrecompileSignatureDetails,
    },
    agave_transaction_view::static_account_keys_frame::MAX_STATIC_ACCOUNTS_PER_PACKET as FILTER_SIZE,
    core::borrow::Borrow,
    solana_pubkey::Pubkey,
    solana_sdk::{
        compute_budget, ed25519_program, saturating_add_assign, secp256k1_program,
        transaction::TransactionError,
    },
    solana_svm_transaction::instruction::SVMInstruction,
};

enum ProgramIdFlag {
    NoMatch,
    Secp256k1,
    Ed25519,
    ComputeBudgetProgram,
}

enum ComputeBudgetProcessorFlag {
    NoMatch,
    ComputeBudgetProgram,
}

enum SignatureDetailsProcessorFlag {
    NoMatch,
    Secp256k1,
    Ed25519,
}

impl<T: Borrow<ProgramIdFlag>> From<T> for ComputeBudgetProcessorFlag {
    fn from(flag: T) -> Self {
        match flag.borrow() {
            ProgramIdFlag::NoMatch | ProgramIdFlag::Secp256k1 | ProgramIdFlag::Ed25519 => {
                ComputeBudgetProcessorFlag::NoMatch
            }
            ProgramIdFlag::ComputeBudgetProgram => ComputeBudgetProcessorFlag::ComputeBudgetProgram,
        }
    }
}

impl<T: Borrow<ProgramIdFlag>> From<T> for SignatureDetailsProcessorFlag {
    fn from(flag: T) -> Self {
        match flag.borrow() {
            ProgramIdFlag::NoMatch | ProgramIdFlag::ComputeBudgetProgram => {
                SignatureDetailsProcessorFlag::NoMatch
            }
            ProgramIdFlag::Secp256k1 => SignatureDetailsProcessorFlag::Secp256k1,
            ProgramIdFlag::Ed25519 => SignatureDetailsProcessorFlag::Ed25519,
        }
    }
}

trait FlaggedInstructionProcessor<T> {
    fn process_instruction(
        &mut self,
        flag: T,
        instruction_index: usize,
        instruction: &SVMInstruction,
    ) -> Result<(), TransactionError>;
}

struct SignatureDetailsProcessor {
    output: PrecompileSignatureDetails,
}

impl FlaggedInstructionProcessor<SignatureDetailsProcessorFlag> for SignatureDetailsProcessor {
    fn process_instruction(
        &mut self,
        flag: SignatureDetailsProcessorFlag,
        _instruction_index: usize,
        instruction: &SVMInstruction,
    ) -> Result<(), TransactionError> {
        match flag {
            SignatureDetailsProcessorFlag::NoMatch => {}
            SignatureDetailsProcessorFlag::Secp256k1 => {
                self.output.num_secp256k1_instruction_signatures = self
                    .output
                    .num_secp256k1_instruction_signatures
                    .wrapping_add(u64::from(instruction.data.first().copied().unwrap_or(0)));
            }
            SignatureDetailsProcessorFlag::Ed25519 => {
                self.output.num_ed25519_instruction_signatures = self
                    .output
                    .num_ed25519_instruction_signatures
                    .wrapping_add(u64::from(instruction.data.first().copied().unwrap_or(0)));
            }
        }

        Ok(())
    }
}

struct ComputeBudgetProcessor {
    output: ComputeBudgetInstructionDetails,
}

impl FlaggedInstructionProcessor<ComputeBudgetProcessorFlag> for ComputeBudgetProcessor {
    fn process_instruction(
        &mut self,
        flag: ComputeBudgetProcessorFlag,
        instruction_index: usize,
        instruction: &SVMInstruction,
    ) -> Result<(), TransactionError> {
        match flag {
            ComputeBudgetProcessorFlag::NoMatch => {
                saturating_add_assign!(self.output.num_non_compute_budget_instructions, 1)
            }
            ComputeBudgetProcessorFlag::ComputeBudgetProgram => {
                self.output
                    .process_instruction(instruction_index as u8, instruction)?;
            }
        }

        Ok(())
    }
}

struct InstructionProcessor {
    flags: [Option<ProgramIdFlag>; FILTER_SIZE as usize],

    signature_details_processor: SignatureDetailsProcessor,
    compute_budget_processor: ComputeBudgetProcessor,
}

impl Default for InstructionProcessor {
    fn default() -> Self {
        Self {
            flags: core::array::from_fn(|_| None),
            signature_details_processor: SignatureDetailsProcessor {
                output: PrecompileSignatureDetails::default(),
            },
            compute_budget_processor: ComputeBudgetProcessor {
                output: ComputeBudgetInstructionDetails::default(),
            },
        }
    }
}

impl InstructionProcessor {
    pub fn process_instructions<'a>(
        instructions: impl Iterator<Item = (&'a Pubkey, SVMInstruction<'a>)>,
    ) -> Result<(PrecompileSignatureDetails, ComputeBudgetInstructionDetails), TransactionError>
    {
        let mut processor = Self::default();

        for (instruction_index, (program_id, instruction)) in instructions.enumerate() {
            let flag = processor.flags[usize::from(instruction.program_id_index)]
                .get_or_insert_with(|| Self::check_program_id(program_id));

            // Passing to processors
            processor.signature_details_processor.process_instruction(
                SignatureDetailsProcessorFlag::from(flag.borrow()),
                instruction_index,
                &instruction,
            )?;
            processor.compute_budget_processor.process_instruction(
                ComputeBudgetProcessorFlag::from(flag.borrow()),
                instruction_index,
                &instruction,
            )?;
        }

        Ok((
            processor.signature_details_processor.output,
            processor.compute_budget_processor.output,
        ))
    }

    fn check_program_id(program_id: &Pubkey) -> ProgramIdFlag {
        if secp256k1_program::check_id(program_id) {
            ProgramIdFlag::Secp256k1
        } else if ed25519_program::check_id(program_id) {
            ProgramIdFlag::Ed25519
        } else if compute_budget::check_id(program_id) {
            ProgramIdFlag::ComputeBudgetProgram
        } else {
            ProgramIdFlag::NoMatch
        }
    }
}
