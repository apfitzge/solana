use {
    crate::{
        instruction_processor::{CachedInstructionProcessor, InstructionProcessor},
        program_id_flags::SignatureDetailsFlag,
    },
    solana_pubkey::Pubkey,
    solana_svm_transaction::instruction::SVMInstruction,
};

#[derive(Default)]
pub struct PrecompileSignatureDetails {
    pub num_secp256k1_instruction_signatures: u64,
    pub num_ed25519_instruction_signatures: u64,
}

/// Processor for collecting signature details.
impl InstructionProcessor<SignatureDetailsFlag> for PrecompileSignatureDetails {
    type OUTPUT = Self;

    fn process_instruction(
        &mut self,
        flag: &SignatureDetailsFlag,
        _instruction_index: usize,
        instruction: &SVMInstruction,
    ) -> Result<(), solana_sdk::transaction::TransactionError> {
        // Wrapping arithmetic is safe below because the maximum number of signatures
        // per instruction is 255, and the maximum number of instructions per transaction
        // is low enough that the sum of all signatures will not overflow a u64.
        match flag {
            SignatureDetailsFlag::Secp256k1 => {
                self.num_secp256k1_instruction_signatures = self
                    .num_secp256k1_instruction_signatures
                    .wrapping_add(get_num_signatures_in_instruction(instruction));
            }
            SignatureDetailsFlag::Ed25519 => {
                self.num_ed25519_instruction_signatures = self
                    .num_ed25519_instruction_signatures
                    .wrapping_add(get_num_signatures_in_instruction(instruction));
            }
            SignatureDetailsFlag::NoMatch => {}
        }
        Ok(())
    }

    fn finalize(self) -> Self::OUTPUT {
        self
    }
}

/// Get transaction signature details.
pub fn get_precompile_signature_details<'a>(
    instructions: impl Iterator<Item = (&'a Pubkey, SVMInstruction<'a>)>,
) -> PrecompileSignatureDetails {
    CachedInstructionProcessor::<SignatureDetailsFlag, PrecompileSignatureDetails>::default()
        .process_instructions(instructions)
        .unwrap()
}

#[inline]
fn get_num_signatures_in_instruction(instruction: &SVMInstruction) -> u64 {
    u64::from(instruction.data.first().copied().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    // simple convenience function so avoid having inconsistent program_id and program_id_index
    fn make_instruction<'a>(
        program_ids: &'a [Pubkey],
        program_id_index: u8,
        data: &'a [u8],
    ) -> (&'a Pubkey, SVMInstruction<'a>) {
        (
            &program_ids[program_id_index as usize],
            SVMInstruction {
                program_id_index,
                accounts: &[],
                data,
            },
        )
    }

    #[test]
    fn test_get_signature_details_no_instructions() {
        let instructions = std::iter::empty();
        let signature_details = get_precompile_signature_details(instructions);

        assert_eq!(signature_details.num_secp256k1_instruction_signatures, 0);
        assert_eq!(signature_details.num_ed25519_instruction_signatures, 0);
    }

    #[test]
    fn test_get_signature_details_no_sigs_unique() {
        let program_ids = [Pubkey::new_unique(), Pubkey::new_unique()];
        let instructions = [
            make_instruction(&program_ids, 0, &[]),
            make_instruction(&program_ids, 1, &[]),
        ];

        let signature_details = get_precompile_signature_details(instructions.into_iter());
        assert_eq!(signature_details.num_secp256k1_instruction_signatures, 0);
        assert_eq!(signature_details.num_ed25519_instruction_signatures, 0);
    }

    #[test]
    fn test_get_signature_details_signatures_mixed() {
        let program_ids = [
            Pubkey::new_unique(),
            solana_sdk::secp256k1_program::ID,
            solana_sdk::ed25519_program::ID,
        ];
        let instructions = [
            make_instruction(&program_ids, 1, &[5]),
            make_instruction(&program_ids, 2, &[3]),
            make_instruction(&program_ids, 0, &[]),
            make_instruction(&program_ids, 2, &[2]),
            make_instruction(&program_ids, 1, &[1]),
            make_instruction(&program_ids, 0, &[]),
        ];

        let signature_details = get_precompile_signature_details(instructions.into_iter());
        assert_eq!(signature_details.num_secp256k1_instruction_signatures, 6);
        assert_eq!(signature_details.num_ed25519_instruction_signatures, 5);
    }

    #[test]
    fn test_get_signature_details_missing_num_signatures() {
        let program_ids = [
            solana_sdk::secp256k1_program::ID,
            solana_sdk::ed25519_program::ID,
        ];
        let instructions = [
            make_instruction(&program_ids, 0, &[]),
            make_instruction(&program_ids, 1, &[]),
        ];

        let signature_details = get_precompile_signature_details(instructions.into_iter());
        assert_eq!(signature_details.num_secp256k1_instruction_signatures, 0);
        assert_eq!(signature_details.num_ed25519_instruction_signatures, 0);
    }
}
