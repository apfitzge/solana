use {
    agave_transaction_view::static_account_keys_frame::MAX_STATIC_ACCOUNTS_PER_PACKET as FILTER_SIZE,
    solana_pubkey::Pubkey,
    solana_sdk::{
        program_utils::limited_deserialize,
        system_instruction::{SystemInstruction, MAX_PERMITTED_DATA_LENGTH},
        system_program,
    },
    solana_svm_transaction::instruction::SVMInstruction,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct AllocationInstructionDetails {
    pub total_space: u64,
}

impl AllocationInstructionDetails {
    pub fn process_instructions<'a>(
        instructions: impl Iterator<Item = (&'a Pubkey, SVMInstruction<'a>)>,
    ) -> Option<Self> {
        let mut filter = SystemProgramIdFilter::default();
        let mut details = AllocationInstructionDetails::default();

        for (program_id, instruction) in instructions {
            if filter.is_system_program(usize::from(instruction.program_id_index), program_id) {
                let requested_space = match parse_system_instruction_allocation(instruction.data) {
                    SystemProgramAccountAllocation::Some(space) => space,
                    SystemProgramAccountAllocation::Failed => return None,
                    SystemProgramAccountAllocation::None => 0,
                };

                details.total_space = details.total_space.saturating_add(requested_space);
            }
        }

        Some(details)
    }
}

struct SystemProgramIdFilter {
    // array of slots for all possible static and sanitized program_id_index,
    // each slot indicates if a program_id_index has not been checked (eg, None),
    // or already checked with result (eg, Some(result)) that can be reused.
    flags: [Option<bool>; FILTER_SIZE as usize],
}

impl Default for SystemProgramIdFilter {
    fn default() -> Self {
        SystemProgramIdFilter {
            flags: [None; FILTER_SIZE as usize],
        }
    }
}

impl SystemProgramIdFilter {
    #[inline]
    fn is_system_program(&mut self, index: usize, program_id: &Pubkey) -> bool {
        *self
            .flags
            .get_mut(index)
            .expect("program id index is sanitized")
            .get_or_insert_with(|| program_id == &system_program::ID)
    }
}

#[derive(Debug, PartialEq)]
enum SystemProgramAccountAllocation {
    None,
    Some(u64),
    Failed,
}

fn parse_system_instruction_allocation(data: &[u8]) -> SystemProgramAccountAllocation {
    let Ok(instruction) = limited_deserialize(data) else {
        return SystemProgramAccountAllocation::Failed;
    };

    match instruction {
        SystemInstruction::CreateAccount { space, .. }
        | SystemInstruction::CreateAccountWithSeed { space, .. }
        | SystemInstruction::Allocate { space }
        | SystemInstruction::AllocateWithSeed { space, .. } => {
            if space > MAX_PERMITTED_DATA_LENGTH {
                SystemProgramAccountAllocation::Failed
            } else {
                SystemProgramAccountAllocation::Some(space)
            }
        }
        _ => SystemProgramAccountAllocation::None,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_sdk::system_instruction};

    #[test]
    fn test_transfer() {
        let instruction =
            system_instruction::transfer(&Pubkey::new_unique(), &Pubkey::new_unique(), 1);
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::None
        );
    }

    #[test]
    fn test_create_account() {
        let mut instruction = system_instruction::create_account(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            1,
            57,
            &Pubkey::new_unique(),
        );
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Failed
        );
    }

    #[test]
    fn test_create_account_with_seed() {
        let mut instruction = system_instruction::create_account_with_seed(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            "seed",
            1,
            57,
            &Pubkey::new_unique(),
        );
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Failed
        );
    }

    #[test]
    fn test_allocate() {
        let mut instruction = system_instruction::allocate(&Pubkey::new_unique(), 57);
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Failed
        );
    }

    #[test]
    fn test_allocate_with_seed() {
        let mut instruction = system_instruction::allocate_with_seed(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            "seed",
            57,
            &Pubkey::new_unique(),
        );
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Some(57)
        );
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(
            parse_system_instruction_allocation(&instruction.data),
            SystemProgramAccountAllocation::Failed
        );
    }
}
