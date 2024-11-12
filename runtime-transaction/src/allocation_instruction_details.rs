use solana_pubkey::Pubkey;

const CREATE_ACCOUNT: u32 = 0;
const CREATE_ACCOUNT_SERIALIZED_SIZE: usize = {
    core::mem::size_of::<u32>() + // discriminant
    core::mem::size_of::<u64>() + // lamports
    core::mem::size_of::<u64>() + // space
    core::mem::size_of::<Pubkey>() // owner
};

const CREATE_ACCOUNT_WITH_SEED: u32 = 3;
const CREATE_ACCOUNT_WITH_SEED_MIN_SERIALIZED_SIZE: usize = {
    core::mem::size_of::<u32>() + // discriminant
    core::mem::size_of::<Pubkey>() + // base
    core::mem::size_of::<u64>() + // seed length
    core::mem::size_of::<u64>() + // lamports
    core::mem::size_of::<u64>() + // space
    core::mem::size_of::<Pubkey>() // owner
};

const ALLOCATE: u32 = 8;
const ALLOCATE_SERIALIZED_SIZE: usize = {
    core::mem::size_of::<u32>() + // discriminant
    core::mem::size_of::<u64>() // space
};

const ALLOCATE_WITH_SEED: u32 = 9;
const ALLOCATE_WITH_SEED_MIN_SERIALIZED_SIZE: usize = {
    core::mem::size_of::<u32>() + // discriminant
    core::mem::size_of::<Pubkey>() + // base
    core::mem::size_of::<u64>() + // seed length
    core::mem::size_of::<u64>() + // space
    core::mem::size_of::<Pubkey>() // owner
};

pub fn parse_system_instruction_allocation(instruction_data: &[u8]) -> u64 {
    match read_discriminant(instruction_data) {
        CREATE_ACCOUNT => {
            if instruction_data.len() < CREATE_ACCOUNT_SERIALIZED_SIZE {
                return 0;
            }
            // We can now read at the static without doing further bounds checks.
            const SPACE_OFFSET: usize = {
                core::mem::size_of::<u32>() + // discriminant
                core::mem::size_of::<u64>() // lamports
            };
            read_u64_at_unchecked(instruction_data, SPACE_OFFSET)
        }
        CREATE_ACCOUNT_WITH_SEED => {
            if instruction_data.len() < CREATE_ACCOUNT_WITH_SEED_MIN_SERIALIZED_SIZE {
                return 0;
            }

            const SEED_LENGTH_OFFSET: usize = {
                core::mem::size_of::<u32>() + // discriminant
                core::mem::size_of::<Pubkey>() // base
            };
            let seed_len = read_u64_at_unchecked(instruction_data, SEED_LENGTH_OFFSET) as usize;
            if instruction_data.len()
                < CREATE_ACCOUNT_WITH_SEED_MIN_SERIALIZED_SIZE.saturating_add(seed_len)
            {
                return 0;
            }
            let space_offset = SEED_LENGTH_OFFSET
                .wrapping_add(core::mem::size_of::<u64>())
                .wrapping_add(seed_len)
                .wrapping_add(core::mem::size_of::<u64>()); // lamports
            read_u64_at_unchecked(instruction_data, space_offset)
        }
        ALLOCATE => {
            if instruction_data.len() < ALLOCATE_SERIALIZED_SIZE {
                return 0;
            }
            const SPACE_OFFSET: usize = core::mem::size_of::<u32>(); // discriminant
            read_u64_at_unchecked(instruction_data, SPACE_OFFSET)
        }
        ALLOCATE_WITH_SEED => {
            if instruction_data.len() < ALLOCATE_WITH_SEED_MIN_SERIALIZED_SIZE {
                return 0;
            }

            const SEED_LENGTH_OFFSET: usize = {
                core::mem::size_of::<u32>() + // discriminant
                core::mem::size_of::<Pubkey>() // base
            };
            let seed_len = read_u64_at_unchecked(instruction_data, SEED_LENGTH_OFFSET) as usize;

            if instruction_data.len()
                < ALLOCATE_WITH_SEED_MIN_SERIALIZED_SIZE.saturating_add(seed_len)
            {
                return 0;
            }
            let space_offset = SEED_LENGTH_OFFSET
                .wrapping_add(core::mem::size_of::<u64>())
                .wrapping_add(seed_len);
            read_u64_at_unchecked(instruction_data, space_offset)
        }
        _ => 0, // no allocation
    }
}

#[inline]
fn read_u64_at_unchecked(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        data[offset..offset + core::mem::size_of::<u64>()]
            .try_into()
            .unwrap(),
    )
}

#[inline]
fn read_discriminant(instruction_data: &[u8]) -> u32 {
    if instruction_data.len() < 4 {
        return 0;
    }
    u32::from_le_bytes(instruction_data[0..4].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use {super::*, solana_pubkey::Pubkey, solana_sdk::system_instruction};

    #[cfg(feature = "frozen-abi")]
    #[test]
    fn test_abi_for_parse_system_instruction_allocation() {
        use {
            solana_frozen_abi::{
                abi_digester::AbiDigester,
                abi_example::{AbiEnumVisitor, AbiExample},
            },
            solana_sdk::{bs58, system_instruction::SystemInstruction},
        };
        let mut digester = AbiDigester::create();
        let example = SystemInstruction::example();
        <_>::visit_for_abi(&&example, &mut digester).unwrap();
        let hash = digester.finalize().0;

        // COPIED FROM `SystemInstruction`
        let mut expected_hash = [0; 32];
        bs58::decode("2LnVTnJg7LxB1FawNZLoQEY8yiYx3MT3paTdx4s5kAXU")
            .onto(&mut expected_hash)
            .unwrap();

        assert_eq!(hash, expected_hash, "If this test breaks, the `parse_system_instruction_allocation` function may need to be updated");
    }

    #[test]
    fn test_discriminants() {
        let instruction = system_instruction::create_account(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            1,
            1,
            &Pubkey::new_unique(),
        );
        assert_eq!(read_discriminant(&instruction.data), CREATE_ACCOUNT);

        let instruction = system_instruction::create_account_with_seed(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            "seed",
            1,
            1,
            &Pubkey::new_unique(),
        );
        assert_eq!(
            read_discriminant(&instruction.data),
            CREATE_ACCOUNT_WITH_SEED
        );

        let instruction = system_instruction::allocate(&Pubkey::new_unique(), 1);
        assert_eq!(read_discriminant(&instruction.data), ALLOCATE);

        let instruction = system_instruction::allocate_with_seed(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            "seed",
            1,
            &Pubkey::new_unique(),
        );
        assert_eq!(read_discriminant(&instruction.data), ALLOCATE_WITH_SEED);
    }

    #[test]
    fn test_transfer() {
        let instruction =
            system_instruction::transfer(&Pubkey::new_unique(), &Pubkey::new_unique(), 1);
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 0);
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
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 0);
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
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 0);
    }

    #[test]
    fn test_allocate() {
        let mut instruction = system_instruction::allocate(&Pubkey::new_unique(), 57);
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 0);
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
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        instruction.data.push(0); // trailing byte is okay
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 57);
        // remaining data must be valid even if we have enough to read `space`
        instruction.data.truncate(instruction.data.len() - 2);
        assert_eq!(parse_system_instruction_allocation(&instruction.data), 0);
    }
}
