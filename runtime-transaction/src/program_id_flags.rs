use {
    solana_pubkey::Pubkey,
    solana_sdk::{compute_budget, ed25519_program, secp256k1_program},
};

/// Top-level program id flags.
/// The main checking point is done with this enum, and for individual
/// processors, the flags are converted to specific sub-enums.
pub enum ProgramIdFlag {
    NoMatch,
    Secp256k1,
    Ed25519,
    ComputeBudgetProgram,
}

/// Translate from a [Pubkey] to a [ProgramIdFlag].
impl From<&Pubkey> for ProgramIdFlag {
    #[inline]
    fn from(program_id: &Pubkey) -> Self {
        if secp256k1_program::check_id(program_id) {
            Self::Secp256k1
        } else if ed25519_program::check_id(program_id) {
            Self::Ed25519
        } else if compute_budget::check_id(program_id) {
            Self::ComputeBudgetProgram
        } else {
            Self::NoMatch
        }
    }
}

/// Program flag for determining how to process instructions for finding the
/// signature details.
pub enum SignatureDetailsFlag {
    NoMatch,
    Secp256k1,
    Ed25519,
}

impl From<&ProgramIdFlag> for SignatureDetailsFlag {
    #[inline]
    fn from(flag: &ProgramIdFlag) -> Self {
        match flag {
            ProgramIdFlag::Secp256k1 => Self::Secp256k1,
            ProgramIdFlag::Ed25519 => Self::Ed25519,
            _ => Self::NoMatch,
        }
    }
}

/// Program flag for determining how to process instructions for finding the
/// compute budget.
pub enum ComputeBudgetFlag {
    NoMatch,
    ComputeBudgetProgram,
}

impl From<&ProgramIdFlag> for ComputeBudgetFlag {
    #[inline]
    fn from(flag: &ProgramIdFlag) -> Self {
        match flag {
            ProgramIdFlag::ComputeBudgetProgram => Self::ComputeBudgetProgram,
            _ => Self::NoMatch,
        }
    }
}
