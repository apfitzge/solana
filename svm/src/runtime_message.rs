use solana_sdk::{message::SanitizedMessage, pubkey::Pubkey};

pub trait RuntimeMessage {
    /// Return the number of instructions in the message.
    fn num_instructions(&self) -> usize;

    /// Return an iterator over the instructions in the message.
    fn instructions_iter(&self) -> impl Iterator<Item = Instruction>;

    /// Return an iterator over the instructions in the message, paired with
    /// the pubkey of the program.
    fn program_instructions_iter(&self) -> impl Iterator<Item = (&Pubkey, Instruction)>;

    /// Returns `true` if the account at `index` is writable.
    fn is_writable(&self, index: usize) -> bool;

    /// Returns `true` if the account at `index` is signer.
    fn is_signer(&self, index: usize) -> bool;
}

/// A non-owning version of [`CompiledInstruction`] that references
/// slices of account indexes and data
///
/// [`Message`]: crate::message::Message
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Instruction<'a> {
    /// Index into the transaction keys array indicating the program account that executes this instruction.
    pub program_id_index: u8,
    /// Ordered indices into the transaction keys array indicating which accounts to pass to the program.
    pub accounts: &'a [u8],
    /// The program input data.
    pub data: &'a [u8],
}

// Implement for the "reference" `SanitizedMessage` type.
impl RuntimeMessage for SanitizedMessage {
    fn num_instructions(&self) -> usize {
        self.instructions().len()
    }

    fn instructions_iter(&self) -> impl Iterator<Item = Instruction> {
        self.instructions()
            .iter()
            .map(|compiled_instruction| Instruction {
                program_id_index: compiled_instruction.program_id_index,
                accounts: &compiled_instruction.accounts,
                data: &compiled_instruction.data,
            })
    }

    fn program_instructions_iter(&self) -> impl Iterator<Item = (&Pubkey, Instruction)> {
        self.instructions_iter().map(|instruction| {
            let program_id = self
                .account_keys()
                .get(instruction.program_id_index as usize)
                .expect("message is sanitized");
            (program_id, instruction)
        })
    }

    fn is_writable(&self, index: usize) -> bool {
        SanitizedMessage::is_writable(self, index)
    }

    fn is_signer(&self, index: usize) -> bool {
        SanitizedMessage::is_signer(self, index)
    }
}
