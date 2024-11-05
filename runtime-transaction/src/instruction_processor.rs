use {
    crate::program_id_flags::ProgramIdFlag,
    agave_transaction_view::static_account_keys_frame::MAX_STATIC_ACCOUNTS_PER_PACKET as FILTER_SIZE,
    core::borrow::Borrow, solana_pubkey::Pubkey, solana_sdk::transaction::TransactionError,
    solana_svm_transaction::instruction::SVMInstruction,
};

pub trait InstructionProcessor<T>: Default {
    type OUTPUT;

    /// Process instructions individually given a `flag` that is derived from
    /// the program id.
    fn process_instruction(
        &mut self,
        flag: &T,
        instruction_index: usize,
        instruction: &SVMInstruction,
    ) -> Result<(), TransactionError>;

    /// Finalize the processor and return the output.
    fn finalize(self) -> Self::OUTPUT;
}

pub struct CachedInstructionProcessor<T, P: InstructionProcessor<T>> {
    flags: [Option<T>; FILTER_SIZE as usize],
    processor: P,
}

impl<T, P> Default for CachedInstructionProcessor<T, P>
where
    P: InstructionProcessor<T> + Default,
{
    #[inline]
    fn default() -> Self {
        Self {
            flags: core::array::from_fn(|_| None),
            processor: P::default(),
        }
    }
}

impl<T, P> CachedInstructionProcessor<T, P>
where
    T: for<'k> From<&'k ProgramIdFlag>,
    P: InstructionProcessor<T>,
{
    #[inline]
    pub fn process_instructions<'b>(
        mut self,
        instructions: impl Iterator<Item = (&'b Pubkey, SVMInstruction<'b>)>,
    ) -> Result<P::OUTPUT, TransactionError> {
        for (instruction_index, (program_id, instruction)) in instructions.enumerate() {
            let flag = self.flags[usize::from(instruction.program_id_index)]
                .get_or_insert_with(|| T::from(&ProgramIdFlag::from(program_id)));

            self.processor
                .process_instruction(flag.borrow(), instruction_index, &instruction)?;
        }

        Ok(self.processor.finalize())
    }
}
