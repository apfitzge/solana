use {
    core::fmt::Debug,
    solana_sdk::{
        feature_set::FeatureSet,
        hash::Hash,
        instruction::CompiledInstruction,
        message::{AccountKeys, SanitizedMessage, TransactionSignatureDetails},
        precompiles::{get_precompiles, is_precompile},
        pubkey::Pubkey,
        signature::Signature,
        sysvar::instructions::{BorrowedAccountMeta, BorrowedInstruction},
        transaction::{
            SanitizedTransaction, TransactionAccountLocks, TransactionError, VersionedTransaction,
        },
    },
};

// - Debug to support legacy logging
pub trait Message: Debug {
    /// Return the number of signatures in the message.
    fn num_signatures(&self) -> u64;

    /// Return the number of writeable accounts in the message.
    fn num_write_locks(&self) -> u64;

    /// Return the recent blockhash.
    fn recent_blockhash(&self) -> &Hash;

    /// Return the number of instructions in the message.
    fn num_instructions(&self) -> usize;

    /// Return an iterator over the instructions in the message.
    fn instructions_iter(&self) -> impl Iterator<Item = Instruction>;

    /// Return an iterator over the instructions in the message, paired with
    /// the pubkey of the program.
    fn program_instructions_iter(&self) -> impl Iterator<Item = (&Pubkey, Instruction)>;

    /// Return the account keys.
    fn account_keys(&self) -> AccountKeys;

    /// Return the fee-payer
    fn fee_payer(&self) -> &Pubkey;

    /// Returns `true` if the account at `index` is writable.
    fn is_writable(&self, index: usize) -> bool;

    /// Returns `true` if the account at `index` is signer.
    fn is_signer(&self, index: usize) -> bool;

    /// Returns true if the account at the specified index is invoked as a
    /// program in this message.
    fn is_invoked(&self, key_index: usize) -> bool;

    /// Returns `true` if the account at `index` is not a loader key.
    fn is_non_loader_key(&self, index: usize) -> bool;

    /// Return signature details.
    fn get_signature_details(&self) -> TransactionSignatureDetails;

    /// Return the durable nonce for the message if it exists
    fn get_durable_nonce(&self) -> Option<&Pubkey>;

    /// Return the signers for the instruction at the given index.
    fn get_ix_signers(&self, index: usize) -> impl Iterator<Item = &Pubkey>;

    /// Checks for duplicate accounts in the message
    fn has_duplicates(&self) -> bool;

    /// Decompile message instructions without cloning account keys
    /// TODO: Remove this - there's an allocation!
    fn decompile_instructions(&self) -> Vec<BorrowedInstruction> {
        let account_keys = self.account_keys();
        self.program_instructions_iter()
            .map(|(program_id, instruction)| {
                let accounts = instruction
                    .accounts
                    .iter()
                    .map(|account_index| {
                        let account_index = *account_index as usize;
                        BorrowedAccountMeta {
                            is_signer: self.is_signer(account_index),
                            is_writable: self.is_writable(account_index),
                            pubkey: account_keys.get(account_index).unwrap(),
                        }
                    })
                    .collect();

                BorrowedInstruction {
                    accounts,
                    data: instruction.data,
                    program_id,
                }
            })
            .collect()
    }

    /// Validate a transaction message against locked accounts
    fn validate_account_locks(&self, tx_account_lock_limit: usize) -> Result<(), TransactionError> {
        if self.has_duplicates() {
            Err(TransactionError::AccountLoadedTwice)
        } else if self.account_keys().len() > tx_account_lock_limit {
            Err(TransactionError::TooManyAccountLocks)
        } else {
            Ok(())
        }
    }

    /// Get the number of lookup tables.
    fn num_lookup_tables(&self) -> usize;

    /// Get message address table lookups used in the message
    fn message_address_table_lookups(&self) -> impl Iterator<Item = MessageAddressTableLookup>;

    /// Verify precompiles in the message
    fn verify_precompiles(&self, feature_set: &FeatureSet) -> Result<(), TransactionError> {
        let is_enabled = |feature_id: &Pubkey| feature_set.is_active(feature_id);
        let has_precompiles = self
            .program_instructions_iter()
            .any(|(program_id, _)| is_precompile(program_id, is_enabled));

        if has_precompiles {
            let instructions_data: Vec<_> = self
                .instructions_iter()
                .map(|instruction| instruction.data)
                .collect();
            for (program_id, instruction) in self.program_instructions_iter() {
                if let Some(precompile) = get_precompiles()
                    .iter()
                    .find(|precompile| precompile.check_id(program_id, is_enabled))
                {
                    precompile
                        .verify(instruction.data, &instructions_data, feature_set)
                        .map_err(|_| TransactionError::InvalidAccountIndex)?;
                }
            }
        }
        Ok(())
    }
}

pub trait SignedMessage: Message {
    /// Get the first signature of the message.
    fn signature(&self) -> &Signature;

    /// Get all the signatures of the message.
    fn signatures(&self) -> &[Signature];

    /// Returns the message hash.
    // TODO: consider moving this to Message
    fn message_hash(&self) -> &Hash;

    /// Returns true if the transaction is a simple vote transaction.
    // TODO: consider moving this to Message
    fn is_simple_vote_transaction(&self) -> bool;

    /// Validate and return the account keys locked by this transaction
    // TODO: Change return type so it has no allocation.
    // TODO: consider moving this to Message
    fn get_account_locks(
        &self,
        tx_account_lock_limit: usize,
    ) -> Result<TransactionAccountLocks, TransactionError> {
        self.validate_account_locks(tx_account_lock_limit)?;
        Ok(self.get_account_locks_unchecked())
    }

    /// Return the account keys locked by this transaction without validation
    fn get_account_locks_unchecked(&self) -> TransactionAccountLocks;

    /// Make a versioned transaction copy of the transaction.
    // TODO: get rid of this.
    fn to_versioned_transaction(&self) -> VersionedTransaction;
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

/// A non-owning version of [`MessageAddressTableLookup`] that references
/// the account key and indexes used to load writable and readonly accounts
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MessageAddressTableLookup<'a> {
    /// Address lookup table account key
    pub account_key: &'a Pubkey,
    /// List of indexes used to load writable account addresses
    pub writable_indexes: &'a [u8],
    /// List of indexes used to load readonly account addresses
    pub readonly_indexes: &'a [u8],
}

// Implement for the "reference" `SanitizedMessage` type.
impl Message for SanitizedMessage {
    fn num_signatures(&self) -> u64 {
        SanitizedMessage::num_signatures(self)
    }

    fn num_write_locks(&self) -> u64 {
        SanitizedMessage::num_write_locks(self)
    }

    fn recent_blockhash(&self) -> &Hash {
        SanitizedMessage::recent_blockhash(self)
    }

    fn num_instructions(&self) -> usize {
        self.instructions().len()
    }

    fn instructions_iter(&self) -> impl Iterator<Item = Instruction> {
        self.instructions().iter().map(Instruction::from)
    }

    fn program_instructions_iter(&self) -> impl Iterator<Item = (&Pubkey, Instruction)> {
        SanitizedMessage::program_instructions_iter(self)
            .map(|(pubkey, ix)| (pubkey, Instruction::from(ix)))
    }

    fn account_keys(&self) -> AccountKeys {
        SanitizedMessage::account_keys(self)
    }

    fn fee_payer(&self) -> &Pubkey {
        SanitizedMessage::fee_payer(self)
    }

    fn is_writable(&self, index: usize) -> bool {
        SanitizedMessage::is_writable(self, index)
    }

    fn is_signer(&self, index: usize) -> bool {
        SanitizedMessage::is_signer(self, index)
    }

    fn is_invoked(&self, key_index: usize) -> bool {
        SanitizedMessage::is_invoked(self, key_index)
    }

    fn is_non_loader_key(&self, index: usize) -> bool {
        SanitizedMessage::is_non_loader_key(self, index)
    }

    fn get_signature_details(&self) -> TransactionSignatureDetails {
        SanitizedMessage::get_signature_details(self)
    }

    fn get_durable_nonce(&self) -> Option<&Pubkey> {
        SanitizedMessage::get_durable_nonce(self)
    }

    fn get_ix_signers(&self, index: usize) -> impl Iterator<Item = &Pubkey> {
        SanitizedMessage::get_ix_signers(self, index)
    }

    fn has_duplicates(&self) -> bool {
        SanitizedMessage::has_duplicates(self)
    }

    fn num_lookup_tables(&self) -> usize {
        SanitizedMessage::message_address_table_lookups(self).len()
    }

    fn message_address_table_lookups(&self) -> impl Iterator<Item = MessageAddressTableLookup> {
        SanitizedMessage::message_address_table_lookups(self)
            .iter()
            .map(|lookup| MessageAddressTableLookup {
                account_key: &lookup.account_key,
                writable_indexes: lookup.writable_indexes.as_slice(),
                readonly_indexes: lookup.readonly_indexes.as_slice(),
            })
    }
}

impl Message for SanitizedTransaction {
    fn num_signatures(&self) -> u64 {
        Message::num_signatures(self.message())
    }

    fn num_write_locks(&self) -> u64 {
        Message::num_write_locks(self.message())
    }

    fn recent_blockhash(&self) -> &Hash {
        Message::recent_blockhash(self.message())
    }

    fn num_instructions(&self) -> usize {
        Message::num_instructions(self.message())
    }

    fn instructions_iter(&self) -> impl Iterator<Item = Instruction> {
        Message::instructions_iter(self.message())
    }

    fn program_instructions_iter(&self) -> impl Iterator<Item = (&Pubkey, Instruction)> {
        Message::program_instructions_iter(self.message())
    }

    fn account_keys(&self) -> AccountKeys {
        Message::account_keys(self.message())
    }

    fn fee_payer(&self) -> &Pubkey {
        Message::fee_payer(self.message())
    }

    fn is_writable(&self, index: usize) -> bool {
        Message::is_writable(self.message(), index)
    }

    fn is_signer(&self, index: usize) -> bool {
        Message::is_signer(self.message(), index)
    }

    fn is_invoked(&self, key_index: usize) -> bool {
        Message::is_invoked(self.message(), key_index)
    }

    /// Returns `true` if the account at `index` is not a loader key.
    fn is_non_loader_key(&self, index: usize) -> bool {
        Message::is_non_loader_key(self.message(), index)
    }

    fn get_signature_details(&self) -> TransactionSignatureDetails {
        Message::get_signature_details(self.message())
    }

    fn get_durable_nonce(&self) -> Option<&Pubkey> {
        Message::get_durable_nonce(self.message())
    }

    fn get_ix_signers(&self, index: usize) -> impl Iterator<Item = &Pubkey> {
        Message::get_ix_signers(self.message(), index)
    }

    fn has_duplicates(&self) -> bool {
        Message::has_duplicates(self.message())
    }

    fn num_lookup_tables(&self) -> usize {
        Message::num_lookup_tables(self.message())
    }

    fn message_address_table_lookups(&self) -> impl Iterator<Item = MessageAddressTableLookup> {
        Message::message_address_table_lookups(self.message())
    }
}

impl SignedMessage for SanitizedTransaction {
    fn signature(&self) -> &Signature {
        self.signatures().first().unwrap()
    }

    fn signatures(&self) -> &[Signature] {
        self.signatures()
    }

    fn message_hash(&self) -> &Hash {
        self.message_hash()
    }

    fn is_simple_vote_transaction(&self) -> bool {
        self.is_simple_vote_transaction()
    }

    fn get_account_locks_unchecked(&self) -> TransactionAccountLocks {
        SanitizedTransaction::get_account_locks_unchecked(self)
    }

    fn to_versioned_transaction(&self) -> VersionedTransaction {
        SanitizedTransaction::to_versioned_transaction(self)
    }
}

impl<'a> From<&'a CompiledInstruction> for Instruction<'a> {
    fn from(ix: &'a CompiledInstruction) -> Self {
        Self {
            program_id_index: ix.program_id_index,
            accounts: ix.accounts.as_slice(),
            data: ix.data.as_slice(),
        }
    }
}
