use {
    solana_sdk::{
        hash::Hash,
        message::{AccountKeys, MESSAGE_VERSION_PREFIX},
        packet::Packet,
        pubkey::Pubkey,
        sanitize::SanitizeError,
        short_vec::decode_shortu16_len,
        signature::Signature,
    },
    solana_signed_message::{Instruction, Message, MessageAddressTableLookup},
};

const MAX_TRASACTION_SIZE: usize = 4096; // not sure this is actually true

#[derive(Clone, Debug)]
#[repr(u8)]
pub enum TransactionVersion {
    Legacy = u8::MAX,
    V0 = 0,
}

#[derive(Clone, Debug)]
pub enum TransactionStatus {
    Uninitialized,
    Raw,
    Sanitized,
    AddressResolved,
}

/// Stores the raw packet and information about the transaction
/// that is stored in this packet.
/// This importantly, does not do any heap allocations, and all
/// signature, pubkey, hash, heap, etc information is stored
/// in the packet itself.
/// This view struct allows calling code to access information
/// about the transaction without needing to do slow deserialization.
#[derive(Clone, Debug)]
pub struct TransactionView {
    status: TransactionStatus,

    buffer: [u8; MAX_TRASACTION_SIZE],
    /// The packet length.
    packet_len: u16,
    /// The number of signatures.
    signature_len: u16,
    /// Offset of signature in the packet.
    signature_offset: u16,
    /// The number of signatures required for this message to be considered
    /// valid.
    num_required_signatures: u8,
    /// The last `num_readonly_signed_accounts` of the signed keys are read-only.
    num_readonly_signed_accounts: u8,
    /// The last `num_readonly_unsigned_accounts` of the unsigned keys are
    /// read-only accounts.
    num_readonly_unsigned_accounts: u8,

    /// Version of the transaction.
    version: TransactionVersion,
    /// Offset of the message in the packet
    message_offset: u16,

    /// Length of the accounts slice in the packet.
    static_accounts_len: u16,
    /// Offset of the accounts slice in the packet.
    static_accounts_offset: u16,

    /// Offset of the recent blockhash in the packet.
    recent_blockhash_offset: u16,

    /// Length of the instructions in the packet.
    instructions_len: u16,
    /// Offset of the instructions in the packet.
    /// This is **not** a slice, as the instruction size
    /// is not known.
    instructions_offset: u16,

    /// Length of the address lookup entries in the packet.
    address_lookups_len: u16,
    /// Offset of the address lookup entries in the packet.
    /// This is **not** a slice, as the entry size is not known.
    address_lookups_offset: u16,
}

impl Default for TransactionView {
    fn default() -> Self {
        Self {
            status: TransactionStatus::Uninitialized,
            buffer: [0; MAX_TRASACTION_SIZE],
            packet_len: 0,
            signature_len: 0,
            signature_offset: 0,
            num_required_signatures: 0,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
            version: TransactionVersion::Legacy,
            message_offset: 0,
            static_accounts_len: 0,
            static_accounts_offset: 0,
            recent_blockhash_offset: 0,
            instructions_len: 0,
            instructions_offset: 0,
            address_lookups_len: 0,
            address_lookups_offset: 0,
        }
    }
}

impl TransactionView {
    /// Return None if the packet is not a transaction.
    pub fn populate_from(&mut self, packet: &Packet) -> Option<()> {
        // The "deserialization" strategy here only works if the internal types
        // of the transaction are aligned on the same boundaries as the packet
        // data. The packet data has no alignment guarantees, which means all
        // internal types must be byte-aligned. This is true for all types used
        // in the transaction, but for guarantees some static asserts should be
        // added.
        const _: () = assert!(core::mem::align_of::<u8>() == 1);
        const _: () = assert!(core::mem::align_of::<Signature>() == 1);
        const _: () = assert!(core::mem::align_of::<Pubkey>() == 1);
        const _: () = assert!(core::mem::align_of::<Hash>() == 1);
        // The above asserts are not necessary, but they are a good sanity
        // check. There is one type that is not byte-aligned, and that is
        // u16. However, because the encoding scheme uses a variable number
        // of bytes, to be converted as a u16, it is also always byte-aligned.

        // Copy the packet data into the buffer
        let packet_len = packet.meta().size;
        self.buffer[..packet_len].copy_from_slice(packet.data(..)?);
        let packet_slice = &self.buffer[..packet_len];

        // Signatures always start at the beginning of the packet.
        let mut offset = 0;
        let signature_len = read_compressed_u16(packet_slice, &mut offset)?;
        let signature_offset = u16::try_from(offset).ok()?;
        offset_array_len::<Signature>(packet_slice, &mut offset, signature_len)?;

        // Get the message offset
        let message_offset = u16::try_from(offset).ok()?;
        let message_prefix = read_byte(packet_slice, &mut offset)?;
        let (version, message_header_offset) = if message_prefix & MESSAGE_VERSION_PREFIX != 0 {
            let version = message_prefix & !MESSAGE_VERSION_PREFIX;
            match version {
                0 => (TransactionVersion::V0, message_offset.checked_add(1)?),
                _ => return None,
            }
        } else {
            (TransactionVersion::Legacy, message_offset)
        };

        // Offset should get reset to header offset - the byte we read may have actually
        // been part of the header instead of the prefix.
        offset = usize::from(message_header_offset);
        let num_required_signatures = read_byte(packet_slice, &mut offset)?;
        let num_readonly_signed_accounts = read_byte(packet_slice, &mut offset)?;
        let num_readonly_unsigned_accounts = read_byte(packet_slice, &mut offset)?;

        // Read the number of accounts, move to end of array.
        let static_accounts_len = read_compressed_u16(packet_slice, &mut offset)?;
        let static_accounts_offset = u16::try_from(offset).ok()?;
        offset_array_len::<Pubkey>(packet_slice, &mut offset, static_accounts_len)?;

        // Move to end of recent blockhash.
        let recent_blockhash_offset = u16::try_from(offset).ok()?;
        offset_type::<Hash>(packet_slice, &mut offset)?;

        // Read the number of instructions. Cannot just move to end of array
        // since the instruction size is not fixed.
        let instructions_len = read_compressed_u16(packet_slice, &mut offset)?;
        let instructions_offset = u16::try_from(offset).ok()?;

        // The instructions do not have a fixed size, so we actually must iterate over them.
        for _ in 0..instructions_len {
            // u8 for program index
            read_byte(packet_slice, &mut offset)?;
            // u16 for accounts len
            let accounts_indexes_len = read_compressed_u16(packet_slice, &mut offset)?;
            offset_array_len::<u8>(packet_slice, &mut offset, accounts_indexes_len)?;
            // u16 for data len
            let data_len = read_compressed_u16(packet_slice, &mut offset)?;
            offset = offset.checked_add(usize::from(data_len))?;
        }

        // If the transaction is a V0 transaction, there may be address lookups
        let (address_lookups_len, address_lookups_offset) = match version {
            TransactionVersion::Legacy => (0, 0),
            TransactionVersion::V0 => {
                // After the instructions, there are address lookup entries.
                // We must iterate over these as well since the size is not fixed.
                let address_lookups_len = read_compressed_u16(packet_slice, &mut offset)?;
                let address_lookups_offset = u16::try_from(offset).ok()?;

                for _ in 0..address_lookups_len {
                    // Pubkey for address
                    offset_type::<Pubkey>(packet_slice, &mut offset)?;
                    // u16 for length of writable_indexes
                    let writable_indexes_len = read_compressed_u16(packet_slice, &mut offset)?;
                    offset_array_len::<u8>(packet_slice, &mut offset, writable_indexes_len)?;
                    // u16 for length of readonly_indexes
                    let readonly_indexes_len = read_compressed_u16(packet_slice, &mut offset)?;
                    offset_array_len::<u8>(packet_slice, &mut offset, readonly_indexes_len)?;
                }

                (address_lookups_len, address_lookups_offset)
            }
        };

        // Check there is no remaining data in the packet
        if offset != packet_len {
            return None;
        }
        let packet_len = u16::try_from(packet_len).ok()?;

        // Assign fields
        self.status = TransactionStatus::Raw;
        self.packet_len = packet_len;
        self.signature_len = signature_len;
        self.signature_offset = signature_offset;
        self.num_required_signatures = num_required_signatures;
        self.num_readonly_signed_accounts = num_readonly_signed_accounts;
        self.num_readonly_unsigned_accounts = num_readonly_unsigned_accounts;
        self.version = version;
        self.message_offset = message_offset;
        self.static_accounts_offset = static_accounts_offset;
        self.static_accounts_len = static_accounts_len;
        self.recent_blockhash_offset = recent_blockhash_offset;
        self.instructions_offset = instructions_offset;
        self.instructions_len = instructions_len;
        self.address_lookups_offset = address_lookups_offset;
        self.address_lookups_len = address_lookups_len;

        Some(())
    }

    pub fn try_new(packet: &Packet) -> Option<Self> {
        let mut transaction_view = Self::default();
        transaction_view.populate_from(packet)?;
        Some(transaction_view)
    }

    pub fn signatures(&self) -> &[Signature] {
        let mut offset = usize::from(self.signature_offset);
        read_array::<Signature>(
            &self.buffer[..usize::from(self.packet_len)],
            &mut offset,
            self.signature_len,
        )
        .expect("signatures verified in construction")
    }

    pub fn recent_blockhash(&self) -> &Hash {
        unsafe {
            &*(&self.buffer[self.recent_blockhash_offset as usize] as *const _ as *const Hash)
        }
    }

    pub fn static_account_keys(&self) -> &[Pubkey] {
        let mut offset = usize::from(self.static_accounts_offset);
        read_array::<Pubkey>(
            &self.buffer[..usize::from(self.packet_len)],
            &mut offset,
            self.static_accounts_len,
        )
        .expect("static account keys verified in construction")
    }

    pub fn instructions(&self) -> impl Iterator<Item = Instruction> {
        InstructionIterator {
            buffer: &self.buffer[..usize::from(self.packet_len)], // all instructions are within original packet
            current_offset: self.instructions_offset as usize,
            instruction_count: usize::from(self.instructions_len),
            current_count: 0,
        }
    }

    pub fn address_lookups(&self) -> impl Iterator<Item = MessageAddressTableLookup> {
        MessageAddressTableLookupIterator {
            buffer: &self.buffer[..usize::from(self.packet_len)], // all address lookups are within original packet
            current_offset: self.address_lookups_offset as usize,
            address_lookup_count: usize::from(self.address_lookups_len),
            current_count: 0,
        }
    }

    pub fn sanitize(&mut self) -> Result<(), SanitizeError> {
        self.sanitize_signatures()?;
        self.sanitize_message()?;

        self.status = TransactionStatus::Sanitized;
        Ok(())
    }

    fn sanitize_signatures(&self) -> Result<(), SanitizeError> {
        let num_required_signatures = usize::from(self.num_required_signatures);
        match num_required_signatures.cmp(&usize::from(self.signature_len)) {
            core::cmp::Ordering::Greater => Err(SanitizeError::IndexOutOfBounds),
            core::cmp::Ordering::Less => Err(SanitizeError::InvalidValue),
            core::cmp::Ordering::Equal => Ok(()),
        }?;

        // Signatures are verified before message keys are loaded so all signers
        // must correspond to static account keys.
        if self.signature_len > self.static_accounts_len {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        Ok(())
    }

    fn sanitize_message(&self) -> Result<(), SanitizeError> {
        if usize::from(self.num_required_signatures)
            .saturating_add(usize::from(self.num_readonly_unsigned_accounts))
            > usize::from(self.static_accounts_len)
        {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        // there should be at least 1 RW fee-payer account.
        if self.num_readonly_signed_accounts >= self.num_required_signatures {
            return Err(SanitizeError::InvalidValue);
        }

        let num_dynamic_account_keys = {
            let mut total_lookup_keys: usize = 0;
            for lookup in self.address_lookups() {
                let num_lookup_indexes = lookup
                    .writable_indexes
                    .len()
                    .saturating_add(lookup.readonly_indexes.len());

                // each lookup table must be used to load at least one account
                if num_lookup_indexes == 0 {
                    return Err(SanitizeError::InvalidValue);
                }

                total_lookup_keys = total_lookup_keys.saturating_add(num_lookup_indexes);
            }
            total_lookup_keys
        };

        // this is redundant with the above sanitization checks which require that:
        // 1) the header describes at least 1 RW account
        // 2) the header doesn't describe more account keys than the number of account keys
        if self.static_accounts_len == 0 {
            return Err(SanitizeError::InvalidValue);
        }

        // the combined number of static and dynamic account keys must be <= 256
        // since account indices are encoded as `u8`
        // Note that this is different from the per-transaction account load cap
        // as defined in `Bank::get_transaction_account_lock_limit`
        let total_account_keys =
            usize::from(self.static_accounts_len).saturating_add(num_dynamic_account_keys);
        if total_account_keys > 256 {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        // `expect` is safe because of earlier check that
        // `num_static_account_keys` is non-zero
        let max_account_ix = total_account_keys
            .checked_sub(1)
            .expect("message doesn't contain any account keys");

        // reject program ids loaded from lookup tables so that
        // static analysis on program instructions can be performed
        // without loading on-chain data from a bank
        let max_program_id_ix =
            // `expect` is safe because of earlier check that
            // `num_static_account_keys` is non-zero
            usize::from(self.static_accounts_len)
                .checked_sub(1)
                .expect("message doesn't contain any static account keys");

        for instruction in self.instructions() {
            if usize::from(instruction.program_id_index) > max_program_id_ix {
                return Err(SanitizeError::IndexOutOfBounds);
            }
            // A program cannot be a payer.
            if instruction.program_id_index == 0 {
                return Err(SanitizeError::IndexOutOfBounds);
            }
            for account_index in instruction.accounts {
                if usize::from(*account_index) > max_account_ix {
                    return Err(SanitizeError::IndexOutOfBounds);
                }
            }
        }

        Ok(())
    }
}

impl Message for TransactionView {
    fn num_signatures(&self) -> u64 {
        u64::from(self.num_required_signatures)
    }

    fn num_write_locks(&self) -> u64 {
        todo!()
    }

    fn recent_blockhash(&self) -> &Hash {
        TransactionView::recent_blockhash(self)
    }

    fn num_instructions(&self) -> usize {
        usize::from(self.instructions_len)
    }

    fn instructions_iter(&self) -> impl Iterator<Item = Instruction> {
        TransactionView::instructions(self)
    }

    fn program_instructions_iter(
        &self,
    ) -> impl Iterator<Item = (&Pubkey, solana_signed_message::Instruction)> {
        let accounts = self.static_account_keys();
        Message::instructions_iter(self).map(move |instruction| {
            (
                &accounts[usize::from(instruction.program_id_index)],
                instruction,
            )
        })
    }

    fn account_keys(&self) -> AccountKeys {
        AccountKeys {
            static_keys: TransactionView::static_account_keys(self),
            dynamic_keys: todo!(),
        }
    }

    fn fee_payer(&self) -> &Pubkey {
        &TransactionView::static_account_keys(self)[0]
    }

    fn is_writable(&self, index: usize) -> bool {
        todo!()
    }

    fn is_signer(&self, index: usize) -> bool {
        todo!()
    }

    fn is_invoked(&self, key_index: usize) -> bool {
        todo!()
    }

    fn is_non_loader_key(&self, index: usize) -> bool {
        todo!()
    }

    fn get_signature_details(&self) -> solana_sdk::message::TransactionSignatureDetails {
        todo!()
    }

    fn get_durable_nonce(&self) -> Option<&Pubkey> {
        todo!()
    }

    fn get_ix_signers(&self, index: usize) -> impl Iterator<Item = &Pubkey> {
        todo!()
    }

    fn has_duplicates(&self) -> bool {
        todo!()
    }

    fn num_lookup_tables(&self) -> usize {
        usize::from(self.address_lookups_len)
    }

    fn message_address_table_lookups(&self) -> impl Iterator<Item = MessageAddressTableLookup> {
        TransactionView::address_lookups(self)
    }
}

struct InstructionIterator<'a> {
    buffer: &'a [u8],
    current_offset: usize,
    instruction_count: usize,
    current_count: usize,
}

impl<'a> Iterator for InstructionIterator<'a> {
    type Item = Instruction<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_count >= self.instruction_count {
            return None;
        }

        // u8 for program index
        let program_id_index = read_byte(self.buffer, &mut self.current_offset)?;
        // u16 for accounts len
        let accounts_indexes_len = read_compressed_u16(self.buffer, &mut self.current_offset)?;
        let accounts =
            read_array::<u8>(self.buffer, &mut self.current_offset, accounts_indexes_len)?;
        // u16 for data len
        let data_len = read_compressed_u16(self.buffer, &mut self.current_offset)?;
        let data = read_array::<u8>(self.buffer, &mut self.current_offset, data_len)?;

        self.current_count += 1;

        Some(Instruction {
            program_id_index,
            accounts,
            data,
        })
    }
}

struct MessageAddressTableLookupIterator<'a> {
    buffer: &'a [u8],
    current_offset: usize,
    address_lookup_count: usize,
    current_count: usize,
}

impl<'a> Iterator for MessageAddressTableLookupIterator<'a> {
    type Item = MessageAddressTableLookup<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_count >= self.address_lookup_count {
            return None;
        }

        // Pubkey for address
        let account_key = read_type::<Pubkey>(self.buffer, &mut self.current_offset)?;
        // u16 for length of writable_indexes
        let writable_indexes_len = read_compressed_u16(self.buffer, &mut self.current_offset)?;
        let writable_indexes =
            read_array::<u8>(self.buffer, &mut self.current_offset, writable_indexes_len)?;
        // u16 for length of readonly_indexes
        let readonly_indexes_len = read_compressed_u16(self.buffer, &mut self.current_offset)?;
        let readonly_indexes =
            read_array::<u8>(self.buffer, &mut self.current_offset, readonly_indexes_len)?;

        self.current_count += 1;

        Some(MessageAddressTableLookup {
            account_key,
            writable_indexes,
            readonly_indexes,
        })
    }
}

#[inline(always)]
fn read_byte(buffer: &[u8], offset: &mut usize) -> Option<u8> {
    if *offset < buffer.len() {
        let value = buffer[*offset];
        *offset = offset.checked_add(1)?;
        Some(value)
    } else {
        None
    }
}

#[inline(always)]
fn read_compressed_u16(buffer: &[u8], offset: &mut usize) -> Option<u16> {
    if *offset >= buffer.len() {
        return None;
    }
    let (value, bytes) = decode_shortu16_len(&buffer[*offset..]).ok()?;
    *offset += bytes;
    u16::try_from(value).ok()
}

#[inline(always)]
fn read_type<'a, T: Sized>(buffer: &'a [u8], offset: &mut usize) -> Option<&'a T> {
    if *offset + core::mem::size_of::<T>() > buffer.len() {
        return None;
    }
    let value = unsafe { &*(buffer.as_ptr().add(*offset) as *const T) };
    *offset += core::mem::size_of::<T>();
    Some(value)
}

#[inline(always)]
fn offset_type<T: Sized>(buffer: &[u8], offset: &mut usize) -> Option<()> {
    *offset = offset.checked_add(core::mem::size_of::<T>())?;
    (*offset <= buffer.len()).then_some(())
}

#[inline(always)]
fn read_array<'a, T: Sized>(buffer: &'a [u8], offset: &mut usize, len: u16) -> Option<&'a [T]> {
    if *offset + usize::from(len).checked_mul(core::mem::size_of::<T>())? > buffer.len() {
        return None;
    }
    let value =
        &buffer[*offset..*offset + usize::from(len).checked_mul(core::mem::size_of::<T>())?];
    *offset += usize::from(len).checked_mul(core::mem::size_of::<T>())?;

    let slice =
        unsafe { core::slice::from_raw_parts(value.as_ptr() as *const T, usize::from(len)) };

    Some(slice)
}

#[inline(always)]
fn offset_array_len<T: Sized>(buffer: &[u8], offset: &mut usize, len: u16) -> Option<()> {
    *offset = offset.checked_add(usize::from(len).checked_mul(core::mem::size_of::<T>())?)?;
    // offset must be within bounds, allowed to be at end
    (*offset <= buffer.len()).then_some(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            message::Message,
            signature::Keypair,
            signer::Signer,
            system_instruction, system_transaction,
            transaction::{Transaction, VersionedTransaction},
        },
    };

    fn compare_view_to_transaction(transaction: &VersionedTransaction, view: &TransactionView) {
        // Compare signatures
        assert_eq!(view.signatures(), transaction.signatures);

        // Compare static account keys
        assert_eq!(
            view.static_account_keys(),
            transaction.message.static_account_keys()
        );

        // Compare recent blockhash
        assert_eq!(
            view.recent_blockhash(),
            transaction.message.recent_blockhash()
        );

        // Compare instructions
        let mut instruction_view_iter = view.instructions();
        for instruction in transaction.message.instructions() {
            let instruction_view = instruction_view_iter.next().unwrap();
            assert_eq!(
                instruction.program_id_index,
                instruction_view.program_id_index
            );
            assert_eq!(instruction.accounts, instruction_view.accounts_indexes);
            assert_eq!(instruction.data, instruction_view.data);
        }
        assert!(instruction_view_iter.next().is_none());

        // Compare address lookup tables if they exist
        if let Some(address_lookup_tables) = transaction.message.address_table_lookups() {
            let mut address_lookup_iter = view.address_lookups();
            for address_lookup_table in address_lookup_tables {
                let address_lookup_view = address_lookup_iter.next().unwrap();
                assert_eq!(
                    address_lookup_table.account_key,
                    *address_lookup_view.account_key
                );
                assert_eq!(
                    address_lookup_table.writable_indexes,
                    address_lookup_view.writable_indexes
                );
                assert_eq!(
                    address_lookup_table.readonly_indexes,
                    address_lookup_view.readonly_indexes
                );
            }
            assert!(address_lookup_iter.next().is_none());
        } else {
            assert_eq!(view.address_lookups().count(), 0); // ensure this function is correct if not present
        }
    }

    #[test]
    fn test_transaction_view_simple() {
        let mut packet = Packet::default();
        assert!(TransactionView::try_new(&packet).is_none());

        let keypair = Keypair::new();
        let pubkey = Pubkey::new_unique();
        let recent_blockhash = Hash::new_unique();
        let transaction = system_transaction::transfer(&keypair, &pubkey, 1, recent_blockhash);
        let transaction = VersionedTransaction::from(transaction);

        packet.populate_packet(None, &transaction).unwrap();

        let mut transaction_view = TransactionView::try_new(&packet).unwrap();
        transaction_view.sanitize().unwrap();
        compare_view_to_transaction(&transaction, &transaction_view);
    }

    #[test]
    fn test_transaction_view_multiple_instructions() {
        let mut packet = Packet::default();
        assert!(TransactionView::try_new(&packet).is_none());

        let keypair = Keypair::new();
        let pubkey = Pubkey::new_unique();
        let recent_blockhash = Hash::new_unique();
        let ixs = vec![
            system_instruction::transfer(&keypair.pubkey(), &pubkey, 1),
            system_instruction::transfer(&keypair.pubkey(), &Pubkey::new_unique(), 1),
        ];

        let message = Message::new(&ixs, Some(&keypair.pubkey()));
        let transaction = Transaction::new(&[&keypair], message, recent_blockhash);
        let transaction = VersionedTransaction::from(transaction);

        packet.populate_packet(None, &transaction).unwrap();

        let mut transaction_view = TransactionView::try_new(&packet).unwrap();
        transaction_view.sanitize().unwrap();
        compare_view_to_transaction(&transaction, &transaction_view);
    }
}
