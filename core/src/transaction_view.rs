use solana_sdk::{
    hash::Hash, message::MESSAGE_VERSION_PREFIX, packet::Packet, pubkey::Pubkey,
    short_vec::decode_shortu16_len, signature::Signature,
};

const MAX_TRASACTION_SIZE: usize = 4096; // not sure this is actually true

#[repr(u8)]
pub enum TransactionVersion {
    Legacy = u8::MAX,
    V0 = 0,
}

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

impl TransactionView {
    pub fn new() -> Self {
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

    /// Return None if the packet is not a transaction.
    pub fn populate_from(&mut self, packet: &Packet) -> Option<()> {
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

        // Assign fields
        self.status = TransactionStatus::Raw;
        self.packet_len = packet_len as u16;
        self.signature_len = signature_len as u16;
        self.signature_offset = signature_offset as u16;
        self.num_required_signatures = num_required_signatures;
        self.num_readonly_signed_accounts = num_readonly_signed_accounts;
        self.num_readonly_unsigned_accounts = num_readonly_unsigned_accounts;
        self.version = version;
        self.message_offset = message_offset as u16;
        self.static_accounts_offset = static_accounts_offset as u16;
        self.static_accounts_len = static_accounts_len as u16;
        self.recent_blockhash_offset = recent_blockhash_offset as u16;
        self.instructions_offset = instructions_offset as u16;
        self.instructions_len = instructions_len as u16;
        self.address_lookups_offset = address_lookups_offset as u16;
        self.address_lookups_len = address_lookups_len as u16;

        Some(())
    }

    pub fn try_new(packet: &Packet) -> Option<Self> {
        let mut transaction_view = Self::new();
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

    pub fn instructions<'a>(&'a self) -> impl Iterator<Item = Instruction<'a>> {
        InstructionIterator {
            buffer: &self.buffer[..usize::from(self.packet_len)], // all instructions are within original packet
            current_offset: self.instructions_offset as usize,
            instruction_count: usize::from(self.instructions_len),
            current_count: 0,
        }
    }

    pub fn address_lookups<'a>(&'a self) -> impl Iterator<Item = AddressLookupEntry<'a>> {
        AddressLookupIterator {
            buffer: &self.buffer[..usize::from(self.packet_len)], // all address lookups are within original packet
            current_offset: self.address_lookups_offset as usize,
            address_lookup_count: usize::from(self.address_lookups_len),
            current_count: 0,
        }
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
        let accounts_indexes =
            read_array::<u8>(self.buffer, &mut self.current_offset, accounts_indexes_len)?;
        // u16 for data len
        let data_len = read_compressed_u16(self.buffer, &mut self.current_offset)?;
        let data = read_array::<u8>(self.buffer, &mut self.current_offset, data_len)?;

        self.current_count += 1;

        Some(Instruction {
            program_id_index,
            accounts_indexes,
            data,
        })
    }
}

pub struct Instruction<'a> {
    pub program_id_index: u8,
    pub accounts_indexes: &'a [u8],
    pub data: &'a [u8],
}

struct AddressLookupIterator<'a> {
    buffer: &'a [u8],
    current_offset: usize,
    address_lookup_count: usize,
    current_count: usize,
}

impl<'a> Iterator for AddressLookupIterator<'a> {
    type Item = AddressLookupEntry<'a>;

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

        Some(AddressLookupEntry {
            account_key,
            writable_indexes,
            readonly_indexes,
        })
    }
}

pub struct AddressLookupEntry<'a> {
    pub account_key: &'a Pubkey,
    pub writable_indexes: &'a [u8],
    pub readonly_indexes: &'a [u8],
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
    Some(u16::try_from(value).ok()?)
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

        let transaction_view = TransactionView::try_new(&packet).unwrap();
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

        let transaction_view = TransactionView::try_new(&packet).unwrap();
        compare_view_to_transaction(&transaction, &transaction_view);
    }
}
