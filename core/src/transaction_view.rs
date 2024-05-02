use {
    solana_perf::sigverify::{do_get_packet_offsets, PacketOffsets},
    solana_sdk::{
        address_lookup_table::instruction,
        blake3::Hash,
        message::{MessageHeader, MESSAGE_VERSION_PREFIX},
        packet::{Packet, PACKET_DATA_SIZE},
        pubkey::Pubkey,
        short_vec::decode_shortu16_len,
        signature::Signature,
    },
};

const MAX_TRASACTION_SIZE: usize = 4096; // not sure this is actually true

#[repr(u8)]
pub enum TransactionVersion {
    Legacy = u8::MAX,
    V0 = 0,
}

/// Stores the raw packet and information about the transaction
/// that is stored in this packet.
/// This importantly, does not do any heap allocations, and all
/// signature, pubkey, hash, heap, etc information is stored
/// in the packet itself.
/// This view struct allows calling code to access information
/// about the transaction without needing to do slow deserialization.
pub struct TransactionView {
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
    pub fn try_new(packet: Packet) -> Option<Self> {
        // Get the offsets of the packet data
        let PacketOffsets {
            sig_len: signature_len,
            sig_start: signature_offset,
            msg_start: message_offset,
            pubkey_start: _static_accounts_offset,
            pubkey_len: _static_accounts_len,
        } = do_get_packet_offsets(&packet, 0).ok()?;

        // Copy the packet data into the buffer
        let packet_len = packet.meta().size;
        let mut buffer = [0u8; MAX_TRASACTION_SIZE];
        buffer[..PACKET_DATA_SIZE].copy_from_slice(packet.data(..)?);

        // Get the transaction version. Only need to load a single byte at the
        // start of the message.
        let message_prefix = *packet.data(message_offset as usize)?;
        let (version, message_header_offset) = if message_prefix & MESSAGE_VERSION_PREFIX != 0 {
            let version = message_prefix & !MESSAGE_VERSION_PREFIX;
            match version {
                0 => (TransactionVersion::V0, message_offset.checked_add(1)?),
                _ => return None,
            }
        } else {
            (TransactionVersion::Legacy, message_offset)
        };

        let mut current_offset = usize::try_from(message_header_offset).ok()?;

        // Read message header.
        let message_header_offset = current_offset;
        let message_header_end =
            message_header_offset.checked_add(core::mem::size_of::<MessageHeader>())?;
        let message_header_bytes = packet.data(message_header_offset..message_header_end)?;
        let num_required_signatures = message_header_bytes[0];
        let num_readonly_signed_accounts = message_header_bytes[1];
        let num_readonly_unsigned_accounts = message_header_bytes[2];
        current_offset = current_offset.checked_add(core::mem::size_of::<MessageHeader>())?;

        // Read the number of accounts - extraordinarily confusing serialization format.
        let (static_accounts_len, bytes_read) =
            decode_shortu16_len(&packet.data(current_offset..)?).ok()?;
        current_offset = current_offset.checked_add(bytes_read)?;
        let static_accounts_offset = current_offset;
        current_offset = static_accounts_offset.checked_add(
            usize::try_from(static_accounts_len).ok()? * core::mem::size_of::<Pubkey>(),
        )?;

        // Read the recent blockhash.
        let recent_blockhash_offset = current_offset;
        current_offset = recent_blockhash_offset.checked_add(core::mem::size_of::<Hash>())?;

        // Read the instructions.
        let (instructions_len, bytes) =
            decode_shortu16_len(&packet.data(current_offset..)?).ok()?;
        current_offset = current_offset.checked_add(bytes)?;
        let instructions_offset = current_offset;

        // The instructions do not have a fixed size, so we actually must iterate over them.
        for _ in 0..instructions_len {
            // u8 for program index
            current_offset = current_offset.checked_add(core::mem::size_of::<u8>())?;
            // u16 for accounts len
            let (accounts_indices_len, bytes) =
                decode_shortu16_len(&packet.data(current_offset..)?).ok()?;
            current_offset = current_offset.checked_add(bytes)?;
            let accounts_indices_offset = current_offset;
            current_offset = accounts_indices_offset.checked_add(
                usize::from(accounts_indices_len).checked_mul(core::mem::size_of::<u8>())?,
            )?;
            // u16 for data len
            let (data_len, bytes) = decode_shortu16_len(&packet.data(current_offset..)?).ok()?;
            current_offset = current_offset.checked_add(bytes)?;
            current_offset = current_offset.checked_add(usize::from(data_len))?;
        }

        // If the transaction is a V0 transaction, there may be address lookups
        let (address_lookups_len, address_lookups_offset) = match version {
            TransactionVersion::Legacy => (0, 0),
            TransactionVersion::V0 => {
                // After the instructions, there are address lookup entries.
                // We must iterate over these as well since the size is not fixed.
                let (address_lookups_len, bytes) =
                    decode_shortu16_len(&packet.data(current_offset..)?).ok()?;
                current_offset = current_offset.checked_add(bytes)?;
                let address_lookups_offset = current_offset;

                for _ in 0..address_lookups_len {
                    // Pubkey for address
                    current_offset = current_offset.checked_add(core::mem::size_of::<Pubkey>())?;
                    // u16 for length of writable_indices
                    let (writable_indices_len, bytes) =
                        decode_shortu16_len(&packet.data(current_offset..)?).ok()?;
                    current_offset = current_offset.checked_add(bytes)?;
                    let writable_indices_offset = current_offset;
                    current_offset = writable_indices_offset.checked_add(
                        usize::from(writable_indices_len).checked_mul(core::mem::size_of::<u8>())?,
                    )?;

                    // u16 for length of readonly_indices
                    let (readonly_indices_len, bytes) =
                        decode_shortu16_len(&packet.data(current_offset..)?).ok()?;
                    current_offset = current_offset.checked_add(bytes)?;
                    let readonly_indices_offset = current_offset;
                    current_offset = readonly_indices_offset.checked_add(
                        usize::from(readonly_indices_len).checked_mul(core::mem::size_of::<u8>())?,
                    )?;
                }
                (address_lookups_len, address_lookups_offset)
            }
        };

        Some(Self {
            buffer,
            packet_len: packet_len as u16,
            signature_len: signature_len as u16,
            signature_offset: signature_offset as u16,
            num_required_signatures,
            num_readonly_signed_accounts,
            num_readonly_unsigned_accounts,
            version,
            message_offset: message_offset as u16,
            static_accounts_offset: static_accounts_offset as u16,
            static_accounts_len: static_accounts_len as u16,
            recent_blockhash_offset: recent_blockhash_offset as u16,
            instructions_offset: instructions_offset as u16,
            instructions_len: instructions_len as u16,
            address_lookups_offset: address_lookups_offset as u16,
            address_lookups_len: address_lookups_len as u16,
        })
    }

    pub fn signatures(&self) -> &[Signature] {
        let start = self.signature_offset as usize;

        // Cast as a slice
        unsafe {
            core::slice::from_raw_parts(
                &self.buffer[start] as *const _ as *const Signature,
                usize::from(self.signature_len),
            )
        }
    }

    pub fn recent_blockhash(&self) -> &Hash {
        unsafe {
            &*(&self.buffer[self.recent_blockhash_offset as usize] as *const _ as *const Hash)
        }
    }

    pub fn static_account_keys(&self) -> &[Pubkey] {
        let start = self.static_accounts_offset as usize;

        // Cast as a slice
        unsafe {
            core::slice::from_raw_parts(
                &self.buffer[start] as *const _ as *const Pubkey,
                usize::from(self.static_accounts_len),
            )
        }
    }
}
