use {
    crate::{
        bytes::{
            offset_array_len, offset_type, read_byte, read_compressed_u16, unchecked_read_byte,
            unchecked_read_compressed_u16,
        },
        transaction_version::TransactionVersion,
    },
    solana_sdk::{
        hash::Hash, message::MESSAGE_VERSION_PREFIX, packet::PACKET_DATA_SIZE, pubkey::Pubkey,
        signature::Signature,
    },
};

/// TransactionViewMeta contains meta-data about a transaction packet.
/// This packet is not owned, nor directly referenced by this struct.
/// This struct simply contains lengths and offsets that we can later use
/// to extract information from the transaction packet.
//
// - All offsets are relative to the start of the packet.
#[derive(Default)]
pub struct TransactionViewMeta {
    /// The total length of the received packet.
    /// This is not the length of the buffer containing the packet, but the
    /// serialized length of the transaction contained in the packet.
    pub(crate) packet_len: u16,

    /// The number of signatures in the transaction.
    pub(crate) num_signatures: u16,
    /// Offset to the first signature in the transaction.
    pub(crate) signature_offset: u16,

    /// The number of signatures required for this message to be considered
    /// valid.
    pub(crate) num_required_signatures: u8,
    /// The last `num_readonly_signed_accounts` of the signed keys are read-only.
    pub(crate) num_readonly_signed_accounts: u8,
    /// The last `num_readonly_unsigned_accounts` of the unsigned keys are
    /// read-only accounts.
    pub(crate) num_readonly_unsigned_accounts: u8,

    /// Version of the transaction packet.
    pub(crate) version: TransactionVersion,
    /// Offset to the first byte of the message in packet.
    pub(crate) message_offset: u16,

    /// Number of static account keys in the message.
    pub(crate) num_static_accounts: u16,
    /// Offset to the first byte of the static accounts slice in the message.
    pub(crate) static_accounts_offset: u16,

    /// Offset of the first byte in the recent blockhash of the message.
    pub(crate) recent_blockhash_offset: u16,

    /// Number of top-level instructions in the message.
    pub(crate) num_instructions: u16,
    /// Offset of the first instruction in the message.
    /// This is **not** a slice, as the instruction size is not known.
    pub(crate) instructions_offset: u16,

    /// Number of address lookups in the message
    pub(crate) num_address_lookups: u16,
    /// Offset of the first address lookup in the message.
    /// This is **not** a slice, as the entry size is not known.
    pub(crate) address_lookups_offset: u16,
}

impl TransactionViewMeta {
    /// Create a new TransactionViewMeta given a slice of bytes.
    /// The slice of bytes should contain a serialized transaction packet.
    /// If the bytes are not a valid transaction packet, this function will
    /// return None.
    pub fn try_new(bytes: &[u8]) -> Option<Self> {
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

        // Additionally, arrays are serialized as a u16 length followed by
        // the array elements. We can avoid a checked multiplication if the
        // types in our arrays are small enough such that
        // `u16::MAX * size_of::<T>` does not overflow a usize.
        #[allow(dead_code)] // This is used in the assert below.
        const MAX_SIZE_OF_T: usize = usize::MAX / (u16::MAX as usize);
        const _: () = assert!(core::mem::size_of::<u8>() <= MAX_SIZE_OF_T);
        const _: () = assert!(core::mem::size_of::<Signature>() <= MAX_SIZE_OF_T);
        const _: () = assert!(core::mem::size_of::<Pubkey>() <= MAX_SIZE_OF_T);

        // A packet may not be larger than u16::MAX bytes.
        // The minimum-sized transaction would have:
        // - 1 byte for the number of signatures,
        // - 1 signature,
        // - 0 bytes for version, 3 bytes for message header,
        // - 1 byte for the number of account keys,
        // - 1 account key,
        // - 1 recent blockhash,
        // - 1 byte for the number of instructions,
        // - 0 instructions,
        // This totals to: 134 bytes.

        let packet_len = bytes.len();
        const _: () = assert!(PACKET_DATA_SIZE <= u16::MAX as usize);
        #[allow(clippy::manual_range_contains)] // skeptical about optimizations here.
        if packet_len > PACKET_DATA_SIZE || packet_len < 134 {
            return None;
        }

        // Signatures always start at the beginning of the packet.
        let mut offset = 0;

        // We have already checked that the length is at least 134 bytes,
        // so it is safe for us to assume that there are at least 3 bytes
        // for our compressed u16 here.
        let num_signatures = unchecked_read_compressed_u16(bytes, &mut offset);
        // We also know that the offset must be less than 3 here, since the
        // compressed u16 can only use up to 3 bytes, so there is no need to
        // check if the offset is greater than u16::MAX.
        let signature_offset = offset as u16;
        // Update offset for array of signatures.
        offset_array_len::<Signature>(bytes, &mut offset, num_signatures)?;

        // Get the message offset.
        // We know the offset does not exceed packet length, and our packet
        // length is less than u16::MAX, so we can safely cast to u16.
        let message_offset = offset as u16;

        // After reading signatures, the minimum-sized transaction will still
        // contain:
        // The minimum-sized transaction would have:
        // - 0 bytes for version, 3 bytes for message header,
        // - 1 byte for the number of account keys,
        // - 1 account key,
        // - 1 recent blockhash,
        // - 1 byte for the number of instructions,
        // - 0 instructions,
        // This totals to: 69 bytes. Nice.
        // If the remaining number of bytes is less than 69 bytes, it is
        // invalid, so we can break early.
        if packet_len - offset < 69 {
            return None;
        }
        // Read the message prefix byte. If this byte is
        let message_prefix = unchecked_read_byte(bytes, &mut offset);
        let (version, message_header_offset) = if message_prefix & MESSAGE_VERSION_PREFIX != 0 {
            let version = message_prefix & !MESSAGE_VERSION_PREFIX;
            match version {
                // add one byte for prefix byte. Do not need to check for
                // overflow here, because we have already checked.
                0 => (TransactionVersion::V0, message_offset + 1),
                _ => return None,
            }
        } else {
            (TransactionVersion::Legacy, message_offset)
        };

        // Offset should get reset to header offset - the byte we read may have actually
        // been part of the header instead of the prefix.
        offset = usize::from(message_header_offset);
        // We have already checked that the remaining bytes are enough to
        // include these message header bytes. We can safely read these bytes
        // without checking for buffer overflows.
        let num_required_signatures = unchecked_read_byte(bytes, &mut offset);
        let num_readonly_signed_accounts = unchecked_read_byte(bytes, &mut offset);
        let num_readonly_unsigned_accounts = unchecked_read_byte(bytes, &mut offset);

        // From our minimum-sized message of 69 bytes. We have read either 3 or 4.
        // This means we have at least 64 bytes remaining.
        // We can safely read the number of static accounts, without checking for overflow.
        let num_static_accounts = unchecked_read_compressed_u16(bytes, &mut offset);
        // Previous guarantees means that the offset is still less than u16::MAX.
        let static_accounts_offset = offset as u16;
        offset_array_len::<Pubkey>(bytes, &mut offset, num_static_accounts)?;

        // Move to end of recent blockhash.
        let recent_blockhash_offset = offset as u16;
        offset_type::<Hash>(bytes, &mut offset)?;

        // It is possible that we have fewer than 3 bytes remaining. A case of
        // this is if we have a message with no instructions, and no address
        // lookups. Therefore, we cannot use the optimized
        // `unchecked_read_u16_compressed` which assumes at least 3 bytes
        // remaining. We run a slower, safer version here.
        let num_instructions = read_compressed_u16(bytes, &mut offset)?;
        // We know the offset does not exceed packet length, and our packet
        // length is less than u16::MAX, so we can safely cast to u16.
        let instructions_offset = offset as u16;

        // The instructions do not have a fixed size. So we must iterate over
        // each instruction to find the total size of the instructions,
        // and check for any malformed instructions or buffer overflows.
        for _ in 0..num_instructions {
            // u8 for the program index.
            read_byte(bytes, &mut offset)?;
            // u16 for number of account indexes.
            let num_accounts = read_compressed_u16(bytes, &mut offset)?;
            offset_array_len::<u8>(bytes, &mut offset, num_accounts)?;
            // u16 for the number of bytes in the instruction data.
            let data_len = read_compressed_u16(bytes, &mut offset)?;
            // Offset is guaranteed to be less than u16::MAX, so we can safely
            // do addition of a u16 here without overflow.
            offset += usize::from(data_len);
            if offset > packet_len {
                return None;
            }
        }

        // If the transaction is a V0 transaction, there may be address
        // lookups after the instructions. We need to read the length and
        // iterate over each one to find the total size of the address lookups,
        // and check for any malformed address lookups or buffer overflows.
        let (address_lookups_len, address_lookups_offset) = match version {
            TransactionVersion::Legacy => (0, 0),
            TransactionVersion::V0 => {
                let address_lookups_len = read_compressed_u16(bytes, &mut offset)?;
                // Offset is guaranteed to be less than u16::MAX, so we can safely
                // cast to u16 here.
                let address_lookups_offset = offset as u16;

                for _ in 0..address_lookups_len {
                    // Pubkey for address
                    offset_type::<Pubkey>(bytes, &mut offset)?;
                    // u16 for length of writable_indexes
                    let writable_indexes_len = read_compressed_u16(bytes, &mut offset)?;
                    offset_array_len::<u8>(bytes, &mut offset, writable_indexes_len)?;
                    // u16 for length of readonly_indexes
                    let readonly_indexes_len = read_compressed_u16(bytes, &mut offset)?;
                    offset_array_len::<u8>(bytes, &mut offset, readonly_indexes_len)?;
                }

                (address_lookups_len, address_lookups_offset)
            }
        };

        // Ensure that we have read the entire packet. If we have not, then
        // the packet is malformed.
        if offset != packet_len {
            return None;
        }

        // We already ensured this was less than u16::MAX, so we can safely cast.
        let packet_len = packet_len as u16;

        Some(Self {
            packet_len,
            num_signatures,
            signature_offset,
            num_required_signatures,
            num_readonly_signed_accounts,
            num_readonly_unsigned_accounts,
            version,
            message_offset,
            num_static_accounts,
            static_accounts_offset,
            recent_blockhash_offset,
            num_instructions,
            instructions_offset,
            num_address_lookups: address_lookups_len,
            address_lookups_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            address_lookup_table::AddressLookupTableAccount,
            message::{v0, Message, MessageHeader, VersionedMessage},
            packet::Packet,
            signature::Keypair,
            signer::Signer,
            system_instruction, system_program,
            transaction::VersionedTransaction,
        },
    };

    fn verify_transaction_view_meta(tx: &VersionedTransaction) {
        let mut packet = Packet::default();
        packet.populate_packet(None, tx).unwrap();
        let bytes = packet.data(..).unwrap();
        let meta = TransactionViewMeta::try_new(bytes).unwrap();

        assert_eq!(meta.packet_len as usize, packet.meta().size);

        assert_eq!(meta.num_signatures, tx.signatures.len() as u16);
        assert_eq!(meta.signature_offset as usize, 1);

        assert_eq!(
            meta.num_required_signatures,
            tx.message.header().num_required_signatures
        );
        assert_eq!(
            meta.num_readonly_signed_accounts,
            tx.message.header().num_readonly_signed_accounts
        );
        assert_eq!(
            meta.num_readonly_unsigned_accounts,
            tx.message.header().num_readonly_unsigned_accounts
        );

        assert_eq!(
            meta.num_static_accounts,
            tx.message.static_account_keys().len() as u16
        );
        assert_eq!(
            meta.num_instructions,
            tx.message.instructions().len() as u16
        );
        assert_eq!(
            meta.num_address_lookups,
            tx.message
                .address_table_lookups()
                .map(|x| x.len())
                .unwrap_or(0) as u16
        );
    }

    fn minimally_sized_transaction() -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![Pubkey::default()],
                recent_blockhash: Hash::default(),
                instructions: vec![],
            }),
        }
    }

    fn simple_transfer() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::Legacy(Message::new(
                &[system_instruction::transfer(
                    &payer,
                    &Pubkey::new_unique(),
                    1,
                )],
                Some(&payer),
            )),
        }
    }

    fn simple_transfer_v0() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::V0(
                v0::Message::try_compile(
                    &payer,
                    &[system_instruction::transfer(
                        &payer,
                        &Pubkey::new_unique(),
                        1,
                    )],
                    &[],
                    Hash::default(),
                )
                .unwrap(),
            ),
        }
    }

    fn v0_with_lookup() -> VersionedTransaction {
        let payer = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        VersionedTransaction {
            signatures: vec![Signature::default()], // 1 signature to be valid.
            message: VersionedMessage::V0(
                v0::Message::try_compile(
                    &payer,
                    &[system_instruction::transfer(&payer, &to, 1)],
                    &[AddressLookupTableAccount {
                        key: Pubkey::new_unique(),
                        addresses: vec![to],
                    }],
                    Hash::default(),
                )
                .unwrap(),
            ),
        }
    }

    #[test]
    fn test_minimal_sized_transaction() {
        verify_transaction_view_meta(&minimally_sized_transaction());
    }

    #[test]
    fn test_simple_transfer() {
        verify_transaction_view_meta(&simple_transfer());
    }

    #[test]
    fn test_simple_transfer_v0() {
        verify_transaction_view_meta(&simple_transfer_v0());
    }

    #[test]
    fn test_v0_with_lookup() {
        verify_transaction_view_meta(&v0_with_lookup());
    }

    #[test]
    fn test_trailing_byte() {
        let tx = simple_transfer();
        let mut packet = Packet::default();
        packet.populate_packet(None, &tx).unwrap();
        let bytes = packet.data(..).unwrap();
        let mut bytes = bytes.to_vec();
        bytes.push(0);
        assert!(TransactionViewMeta::try_new(&bytes).is_none());
    }

    #[test]
    fn test_insufficient_bytes() {
        let tx = simple_transfer();
        let mut packet = Packet::default();
        packet.populate_packet(None, &tx).unwrap();
        let bytes = packet.data(..).unwrap();
        let bytes = &bytes[..bytes.len() - 1];
        assert!(TransactionViewMeta::try_new(bytes).is_none());
    }

    #[test]
    fn test_signature_overflow() {
        let tx = simple_transfer();
        let mut packet = Packet::default();
        packet.populate_packet(None, &tx).unwrap();
        let bytes = packet.data(..).unwrap();
        let mut bytes = bytes.to_vec();
        // Set the number of signatures to u16::MAX
        bytes[0] = 0xff;
        bytes[1] = 0xff;
        bytes[2] = 0xff;
        assert!(TransactionViewMeta::try_new(&bytes).is_none());
    }

    #[test]
    fn test_account_key_overflow() {
        let tx = simple_transfer();
        let mut packet = Packet::default();
        packet.populate_packet(None, &tx).unwrap();
        let bytes = packet.data(..).unwrap();
        let mut bytes = bytes.to_vec();
        // Set the number of accounts to u16::MAX
        let offset = 1 + core::mem::size_of::<Signature>() + 3;
        bytes[offset + 0] = 0xff;
        bytes[offset + 1] = 0xff;
        bytes[offset + 2] = 0xff;
        assert!(TransactionViewMeta::try_new(&bytes).is_none());
    }

    #[test]
    fn test_instructions_overflow() {
        let tx = simple_transfer();
        let mut packet = Packet::default();
        packet.populate_packet(None, &tx).unwrap();
        let bytes = packet.data(..).unwrap();
        let mut bytes = bytes.to_vec();
        // Set the number of instructions to u16::MAX
        let offset = 1
            + core::mem::size_of::<Signature>()
            + 3
            + 1
            + 3 * core::mem::size_of::<Pubkey>()
            + core::mem::size_of::<Hash>();
        bytes[offset + 0] = 0xff;
        bytes[offset + 1] = 0xff;
        bytes[offset + 2] = 0xff;
        assert!(TransactionViewMeta::try_new(&bytes).is_none());
    }

    #[test]
    fn test_alt_overflow() {
        let tx = simple_transfer_v0();
        let mut packet = Packet::default();
        packet.populate_packet(None, &tx).unwrap();
        let bytes = packet.data(..).unwrap();
        let mut bytes = bytes.to_vec();
        // Set the number of instructions to u16::MAX
        let offset = 1 // byte for num signatures
            + core::mem::size_of::<Signature>() // signature
            + 1 // version byte
            + 3 // message header
            + 1 // byte for num account keys
            + 3 * core::mem::size_of::<Pubkey>() // account keys
            + core::mem::size_of::<Hash>() // recent blockhash
            + 1 // byte for num instructions
            + 1 // program index
            + 1 // byte for num accounts
            + 2 // bytes for account index
            + 1
            + core::mem::size_of::<u64>();
        bytes[offset + 0] = 0xff;
        bytes[offset + 1] = 0xff;
        bytes[offset + 2] = 0xff;
        assert!(TransactionViewMeta::try_new(&bytes).is_none());
    }
}
