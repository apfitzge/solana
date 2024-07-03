use {
    crate::transaction_version::TransactionVersion,
    solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature},
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
        if packet_len > u16::MAX as usize || packet_len < 134 {
            return None;
        }

        let mut meta = Self::default();

        // Signatures always start at the beginning of the packet.
        let mut offset = 0;

        todo!()
    }
}
