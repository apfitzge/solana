use {
    crate::{
        instructions_meta::InstructionsIterator, message_header_meta::TransactionVersion,
        result::Result, transaction_meta::TransactionMeta,
    },
    solana_sdk::{hash::Hash, packet::Packet, pubkey::Pubkey, signature::Signature},
    std::sync::Arc,
};

pub trait PacketAccess {
    fn packet_data(&self) -> &[u8];
}

pub struct TransactionView<P: PacketAccess> {
    packet: P,
    meta: TransactionMeta,
}

impl<P: PacketAccess> TransactionView<P> {
    /// Attempt to create a new `TransactionView` from some packet accessor.
    pub fn try_new(packet: P) -> Result<Self> {
        let meta = TransactionMeta::try_new(packet.packet_data())?;
        Ok(Self { packet, meta })
    }

    /// Return the number of signatures in the transaction.
    pub fn num_signatures(&self) -> u8 {
        self.meta.num_signatures()
    }

    /// Return the slice of signatures in the transaction.
    pub fn signatures(&self) -> &[Signature] {
        // SAFETY: The `TransactionMeta` instance was created from the same
        //         `packet_data` slice that is being accessed here.
        unsafe { self.meta.signatures(self.packet_data()) }
    }

    /// Return the version of the transaction.
    pub fn version(&self) -> TransactionVersion {
        self.meta.version()
    }

    /// Return the number of required signatures in the transaction.
    pub fn num_required_signatures(&self) -> u8 {
        self.meta.num_required_signatures()
    }

    /// Return the number of readonly signed accounts in the transaction.
    pub fn num_readonly_signed_accounts(&self) -> u8 {
        self.meta.num_readonly_signed_accounts()
    }

    /// Return the number of readonly unsigned accounts in the transaction.
    pub fn num_readonly_unsigned_accounts(&self) -> u8 {
        self.meta.num_readonly_unsigned_accounts()
    }

    /// Return the number of static account keys in the transaction.
    pub fn num_static_account_keys(&self) -> u8 {
        self.meta.num_static_account_keys()
    }

    /// Return the slice of static account keys in the transaction.
    pub fn static_account_keys(&self) -> &[Pubkey] {
        // SAFETY: The `TransactionMeta` instance was created from the same
        //         `packet_data` slice that is being accessed here.
        unsafe { self.meta.static_account_keys(self.packet_data()) }
    }

    /// Return the number of instructions in the transaction.
    pub fn num_instructions(&self) -> u16 {
        self.meta.num_instructions()
    }

    /// Return an iterator over the instructions in the transaction.
    pub fn instructions_iter(&self) -> InstructionsIterator {
        unsafe { self.meta.instructions_iter(self.packet_data()) }
    }

    /// Return the number of address table lookups in the transaction.
    pub fn num_address_table_lookups(&self) -> u8 {
        self.meta.num_address_table_lookups()
    }

    /// Return the recent blockhash in the transaction.
    pub fn recent_blockhash(&self) -> &Hash {
        // SAFETY: The `TransactionMeta` instance was created from the same
        //         `packet_data` slice that is being accessed here.
        unsafe { self.meta.recent_blockhash(self.packet_data()) }
    }

    #[inline(always)]
    pub fn packet_data(&self) -> &[u8] {
        self.packet.packet_data()
    }
}

impl PacketAccess for Packet {
    fn packet_data(&self) -> &[u8] {
        // If the packet is discard, the data is `None`.
        // Simply return an empty slice in this case so that
        // parsing fails gracefully.
        self.data(..).unwrap_or(&[])
    }
}

impl PacketAccess for Arc<[u8]> {
    fn packet_data(&self) -> &[u8] {
        self.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            message::{Message, VersionedMessage},
            system_instruction,
            transaction::VersionedTransaction,
        },
    };

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

    #[test]
    fn test_discard_packet() {
        let mut packet = Packet::default();
        packet.populate_packet(None, &simple_transfer()).unwrap();
        packet.meta_mut().set_discard(true);

        assert!(TransactionView::try_new(packet.clone()).is_err());
    }

    #[test]
    fn test_simple_transfer_packet() {
        let mut packet = Packet::default();
        packet.populate_packet(None, &simple_transfer()).unwrap();

        let view = TransactionView::try_new(packet).unwrap();
        assert_eq!(view.num_signatures(), 1);
        assert!(matches!(view.version(), TransactionVersion::Legacy));
        assert_eq!(view.num_required_signatures(), 1);
        assert_eq!(view.num_readonly_signed_accounts(), 1);
        assert_eq!(view.num_readonly_unsigned_accounts(), 0);
        assert_eq!(view.num_static_account_keys(), 1);
        assert_eq!(view.num_instructions(), 1);
        assert_eq!(view.num_address_table_lookups(), 0);
        assert_eq!(view.recent_blockhash(), &Hash::default());
    }

    #[test]
    fn test_simple_transfer_arc() {
        let bytes: Arc<[u8]> = Arc::from(
            bincode::serialize(&simple_transfer())
                .unwrap()
                .into_boxed_slice(),
        );

        let view = TransactionView::try_new(bytes).unwrap();
        assert_eq!(view.num_signatures(), 1);
        assert!(matches!(view.version(), TransactionVersion::Legacy));
        assert_eq!(view.num_required_signatures(), 1);
        assert_eq!(view.num_readonly_signed_accounts(), 1);
        assert_eq!(view.num_readonly_unsigned_accounts(), 0);
        assert_eq!(view.num_static_account_keys(), 1);
        assert_eq!(view.num_instructions(), 1);
        assert_eq!(view.num_address_table_lookups(), 0);
        assert_eq!(view.recent_blockhash(), &Hash::default());
    }
}
