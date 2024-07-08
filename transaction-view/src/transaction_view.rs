use {
    crate::{bytes::unchecked_read_array, transaction_view_meta::TransactionViewMeta},
    solana_sdk::{hash::Hash, packet::PACKET_DATA_SIZE, pubkey::Pubkey, signature::Signature},
};

pub struct TransactionView {
    /// The actual serialized data of the transaction.
    data: Box<[u8; PACKET_DATA_SIZE]>,
    /// The number of bytes actually used in the data.
    len: usize,
    /// Calculated offsets of the transaction's fields.
    meta: TransactionViewMeta,
}

impl Default for TransactionView {
    fn default() -> Self {
        Self {
            data: Box::new([0u8; PACKET_DATA_SIZE]),
            len: 0,
            meta: TransactionViewMeta::default(),
        }
    }
}

impl TransactionView {
    /// Attempts to create a new `TransactionView` from the given serialized
    /// boxed data. This will simply take ownership of the boxed data and not
    /// perform an allocation. This also avoids a copy since the data is
    /// already in a Box. All basic checks on data are performed.
    pub fn try_new_from_boxed_data(data: Box<[u8; PACKET_DATA_SIZE]>, len: usize) -> Option<Self> {
        let mut transaction_view = Self {
            data,
            len,
            ..Self::default()
        };
        transaction_view.populate_meta()?;
        Some(transaction_view)
    }

    /// Attempts to create a new `TransactionView` from the given serialized
    /// data. This will allocate a new Box to store the data on the heap.
    pub fn try_new_from_slice(data: &[u8]) -> Option<Self> {
        let mut transaction_view = Self::default();
        transaction_view.copy_from_slice(data)?;
        Some(transaction_view)
    }

    /// Copy data from passed slice to the transaction view, and perform basic
    /// checks on the data.
    pub fn copy_from_slice(&mut self, data: &[u8]) -> Option<()> {
        // Check that the length of the data is correct.
        if data.len() > PACKET_DATA_SIZE {
            return None;
        }
        // Copy the data into the boxed data and set length.
        self.data[..data.len()].copy_from_slice(data);
        self.len = data.len();
        self.populate_meta()
    }

    /// Consume the `TransactionView` and return the boxed data and length.
    pub fn take_data(self) -> (Box<[u8; PACKET_DATA_SIZE]>, usize) {
        (self.data, self.len)
    }

    fn populate_meta(&mut self) -> Option<()> {
        self.meta = TransactionViewMeta::try_new(&self.data[..self.len])?;
        Some(())
    }
}

impl TransactionView {
    /// Returns the number of signatures.
    pub fn num_signatures(&self) -> u16 {
        self.meta.num_signatures
    }

    /// Returns a slice of the signatures.
    pub fn signatures(&self) -> &[Signature] {
        unchecked_read_array(
            &self.data[..self.len],
            usize::from(self.meta.signature_offset),
            usize::from(self.meta.num_signatures),
        )
    }

    /// Return the number of required signatures.
    pub fn num_required_signatures(&self) -> u8 {
        self.meta.num_required_signatures
    }

    /// Return the number of read-only signed accounts.
    pub fn num_readonly_signed_accounts(&self) -> u8 {
        self.meta.num_readonly_signed_accounts
    }

    /// Return the number of read-only unsigned accounts.
    pub fn num_readonly_unsigned_accounts(&self) -> u8 {
        self.meta.num_readonly_unsigned_accounts
    }

    /// Return number of static account keys.
    pub fn num_static_accounts(&self) -> u16 {
        self.meta.num_static_accounts
    }

    /// Returns a slice of the static account keys.
    pub fn static_account_keys(&self) -> &[Pubkey] {
        unchecked_read_array(
            &self.data[..self.len],
            usize::from(self.meta.static_accounts_offset),
            usize::from(self.meta.num_static_accounts),
        )
    }

    /// Returns reference to the recent blockhash.
    pub fn recent_blockhash(&self) -> &Hash {
        unsafe {
            &*(self
                .data
                .as_ptr()
                .add(usize::from(self.meta.recent_blockhash_offset)) as *const Hash)
        }
    }

    /// Returns the number of instructions.
    pub fn num_instructions(&self) -> u16 {
        self.meta.num_instructions
    }

    /// Returns the number of address lookup tables.
    pub fn num_address_lookups(&self) -> u16 {
        self.meta.num_address_lookups
    }
}
