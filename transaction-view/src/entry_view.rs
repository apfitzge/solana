use {
    crate::{
        bytes::{check_remaining, read_type},
        result::{Result, TransactionViewError},
        transaction_data::TransactionData,
        transaction_frame::TransactionFrame,
        transaction_view::{TransactionView, UnsanitizedTransactionView},
    },
    bytes::{Buf, Bytes},
    solana_sdk::hash::Hash,
};

pub trait EntryData: Clone {
    /// Associated type for the transaction data.
    type Tx: TransactionData;

    /// Get the remaining data slice for the entry.
    fn data(&self) -> &[u8];

    /// Move read offset to specific index.
    fn move_offset(&mut self, index: usize);

    /// Split the a `TransactionData` off the front of the entry.
    fn split_to(&mut self, index: usize) -> Self::Tx;
}

pub fn read_entry<D: EntryData>(
    mut data: D,
) -> Result<(
    u64,
    Hash,
    impl ExactSizeIterator<Item = Result<TransactionView<false, D::Tx>>>,
)> {
    let (num_hashes, hash, num_transactions) = read_entry_header(&mut data)?;
    let transactions_iterator =
        (0..num_transactions as usize).map(move |_| read_transaction_view(&mut data));
    Ok((num_hashes, hash, transactions_iterator))
}

/// Read the header information from an entry.
/// This includes:
/// - `num_hashes` - u64
/// - `hash` - Hash
/// - `num_transactions` - u64
#[inline]
fn read_entry_header(entry_data: &mut impl EntryData) -> Result<(u64, Hash, u64)> {
    let mut offset = 0;
    let bytes = entry_data.data();
    let num_hashes = read_u64(bytes, &mut offset)?;
    let hash = *unsafe { read_type::<Hash>(bytes, &mut offset)? };
    let num_transactions = read_u64(bytes, &mut offset)?;
    entry_data.move_offset(offset);

    Ok((num_hashes, hash, num_transactions))
}

/// This function reads a `u64` WITHOUT assuming the alignment of the data.
#[inline]
fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    check_remaining(bytes, *offset, core::mem::size_of::<u64>())?;
    let value = u64::from_le_bytes(
        bytes[*offset..*offset + core::mem::size_of::<u64>()]
            .try_into()
            .map_err(|_| TransactionViewError::ParseError)?,
    );
    *offset += core::mem::size_of::<u64>();
    Ok(value)
}

#[inline]
fn read_transaction_view<D: EntryData>(data: &mut D) -> Result<UnsanitizedTransactionView<D::Tx>> {
    let (frame, bytes_read) = TransactionFrame::try_new(&data.data())?;
    let transaction_data = data.split_to(bytes_read);
    // SAFETY: The format was verified by `TransactionFrame::try_new`.
    let view = unsafe { UnsanitizedTransactionView::from_data_unchecked(transaction_data, frame) };
    Ok(view)
}

// Implement `EntryData` for `Bytes`.
impl EntryData for Bytes {
    type Tx = Bytes;

    #[inline]
    fn data(&self) -> &[u8] {
        self
    }

    #[inline]
    fn move_offset(&mut self, index: usize) {
        self.advance(index);
    }

    #[inline]
    fn split_to(&mut self, index: usize) -> Self::Tx {
        let data = self.split_to(index);
        data
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::BufMut,
        solana_entry::entry::Entry,
        solana_sdk::{
            message::{Message, VersionedMessage},
            pubkey::Pubkey,
            signature::Signature,
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

    fn create_entry_and_serialize(transactions: Vec<VersionedTransaction>) -> (Entry, Bytes) {
        let entry = Entry {
            num_hashes: 42,
            hash: Hash::default(),
            transactions,
        };

        let serialized_entry = Bytes::copy_from_slice(&bincode::serialize(&entry).unwrap());
        (entry, serialized_entry)
    }

    #[test]
    fn test_tick_entry() {
        let (entry, bytes) = create_entry_and_serialize(vec![]);
        let (num_hashes, hash, transactions) = read_entry(bytes).unwrap();
        let transactions = transactions.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(num_hashes, entry.num_hashes);
        assert_eq!(hash, entry.hash);
        assert_eq!(transactions.len(), entry.transactions.len());
    }

    #[test]
    fn test_single_transaction() {
        let (entry, bytes) = create_entry_and_serialize(vec![simple_transfer()]);
        let (num_hashes, hash, transactions) = read_entry(bytes).unwrap();
        let transactions = transactions.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(num_hashes, entry.num_hashes);
        assert_eq!(hash, entry.hash);
        assert_eq!(transactions.len(), entry.transactions.len());

        let transaction = &transactions[0];
        assert_eq!(transaction.signatures().len(), 1);
        assert_eq!(
            transaction
                .instructions_iter()
                .next()
                .unwrap()
                .program_id_index,
            2
        );
    }

    #[test]
    fn test_multiple_transactions() {
        let (entry, bytes) = create_entry_and_serialize(vec![
            simple_transfer(),
            simple_transfer(),
            simple_transfer(),
        ]);
        let (num_hashes, hash, transactions) = read_entry(bytes).unwrap();
        let transactions = transactions.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(num_hashes, entry.num_hashes);
        assert_eq!(hash, entry.hash);
        assert_eq!(transactions.len(), entry.transactions.len());

        for transaction in transactions.iter() {
            assert_eq!(transaction.signatures().len(), 1);
            assert_eq!(
                transaction
                    .instructions_iter()
                    .next()
                    .unwrap()
                    .program_id_index,
                2
            );
        }
    }

    #[test]
    fn test_trailing_bytes() {
        let (entry, bytes) = create_entry_and_serialize(vec![simple_transfer()]);
        let mut bytes = bytes.try_into_mut().unwrap();
        bytes.put_u8(0); // trailing bytes are okay for entries
        let bytes = bytes.freeze();

        let (num_hashes, hash, transactions) = read_entry(bytes).unwrap();
        let transactions = transactions.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(num_hashes, entry.num_hashes);
        assert_eq!(hash, entry.hash);
        assert_eq!(transactions.len(), entry.transactions.len());
    }
}
