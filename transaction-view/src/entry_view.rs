use {
    crate::{
        bytes::check_remaining,
        result::Result,
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

    /// Move offset forward `num_bytes`.
    fn advance(&mut self, num_bytes: usize);

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
    const HEADER_SIZE: usize =
        core::mem::size_of::<u64>() + core::mem::size_of::<Hash>() + core::mem::size_of::<u64>();

    // Check that there are enough bytes to read the header.
    let bytes = entry_data.data();
    check_remaining(bytes, 0, HEADER_SIZE)?;

    // Read the header without checking bounds.
    let (num_hashes, hash, num_transactions) = unsafe {
        let ptr = bytes.as_ptr();
        let num_hashes = u64::from_le_bytes(ptr.cast::<[u8; 8]>().read_unaligned());
        let hash = ptr
            .add(core::mem::size_of::<u64>())
            .cast::<Hash>()
            .read_unaligned();
        let num_transactions = u64::from_le_bytes(
            ptr.add(core::mem::size_of::<u64>() + core::mem::size_of::<Hash>())
                .cast::<[u8; 8]>()
                .read_unaligned(),
        );
        (num_hashes, hash, num_transactions)
    };
    entry_data.advance(HEADER_SIZE);

    Ok((num_hashes, hash, num_transactions))
}

#[inline]
fn read_transaction_view<D: EntryData>(data: &mut D) -> Result<UnsanitizedTransactionView<D::Tx>> {
    let (frame, bytes_read) = TransactionFrame::try_new(data.data())?;
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
    fn advance(&mut self, index: usize) {
        Buf::advance(self, index);
    }

    #[inline]
    fn split_to(&mut self, index: usize) -> Self::Tx {
        Bytes::split_to(self, index)
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
    fn test_incomplete_header() {
        let bytes = Bytes::copy_from_slice(&[0; 47]);
        let result = read_entry(bytes);
        assert!(result.is_err());
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
        bytes.put_u8(1); // trailing bytes are okay for entries
        let bytes = bytes.freeze();

        let (num_hashes, hash, transactions) = read_entry(bytes).unwrap();
        let transactions = transactions.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(num_hashes, entry.num_hashes);
        assert_eq!(hash, entry.hash);
        assert_eq!(transactions.len(), entry.transactions.len());
    }

    #[test]
    fn test_missing_transaction_byte() {
        let (entry, bytes) = create_entry_and_serialize(vec![simple_transfer()]);
        let mut bytes = bytes.try_into_mut().unwrap();
        bytes.truncate(bytes.len() - 1); // remove last byte of last transaction
        let bytes = bytes.freeze();

        let (num_hashes, hash, transactions) = read_entry(bytes).unwrap();
        assert_eq!(num_hashes, entry.num_hashes);
        assert_eq!(hash, entry.hash);

        let transactions = transactions.collect::<Vec<Result<_>>>();
        assert!(transactions[0].is_err());
    }

    #[test]
    fn test_missing_transaction() {
        let (entry, bytes) = create_entry_and_serialize(vec![simple_transfer()]);
        let mut bytes = bytes.try_into_mut().unwrap();
        // modify `num_transactions` to expect 2 transactions
        bytes[40..48].copy_from_slice(2u64.to_le_bytes().as_ref());
        let bytes = bytes.freeze();

        let (num_hashes, hash, transactions) = read_entry(bytes).unwrap();
        assert_eq!(num_hashes, entry.num_hashes);
        assert_eq!(hash, entry.hash);
        assert_eq!(transactions.len(), 2); // exact-sized iterator should expect 2 transactions

        let transactions = transactions.collect::<Vec<Result<_>>>();
        assert!(transactions[0].is_ok());
        assert!(transactions[1].is_err());
    }
}
