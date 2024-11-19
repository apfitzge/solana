use {
    super::{
        transaction_priority_id::TransactionPriorityId,
        transaction_state::{SanitizedTransactionTTL, TransactionState},
    },
    crate::banking_stage::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        scheduler_messages::TransactionId,
    },
    agave_transaction_view::resolved_transaction_view::ResolvedTransactionView,
    bytes::{Bytes, BytesMut},
    itertools::MinMaxResult,
    min_max_heap::MinMaxHeap,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_with_meta::TransactionWithMeta,
    },
    solana_sdk::packet::PACKET_DATA_SIZE,
    std::{collections::HashMap, sync::Arc},
};

/// This structure will hold `TransactionState` for the entirety of a
/// transaction's lifetime in the scheduler and BankingStage as a whole.
///
/// Transaction Lifetime:
/// 1. Received from `SigVerify` by `BankingStage`
/// 2. Inserted into `TransactionStateContainer` by `BankingStage`
/// 3. Popped in priority-order by scheduler, and transitioned to `Pending` state
/// 4. Processed by `ConsumeWorker`
///    a. If consumed, remove `Pending` state from the `TransactionStateContainer`
///    b. If retryable, transition back to `Unprocessed` state.
///       Re-insert to the queue, and return to step 3.
///
/// The structure is composed of two main components:
/// 1. A priority queue of wrapped `TransactionId`s, which are used to
///    order transactions by priority for selection by the scheduler.
/// 2. A map of `TransactionId` to `TransactionState`, which is used to
///    track the state of each transaction.
///
/// When `Pending`, the associated `TransactionId` is not in the queue, but
/// is still in the map.
/// The entry in the map should exist before insertion into the queue, and be
/// be removed only after the id is removed from the queue.
///
/// The container maintains a fixed capacity. If the queue is full when pushing
/// a new transaction, the lowest priority transaction will be dropped.
pub(crate) struct TransactionStateContainer<Tx: TransactionWithMeta> {
    priority_queue: MinMaxHeap<TransactionPriorityId>,
    id_to_transaction_state: HashMap<TransactionId, TransactionState<Tx>>,
}

pub(crate) trait StateContainer<Tx: TransactionWithMeta> {
    /// Create a new `TransactionStateContainer` with the given capacity.
    fn with_capacity(capacity: usize) -> Self;

    /// Returns true if the queue is empty.
    fn is_empty(&self) -> bool;

    /// Returns the remaining capacity of the queue
    fn remaining_queue_capacity(&self) -> usize;

    /// Get the top transaction id in the priority queue.
    fn pop(&mut self) -> Option<TransactionPriorityId>;

    /// Get mutable transaction state by id.
    fn get_mut_transaction_state(
        &mut self,
        id: &TransactionId,
    ) -> Option<&mut TransactionState<Tx>>;

    /// Get reference to `SanitizedTransactionTTL` by id.
    /// Panics if the transaction does not exist.
    fn get_transaction_ttl(&self, id: &TransactionId) -> Option<&SanitizedTransactionTTL<Tx>>;

    /// Insert a new transaction into the container's queues and maps.
    /// Returns `true` if a packet was dropped due to capacity limits.
    fn insert_new_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction_ttl: SanitizedTransactionTTL<Tx>,
        packet: Arc<ImmutableDeserializedPacket>,
        priority: u64,
        cost: u64,
    ) -> bool;

    /// Retries a transaction - inserts transaction back into map (but not packet).
    /// This transitions the transaction to `Unprocessed` state.
    fn retry_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction_ttl: SanitizedTransactionTTL<Tx>,
    );

    /// Pushes a transaction id into the priority queue. If the queue is full, the lowest priority
    /// transaction will be dropped (removed from the queue and map).
    /// Returns `true` if a packet was dropped due to capacity limits.
    fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool;

    /// Remove transaction by id.
    fn remove_by_id(&mut self, id: &TransactionId);

    fn get_min_max_priority(&self) -> MinMaxResult<u64>;
}

impl<Tx: TransactionWithMeta> StateContainer<Tx> for TransactionStateContainer<Tx> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            priority_queue: MinMaxHeap::with_capacity(capacity),
            id_to_transaction_state: HashMap::with_capacity(capacity),
        }
    }

    fn is_empty(&self) -> bool {
        self.priority_queue.is_empty()
    }

    fn remaining_queue_capacity(&self) -> usize {
        self.priority_queue.capacity() - self.priority_queue.len()
    }

    fn pop(&mut self) -> Option<TransactionPriorityId> {
        self.priority_queue.pop_max()
    }

    fn get_mut_transaction_state(
        &mut self,
        id: &TransactionId,
    ) -> Option<&mut TransactionState<Tx>> {
        self.id_to_transaction_state.get_mut(id)
    }

    fn get_transaction_ttl(&self, id: &TransactionId) -> Option<&SanitizedTransactionTTL<Tx>> {
        self.id_to_transaction_state
            .get(id)
            .map(|state| state.transaction_ttl())
    }

    fn insert_new_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction_ttl: SanitizedTransactionTTL<Tx>,
        packet: Arc<ImmutableDeserializedPacket>,
        priority: u64,
        cost: u64,
    ) -> bool {
        let priority_id = TransactionPriorityId::new(priority, transaction_id);
        self.id_to_transaction_state.insert(
            transaction_id,
            TransactionState::new(transaction_ttl, packet, priority, cost),
        );
        self.push_id_into_queue(priority_id)
    }

    fn retry_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction_ttl: SanitizedTransactionTTL<Tx>,
    ) {
        let transaction_state = self
            .get_mut_transaction_state(&transaction_id)
            .expect("transaction must exist");
        let priority_id = TransactionPriorityId::new(transaction_state.priority(), transaction_id);
        transaction_state.transition_to_unprocessed(transaction_ttl);
        self.push_id_into_queue(priority_id);
    }

    fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool {
        if self.remaining_queue_capacity() == 0 {
            let popped_id = self.priority_queue.push_pop_min(priority_id);
            self.remove_by_id(&popped_id.id);
            true
        } else {
            self.priority_queue.push(priority_id);
            false
        }
    }

    fn remove_by_id(&mut self, id: &TransactionId) {
        self.id_to_transaction_state
            .remove(id)
            .expect("transaction must exist");
    }

    fn get_min_max_priority(&self) -> MinMaxResult<u64> {
        match self.priority_queue.peek_min() {
            Some(min) => match self.priority_queue.peek_max() {
                Some(max) => MinMaxResult::MinMax(min.priority, max.priority),
                None => MinMaxResult::OneElement(min.priority),
            },
            None => MinMaxResult::NoElements,
        }
    }
}

/// A wrapper around `TransactionStateContainer` that allows re-uses
/// pre-allocated `Bytes` to copy packet data into and use for serialization.
/// This is used to avoid allocations in parsing transactions.
pub struct TransactionStateContainerWithBytes {
    inner: TransactionStateContainer<RuntimeTransaction<ResolvedTransactionView<Bytes>>>,
    bytes_buffer: Box<[MaybeBytes]>,
    index_stack: Vec<u32>,
}

enum MaybeBytes {
    None,
    Bytes(Bytes),
    BytesMut(BytesMut),
}

impl MaybeBytes {
    fn reserve(&mut self) -> BytesMut {
        match core::mem::replace(self, MaybeBytes::None) {
            MaybeBytes::BytesMut(bytes_mut) => bytes_mut,
            _ => panic!("invalid state to reserve"),
        }
    }

    fn as_mut(&mut self) {
        match core::mem::replace(self, MaybeBytes::None) {
            MaybeBytes::Bytes(bytes) => {
                *self = MaybeBytes::BytesMut(
                    bytes
                        .try_into_mut()
                        .expect("all `Bytes` copies should be dropped before this call"),
                );
            }
            _ => panic!("invalid state to as_mut"),
        }
    }
}

impl TransactionStateContainerWithBytes {
    /// Returns an index and a `BytesMut` to copy packet data into.
    /// The caller **must** return this space to the container either by
    /// calling `return_space` or `freeze`ing and pushing the transaction.
    pub fn reserve_space(&mut self) -> Option<(u32, BytesMut)> {
        self.index_stack
            .pop()
            .map(|index| (index, self.bytes_buffer[index as usize].reserve()))
    }

    /// Return space that is will not be used for now.
    pub fn return_space(&mut self, index: u32, bytes: BytesMut) {
        self.bytes_buffer[index as usize] = MaybeBytes::BytesMut(bytes);
        self.index_stack.push(index);
    }

    /// Freeze the space and push the transaction into the container.
    /// This is only used to store a copy of the `Bytes` which can be
    /// re-used when a transaction is eventually removed via
    /// `remove_by_id`.
    pub fn freeze(&mut self, index: u32, bytes: Bytes) {
        self.bytes_buffer[index as usize] = MaybeBytes::Bytes(bytes);
    }
}

type TxB = RuntimeTransaction<ResolvedTransactionView<Bytes>>;
impl StateContainer<TxB> for TransactionStateContainerWithBytes {
    fn with_capacity(capacity: usize) -> Self {
        const EXTRA_CAPACITY: usize = 256;
        assert!(capacity + EXTRA_CAPACITY < u32::MAX as usize);
        let mut bytes = BytesMut::with_capacity(PACKET_DATA_SIZE * (capacity + EXTRA_CAPACITY));
        let bytes_buffer = (0..capacity)
            .map(|_| bytes.split_to(PACKET_DATA_SIZE))
            .map(MaybeBytes::BytesMut)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let index_stack = (0..capacity).rev().map(|index| index as u32).collect();

        Self {
            inner: TransactionStateContainer::with_capacity(capacity),
            bytes_buffer,
            index_stack,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    fn remaining_queue_capacity(&self) -> usize {
        self.inner.remaining_queue_capacity()
    }

    #[inline]
    fn pop(&mut self) -> Option<TransactionPriorityId> {
        self.inner.pop()
    }

    #[inline]
    fn get_mut_transaction_state(
        &mut self,
        id: &TransactionId,
    ) -> Option<&mut TransactionState<TxB>> {
        self.inner.get_mut_transaction_state(id)
    }

    #[inline]
    fn get_transaction_ttl(&self, id: &TransactionId) -> Option<&SanitizedTransactionTTL<TxB>> {
        self.inner.get_transaction_ttl(id)
    }

    #[inline]
    fn insert_new_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction_ttl: SanitizedTransactionTTL<TxB>,
        packet: Arc<ImmutableDeserializedPacket>,
        priority: u64,
        cost: u64,
    ) -> bool {
        self.inner
            .insert_new_transaction(transaction_id, transaction_ttl, packet, priority, cost)
    }

    #[inline]
    fn retry_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction_ttl: SanitizedTransactionTTL<TxB>,
    ) {
        self.inner
            .retry_transaction(transaction_id, transaction_ttl);
    }

    #[inline]
    fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool {
        self.inner.push_id_into_queue(priority_id)
    }

    fn remove_by_id(&mut self, id: &TransactionId) {
        // Removing the transaction from the map will drop the `Bytes` copy.
        self.inner.remove_by_id(id);

        // Re-use the `Bytes` copy by pushing the index back to the stack.
        let index = *id;
        self.bytes_buffer[index as usize].as_mut();
    }

    #[inline]
    fn get_min_max_priority(&self) -> MinMaxResult<u64> {
        self.inner.get_min_max_priority()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::scheduler_messages::MaxAge,
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction,
            hash::Hash,
            message::Message,
            packet::Packet,
            signature::Keypair,
            signer::Signer,
            system_instruction,
            transaction::{SanitizedTransaction, Transaction},
        },
    };

    /// Returns (transaction_ttl, priority, cost)
    fn test_transaction(
        priority: u64,
    ) -> (
        SanitizedTransactionTTL<RuntimeTransaction<SanitizedTransaction>>,
        Arc<ImmutableDeserializedPacket>,
        u64,
        u64,
    ) {
        let from_keypair = Keypair::new();
        let ixs = vec![
            system_instruction::transfer(
                &from_keypair.pubkey(),
                &solana_sdk::pubkey::new_rand(),
                1,
            ),
            ComputeBudgetInstruction::set_compute_unit_price(priority),
        ];
        let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
        let tx = RuntimeTransaction::from_transaction_for_tests(Transaction::new(
            &[&from_keypair],
            message,
            Hash::default(),
        ));
        let packet = Arc::new(
            ImmutableDeserializedPacket::new(
                Packet::from_data(None, tx.to_versioned_transaction()).unwrap(),
            )
            .unwrap(),
        );
        let transaction_ttl = SanitizedTransactionTTL {
            transaction: tx,
            max_age: MaxAge::MAX,
        };
        const TEST_TRANSACTION_COST: u64 = 5000;
        (transaction_ttl, packet, priority, TEST_TRANSACTION_COST)
    }

    fn push_to_container(
        container: &mut TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>>,
        num: usize,
    ) {
        for id in 0..num as u32 {
            let priority = id as u64;
            let (transaction_ttl, packet, priority, cost) = test_transaction(priority);
            container.insert_new_transaction(id, transaction_ttl, packet, priority, cost);
        }
    }

    #[test]
    fn test_is_empty() {
        let mut container = TransactionStateContainer::with_capacity(1);
        assert!(container.is_empty());

        push_to_container(&mut container, 1);
        assert!(!container.is_empty());
    }

    #[test]
    fn test_priority_queue_capacity() {
        let mut container = TransactionStateContainer::with_capacity(1);
        push_to_container(&mut container, 5);

        assert_eq!(container.priority_queue.len(), 1);
        assert_eq!(container.id_to_transaction_state.len(), 1);
        assert_eq!(
            container
                .id_to_transaction_state
                .iter()
                .map(|ts| ts.1.priority())
                .next()
                .unwrap(),
            4
        );
    }

    #[test]
    fn test_get_mut_transaction_state() {
        let mut container = TransactionStateContainer::with_capacity(5);
        push_to_container(&mut container, 5);

        let existing_id = 3;
        let non_existing_id = 7;
        assert!(container.get_mut_transaction_state(&existing_id).is_some());
        assert!(container.get_mut_transaction_state(&existing_id).is_some());
        assert!(container
            .get_mut_transaction_state(&non_existing_id)
            .is_none());
    }
}
