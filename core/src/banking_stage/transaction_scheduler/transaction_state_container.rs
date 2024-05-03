use {
    super::{
        transaction_priority_id::TransactionPriorityId,
        transaction_state::{SanitizedTransactionTTL, TransactionState},
    },
    crate::{
        banking_stage::{
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            scheduler_messages::TransactionId,
        },
        transaction_view::TransactionView,
    },
    itertools::MinMaxResult,
    min_max_heap::MinMaxHeap,
    solana_sdk::packet::Packet,
    std::{collections::HashMap, sync::Arc},
    valet::Valet,
};

/// This structure will hold `TransactionState` for the entirety of a
/// transaction's lifetime in the scheduler and BankingStage as a whole.
///
/// Transaction Lifetime:
/// 1. Received from `SigVerify` by `BankingStage`
/// 2. Inserted into `TransactionStateContainer` by `BankingStage`
/// 3. Popped in priority-order by scheduler, and transitioned to `Pending` state
/// 4. Processed by `ConsumeWorker`
///   a. If consumed, remove `Pending` state from the `TransactionStateContainer`
///   b. If retryable, transition back to `Unprocessed` state.
///      Re-insert to the queue, and return to step 3.
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
pub(crate) struct TransactionStateContainer {
    priority_queue: MinMaxHeap<TransactionPriorityId>,
    id_to_transaction_state: Valet<TransactionView>,
}

impl TransactionStateContainer {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            priority_queue: MinMaxHeap::with_capacity(capacity),
            id_to_transaction_state: Valet::with_capacity(capacity + 10_000), // some additional room for txs we are buffering
        }
    }

    /// Returns true if the queue is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.priority_queue.is_empty()
    }

    /// Returns the remaining capacity of the queue
    pub(crate) fn remaining_queue_capacity(&self) -> usize {
        self.priority_queue.capacity() - self.priority_queue.len()
    }

    /// Get the top transaction id in the priority queue.
    pub(crate) fn pop(&mut self) -> Option<TransactionPriorityId> {
        self.priority_queue.pop_max()
    }

    /// Insert a packet
    pub(crate) fn insert_new_packet(&mut self, packet: &Packet) -> bool {
        let transaction_id = self.id_to_transaction_state.get_free_index().unwrap();
        let inserted = self
            .id_to_transaction_state
            .with_mut(transaction_id, |transaction_view| {
                transaction_view.populate_from(packet);
                transaction_view.sanitize();
            })
            .is_ok();

        // Ignore priority for now - all packets are equal in test case.
        // Priority = reward / cost
        // cost = 720 + 150 + 150 + 300 = 1320
        // reward = 2500
        // priority = 2500 / 1320 => 1
        let priority = 1;
        let priority_id = TransactionPriorityId::new(priority, transaction_id);
        self.push_id_into_queue(priority_id)
    }

    // /// Insert a new transaction into the container's queues and maps.
    // /// Returns `true` if a packet was dropped due to capacity limits.
    // pub(crate) fn insert_new_transaction(
    //     &mut self,
    //     transaction_ttl: SanitizedTransactionTTL,
    //     packet: Arc<ImmutableDeserializedPacket>,
    //     priority: u64,
    //     cost: u64,
    // ) -> bool {
    //     let transaction_id = self
    //         .id_to_transaction_state
    //         .insert(TransactionState::new(
    //             transaction_ttl,
    //             packet,
    //             priority,
    //             cost,
    //         ))
    //         .unwrap_or_else(|_| panic!("womp"));
    //     let priority_id = TransactionPriorityId::new(priority, transaction_id);
    //     self.push_id_into_queue(priority_id)
    // }

    /// Pushes a transaction id into the priority queue. If the queue is full, the lowest priority
    /// transaction will be dropped (removed from the queue and map).
    /// Returns `true` if a packet was dropped due to capacity limits.
    pub(crate) fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool {
        if self.remaining_queue_capacity() == 0 {
            let popped_id = self.priority_queue.push_pop_min(priority_id);
            self.remove_by_id(&popped_id.id);
            true
        } else {
            self.priority_queue.push(priority_id);
            false
        }
    }

    /// Remove transaction by id.
    pub(crate) fn remove_by_id(&mut self, id: &TransactionId) {
        // release index - does not clear it out
        self.id_to_transaction_state.release(*id).unwrap();
    }

    pub(crate) fn get_min_max_priority(&self) -> MinMaxResult<u64> {
        match self.priority_queue.peek_min() {
            Some(min) => match self.priority_queue.peek_max() {
                Some(max) => MinMaxResult::MinMax(min.priority, max.priority),
                None => MinMaxResult::OneElement(min.priority),
            },
            None => MinMaxResult::NoElements,
        }
    }
}
