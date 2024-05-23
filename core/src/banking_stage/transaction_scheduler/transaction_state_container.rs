use {
    super::{
        transaction_priority_id::TransactionPriorityId,
        transaction_state::{SanitizedTransactionTTL, TransactionState},
    },
    crate::{banking_stage::scheduler_messages::TransactionId, transaction_view::TransactionView},
    itertools::MinMaxResult,
    min_max_heap::MinMaxHeap,
    solana_sdk::{packet::PacketFlags, transaction::SanitizedTransaction},
    solana_signed_message::SignedMessage,
    std::sync::Arc,
    valet::{ConcurrentValet, ValetBasics, ValetInsert, ValetWith},
};

// Rust impls do not support non-overlapping traits, so we are stuck with a
// hacky macro solution.
macro_rules! define_container {
    ($name:ident, $tx:ty, $valet:ty) => {
        pub(crate) struct $name {
            priority_queue: MinMaxHeap<TransactionPriorityId>,
            id_to_transaction_state: Arc<$valet>,
        }

        #[allow(dead_code)]
        impl $name {
            /// Half capacity priority-queue
            pub(crate) fn with_valet(id_to_transaction_state: Arc<$valet>) -> Self {
                Self {
                    priority_queue: MinMaxHeap::with_capacity(
                        id_to_transaction_state.capacity() / 2,
                    ),
                    id_to_transaction_state,
                }
            }

            /// Returns true if the queue is empty.
            fn is_empty(&self) -> bool {
                self.priority_queue.is_empty()
            }

            /// Returns the remaining capacity of the queue
            fn remaining_queue_capacity(&self) -> usize {
                self.priority_queue.capacity() - self.priority_queue.len()
            }

            /// Get the top transaction id in the priority queue.
            fn pop(&mut self) -> Option<TransactionPriorityId> {
                self.priority_queue.pop_max()
            }

            /// Perform operation with immutable transaction state.
            fn with_transaction_state<R>(
                &self,
                id: &TransactionId,
                f: impl FnOnce(&TransactionState<$tx>) -> R,
            ) -> Option<R> {
                self.id_to_transaction_state.with(*id, f).ok()
            }

            /// Perform batch operation with immutable transaction state.
            fn batched_with_ref_transaction_state<const SIZE: usize, M, R>(
                &self,
                ids: &[TransactionId],
                m: impl Fn(&TransactionState<$tx>) -> &M,
                f: impl FnOnce(&[&M]) -> R,
            ) -> Option<R> {
                self.id_to_transaction_state
                    .batched_with_ref::<SIZE, _, _>(ids, m, f)
                    .ok()
            }

            /// Perform operation with mutable transaction state.
            fn with_mut_transaction_state<R>(
                &mut self,
                id: &TransactionId,
                f: impl FnOnce(&mut TransactionState<$tx>) -> R,
            ) -> Option<R> {
                self.id_to_transaction_state.with_mut(*id, f).ok()
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

            /// Pushes a transaction id into the priority queue. If the queue is full, the lowest priority
            /// transaction will be dropped (removed from the queue and map).
            /// Returns `true` if a packet was dropped due to capacity limits.
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

            /// Retries a transaction - inserts transaction back into map (but not packet).
            fn retry_transaction(&mut self, transaction_id: TransactionId) {
                let priority = self
                    .id_to_transaction_state
                    .with(transaction_id, |state| state.priority())
                    .expect("transaction must exist");
                let priority_id = TransactionPriorityId::new(priority, transaction_id);
                self.push_id_into_queue(priority_id);
            }
        }

        impl TransactionStateContainerInterface<$tx> for $name {
            /// Returns true if the queue is empty.
            fn is_empty(&self) -> bool {
                $name::is_empty(self)
            }

            /// Returns the remaining capacity of the queue
            fn remaining_queue_capacity(&self) -> usize {
                $name::remaining_queue_capacity(self)
            }

            /// Get the top transaction id in the priority queue.
            fn pop(&mut self) -> Option<TransactionPriorityId> {
                $name::pop(self)
            }

            /// Perform operation with immutable transaction state.
            fn with_transaction_state<R>(
                &self,
                id: &TransactionId,
                f: impl FnOnce(&TransactionState<$tx>) -> R,
            ) -> Option<R> {
                $name::with_transaction_state(self, id, f)
            }

            /// Perform batch operation with immutable transaction state.
            fn batched_with_ref_transaction_state<const SIZE: usize, M, R>(
                &self,
                ids: &[TransactionId],
                m: impl Fn(&TransactionState<$tx>) -> &M,
                f: impl FnOnce(&[&M]) -> R,
            ) -> Option<R> {
                $name::batched_with_ref_transaction_state::<SIZE, M, R>(self, ids, m, f)
            }

            /// Perform operation with mutable transaction state.
            fn with_mut_transaction_state<R>(
                &mut self,
                id: &TransactionId,
                f: impl FnOnce(&mut TransactionState<$tx>) -> R,
            ) -> Option<R> {
                $name::with_mut_transaction_state(self, id, f)
            }

            fn get_min_max_priority(&self) -> MinMaxResult<u64> {
                $name::get_min_max_priority(self)
            }

            /// Pushes a transaction id into the priority queue. If the queue is full, the lowest priority
            /// transaction will be dropped (removed from the queue and map).
            /// Returns `true` if a packet was dropped due to capacity limits.
            fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool {
                $name::push_id_into_queue(self, priority_id)
            }

            /// Retries a transaction - inserts transaction back into map (but not packet).
            fn retry_transaction(&mut self, transaction_id: TransactionId) {
                $name::retry_transaction(self, transaction_id)
            }

            fn remove_by_id(&mut self, transaction_id: &TransactionId) {
                $name::remove_by_id(self, transaction_id)
            }
        }
    };
}

define_container!(
    SanitizedTransactionStateContainer,
    SanitizedTransaction,
    ConcurrentValet<TransactionState<SanitizedTransaction>>
);

impl SanitizedTransactionStateContainer {
    fn remove_by_id(&mut self, transaction_id: &TransactionId) {
        self.id_to_transaction_state
            .remove(*transaction_id)
            .expect("transaction must exist");
    }

    /// Insert a new transaction into the container's queues and maps.
    /// Returns `true` if a packet was dropped due to capacity limits.
    pub(crate) fn insert_new_transaction(
        &mut self,
        packet_flags: PacketFlags,
        transaction_ttl: SanitizedTransactionTTL<SanitizedTransaction>,
        priority: u64,
        cost: u64,
    ) -> bool {
        let transaction_id = self
            .id_to_transaction_state
            .insert(TransactionState::new(
                packet_flags,
                transaction_ttl,
                priority,
                cost,
            ))
            .unwrap_or_else(|_| panic!("container must not be full"));
        let priority_id = TransactionPriorityId::new(priority, transaction_id);
        self.push_id_into_queue(priority_id)
    }
}

define_container!(
    TransactionViewStateContainer,
    TransactionView,
    ConcurrentValet<TransactionState<TransactionView>>
);

impl TransactionViewStateContainer {
    fn remove_by_id(&mut self, transaction_id: &TransactionId) {
        self.id_to_transaction_state
            .remove(*transaction_id)
            .expect("transaction must exist");
    }

    /// Insert a new transaction into the container's queues and maps.
    /// Returns `true` if a packet was dropped due to capacity limits.
    pub(crate) fn insert_new_transaction(
        &mut self,
        packet_flags: PacketFlags,
        transaction_ttl: SanitizedTransactionTTL<TransactionView>,
        priority: u64,
        cost: u64,
    ) -> bool {
        let transaction_id = self
            .id_to_transaction_state
            .insert(TransactionState::new(
                packet_flags,
                transaction_ttl,
                priority,
                cost,
            ))
            .unwrap_or_else(|_| panic!("container must not be full"));
        let priority_id = TransactionPriorityId::new(priority, transaction_id);
        self.push_id_into_queue(priority_id)
    }
}

pub trait TransactionStateContainerInterface<T: SignedMessage> {
    fn is_empty(&self) -> bool;
    fn remaining_queue_capacity(&self) -> usize;
    fn pop(&mut self) -> Option<TransactionPriorityId>;
    fn with_transaction_state<R>(
        &self,
        id: &TransactionId,
        f: impl FnOnce(&TransactionState<T>) -> R,
    ) -> Option<R>;
    fn batched_with_ref_transaction_state<const SIZE: usize, M, R>(
        &self,
        ids: &[TransactionId],
        m: impl Fn(&TransactionState<T>) -> &M,
        f: impl FnOnce(&[&M]) -> R,
    ) -> Option<R>;
    fn with_mut_transaction_state<R>(
        &mut self,
        id: &TransactionId,
        f: impl FnOnce(&mut TransactionState<T>) -> R,
    ) -> Option<R>;
    fn retry_transaction(&mut self, transaction_id: TransactionId);
    fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool;
    fn remove_by_id(&mut self, id: &TransactionId);
    fn get_min_max_priority(&self) -> MinMaxResult<u64>;
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction,
            hash::Hash,
            message::Message,
            signature::Keypair,
            signer::Signer,
            slot_history::Slot,
            system_instruction,
            transaction::{SanitizedTransaction, Transaction},
        },
        valet::ConcurrentValet,
    };

    /// Returns (transaction_ttl, priority, cost)
    fn test_transaction(
        priority: u64,
    ) -> (SanitizedTransactionTTL<SanitizedTransaction>, u64, u64) {
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
        let tx = SanitizedTransaction::from_transaction_for_tests(Transaction::new(
            &[&from_keypair],
            message,
            Hash::default(),
        ));
        let transaction_ttl = SanitizedTransactionTTL {
            transaction: tx,
            max_age_slot: Slot::MAX,
        };
        const TEST_TRANSACTION_COST: u64 = 5000;
        (transaction_ttl, priority, TEST_TRANSACTION_COST)
    }

    fn push_to_container(container: &mut SanitizedTransactionStateContainer, num: usize) {
        for priority in 0..num as u64 {
            let (transaction_ttl, priority, cost) = test_transaction(priority);
            container.insert_new_transaction(PacketFlags::empty(), transaction_ttl, priority, cost);
        }
    }

    #[test]
    fn test_is_empty() {
        let mut container = SanitizedTransactionStateContainer::with_valet(Arc::new(
            ConcurrentValet::with_capacity(1),
        ));
        assert!(container.is_empty());

        push_to_container(&mut container, 1);
        assert!(!container.is_empty());
    }

    #[test]
    fn test_priority_queue_capacity() {
        let mut container = SanitizedTransactionStateContainer::with_valet(Arc::new(
            ConcurrentValet::with_capacity(1),
        ));
        push_to_container(&mut container, 5);

        assert_eq!(container.priority_queue.len(), 1);
        assert_eq!(container.id_to_transaction_state.len(), 1);
    }

    #[test]
    fn test_with_mut_transaction_state() {
        let mut container = SanitizedTransactionStateContainer::with_valet(Arc::new(
            ConcurrentValet::with_capacity(5),
        ));
        push_to_container(&mut container, 5);

        let existing_id = 3;
        let non_existing_id = 7;
        assert!(container
            .with_mut_transaction_state(&existing_id, |_| ())
            .is_some());
        assert!(container
            .with_mut_transaction_state(&existing_id, |_| ())
            .is_some());
        assert!(container
            .with_mut_transaction_state(&non_existing_id, |_| ())
            .is_none());
    }
}
