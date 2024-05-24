use {
    solana_sdk::{clock::Slot, packet::PacketFlags, transaction::SanitizedTransaction},
    solana_signed_message::SignedMessage,
};

/// Simple wrapper type to tie a sanitized transaction to max age slot.
pub(crate) struct SanitizedTransactionTTL<T: SignedMessage> {
    pub(crate) transaction: T,
    pub(crate) max_age_slot: Slot,
}

/// TransactionState is used to track the state of a transaction in the transaction scheduler
/// and banking stage as a whole.
///
/// There are two states a transaction can be in:
///     1. `Unprocessed` - The transaction is available for scheduling.
///     2. `Pending` - The transaction is currently scheduled or being processed.
///
/// Newly received transactions are initially in the `Unprocessed` state.
/// When a transaction is scheduled, it is transitioned to the `Pending` state,
///   using the `transition_to_pending` method.
/// When a transaction finishes processing it may be retryable. If it is retryable,
///   the transaction is transitioned back to the `Unprocessed` state using the
///   `transition_to_unprocessed` method. If it is not retryable, the state should
///   be dropped.
///
/// For performance, when a transaction is transitioned to the `Pending` state, the
///   internal `SanitizedTransaction` is moved out of the `TransactionState` and sent
///   to the appropriate thread for processing. This is done to avoid cloning the
///  `SanitizedTransaction`.
pub(crate) struct TransactionState<T: SignedMessage> {
    transaction_ttl: SanitizedTransactionTTL<T>,
    priority: u64,
    cost: u64,
    should_forward: bool,
}

impl<T: SignedMessage> TransactionState<T> {
    /// Creates a new `TransactionState` in the `Unprocessed` state.
    pub(crate) fn new(
        packet_flags: PacketFlags,
        transaction_ttl: SanitizedTransactionTTL<T>,
        priority: u64,
        cost: u64,
    ) -> Self {
        let should_forward = !packet_flags.contains(PacketFlags::FORWARDED)
            && packet_flags.contains(PacketFlags::FROM_STAKED_NODE);
        Self {
            transaction_ttl,
            priority,
            cost,
            should_forward,
        }
    }

    /// Return the priority of the transaction.
    /// This is *not* the same as the `compute_unit_price` of the transaction.
    /// The priority is used to order transactions for processing.
    pub(crate) fn priority(&self) -> u64 {
        self.priority
    }

    pub(crate) fn set_priority(&mut self, priority: u64) {
        self.priority = priority;
    }

    /// Return the cost of the transaction.
    pub(crate) fn cost(&self) -> u64 {
        self.cost
    }

    /// Set the cost of the transaction.
    pub(crate) fn set_cost(&mut self, cost: u64) {
        self.cost = cost;
    }

    /// Return whether packet should be attempted to be forwarded.
    pub(crate) fn should_forward(&self) -> bool {
        self.should_forward
    }

    /// Set the should_forward flag.
    pub(crate) fn set_should_forward(&mut self, should_forward: bool) {
        self.should_forward = should_forward;
    }

    /// Mark the packet as forwarded.
    /// This is used to prevent the packet from being forwarded multiple times.
    pub(crate) fn mark_forwarded(&mut self) {
        self.should_forward = false;
    }

    /// Get a reference to the `SanitizedTransactionTTL` for the transaction.
    ///
    /// # Panics
    /// This method will panic if the transaction is in the `Pending` state.
    pub(crate) fn transaction_ttl(&self) -> &SanitizedTransactionTTL<T> {
        &self.transaction_ttl
    }

    /// Get a mutable refernce to the `SanitizedTransactionTTL`` for the transaction.
    pub(crate) fn mut_transaction_ttl(&mut self) -> &mut SanitizedTransactionTTL<T> {
        &mut self.transaction_ttl
    }
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
            system_instruction,
            transaction::{SanitizedTransaction, Transaction},
        },
    };

    fn create_transaction_state(compute_unit_price: u64) -> TransactionState<SanitizedTransaction> {
        let from_keypair = Keypair::new();
        let ixs = vec![
            system_instruction::transfer(
                &from_keypair.pubkey(),
                &solana_sdk::pubkey::new_rand(),
                1,
            ),
            ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price),
        ];
        let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
        let tx = Transaction::new(&[&from_keypair], message, Hash::default());

        let transaction_ttl = SanitizedTransactionTTL {
            transaction: SanitizedTransaction::from_transaction_for_tests(tx),
            max_age_slot: Slot::MAX,
        };
        const TEST_TRANSACTION_COST: u64 = 5000;
        TransactionState::new(
            PacketFlags::empty(),
            transaction_ttl,
            compute_unit_price,
            TEST_TRANSACTION_COST,
        )
    }

    #[test]
    fn test_priority() {
        let priority = 15;
        let transaction_state = create_transaction_state(priority);
        assert_eq!(transaction_state.priority(), priority);
    }

    #[test]
    fn test_transaction_ttl() {
        let transaction_state = create_transaction_state(0);
        let transaction_ttl = transaction_state.transaction_ttl();
        assert_eq!(transaction_ttl.max_age_slot, Slot::MAX);
    }
}
