use {
    super::thread_aware_account_locks::ThreadId,
    crate::banking_stage::scheduler_messages::TransactionBatchId,
    dashmap::DashMap,
    std::sync::atomic::{AtomicUsize, Ordering},
};

pub struct InFlightTracker {
    num_in_flight: AtomicUsize,
    num_in_flight_per_thread: Vec<AtomicUsize>,
    batch_id_to_thread_id: DashMap<TransactionBatchId, ThreadId>,
}

impl InFlightTracker {
    pub fn new(num_threads: usize) -> Self {
        Self {
            num_in_flight: AtomicUsize::new(0),
            num_in_flight_per_thread: (0..num_threads).map(|_| AtomicUsize::new(0)).collect(),
            batch_id_to_thread_id: DashMap::new(),
        }
    }

    pub fn track_batch(
        &self,
        batch_id: TransactionBatchId,
        num_transactions: usize,
        thread_id: ThreadId,
    ) {
        self.num_in_flight
            .fetch_add(num_transactions, Ordering::Relaxed);
        self.num_in_flight_per_thread[thread_id].fetch_add(num_transactions, Ordering::Relaxed);
        self.batch_id_to_thread_id.insert(batch_id, thread_id);
    }

    // returns the thread id of the batch
    pub fn complete_batch(
        &self,
        batch_id: TransactionBatchId,
        num_transactions: usize,
    ) -> ThreadId {
        let (_batch_id, thread_id) = self
            .batch_id_to_thread_id
            .remove(&batch_id)
            .expect("transaction batch id should exist in in-flight tracker");
        self.num_in_flight
            .fetch_sub(num_transactions, Ordering::Relaxed);
        self.num_in_flight_per_thread[thread_id].fetch_sub(num_transactions, Ordering::Relaxed);

        thread_id
    }

    pub fn num_in_flight_for_thread(&self, thread_id: ThreadId) -> usize {
        self.num_in_flight_per_thread[thread_id].load(Ordering::Relaxed)
    }

    pub fn num_in_flight_per_thread(&self) -> &[AtomicUsize] {
        &self.num_in_flight_per_thread
    }
}
