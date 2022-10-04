//! Bitset for marking thread conflicts for transaction scheduling.
//1

#[derive(Debug, Default)]
pub struct ConflictSet {
    /// Bitset of threads that conflict for a transaction.
    bitset: u8, // TODO: support more than 8 threads?
}

impl ConflictSet {
    /// Mark a thread as conflicting in the set.
    pub fn mark_conflict(&mut self, execution_thread_index: usize) {
        assert!(execution_thread_index < 8);
        self.bitset |= 1 << execution_thread_index;
    }

    /// Check if we can schedule to any thread.
    pub fn can_schedule_to_any_thread(&self) -> bool {
        self.num_conflicts() == 0
    }

    /// Check if we can only schedule to a single thread.
    pub fn can_schedule_to_single_thread(&self) -> bool {
        self.num_conflicts() == 1
    }

    /// Check if we can schedule: either to any thread, or to a specific thread.
    pub fn is_schedulable(&self) -> bool {
        self.num_conflicts() <= 1
    }

    /// If we can only schedule to a single thread, return that thread, otherwise return None.
    pub fn get_conflicting_thread_index(&self) -> Option<usize> {
        self.can_schedule_to_single_thread()
            .then(|| self.bitset.trailing_zeros() as usize)
    }

    fn num_conflicts(&self) -> u32 {
        self.bitset.count_ones()
    }
}
