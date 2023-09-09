use {
    crate::banking_stage::scheduler_messages::TransactionId,
    prio_graph::PriorityId,
    std::fmt::{Display, Formatter},
};

/// Pair transaction id with priority for use in the priority queue
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TransactionPriorityId {
    pub(crate) priority: u64,
    pub(crate) id: TransactionId,
}

impl TransactionPriorityId {
    pub(crate) fn new(priority: u64, id: TransactionId) -> Self {
        Self { priority, id }
    }
}

impl Ord for TransactionPriorityId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for TransactionPriorityId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for TransactionPriorityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(id: {}, priority: {})", self.id, self.priority)
    }
}
