use {
    super::{
        batch_id_generator::BatchIdGenerator, thread_aware_account_locks::ThreadAwareAccountLocks,
        transaction_packet_container::TransactionPacketContainer,
    },
    crate::banking_stage::scheduler_messages::{ConsumeWork, FinishedConsumeWork},
    crossbeam_channel::{Receiver, Sender},
    itertools::Itertools,
    prio_graph::PrioGraph,
    solana_sdk::pubkey::Pubkey,
};

pub(crate) struct PrioGraphScheduler {
    account_locks: ThreadAwareAccountLocks<Pubkey>,
    batch_id_generator: BatchIdGenerator,
    consume_work_senders: Vec<Sender<ConsumeWork>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
}

impl PrioGraphScheduler {
    pub(crate) fn new(
        account_locks: ThreadAwareAccountLocks<Pubkey>,
        batch_id_generator: BatchIdGenerator,
        consume_work_senders: Vec<Sender<ConsumeWork>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
    ) -> Self {
        Self {
            account_locks,
            batch_id_generator,
            consume_work_senders,
            finished_consume_work_receiver,
        }
    }

    pub(crate) fn schedule(&mut self, container: &mut TransactionPacketContainer) {
        const MAX_TRANSACTIONS_PER_SCHEDULING_PASS: usize = 10_000;
        let ids = container
            .take_top_n(MAX_TRANSACTIONS_PER_SCHEDULING_PASS)
            .collect_vec();

        // let mut graph = PrioGraph::new();
    }
}
