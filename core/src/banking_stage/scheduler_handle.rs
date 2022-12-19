use {
    super::{
        consume_executor::ConsumeExecutor, decision_maker::DecisionMaker,
        external_scheduler::ExternalSchedulerHandle, forward_executor::ForwardExecutor,
        packet_receiver::PacketReceiver, scheduler_error::SchedulerError,
        thread_local_scheduler::ThreadLocalScheduler,
    },
    crate::{
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        scheduler_stage::{ProcessedTransactionsSender, ScheduledTransactionsReceiver},
        tracer_packet_stats::TracerPacketStats,
        unprocessed_transaction_storage::UnprocessedTransactionStorage,
    },
    solana_runtime::bank_forks::BankForks,
    std::sync::{Arc, RwLock},
};

#[allow(dead_code)]
pub(crate) enum SchedulerHandle {
    ThreadLocalScheduler(ThreadLocalScheduler),
    ExternalScheduler(ExternalSchedulerHandle),
}

impl SchedulerHandle {
    pub fn new_thread_local_scheduler(
        id: u32,
        decision_maker: DecisionMaker,
        unprocessed_transaction_storage: UnprocessedTransactionStorage,
        bank_forks: Arc<RwLock<BankForks>>,
        packet_receiver: PacketReceiver,
    ) -> Self {
        Self::ThreadLocalScheduler(ThreadLocalScheduler::new(
            id,
            decision_maker,
            unprocessed_transaction_storage,
            bank_forks,
            packet_receiver,
        ))
    }

    pub fn new_external_scheduler(
        id: u32,
        scheduled_transactions_receiver: ScheduledTransactionsReceiver,
        processed_transactions_sender: ProcessedTransactionsSender,
    ) -> Self {
        Self::ExternalScheduler(ExternalSchedulerHandle::new(
            id,
            scheduled_transactions_receiver,
            processed_transactions_sender,
        ))
    }

    /// Do necessary updates to the scheduler interface
    pub fn tick(&mut self) -> Result<(), SchedulerError> {
        match self {
            Self::ThreadLocalScheduler(thread_local_scheduler) => thread_local_scheduler.tick(),
            Self::ExternalScheduler(external_scheduler) => external_scheduler.tick(),
        }
    }

    /// Do work that is scheduled
    pub fn do_scheduled_work(
        &mut self,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        tracer_packet_stats: &mut TracerPacketStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> Result<(), SchedulerError> {
        match self {
            Self::ThreadLocalScheduler(thread_local_scheduler) => thread_local_scheduler
                .do_scheduled_work(
                    consume_executor,
                    forward_executor,
                    tracer_packet_stats,
                    slot_metrics_tracker,
                ),
            Self::ExternalScheduler(external_scheduler) => external_scheduler.do_scheduled_work(
                consume_executor,
                forward_executor,
                slot_metrics_tracker,
            ),
        }
    }
}
