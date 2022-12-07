use {
    super::{
        consume_executor::ConsumeExecutor, decision_maker::DecisionMaker,
        forward_executor::ForwardExecutor, packet_receiver::PacketReceiver,
        scheduler_error::SchedulerError, thread_local_scheduler::ThreadLocalScheduler,
    },
    crate::{
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        tracer_packet_stats::TracerPacketStats,
        unprocessed_transaction_storage::UnprocessedTransactionStorage,
    },
};

pub(crate) enum SchedulerHandle {
    ThreadLocalScheduler(ThreadLocalScheduler),
}

impl SchedulerHandle {
    pub fn new_thread_local_scheduler(
        id: u32,
        decision_maker: DecisionMaker,
        unprocessed_transaction_storage: UnprocessedTransactionStorage,
        packet_receiver: PacketReceiver,
    ) -> Self {
        Self::ThreadLocalScheduler(ThreadLocalScheduler::new(
            id,
            decision_maker,
            unprocessed_transaction_storage,
            packet_receiver,
        ))
    }

    /// Do necessary updates to the scheduler interface
    pub fn tick(&mut self) -> Result<(), SchedulerError> {
        match self {
            Self::ThreadLocalScheduler(thread_local_scheduler) => thread_local_scheduler.tick(),
        }
    }

    /// Do work that is scheduled
    pub fn do_scheduled_work(
        &mut self,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        tracer_packet_stats: &mut TracerPacketStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        match self {
            Self::ThreadLocalScheduler(thread_local_scheduler) => thread_local_scheduler
                .do_scheduled_work(
                    consume_executor,
                    forward_executor,
                    tracer_packet_stats,
                    slot_metrics_tracker,
                ),
        }
    }
}
