//! Control flow for BankingStage's transaction scheduler.
//!

use {
    super::{
        prio_graph_scheduler::PrioGraphScheduler, scheduler_error::SchedulerError,
        transaction_state::SanitizedTransactionTTL,
        transaction_state_container::TransactionStateContainer,
    },
    crate::banking_stage::{
        consume_worker::ConsumeWorkerMetrics,
        consumer::Consumer,
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
    },
    solana_accounts_db::transaction_error_metrics::TransactionErrorMetrics,
    solana_cost_model::cost_model::CostModel,
    solana_measure::measure_us,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_sdk::{
        clock::MAX_PROCESSING_AGE, saturating_add_assign, timing::AtomicInterval,
        transaction::SanitizedTransaction,
    },
    std::sync::{Arc, RwLock},
};

/// Controls packet and transaction flow into scheduler, and scheduling execution.
pub(crate) struct SchedulerController {
    /// Decision maker for determining what should be done with transactions.
    decision_maker: DecisionMaker,
    bank_forks: Arc<RwLock<BankForks>>,
    /// Container for transaction state.
    /// Shared resource between `receive_worker` and `scheduler`.
    container: Arc<TransactionStateContainer>,
    /// State for scheduling and communicating with worker threads.
    scheduler: PrioGraphScheduler,
    /// Metrics tracking counts on transactions in different states.
    count_metrics: SchedulerCountMetrics,
    /// Metrics tracking time spent in different code sections.
    timing_metrics: SchedulerTimingMetrics,
    /// Metric report handles for the worker threads.
    worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
}

impl SchedulerController {
    pub fn new(
        decision_maker: DecisionMaker,
        container: Arc<TransactionStateContainer>,
        bank_forks: Arc<RwLock<BankForks>>,
        scheduler: PrioGraphScheduler,
        worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
    ) -> Self {
        Self {
            decision_maker,
            bank_forks,
            container,
            scheduler,
            count_metrics: SchedulerCountMetrics::default(),
            timing_metrics: SchedulerTimingMetrics::default(),
            worker_metrics,
        }
    }

    pub fn run(mut self) -> Result<(), SchedulerError> {
        loop {
            // BufferedPacketsDecision is shared with legacy BankingStage, which will forward
            // packets. Initially, not renaming these decision variants but the actions taken
            // are different, since new BankingStage will not forward packets.
            // For `Forward` and `ForwardAndHold`, we want to receive packets but will not
            // forward them to the next leader. In this case, `ForwardAndHold` is
            // indistiguishable from `Hold`.
            //
            // `Forward` will drop packets from the buffer instead of forwarding.
            // During receiving, since packets would be dropped from buffer anyway, we can
            // bypass sanitization and buffering and immediately drop the packets.
            let (decision, decision_time_us) =
                measure_us!(self.decision_maker.make_consume_or_forward_decision());
            saturating_add_assign!(self.timing_metrics.decision_time_us, decision_time_us);

            self.process_transactions(&decision)?;
            self.receive_completed()?;

            // Need to add an exit signal here from the receiving thread!

            // Report metrics only if there is data.
            // Reset intervals when appropriate, regardless of report.
            let should_report = self.count_metrics.has_data();
            self.count_metrics.maybe_report_and_reset(should_report);
            self.timing_metrics.maybe_report_and_reset(should_report);
            self.worker_metrics
                .iter()
                .for_each(|metrics| metrics.maybe_report_and_reset());
        }

        Ok(())
    }

    /// Process packets based on decision.
    fn process_transactions(
        &mut self,
        decision: &BufferedPacketsDecision,
    ) -> Result<(), SchedulerError> {
        match decision {
            BufferedPacketsDecision::Consume(bank_start) => {
                let (scheduling_summary, schedule_time_us) = measure_us!(self.scheduler.schedule(
                    &self.container,
                    |txs, results| {
                        Self::pre_graph_filter(txs, results, &bank_start.working_bank)
                    },
                    |_| true // no pre-lock filter for now
                )?);
                saturating_add_assign!(
                    self.count_metrics.num_scheduled,
                    scheduling_summary.num_scheduled
                );
                saturating_add_assign!(
                    self.count_metrics.num_unschedulable,
                    scheduling_summary.num_unschedulable
                );
                saturating_add_assign!(
                    self.count_metrics.num_schedule_filtered_out,
                    scheduling_summary.num_filtered_out
                );
                saturating_add_assign!(
                    self.timing_metrics.schedule_filter_time_us,
                    scheduling_summary.filter_time_us
                );
                saturating_add_assign!(self.timing_metrics.schedule_time_us, schedule_time_us);
            }
            BufferedPacketsDecision::Forward => {
                let (_, clear_time_us) = measure_us!(self.clear_container());
                saturating_add_assign!(self.timing_metrics.clear_time_us, clear_time_us);
            }
            BufferedPacketsDecision::ForwardAndHold => {
                let (_, clean_time_us) = measure_us!(self.clean_queue());
                saturating_add_assign!(self.timing_metrics.clean_time_us, clean_time_us);
            }
            BufferedPacketsDecision::Hold => {}
        }

        Ok(())
    }

    fn pre_graph_filter(transactions: &[&SanitizedTransaction], results: &mut [bool], bank: &Bank) {
        let lock_results = vec![Ok(()); transactions.len()];
        let mut error_counters = TransactionErrorMetrics::default();
        let check_results = bank.check_transactions(
            transactions,
            &lock_results,
            MAX_PROCESSING_AGE,
            &mut error_counters,
        );

        let fee_check_results: Vec<_> = check_results
            .into_iter()
            .zip(transactions)
            .map(|((result, _nonce), tx)| {
                result?; // if there's already error do nothing
                Consumer::check_fee_payer_unlocked(bank, tx.message(), &mut error_counters)
            })
            .collect();

        for (fee_check_result, result) in fee_check_results.into_iter().zip(results.iter_mut()) {
            *result = fee_check_result.is_ok();
        }
    }

    /// Clears the transaction state container.
    /// This only clears pending transactions, and does **not** clear in-flight transactions.
    fn clear_container(&mut self) {
        while let Some(id) = self.container.pop() {
            self.container.remove_by_id(&id.id);
            saturating_add_assign!(self.count_metrics.num_dropped_on_clear, 1);
        }
    }

    /// Clean unprocessable transactions from the queue. These will be transactions that are
    /// expired, already processed, or are no longer sanitizable.
    /// This only clears pending transactions, and does **not** clear in-flight transactions.
    fn clean_queue(&mut self) {
        // Clean up any transactions that have already been processed, are too old, or do not have
        // valid nonce accounts.
        const MAX_TRANSACTION_CHECKS: usize = 10_000;
        let mut transaction_ids = Vec::with_capacity(MAX_TRANSACTION_CHECKS);

        while let Some(id) = self.container.pop() {
            transaction_ids.push(id);
        }

        let bank = self.bank_forks.read().unwrap().working_bank();

        const CHUNK_SIZE: usize = 128;
        let mut error_counters = TransactionErrorMetrics::default();

        for chunk in transaction_ids.chunks(CHUNK_SIZE) {
            let lock_results = vec![Ok(()); chunk.len()];
            let state_refs: Vec<_> = chunk
                .iter()
                .map(|id| self.container.get_transaction_state(&id.id))
                .collect();

            let sanitized_txs: Vec<_> = state_refs
                .iter()
                .map(|state_ref| {
                    &state_ref
                        .as_ref()
                        .expect("transaction must exist")
                        .transaction_ttl()
                        .transaction
                })
                .collect();

            let check_results = bank.check_transactions(
                &sanitized_txs,
                &lock_results,
                MAX_PROCESSING_AGE,
                &mut error_counters,
            );

            for ((result, _nonce), id) in check_results.into_iter().zip(chunk.iter()) {
                if result.is_err() {
                    saturating_add_assign!(self.count_metrics.num_dropped_on_age_and_status, 1);
                    self.container.remove_by_id(&id.id);
                }
            }
        }
    }

    /// Receives completed transactions from the workers and updates metrics.
    fn receive_completed(&mut self) -> Result<(), SchedulerError> {
        let ((num_transactions, num_retryable), receive_completed_time_us) =
            measure_us!(self.scheduler.receive_completed(&self.container)?);
        saturating_add_assign!(self.count_metrics.num_finished, num_transactions);
        saturating_add_assign!(self.count_metrics.num_retryable, num_retryable);
        saturating_add_assign!(
            self.timing_metrics.receive_completed_time_us,
            receive_completed_time_us
        );
        Ok(())
    }
}

#[derive(Default)]
struct SchedulerCountMetrics {
    interval: AtomicInterval,

    /// Number of packets received.
    num_received: usize,
    /// Number of packets buffered.
    num_buffered: usize,

    /// Number of transactions scheduled.
    num_scheduled: usize,
    /// Number of transactions that were unschedulable.
    num_unschedulable: usize,
    /// Number of transactions that were filtered out during scheduling.
    num_schedule_filtered_out: usize,
    /// Number of completed transactions received from workers.
    num_finished: usize,
    /// Number of transactions that were retryable.
    num_retryable: usize,

    /// Number of transactions that were immediately dropped on receive.
    num_dropped_on_receive: usize,
    /// Number of transactions that were dropped due to sanitization failure.
    num_dropped_on_sanitization: usize,
    /// Number of transactions that were dropped due to failed lock validation.
    num_dropped_on_validate_locks: usize,
    /// Number of transactions that were dropped due to failed transaction
    /// checks during receive.
    num_dropped_on_receive_transaction_checks: usize,
    /// Number of transactions that were dropped due to clearing.
    num_dropped_on_clear: usize,
    /// Number of transactions that were dropped due to age and status checks.
    num_dropped_on_age_and_status: usize,
    /// Number of transactions that were dropped due to exceeded capacity.
    num_dropped_on_capacity: usize,
}

impl SchedulerCountMetrics {
    fn maybe_report_and_reset(&mut self, should_report: bool) {
        const REPORT_INTERVAL_MS: u64 = 1000;
        if self.interval.should_update(REPORT_INTERVAL_MS) {
            if should_report {
                self.report();
            }
            self.reset();
        }
    }

    fn report(&self) {
        datapoint_info!(
            "banking_stage_scheduler_counts",
            ("num_received", self.num_received, i64),
            ("num_buffered", self.num_buffered, i64),
            ("num_scheduled", self.num_scheduled, i64),
            ("num_unschedulable", self.num_unschedulable, i64),
            (
                "num_schedule_filtered_out",
                self.num_schedule_filtered_out,
                i64
            ),
            ("num_finished", self.num_finished, i64),
            ("num_retryable", self.num_retryable, i64),
            ("num_dropped_on_receive", self.num_dropped_on_receive, i64),
            (
                "num_dropped_on_sanitization",
                self.num_dropped_on_sanitization,
                i64
            ),
            (
                "num_dropped_on_validate_locks",
                self.num_dropped_on_validate_locks,
                i64
            ),
            (
                "num_dropped_on_receive_transaction_checks",
                self.num_dropped_on_receive_transaction_checks,
                i64
            ),
            ("num_dropped_on_clear", self.num_dropped_on_clear, i64),
            (
                "num_dropped_on_age_and_status",
                self.num_dropped_on_age_and_status,
                i64
            ),
            ("num_dropped_on_capacity", self.num_dropped_on_capacity, i64)
        );
    }

    fn has_data(&self) -> bool {
        self.num_received != 0
            || self.num_buffered != 0
            || self.num_scheduled != 0
            || self.num_unschedulable != 0
            || self.num_schedule_filtered_out != 0
            || self.num_finished != 0
            || self.num_retryable != 0
            || self.num_dropped_on_receive != 0
            || self.num_dropped_on_sanitization != 0
            || self.num_dropped_on_validate_locks != 0
            || self.num_dropped_on_receive_transaction_checks != 0
            || self.num_dropped_on_clear != 0
            || self.num_dropped_on_age_and_status != 0
            || self.num_dropped_on_capacity != 0
    }

    fn reset(&mut self) {
        self.num_received = 0;
        self.num_buffered = 0;
        self.num_scheduled = 0;
        self.num_unschedulable = 0;
        self.num_schedule_filtered_out = 0;
        self.num_finished = 0;
        self.num_retryable = 0;
        self.num_dropped_on_receive = 0;
        self.num_dropped_on_sanitization = 0;
        self.num_dropped_on_validate_locks = 0;
        self.num_dropped_on_receive_transaction_checks = 0;
        self.num_dropped_on_clear = 0;
        self.num_dropped_on_age_and_status = 0;
        self.num_dropped_on_capacity = 0;
    }
}

#[derive(Default)]
struct SchedulerTimingMetrics {
    interval: AtomicInterval,
    /// Time spent making processing decisions.
    decision_time_us: u64,
    /// Time spent receiving packets.
    receive_time_us: u64,
    /// Time spent buffering packets.
    buffer_time_us: u64,
    /// Time spent filtering transactions during scheduling.
    schedule_filter_time_us: u64,
    /// Time spent scheduling transactions.
    schedule_time_us: u64,
    /// Time spent clearing transactions from the container.
    clear_time_us: u64,
    /// Time spent cleaning expired or processed transactions from the container.
    clean_time_us: u64,
    /// Time spent receiving completed transactions.
    receive_completed_time_us: u64,
}

impl SchedulerTimingMetrics {
    fn maybe_report_and_reset(&mut self, should_report: bool) {
        const REPORT_INTERVAL_MS: u64 = 1000;
        if self.interval.should_update(REPORT_INTERVAL_MS) {
            if should_report {
                self.report();
            }
            self.reset();
        }
    }

    fn report(&self) {
        datapoint_info!(
            "banking_stage_scheduler_timing",
            ("decision_time_us", self.decision_time_us, i64),
            ("receive_time_us", self.receive_time_us, i64),
            ("buffer_time_us", self.buffer_time_us, i64),
            ("schedule_filter_time_us", self.schedule_filter_time_us, i64),
            ("schedule_time_us", self.schedule_time_us, i64),
            ("clear_time_us", self.clear_time_us, i64),
            ("clean_time_us", self.clean_time_us, i64),
            (
                "receive_completed_time_us",
                self.receive_completed_time_us,
                i64
            )
        );
    }

    fn reset(&mut self) {
        self.decision_time_us = 0;
        self.receive_time_us = 0;
        self.buffer_time_us = 0;
        self.schedule_filter_time_us = 0;
        self.schedule_time_us = 0;
        self.clear_time_us = 0;
        self.clean_time_us = 0;
        self.receive_completed_time_us = 0;
    }
}
