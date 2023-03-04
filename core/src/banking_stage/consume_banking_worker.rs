use {
    super::{consumer::Consumer, decision_maker::BufferedPacketsDecision},
    crossbeam_channel::{Receiver, Sender},
    solana_poh::poh_recorder::BankStart,
    solana_sdk::transaction::SanitizedTransaction,
};

pub struct ScheduledWork {
    pub decision: BufferedPacketsDecision,
    pub transactions: Vec<SanitizedTransaction>,
}

pub struct FinishedWork {
    pub decision: BufferedPacketsDecision,
    pub transactions: Vec<SanitizedTransaction>,
    pub retryable_indexes: Vec<usize>,
}

pub struct ConsumeBankingWorker {
    receiver: Receiver<ScheduledWork>,
    sender: Sender<FinishedWork>,
    consumer: Consumer,
}

impl ConsumeBankingWorker {
    pub fn new(
        receiver: Receiver<ScheduledWork>,
        sender: Sender<FinishedWork>,
        consumer: Consumer,
    ) -> Self {
        Self {
            receiver,
            sender,
            consumer,
        }
    }
    pub fn run(self) {
        while let Ok(scheduled_work) = self.receiver.recv() {
            let ScheduledWork {
                decision,
                transactions,
            } = scheduled_work;

            let retryable_indexes = match &decision {
                BufferedPacketsDecision::Consume(bank_start) => {
                    self.consume(bank_start, &transactions)
                }
                BufferedPacketsDecision::Forward | BufferedPacketsDecision::ForwardAndHold => {
                    panic!("Should not be forwarding packets")
                }
                BufferedPacketsDecision::Hold => panic!("Should not be holding packets"),
            };

            let finished_work = FinishedWork {
                decision,
                transactions,
                retryable_indexes,
            };
            self.sender.send(finished_work).unwrap();
        }
    }

    fn consume(&self, bank_start: &BankStart, transactions: &[SanitizedTransaction]) -> Vec<usize> {
        let summary = self.consumer.process_and_record_transactions(
            &bank_start.working_bank,
            transactions,
            0,
        );

        summary
            .execute_and_commit_transactions_output
            .retryable_transaction_indexes
    }
}
