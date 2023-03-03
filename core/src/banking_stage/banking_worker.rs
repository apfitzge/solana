use {
    super::{
        consumer::Consumer, decision_maker::BufferedPacketsDecision, forwarder::Forwarder,
        BankingStageStats, ForwardOption,
    },
    crate::unprocessed_packet_batches::DeserializedPacket,
    crossbeam_channel::{Receiver, Sender},
    solana_poh::poh_recorder::BankStart,
    solana_sdk::transaction::SanitizedTransaction,
};

pub struct ScheduledWork {
    pub decision: BufferedPacketsDecision,
    pub packets: Vec<DeserializedPacket>,
    pub transactions: Vec<SanitizedTransaction>,
}

pub struct FinishedWork {
    pub decision: BufferedPacketsDecision,
    pub packets: Vec<DeserializedPacket>,
    pub transactions: Vec<SanitizedTransaction>,
    pub retryable_indexes: Vec<usize>,
}

pub struct BankingWorker {
    id: u32,
    receiver: Receiver<ScheduledWork>,
    sender: Sender<FinishedWork>,
    forwarder: Forwarder,
    consumer: Consumer,
}

impl BankingWorker {
    pub fn run(self) {
        // TODO: refactor forwarding interface so we can forward w/o this.
        let mut banking_stats = BankingStageStats::new(self.id);

        while let Ok(scheduled_work) = self.receiver.recv() {
            let ScheduledWork {
                decision,
                mut packets,
                transactions,
            } = scheduled_work;

            let retryable_indexes = match &decision {
                BufferedPacketsDecision::Consume(bank_start) => {
                    self.consume(bank_start, &transactions)
                }
                BufferedPacketsDecision::Forward | BufferedPacketsDecision::ForwardAndHold => {
                    self.forward(&mut packets, &mut banking_stats);
                    vec![] // no need to mark any as retryable
                }
                BufferedPacketsDecision::Hold => panic!("Should not be holding packets"),
            };

            let finished_work = FinishedWork {
                decision,
                packets,
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

    fn forward(
        &self,
        packets: &mut Vec<DeserializedPacket>,
        banking_stats: &mut BankingStageStats,
    ) {
        let _ = self.forwarder.forward_buffered_packets(
            &ForwardOption::ForwardTransaction,
            packets
                .iter()
                .map(|p| p.immutable_section().original_packet()),
            banking_stats,
        );
        packets.iter_mut().for_each(|p| p.forwarded = true);
    }
}
