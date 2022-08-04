//! The `scheduler_stage` schedules transaction messages to the `banking_stage`.

use {
    crate::{
        sigverify::SigverifyTracerPacketStats,
        unprocessed_packet_batches::{self, DeserializedPacket},
    },
    crossbeam_channel::{Receiver, Sender},
    solana_perf::packet::PacketBatch,
    std::thread::{Builder, JoinHandle},
};

/// Stores the stage's thread handle(s)
pub struct SchedulerStage {
    pub scheduler_thread_hdls: Vec<JoinHandle<()>>,
}

/// Stores the stage's communication channels
pub struct SchedulerStageChannels {
    /// Transaction receivers for each banking transaction thread
    pub verified_receivers: Vec<Receiver<TransactionBatchMessage>>,
    /// Completed transaction sender for each banking transaction thread
    pub completed_transaction_sender: Sender<CompletedTransactionMessage>,

    /// Transaction receiver for tpu votes
    pub tpu_verified_vote_receiverr: Receiver<TransactionBatchMessage>,
    /// Completed Transaction sender for tpu votes
    pub tpu_verified_vote_sender: Sender<CompletedTransactionMessage>,

    /// Transaction receiver for votes
    pub verified_vote_receiver: Receiver<TransactionBatchMessage>,
    /// Completed Transaction sender for tpu votes
    pub verified_vote_sender: Sender<CompletedTransactionMessage>,
}

pub type BankingPacketBatch = (Vec<PacketBatch>, Option<SigverifyTracerPacketStats>);
pub type BankingPacketReceiver = Receiver<BankingPacketBatch>;
pub type TransactionMessage = DeserializedPacket;
pub type TransactionBatchMessage = Vec<TransactionMessage>;
pub type CompletedTransactionMessage = (usize, TransactionMessage);

impl SchedulerStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        num_transaction_threads: usize,
        verified_receiver: BankingPacketReceiver,
        tpu_verified_vote_receiver: BankingPacketReceiver,
        verified_vote_receiver: BankingPacketReceiver,
    ) -> (Self, SchedulerStageChannels) {
        assert!(num_transaction_threads > 0);

        let (mut scheduler, channels) = SimpleScheduler::with_num_transaction_threads(
            num_transaction_threads,
            verified_receiver,
            tpu_verified_vote_receiver,
            verified_vote_receiver,
        );

        let scheduler_hdl = Builder::new()
            .name("solana-transaction-scheduler".to_string())
            .spawn(move || while scheduler.process() {})
            .unwrap();

        (
            Self {
                scheduler_thread_hdls: vec![scheduler_hdl],
            },
            channels,
        )
    }

    pub fn join(self) -> std::thread::Result<()> {
        for scheduler_thread_hdl in self.scheduler_thread_hdls {
            scheduler_thread_hdl.join()?;
        }
        Ok(())
    }
}

struct SimpleScheduler {
    transaction_scheduler: PassThroughScheduler,
    tpu_vote_scheduler: PassThroughScheduler,
    vote_scheduler: PassThroughScheduler,
}

impl SimpleScheduler {
    pub fn with_num_transaction_threads(
        num_transaction_threads: usize,
        verified_receiver: BankingPacketReceiver,
        tpu_verified_vote_receiver: BankingPacketReceiver,
        verified_vote_receiver: BankingPacketReceiver,
    ) -> (Self, SchedulerStageChannels) {
        let (transaction_batch_sender, transaction_batch_receiver) = crossbeam_channel::unbounded();
        let (completed_transaction_sender, completed_transaction_receiver) =
            crossbeam_channel::bounded(0);
        let transaction_batch_receivers = (0..num_transaction_threads)
            .map(|_| transaction_batch_receiver.clone())
            .collect();
        let transaction_scheduler = PassThroughScheduler {
            banking_packet_receiver: verified_receiver,
            transaction_batch_sender,
            _completed_transaction_receiver: completed_transaction_receiver,
        };

        let (tpu_vote_sender, tpu_vote_receiver) = crossbeam_channel::unbounded();
        let (completed_tpu_vote_sender, completed_tpu_vote_receiver) =
            crossbeam_channel::bounded(0);
        let tpu_vote_scheduler = PassThroughScheduler {
            banking_packet_receiver: tpu_verified_vote_receiver,
            transaction_batch_sender: tpu_vote_sender,
            _completed_transaction_receiver: completed_tpu_vote_receiver,
        };

        let (vote_sender, vote_receiver) = crossbeam_channel::unbounded();
        let (completed_vote_sender, completed_vote_receiver) = crossbeam_channel::bounded(0);
        let vote_scheduler = PassThroughScheduler {
            banking_packet_receiver: verified_vote_receiver,
            transaction_batch_sender: vote_sender,
            _completed_transaction_receiver: completed_vote_receiver,
        };

        let channels = SchedulerStageChannels {
            verified_receivers: transaction_batch_receivers,
            completed_transaction_sender,
            tpu_verified_vote_receiverr: tpu_vote_receiver,
            tpu_verified_vote_sender: completed_tpu_vote_sender,
            verified_vote_receiver: vote_receiver,
            verified_vote_sender: completed_vote_sender,
        };
        let scheduler = SimpleScheduler {
            transaction_scheduler,
            tpu_vote_scheduler,
            vote_scheduler,
        };
        (scheduler, channels)
    }

    pub fn process(&mut self) -> bool {
        self.transaction_scheduler.process()
            && self.tpu_vote_scheduler.process()
            && self.vote_scheduler.process()
    }
}

struct PassThroughScheduler {
    banking_packet_receiver: BankingPacketReceiver,
    transaction_batch_sender: Sender<TransactionBatchMessage>,
    _completed_transaction_receiver: Receiver<CompletedTransactionMessage>,
}

impl PassThroughScheduler {
    /// processing loop - return whether thread should exit or not
    pub fn process(&mut self) -> bool {
        if let Ok((packet_batches, _)) = self.banking_packet_receiver.recv() {
            self.process_packet_batches(packet_batches)
        } else {
            false
        }
    }

    fn process_packet_batches(&mut self, packet_batches: Vec<PacketBatch>) -> bool {
        for packet_batch in packet_batches {
            let packet_indexes = Self::generate_packet_indexes(&packet_batch);
            let deserialized_packets =
                unprocessed_packet_batches::deserialize_packets(&packet_batch, &packet_indexes);

            let deserialized_packets: Vec<_> = deserialized_packets.collect();
            let res = self.transaction_batch_sender.send(deserialized_packets);
            if res.is_err() {
                return false;
            }
        }

        true
    }

    fn generate_packet_indexes(packet_batch: &PacketBatch) -> Vec<usize> {
        packet_batch
            .iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta.discard())
            .map(|(index, _)| index)
            .collect()
    }
}
