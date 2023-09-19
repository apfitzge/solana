use {
    crate::process::process_event_files,
    solana_core::{
        banking_stage::immutable_deserialized_packet::ImmutableDeserializedPacket,
        banking_trace::{BankingPacketBatch, ChannelLabel, TimedTracedEvent, TracedEvent},
    },
    solana_sdk::signature::Signature,
    std::{
        collections::HashMap,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    },
};

pub fn do_count_duplicate_packets(event_file_paths: &[PathBuf]) -> std::io::Result<()> {
    let mut scanner = DuplicatePacketScanner::default();
    process_event_files(event_file_paths, &mut |event| scanner.handle_event(event))?;
    scanner.report();
    Ok(())
}

#[derive(Default)]
struct DuplicatePacketScanner {
    signatures: HashMap<Signature, SystemTime>,
    duplicate_count: usize,
    total_count: usize,
    time_since_last_ms_accumulator: u64,
}

impl DuplicatePacketScanner {
    fn handle_event(&mut self, TimedTracedEvent(time, event): TimedTracedEvent) {
        match event {
            TracedEvent::PacketBatch(label, banking_packet_batch) => {
                self.handle_packets(time, label, banking_packet_batch)
            }
            TracedEvent::BlockAndBankHash(_, _, _) => {}
        }
    }

    fn handle_packets(
        &mut self,
        time: SystemTime,
        label: ChannelLabel,
        banking_packet_batch: BankingPacketBatch,
    ) {
        if !matches!(label, ChannelLabel::NonVote) {
            return;
        }
        if banking_packet_batch.0.is_empty() {
            return;
        }

        let mut total_packets = 0;
        let num_unique_packets = banking_packet_batch
            .0
            .iter()
            .flatten()
            .cloned()
            .filter_map(|p| ImmutableDeserializedPacket::new(p).ok())
            .filter(|p| {
                total_packets += 1;
                let sig = p.transaction().get_signatures()[0];
                if let Some(last_time) = self.signatures.insert(sig, time) {
                    let time_ms = time.duration_since(UNIX_EPOCH).unwrap().as_millis();
                    let last_time_ms = last_time.duration_since(UNIX_EPOCH).unwrap().as_millis();
                    self.time_since_last_ms_accumulator +=
                        u64::try_from(time_ms.checked_sub(last_time_ms).unwrap()).unwrap();
                    false
                } else {
                    true
                }
            })
            .count();

        self.duplicate_count += total_packets - num_unique_packets;
        self.total_count += total_packets;
    }

    fn report(&self) {
        println!(
            "Duplicate packets: {}/{}",
            self.duplicate_count, self.total_count
        );
        println!(
            "Average time between duplicates: {}ms",
            self.time_since_last_ms_accumulator / self.duplicate_count as u64
        );
    }
}
