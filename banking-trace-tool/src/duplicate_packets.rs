use {
    crate::process::process_event_files,
    solana_core::{
        banking_stage::immutable_deserialized_packet::ImmutableDeserializedPacket,
        banking_trace::{BankingPacketBatch, ChannelLabel, TimedTracedEvent, TracedEvent},
    },
    solana_sdk::signature::Signature,
    std::{collections::HashSet, path::PathBuf},
};

pub fn do_count_duplicate_packets(event_file_paths: &[PathBuf]) -> std::io::Result<()> {
    let mut scanner = DuplicatePacketScanner::default();
    process_event_files(event_file_paths, &mut |event| scanner.handle_event(event))?;
    println!(
        "Duplicate packets: {}/{}",
        scanner.duplicate_count, scanner.total_count
    );
    Ok(())
}

#[derive(Default)]
struct DuplicatePacketScanner {
    signatures: HashSet<Signature>,
    duplicate_count: usize,
    total_count: usize,
}

impl DuplicatePacketScanner {
    fn handle_event(&mut self, TimedTracedEvent(_, event): TimedTracedEvent) {
        match event {
            TracedEvent::PacketBatch(label, banking_packet_batch) => {
                self.handle_packets(label, banking_packet_batch)
            }
            TracedEvent::BlockAndBankHash(_, _, _) => {}
        }
    }

    fn handle_packets(&mut self, label: ChannelLabel, banking_packet_batch: BankingPacketBatch) {
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
                self.signatures.insert(sig)
            })
            .count();

        self.duplicate_count += total_packets - num_unique_packets;
        self.total_count += total_packets;
    }
}
