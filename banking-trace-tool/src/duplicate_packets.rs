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
    last_seen: HashMap<Signature, SigEntry>,
    duplicate_packets: usize,
    total_packets: usize,
    total_forwarded_packets: usize,
    forwarded_before_unforwarded: usize,
    unforwarded_before_forwarded: usize,
    time_since_last_ms_accumulator: u64,
}

struct SigEntry {
    time: SystemTime,
    forwarded: bool,
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
        let mut duplicate_packets = 0;
        let mut total_forwarded_packets = 0;
        let mut forwarded_before_unforwarded = 0;
        let mut unforwarded_before_forwarded = 0;
        for packet in banking_packet_batch
            .0
            .iter()
            .flatten()
            .cloned()
            .filter_map(|p| ImmutableDeserializedPacket::new(p).ok())
        {
            let sig = packet.transaction().get_signatures()[0];
            let forwarded = packet.original_packet().meta().forwarded();

            total_packets += 1;
            if forwarded {
                total_forwarded_packets += 1;
            }

            if let Some(last_seen) = self.last_seen.insert(sig, SigEntry { time, forwarded }) {
                duplicate_packets += 1;
                match (last_seen.forwarded, forwarded) {
                    (false, true) => unforwarded_before_forwarded += 1,
                    (true, false) => forwarded_before_unforwarded += 1,
                    _ => {}
                }

                let time_ms = time.duration_since(UNIX_EPOCH).unwrap().as_millis();
                let last_time_ms = last_seen
                    .time
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                self.time_since_last_ms_accumulator +=
                    u64::try_from(time_ms.checked_sub(last_time_ms).unwrap()).unwrap();
            }

            self.total_packets += total_packets;
            self.duplicate_packets += duplicate_packets;
            self.total_forwarded_packets += total_forwarded_packets;
            self.forwarded_before_unforwarded += forwarded_before_unforwarded;
            self.unforwarded_before_forwarded += unforwarded_before_forwarded;
        }
    }

    fn report(&self) {
        println!(
            "Duplicate packets: {}/{}",
            self.duplicate_packets, self.total_packets
        );
        println!(
            "Forwarded packes: {}/{}",
            self.total_forwarded_packets, self.total_packets
        );
        println!(
            "Forwarded before unforwarded: {}",
            self.forwarded_before_unforwarded
        );
        println!(
            "Unforwarded before forwarded: {}",
            self.unforwarded_before_forwarded
        );
        println!(
            "Average time between duplicates: {}ms",
            self.time_since_last_ms_accumulator / self.duplicate_packets as u64
        );
    }
}
