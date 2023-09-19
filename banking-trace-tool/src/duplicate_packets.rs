use {
    crate::process::process_event_files,
    histogram::Histogram,
    solana_core::{
        banking_stage::immutable_deserialized_packet::ImmutableDeserializedPacket,
        banking_trace::{BankingPacketBatch, ChannelLabel, TimedTracedEvent, TracedEvent},
    },
    solana_sdk::signature::Signature,
    std::{collections::HashMap, path::PathBuf, time::SystemTime},
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
    duplicate_forwarded_packets: usize,
    forwarded_before_unforwarded: usize,
    unforwarded_before_forwarded: usize,
    time_since_last_seen_hist: Histogram,
}

struct SigEntry {
    time: SystemTime,
    forwarded: bool,
    count: usize,
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

        for packet in banking_packet_batch
            .0
            .iter()
            .flatten()
            .cloned()
            .filter_map(|p| ImmutableDeserializedPacket::new(p).ok())
        {
            let sig = packet.transaction().get_signatures()[0];
            let forwarded = packet.original_packet().meta().forwarded();

            self.total_packets += 1;
            if forwarded {
                self.total_forwarded_packets += 1;
            }

            let count = if let Some(last_seen) = self.last_seen.remove(&sig) {
                self.duplicate_packets += 1;
                if forwarded {
                    self.duplicate_forwarded_packets += 1;
                }

                match (last_seen.forwarded, forwarded) {
                    (false, true) => self.unforwarded_before_forwarded += 1,
                    (true, false) => self.forwarded_before_unforwarded += 1,
                    _ => {}
                }

                let time_diff = time.duration_since(last_seen.time).unwrap();
                let time_diff_ms = time_diff.as_millis();
                let _ = self
                    .time_since_last_seen_hist
                    .increment(u64::try_from(time_diff_ms).unwrap());
                last_seen.count + 1
            } else {
                0
            };

            self.last_seen.insert(
                sig,
                SigEntry {
                    time,
                    forwarded,
                    count,
                },
            );
        }
    }

    fn report(&self) {
        println!(
            "Duplicate packets: {}/{}",
            self.duplicate_packets, self.total_packets
        );
        println!(
            "Forwarded packets: {}/{}",
            self.total_forwarded_packets, self.total_packets
        );
        println!(
            "Duplicate forwarded packets: {}/{}",
            self.duplicate_forwarded_packets, self.total_forwarded_packets
        );
        println!(
            "Forwarded before unforwarded: {}",
            self.forwarded_before_unforwarded
        );
        println!(
            "Unforwarded before forwarded: {}",
            self.unforwarded_before_forwarded
        );
        pretty_print_histogram("ms between duplicates", &self.time_since_last_seen_hist);

        let mut duplicate_count_histogram = Histogram::new();
        for entry in self.last_seen.values() {
            duplicate_count_histogram
                .increment(entry.count as u64)
                .unwrap();
        }
        pretty_print_histogram("duplicate count", &duplicate_count_histogram);
    }
}

fn pretty_print_histogram(name: &str, histogram: &Histogram) {
    print!("{name}: [");

    const PERCENTILES: &[f64] = &[5.0, 10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0, 99.9];
    for percentile in PERCENTILES.iter().copied() {
        print!(
            "{}: {}, ",
            percentile,
            histogram.percentile(percentile).unwrap()
        );
    }
    println!("]");
}
