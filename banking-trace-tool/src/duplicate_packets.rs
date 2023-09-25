use {
    crate::process::process_event_files,
    histogram::Histogram,
    solana_core::{
        banking_stage::immutable_deserialized_packet::ImmutableDeserializedPacket,
        banking_trace::{BankingPacketBatch, ChannelLabel, TimedTracedEvent, TracedEvent},
    },
    solana_sdk::signature::Signature,
    std::{collections::HashMap, net::IpAddr, path::PathBuf, time::SystemTime},
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
    total_unique_packets: usize,
    total_forwarded_packets: usize,
    total_unforwarded_packets_first: usize,
    duplicate_forwarded_packets: usize,
    forwarded_before_unforwarded: usize,
    unforwarded_before_forwarded: usize,
    unforwarded_unforwarded: usize,
    forwarded_forwarded: usize,
    time_since_last_seen_hist: Histogram,
    excessively_late_packets: HashMap<IpAddr, (usize, usize)>,
}

struct SigEntry {
    time: SystemTime,
    first_received_forwarded: bool,
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

            match self.last_seen.entry(sig) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    self.duplicate_packets += 1;
                    if forwarded {
                        self.duplicate_forwarded_packets += 1;
                    }

                    match (entry.get().first_received_forwarded, forwarded) {
                        (false, false) => self.unforwarded_unforwarded += 1,
                        (false, true) => self.unforwarded_before_forwarded += 1,
                        (true, false) => self.forwarded_before_unforwarded += 1,
                        (true, true) => self.forwarded_forwarded += 1,
                    }

                    let time_diff = time.duration_since(entry.get().time).unwrap();
                    let time_diff_ms = time_diff.as_millis();
                    let _ = self
                        .time_since_last_seen_hist
                        .increment(u64::try_from(time_diff_ms).unwrap());

                    // Packet hasn't been seen for 30s
                    if time_diff_ms > 30_000 {
                        let (forwarded_count, non_forwarded_count) = self
                            .excessively_late_packets
                            .entry(packet.original_packet().meta().addr)
                            .or_default();

                        if forwarded {
                            *forwarded_count += 1;
                        } else {
                            *non_forwarded_count += 1;
                        }
                    }

                    // Update count and time.
                    // Do not update the forwarded flag, as it is only set once, as we want to
                    // track the original forwarded status.
                    entry.get_mut().count += 1;
                    entry.get_mut().time = time;
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if !forwarded {
                        self.total_unforwarded_packets_first += 1;
                    }
                    self.total_unique_packets += 1;

                    entry.insert(SigEntry {
                        time,
                        first_received_forwarded: forwarded,
                        count: 0,
                    });
                }
            }
        }
    }

    fn report(&self) {
        println!(
            "Duplicate packets: {}/{}",
            self.duplicate_packets, self.total_packets
        );
        println!(
            "Unforwarded packets first: {}/{}",
            self.total_unforwarded_packets_first, self.total_unique_packets
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
        println!(
            "Unforwarded-Unforwarded duplicates: {}",
            self.unforwarded_unforwarded
        );
        println!(
            "Forwarded-Forwarded duplicates: {}",
            self.forwarded_forwarded
        );
        pretty_print_histogram("ms between duplicates", &self.time_since_last_seen_hist);

        let mut duplicate_count_histogram = Histogram::new();
        for entry in self.last_seen.values() {
            let duplicate_count = entry.count as u64;
            if duplicate_count > 0 {
                duplicate_count_histogram
                    .increment(duplicate_count)
                    .unwrap();
            }
        }
        pretty_print_histogram("duplicate count", &duplicate_count_histogram);

        println!("Excessively late packets:");
        let mut total = 0;
        for (addr, (forwaded_count, nonforwarded_count)) in &self.excessively_late_packets {
            total += forwaded_count + nonforwarded_count;
            println!("  {addr}: forwarded={forwaded_count} non_forwarded={nonforwarded_count}");
        }
        println!("Total excessively late packets: {total}");
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
