use {
    crate::{
        block_history::{load_history, FilterKind},
        process::process_event_files,
    },
    chrono::{DateTime, Utc},
    solana_core::{
        banking_stage::immutable_deserialized_packet::ImmutableDeserializedPacket,
        banking_trace::{BankingPacketBatch, ChannelLabel, TimedTracedEvent, TracedEvent},
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::{collections::HashSet, path::PathBuf, time::SystemTime},
};

pub fn do_log_slot_range(
    event_file_paths: &[PathBuf],
    start: Slot,
    end: Slot,
    priority_sort: bool,
    check_history: bool,
    filter_keys: Vec<Pubkey>,
) -> std::io::Result<()> {
    let mut collector = SlotRangeCollector::new(start, end);
    process_event_files(event_file_paths, &mut |event| collector.handle_event(event))?;
    collector.report(priority_sort, check_history, filter_keys);
    Ok(())
}

/// Collects all non-vote transactions for specific slot range
pub struct SlotRangeCollector {
    /// Start of slot range to collect data for.
    start: Slot,
    /// End of slot range (inclusive) to collect data for.
    end: Slot,
    /// Packets in the range separated by slot.
    packets: Vec<(
        Slot,
        DateTime<Utc>,
        Vec<(DateTime<Utc>, ImmutableDeserializedPacket)>,
    )>,

    /// Packets being accumulated before we know the slot.
    pending_packets: Vec<(DateTime<Utc>, ImmutableDeserializedPacket)>,
}

impl SlotRangeCollector {
    fn new(start: Slot, end: Slot) -> Self {
        Self {
            start,
            end,
            packets: Vec::new(),
            pending_packets: Vec::new(),
        }
    }

    fn report(self, priority_sort: bool, check_history: bool, filter_keys: Vec<Pubkey>) {
        let filter_keys = filter_keys.into_iter().collect::<HashSet<_>>();
        let history_checker = if check_history {
            assert!(
                self.packets.len() == 1,
                "only support history filtering for single slot"
            );

            Some(load_history(self.packets.first().unwrap().0))
        } else {
            None
        };

        for (slot, slot_timestamp, mut packets) in self.packets.into_iter() {
            println!("{slot} ({slot_timestamp}): [");

            if check_history {
                let mut blockhash_missing_count = 0;
                let mut already_processed_count = 0;
                if let Some(history_checker) = history_checker.as_ref() {
                    packets.retain(|x| {
                        let recent_blockhash = format!(
                            "{}",
                            x.1.transaction().get_message().message.recent_blockhash()
                        );
                        let sig = x.1.transaction().get_signatures()[0];
                        match history_checker.should_filter(&recent_blockhash, &sig) {
                            Some(FilterKind::MissingBlockhash) => {
                                blockhash_missing_count += 1;
                                false
                            }
                            Some(FilterKind::AlreadyProcessed) => {
                                already_processed_count += 1;
                                false
                            }
                            None => true,
                        }
                    });

                    println!(
                        "Retained: {}, Filtered: {} missing blockhash, {} already processed",
                        packets.len(),
                        blockhash_missing_count,
                        already_processed_count
                    );
                } else {
                    panic!("required");
                }
            }

            let mut filtered_key_count = 0;
            packets.retain(|p| {
                let message = &p.1.transaction().get_message().message;
                let account_keys = message.static_account_keys();
                if account_keys.iter().any(|k| filter_keys.contains(k)) {
                    filtered_key_count += 1;
                    false
                } else {
                    true
                }
            });

            println!(
                "Retained: {} Filtered: {} packets with filter keys",
                packets.len(),
                filtered_key_count
            );

            if priority_sort {
                packets.sort_by(|a, b| b.1.priority().cmp(&a.1.priority()));
            }

            for (timestamp, packet) in packets {
                let ip = packet.original_packet().meta().addr;
                // TODO: verbosity
                let priority = packet.priority();
                let compute_units = packet.compute_unit_limit();

                let transaction = packet.transaction();
                let Some(signature) = transaction.get_signatures().first().copied() else {
                    // Should never fail here because sigverify?
                    continue;
                };

                let included = history_checker
                    .as_ref()
                    .map(|hc| hc.actually_contained(&signature))
                    .unwrap_or_default()
                    .map(|index| format!("({index:04})"))
                    .unwrap_or("      ".to_string());
                let message = &transaction.get_message().message;
                let account_keys = message.static_account_keys();
                let write_keys: Vec<_> = account_keys
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| message.is_maybe_writable(*index))
                    .map(|(_, k)| *k)
                    .collect();
                let read_keys: Vec<_> = account_keys
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| !message.is_maybe_writable(*index))
                    .map(|(_, k)| *k)
                    .collect();

                println!("  [{timestamp}] {included}{signature}: ({ip}, {priority}, {compute_units}) - [{write_keys:?}] [{read_keys:?}],");

                // println!("  {:?},", packet);
            }
            println!("]");
        }
    }

    fn handle_event(&mut self, TimedTracedEvent(timestamp, event): TimedTracedEvent) {
        match event {
            TracedEvent::PacketBatch(label, packets) => {
                self.handle_packets(timestamp, label, packets)
            }
            TracedEvent::BlockAndBankHash(slot, _, _) => self.handle_slot(timestamp, slot),
        }
    }

    fn handle_packets(
        &mut self,
        timestamp: SystemTime,
        label: ChannelLabel,
        banking_packet_batch: BankingPacketBatch,
    ) {
        if !matches!(label, ChannelLabel::NonVote) {
            return;
        }
        if banking_packet_batch.0.is_empty() {
            return;
        }

        let timestamp = DateTime::<Utc>::from(timestamp);

        let packets = banking_packet_batch
            .0
            .iter()
            .flatten()
            .cloned()
            .filter_map(|p| ImmutableDeserializedPacket::new(p).ok())
            .map(|p| (timestamp, p));
        self.pending_packets.extend(packets);
    }

    fn handle_slot(&mut self, timestamp: SystemTime, slot: Slot) {
        if slot < self.start || slot > self.end {
            self.pending_packets.clear();
            return;
        }

        let timestamp = DateTime::<Utc>::from(timestamp);
        self.packets
            .push((slot, timestamp, core::mem::take(&mut self.pending_packets)));
    }
}
