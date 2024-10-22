//! Utility to deduplicate baches of incoming network packets.

use {
    crate::packet::{Packet, PacketBatch},
    ahash::RandomState,
    rand::Rng,
    std::{
        hash::{BuildHasher, Hash, Hasher},
        iter::repeat_with,
        marker::PhantomData,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    },
};

pub struct Deduper<const K: usize, T: ?Sized> {
    num_bits: u64,
    bits: Vec<AtomicU64>,
    state: [RandomState; K],
    clock: Instant,
    popcount: AtomicU64, // Number of one bits in self.bits.
    _phantom: PhantomData<T>,
}

impl<const K: usize, T: ?Sized + Hash> Deduper<K, T> {
    pub fn new<R: Rng>(rng: &mut R, num_bits: u64) -> Self {
        let size = num_bits.checked_add(63).unwrap() / 64;
        let size = usize::try_from(size).unwrap();
        Self {
            num_bits,
            state: std::array::from_fn(|_| new_random_state(rng)),
            clock: Instant::now(),
            bits: repeat_with(AtomicU64::default).take(size).collect(),
            popcount: AtomicU64::default(),
            _phantom: PhantomData::<T>,
        }
    }

    fn false_positive_rate(&self) -> f64 {
        let popcount = self.popcount.load(Ordering::Relaxed);
        let ones_ratio = popcount.min(self.num_bits) as f64 / self.num_bits as f64;
        ones_ratio.powi(K as i32)
    }

    /// Resets the Deduper if either it is older than the reset_cycle or it is
    /// saturated enough that false positive rate exceeds specified threshold.
    /// Returns true if the deduper was saturated.
    pub fn maybe_reset<R: Rng>(
        &mut self,
        rng: &mut R,
        false_positive_rate: f64,
        reset_cycle: Duration,
    ) -> bool {
        assert!(0.0 < false_positive_rate && false_positive_rate < 1.0);
        let saturated = self.false_positive_rate() >= false_positive_rate;
        if saturated || self.clock.elapsed() >= reset_cycle {
            self.state = std::array::from_fn(|_| new_random_state(rng));
            self.clock = Instant::now();
            self.bits.fill_with(AtomicU64::default);
            self.popcount = AtomicU64::default();
        }
        saturated
    }

    // Returns true if the data is duplicate.
    #[must_use]
    pub fn dedup(&self, data: &T) -> bool {
        let hashes = self
            .state
            .iter()
            .map(RandomState::build_hasher)
            .map(|mut hasher| {
                data.hash(&mut hasher);
                hasher.finish()
            });
        self.dedup_hashed(hashes)
    }

    // `Deduper` is a probabilistic data structure, and hashing may be
    // inconsistent between machines. This method allows tests to provide
    // consistent "hashed" values that get inserted into the data-structure,
    // so that they may assert the expected behavior
    #[allow(clippy::arithmetic_side_effects)]
    fn dedup_hashed(&self, hashes: impl ExactSizeIterator<Item = u64>) -> bool {
        let mut out = true;
        for hash in hashes {
            let mod_hash = hash % self.num_bits;
            let index = (mod_hash >> 6) as usize;
            let mask: u64 = 1u64 << (mod_hash & 63);
            let old = self.bits[index].fetch_or(mask, Ordering::Relaxed);
            if old & mask == 0u64 {
                self.popcount.fetch_add(1, Ordering::Relaxed);
                out = false;
            }
        }

        out
    }
}

fn new_random_state<R: Rng>(rng: &mut R) -> RandomState {
    RandomState::with_seeds(rng.gen(), rng.gen(), rng.gen(), rng.gen())
}

pub fn dedup_packets_and_count_discards<const K: usize>(
    deduper: &Deduper<K, [u8]>,
    batches: &mut [PacketBatch],
    mut process_received_packet: impl FnMut(&mut Packet, bool, bool),
) -> u64 {
    batches
        .iter_mut()
        .flat_map(PacketBatch::iter_mut)
        .map(|packet| {
            if packet.meta().discard() {
                process_received_packet(packet, true, false);
            } else if packet
                .data(..)
                .map(|data| deduper.dedup(data))
                .unwrap_or(true)
            {
                packet.meta_mut().set_discard(true);
                process_received_packet(packet, false, true);
            } else {
                process_received_packet(packet, false, false);
            }
            u64::from(packet.meta().discard())
        })
        .sum()
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use {
        super::*,
        crate::{packet::to_packet_batches, sigverify, test_tx::test_tx},
        test_case::test_case,
    };

    #[test]
    fn test_dedup_same() {
        let tx = test_tx();

        let mut batches =
            to_packet_batches(&std::iter::repeat(tx).take(1024).collect::<Vec<_>>(), 128);
        let packet_count = sigverify::count_packets_in_batches(&batches);
        let mut rng = rand::thread_rng();
        let filter = Deduper::<2, [u8]>::new(&mut rng, /*num_bits:*/ 63_999_979);
        let mut num_deduped = 0;
        let discard = dedup_packets_and_count_discards(
            &filter,
            &mut batches,
            |_deduped_packet, _removed_before_sigverify_stage, _is_dup| {
                num_deduped += 1;
            },
        ) as usize;
        assert_eq!(num_deduped, discard + 1);
        assert_eq!(packet_count, discard + 1);
    }

    #[test]
    fn test_dedup_diff() {
        let mut rng = rand::thread_rng();
        let mut filter = Deduper::<2, [u8]>::new(&mut rng, /*num_bits:*/ 63_999_979);
        let mut batches = to_packet_batches(&(0..1024).map(|_| test_tx()).collect::<Vec<_>>(), 128);
        let discard =
            dedup_packets_and_count_discards(&filter, &mut batches, |_, _, _| ()) as usize;
        // because dedup uses a threadpool, there maybe up to N threads of txs that go through
        assert_eq!(discard, 0);
        assert!(!filter.maybe_reset(
            &mut rng,
            0.001,                    // false_positive_rate
            Duration::from_millis(0), // reset_cycle
        ));
        for i in filter.bits {
            assert_eq!(i.load(Ordering::Relaxed), 0);
        }
    }

    fn get_capacity<const K: usize>(num_bits: u64, false_positive_rate: f64) -> u64 {
        (num_bits as f64 * false_positive_rate.powf(1f64 / K as f64)) as u64
    }

    #[test]
    #[ignore]
    fn test_dedup_saturated() {
        const NUM_BITS: u64 = 63_999_979;
        const FALSE_POSITIVE_RATE: f64 = 0.001;
        let mut rng = rand::thread_rng();
        let mut filter = Deduper::<2, [u8]>::new(&mut rng, NUM_BITS);
        let capacity = get_capacity::<2>(NUM_BITS, FALSE_POSITIVE_RATE);
        let mut discard = 0;
        assert!(filter.popcount.load(Ordering::Relaxed) < capacity);
        for i in 0..1000 {
            let mut batches =
                to_packet_batches(&(0..1000).map(|_| test_tx()).collect::<Vec<_>>(), 128);
            discard +=
                dedup_packets_and_count_discards(&filter, &mut batches, |_, _, _| ()) as usize;
            trace!("{} {}", i, discard);
            if filter.popcount.load(Ordering::Relaxed) > capacity {
                break;
            }
        }
        assert!(filter.popcount.load(Ordering::Relaxed) > capacity);
        assert!(filter.false_positive_rate() >= FALSE_POSITIVE_RATE);
        assert!(filter.maybe_reset(
            &mut rng,
            FALSE_POSITIVE_RATE,
            Duration::from_millis(0), // reset_cycle
        ));
    }

    #[test]
    fn test_dedup_false_positive() {
        let mut rng = rand::thread_rng();
        let filter = Deduper::<2, [u8]>::new(&mut rng, /*num_bits:*/ 63_999_979);
        let mut discard = 0;
        for i in 0..10 {
            let mut batches =
                to_packet_batches(&(0..1024).map(|_| test_tx()).collect::<Vec<_>>(), 128);
            discard +=
                dedup_packets_and_count_discards(&filter, &mut batches, |_, _, _| ()) as usize;
            debug!("false positive rate: {}/{}", discard, i * 1024);
        }
        //allow for 1 false positive even if extremely unlikely
        assert!(discard < 2);
    }

    #[test_case(63_999_979, 0.001, 2_023_857)]
    #[test_case(622_401_961, 0.001, 19_682_078)]
    #[test_case(622_401_979, 0.001, 19_682_078)]
    #[test_case(629_145_593, 0.001, 19_895_330)]
    #[test_case(632_455_543, 0.001, 20_000_000)]
    #[test_case(637_534_199, 0.001, 20_160_601)]
    #[test_case(622_401_961, 0.0001, 6_224_019)]
    #[test_case(622_401_979, 0.0001, 6_224_019)]
    #[test_case(629_145_593, 0.0001, 6_291_455)]
    #[test_case(632_455_543, 0.0001, 6_324_555)]
    #[test_case(637_534_199, 0.0001, 6_375_341)]
    fn test_dedup_capacity(num_bits: u64, false_positive_rate: f64, capacity: u64) {
        let mut rng = rand::thread_rng();
        assert_eq!(get_capacity::<2>(num_bits, false_positive_rate), capacity);
        let mut deduper = Deduper::<2, [u8]>::new(&mut rng, num_bits);
        assert_eq!(deduper.false_positive_rate(), 0.0);
        deduper.popcount.store(capacity, Ordering::Relaxed);
        assert!(deduper.false_positive_rate() < false_positive_rate);
        deduper.popcount.store(capacity + 1, Ordering::Relaxed);
        assert!(deduper.false_positive_rate() >= false_positive_rate);
        assert!(deduper.maybe_reset(
            &mut rng,
            false_positive_rate,
            Duration::from_millis(0), // reset_cycle
        ));
    }

    #[test_case(&[(&[10, 20, 30], false), (&[40, 50, 60], false)], 128, 6; "no_duplicates")]
    #[test_case(&[(&[1, 2, 3], false), (&[1, 4, 5], false)], 64, 5; "with_some_duplicates")]
    #[test_case(&[(&[100, 200, 300], false), (&[100, 200, 300], true)], 64, 3; "all_duplicates_in_second_set")]
    #[test_case(&[(&[100, 200, 300], false), (&[164, 264, 364], true)], 64, 3; "all_duplicates_after_mod")]
    fn test_dedup_hashed(hashes_and_duplicate: &[(&[u64], bool)], num_bits: u64, popcount: u64) {
        // rng only used to construct `Deduper`, does not affect test outcomes
        let mut rng = rand::thread_rng();
        let deduper = Deduper::<2, [u8]>::new(&mut rng, num_bits);

        for (hashes, is_duplicate) in hashes_and_duplicate {
            assert_eq!(deduper.dedup_hashed(hashes.iter().copied()), *is_duplicate);
        }

        assert_eq!(deduper.popcount.load(Ordering::Relaxed), popcount);
    }
}
