//! The key table of the hot path (spec §5.2): fixed slots of atomics, no
//! locks, no `unsafe`, allocated once. A key has two buckets of [`BUCKET`]
//! slots, picked by two independent parts of its hash, and lives in one of
//! their slots; removing it never breaks another key's lookup. One bucket
//! per key sent 25 of 4096 keys to the overflow slot in a table sized for
//! 4096, and over 500 of 7000; with two, a key overflows only when both are
//! full.

use rateguard_core::gcra::{AtomicGcra, Quota};

use crate::sync::{AtomicU64, Ordering, fence};

pub(crate) const BUCKET: usize = 8;
const EMPTY: u64 = 0;
/// A slot being reset: neither a key nor free.
const BUSY: u64 = u64::MAX;

/// One key's state: its GCRA and the attempts since the last tick.
#[derive(Debug)]
pub(crate) struct Slot {
    key: AtomicU64,
    pub gcra: AtomicGcra,
    pub attempts: AtomicU64,
}
impl Slot {
    fn new(quota: Quota) -> Self {
        Self {
            key: AtomicU64::new(EMPTY),
            gcra: AtomicGcra::new(quota),
            attempts: AtomicU64::new(0),
        }
    }
}

pub(crate) struct KeyTable {
    slots: Box<[Slot]>,
    buckets: usize,
    overflow: Slot,
}
// Not derived: up to two million slots would go into a log line.
impl std::fmt::Debug for KeyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyTable")
            .field("slots", &self.slots.len())
            .field("buckets", &self.buckets)
            .finish_non_exhaustive()
    }
}
impl KeyTable {
    /// Room for twice `tracked_keys`, in whole buckets, a power of two of
    /// them. Every slot starts free, carrying `quota`.
    pub fn new(tracked_keys: usize, quota: Quota) -> Self {
        let buckets = (2 * tracked_keys).div_ceil(BUCKET).next_power_of_two();
        Self {
            slots: (0..buckets * BUCKET).map(|_| Slot::new(quota)).collect(),
            buckets,
            overflow: Slot::new(quota),
        }
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn bucket(&self, index: usize) -> &[Slot] {
        let start = index * BUCKET;
        &self.slots[start..start + BUCKET]
    }

    /// The key's two buckets, picked by the low and the high half of its
    /// hash; distinct whenever the table has more than one.
    fn buckets_of(&self, key: u64) -> [&[Slot]; 2] {
        let mask = self.buckets - 1;
        let first = key as usize & mask;
        let mut second = (key >> 32) as usize & mask;
        if second == first {
            second = (first + 1) & mask;
        }
        [self.bucket(first), self.bucket(second)]
    }

    /// The key's candidate slots in their canonical order: the first bucket,
    /// then the second (once, if they are the same).
    fn candidates(&self, key: u64) -> impl Iterator<Item = &Slot> + Clone {
        let [first, second] = self.buckets_of(key);
        let second = (!std::ptr::eq(first, second)).then_some(second);
        first.iter().chain(second.into_iter().flatten())
    }

    /// The slot of `key`: found, newly taken in the emptier of its two
    /// buckets, or, both full, the shared overflow slot.
    ///
    /// Taking a slot leaves its GCRA alone: a free slot is fresh and carries
    /// the quota a new key starts with, kept current by the background task.
    /// Setting a quota here instead would rescale the debt of a key another
    /// thread has already admitted through the slot, from the wrong moment.
    pub fn slot(&self, key: u64) -> &Slot {
        debug_assert!(key != EMPTY && key != BUSY, "key_hash never yields those");
        let candidates = self.candidates(key);
        if let Some(slot) = candidates
            .clone()
            .find(|slot| slot.key.load(Ordering::Acquire) == key)
        {
            return slot;
        }

        let [first, second] = self.buckets_of(key);
        let taken = |bucket: &[Slot]| {
            bucket
                .iter()
                .filter(|slot| slot.key.load(Ordering::Relaxed) != EMPTY)
                .count()
        };
        let order = if taken(second) < taken(first) {
            [second, first]
        } else {
            [first, second]
        };
        let claimed = order.iter().flat_map(|bucket| bucket.iter()).find(|slot| {
            slot.key.load(Ordering::Relaxed) == EMPTY
                && slot
                    .key
                    .compare_exchange(EMPTY, key, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
        });
        let Some(mine) = claimed else {
            return &self.overflow;
        };

        // Our claim must be visible before we look for theirs, and theirs
        // before they look for ours: a store followed by a load of another
        // location (Dekker's pattern), which acquire/release does not order.
        // Without this fence both may miss each other and both copies live;
        // loom finds that interleaving.
        fence(Ordering::SeqCst);

        // Two threads may take two slots for one key at once, in either
        // bucket. The copy earliest in the canonical order wins, and
        // whoever looks last sees both: it gives its own back if it is the
        // later one, or takes the other back if that is. Release goes by
        // compare-and-swap, so a slot already handed to another key is
        // never touched.
        let mut winner = mine;
        let mut mine_seen = false;
        for other in candidates {
            if std::ptr::eq(other, mine) {
                mine_seen = true;
                continue;
            }
            if other.key.load(Ordering::Acquire) != key {
                continue;
            }
            if mine_seen {
                self.release(other, key, other.gcra.quota());
            } else if std::ptr::eq(winner, mine) {
                winner = other;
            }
        }
        if !std::ptr::eq(winner, mine) {
            self.release(mine, key, mine.gcra.quota());
        }
        winner
    }

    pub fn is_overflow(&self, slot: &Slot) -> bool {
        std::ptr::eq(slot, &self.overflow)
    }

    /// The slot shared by every key that finds its bucket full, at the
    /// quota of a new key.
    pub fn overflow(&self) -> &Slot {
        &self.overflow
    }

    /// Every taken slot with its key, as seen at the moment of the visit.
    pub fn for_each(&self, mut f: impl FnMut(u64, &Slot)) {
        for slot in self.slots.iter() {
            let key = slot.key.load(Ordering::Acquire);
            if key != EMPTY && key != BUSY {
                f(key, slot);
            }
        }
    }

    /// Every free slot, for the background task to keep the quota of a new
    /// key current in them.
    pub fn for_each_free(&self, mut f: impl FnMut(&Slot)) {
        for slot in self.slots.iter() {
            if slot.key.load(Ordering::Acquire) == EMPTY {
                f(slot);
            }
        }
    }

    /// Frees the slot of `key`, if it still holds it: claimed for the reset
    /// by compare-and-swap, then fields first and the key last, so that
    /// whoever takes it next finds it fresh. A thread still writing to it
    /// may leave the next key one interval of debt or one attempt: harmless,
    /// and accepted. Whether this call freed it.
    pub fn release(&self, slot: &Slot, key: u64, quota: Quota) -> bool {
        if slot
            .key
            .compare_exchange(key, BUSY, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        slot.gcra.reset(quota);
        slot.attempts.store(0, Ordering::Relaxed);
        slot.key.store(EMPTY, Ordering::Release);
        true
    }
}

#[cfg(all(test, not(rateguard_loom)))]
mod tests {
    use super::*;

    fn q() -> Quota {
        Quota::new(100, 10)
    }

    #[test]
    fn a_key_finds_its_slot_again() {
        let t = KeyTable::new(16, q());
        let a = t.slot(42) as *const Slot;
        assert_eq!(t.slot(42) as *const Slot, a);
        assert_ne!(t.slot(43) as *const Slot, a);
    }

    #[test]
    fn the_size_is_twice_the_tracked_keys_in_whole_buckets() {
        assert_eq!(KeyTable::new(4096, q()).capacity(), 8192);
        assert_eq!(KeyTable::new(1, q()).capacity(), BUCKET);
    }

    #[test]
    fn a_full_bucket_sends_newcomers_to_the_overflow_slot() {
        let t = KeyTable::new(1, q()); // a single bucket of 8
        for key in 1..=BUCKET as u64 {
            assert!(!t.is_overflow(t.slot(key)));
        }
        assert!(t.is_overflow(t.slot(99)));
    }

    #[test]
    fn a_released_slot_is_fresh_for_the_next_key() {
        let t = KeyTable::new(1, q());
        for _ in 0..10 {
            t.slot(1).gcra.check(0);
        }
        t.slot(1).attempts.fetch_add(3, Ordering::Relaxed);
        let slot = t.slot(1);
        assert!(t.release(slot, 1, q()));
        assert!(!t.release(slot, 1, q()), "freed once only");
        let next = t.slot(2);
        assert_eq!(
            next as *const Slot, slot as *const Slot,
            "the freed slot is reused"
        );
        assert!(!next.gcra.has_debt(0));
        assert_eq!(next.attempts.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_free_slot_carries_the_quota_a_new_key_starts_with() {
        let t = KeyTable::new(16, q());
        t.for_each_free(|slot| slot.gcra.set_quota(Quota::new(7, 1), 0));
        assert_eq!(t.slot(5).gcra.quota(), Quota::new(7, 1));
    }

    #[test]
    fn for_each_visits_the_taken_slots_only() {
        let t = KeyTable::new(16, q());
        t.slot(5);
        t.slot(6);
        let mut keys = Vec::new();
        t.for_each(|key, _| keys.push(key));
        keys.sort();
        assert_eq!(keys, [5, 6]);
        let mut free = 0;
        t.for_each_free(|_| free += 1);
        assert_eq!(free, t.capacity() - 2);
    }

    fn overflowed(tracked_keys: usize, keys: u64) -> usize {
        let t = KeyTable::new(tracked_keys, q());
        (0..keys)
            .filter(|i| t.is_overflow(t.slot(crate::key::key_hash(&format!("user-{i}")))))
            .count()
    }

    // Measured by the final review: with one bucket per key, 4096 keys in a
    // table for 4096 put ~25 into the shared overflow slot, and 6000 denied
    // 4% of requests from well-behaved users.
    #[test]
    fn as_many_keys_as_tracked_all_get_a_slot_of_their_own() {
        assert_eq!(overflowed(4096, 4096), 0);
    }

    // Two buckets per key, the emptier one taken: up to half again as many
    // keys as tracked, the table three quarters full, nobody overflows.
    // Measured beyond: 13 of 7000, 337 of 8192.
    #[test]
    fn half_again_as_many_keys_as_tracked_still_all_get_a_slot() {
        assert_eq!(overflowed(4096, 6144), 0);
    }

    #[test]
    fn many_threads_inserting_many_keys_leave_one_slot_per_key() {
        let t = KeyTable::new(512, q());
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for key in 1..=300u64 {
                        t.slot(key).attempts.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        let mut seen = std::collections::HashMap::new();
        t.for_each(|key, slot| {
            *seen.entry(key).or_insert(0) += slot.attempts.load(Ordering::Relaxed);
        });
        assert_eq!(seen.len(), 300, "one slot per key");
        let lost = 8 * 300 - seen.values().sum::<u64>();
        assert!(
            lost < 8 * 300 / 100,
            "a racing loser's attempt may be lost, rarely: {lost}"
        );
    }
}

#[cfg(all(test, rateguard_loom))]
mod loom_tests {
    use super::*;

    fn q() -> Quota {
        Quota::new(100, 10)
    }

    #[test]
    fn two_threads_inserting_one_key_agree_on_its_slot() {
        loom::model(|| {
            let t = loom::sync::Arc::new(KeyTable::new(1, q()));
            let (a, b) = (t.clone(), t.clone());
            let ha = loom::thread::spawn(move || a.slot(7) as *const Slot as usize);
            let hb = loom::thread::spawn(move || b.slot(7) as *const Slot as usize);
            let (pa, pb) = (ha.join().unwrap(), hb.join().unwrap());
            assert_eq!(pa, pb);
            let mut count = 0;
            t.for_each(|key, _| count += usize::from(key == 7));
            assert_eq!(count, 1);
        });
    }

    // The race the final review found: two threads insert one key while a
    // third frees a slot ahead of theirs. One copy must remain.
    #[test]
    fn two_inserters_and_a_release_leave_one_copy() {
        loom::model(|| {
            let t = loom::sync::Arc::new(KeyTable::new(1, q()));
            t.slot(1);
            let threads: Vec<_> = (0..3)
                .map(|n| {
                    let t = t.clone();
                    loom::thread::spawn(move || {
                        if n == 0 {
                            let slot = t.slot(1);
                            t.release(slot, 1, q());
                        } else {
                            t.slot(7);
                        }
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            let mut copies = 0;
            t.for_each(|key, _| copies += usize::from(key == 7));
            assert_eq!(copies, 1);
        });
    }

    #[test]
    fn a_release_racing_an_insert_leaves_a_consistent_bucket() {
        loom::model(|| {
            let t = loom::sync::Arc::new(KeyTable::new(1, q()));
            t.slot(1);
            let (a, b) = (t.clone(), t.clone());
            let ha = loom::thread::spawn(move || {
                let slot = a.slot(1);
                a.release(slot, 1, q());
            });
            let hb = loom::thread::spawn(move || {
                b.slot(2);
            });
            ha.join().unwrap();
            hb.join().unwrap();
            let mut keys = Vec::new();
            t.for_each(|key, _| keys.push(key));
            assert_eq!(keys, vec![2]);
        });
    }
}
