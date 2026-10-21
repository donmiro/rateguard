//! The keys behind the hashes of [`Stats::most_denied`](rateguard::Stats::most_denied).

/// A denied key's class and string, by the hash this node checks it under,
/// for naming the entries of `/v1/stats` `most_denied`. Clients in other
/// languages cannot compute a hash, and with a key salt nobody outside the
/// node can.
///
/// Bounded: two tables of `capacity` keys each, one for keys really denied
/// and one for keys only a shadow denied, so that a shadow being tried out
/// on many keys pushes out no name of a real denial. In each, past
/// `capacity`, the entry recorded or refreshed longest ago goes first.
pub struct DeniedKeys {
    real: Recent,
    shadow: Recent,
}

/// What a hash in [`DeniedKeys`] stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Named {
    /// The key's class, `""` for the default one.
    pub class: String,
    /// The key, its first [`MAX_LEN`] bytes at most.
    pub key: String,
    /// Whether `key` was cut.
    pub truncated: bool,
}
impl Named {
    fn new(class: &str, key: &str) -> Self {
        let (key, truncated) = cut(key);
        // A class this node does not know is taken as sent, so it is cut as
        // a key is; a class configured is far shorter.
        let (class, _) = cut(class);
        Named {
            class: class.to_owned(),
            key: key.to_owned(),
            truncated,
        }
    }
}

/// The longest key or class kept, in bytes: the table must not grow with
/// long keys.
pub const MAX_LEN: usize = 256;

impl DeniedKeys {
    /// Two empty tables of at most `capacity` keys each.
    pub fn new(capacity: usize) -> Self {
        DeniedKeys {
            real: Recent::new(capacity),
            shadow: Recent::new(capacity),
        }
    }

    /// Remembers that `hash`, which was just denied, is `key` of `class`,
    /// as its newest entry.
    pub fn record(&mut self, hash: u64, class: &str, key: &str) {
        if self.real.refresh(hash) {
            return;
        }
        let named = self
            .shadow
            .remove(hash)
            .unwrap_or_else(|| Named::new(class, key));
        self.real.insert(hash, named);
    }

    /// Like [`record`](DeniedKeys::record), for a key only its shadow
    /// denied. A key named for a real denial stays where it is: refreshing
    /// it here would let shadow denials keep a name the real ones no longer
    /// need, and it is named either way.
    pub(crate) fn record_shadow(&mut self, hash: u64, class: &str, key: &str) {
        if self.real.contains(hash) || self.shadow.refresh(hash) {
            return;
        }
        self.shadow.insert(hash, Named::new(class, key));
    }

    /// What `hash` stands for, if it is here.
    pub fn get(&self, hash: u64) -> Option<&Named> {
        self.real.get(hash).or_else(|| self.shadow.get(hash))
    }

    /// How many keys are here, in both tables.
    pub fn len(&self) -> usize {
        self.real.named.len() + self.shadow.named.len()
    }

    /// Whether no key is here.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for DeniedKeys {
    /// The tables [`App::new`](crate::App::new) gives a sidecar: 1024 keys
    /// each.
    fn default() -> Self {
        DeniedKeys::new(1024)
    }
}

/// Names, the one recorded or refreshed longest ago going first.
///
/// Every entry carries the stamp of its last record, and `order` the
/// stamps in the order given: a refresh pushes a new stamp and leaves the
/// old one behind, stale, rather than looking for it, so that it costs the
/// same whatever the size of the table. The stale ones are skipped when
/// they come up for eviction, and dropped all at once whenever they make
/// up half of `order`: amortized, still O(1).
struct Recent {
    capacity: usize,
    named: std::collections::HashMap<u64, (Named, u64)>,
    // (hash, stamp), oldest first.
    order: std::collections::VecDeque<(u64, u64)>,
    next: u64,
}
impl Recent {
    fn new(capacity: usize) -> Self {
        Recent {
            capacity,
            named: std::collections::HashMap::with_capacity(capacity),
            order: std::collections::VecDeque::with_capacity(2 * capacity + 1),
            next: 0,
        }
    }

    fn contains(&self, hash: u64) -> bool {
        self.named.contains_key(&hash)
    }

    fn get(&self, hash: u64) -> Option<&Named> {
        self.named.get(&hash).map(|(named, _)| named)
    }

    // Makes `hash` the newest, if it is here.
    fn refresh(&mut self, hash: u64) -> bool {
        let Some((_, stamp)) = self.named.get_mut(&hash) else {
            return false;
        };
        // A key under attack is denied over and over: when it is already
        // the newest, nothing moves.
        if self.order.back() != Some(&(hash, *stamp)) {
            *stamp = self.next;
            self.order.push_back((hash, self.next));
            self.next += 1;
            self.compact();
        }
        true
    }

    fn insert(&mut self, hash: u64, named: Named) {
        if self.capacity == 0 {
            return;
        }
        while self.named.len() >= self.capacity {
            let Some((oldest, stamp)) = self.order.pop_front() else {
                break;
            };
            if self.named.get(&oldest).is_some_and(|&(_, s)| s == stamp) {
                self.named.remove(&oldest);
            }
        }
        self.named.insert(hash, (named, self.next));
        self.order.push_back((hash, self.next));
        self.next += 1;
        self.compact();
    }

    fn remove(&mut self, hash: u64) -> Option<Named> {
        // Its stamp in `order` goes stale.
        let (named, _) = self.named.remove(&hash)?;
        self.compact();
        Some(named)
    }

    // Drops the stale stamps once they are as many as the live ones: one
    // pass over `order` for every `capacity` records or so.
    fn compact(&mut self) {
        if self.order.len() > 2 * self.capacity {
            let named = &self.named;
            self.order
                .retain(|(hash, stamp)| named.get(hash).is_some_and(|&(_, s)| s == *stamp));
        }
    }
}

// At the last char boundary at or below MAX_LEN bytes.
// `str::floor_char_boundary` is not stable on the MSRV.
fn cut(s: &str) -> (&str, bool) {
    if s.len() <= MAX_LEN {
        return (s, false);
    }
    let mut end = MAX_LEN;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_oldest_goes_first() {
        let mut keys = DeniedKeys::new(2);
        keys.record(1, "", "a");
        keys.record(2, "", "b");
        keys.record(3, "pro", "c");
        assert_eq!(keys.get(1), None);
        assert_eq!(keys.get(2).unwrap().key, "b");
        let c = keys.get(3).unwrap();
        assert_eq!((c.class.as_str(), c.key.as_str()), ("pro", "c"));
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn a_long_key_is_cut_at_a_char_boundary() {
        let mut keys = DeniedKeys::new(4);
        // "é" takes bytes 255 and 256: byte 256 is inside it.
        let key = format!("{}é{}", "a".repeat(255), "b".repeat(100));
        keys.record(1, "", &key);
        let named = keys.get(1).unwrap();
        assert_eq!(named.key, "a".repeat(255));
        assert!(named.truncated);

        keys.record(2, "", &"a".repeat(256));
        let named = keys.get(2).unwrap();
        assert_eq!(named.key.len(), 256);
        assert!(!named.truncated, "256 bytes is not over");
    }

    #[test]
    fn recording_a_known_hash_again_refreshes_it() {
        let mut keys = DeniedKeys::new(2);
        keys.record(1, "", "a");
        keys.record(2, "", "b");
        keys.record(1, "", "a");
        keys.record(3, "", "c");
        assert_eq!(keys.get(1).unwrap().key, "a", "refreshed, so kept");
        assert_eq!(keys.get(2), None, "now the oldest");
        assert_eq!(keys.len(), 2);
    }

    // Keys only a shadow denies, however many, push out none of the names
    // of keys really denied: they have their own table.
    #[test]
    fn shadow_churn_does_not_evict_real_denials() {
        let mut keys = DeniedKeys::new(2);
        keys.record(1, "", "real");
        for hash in 2..1000 {
            keys.record_shadow(hash, "", "shadow");
        }
        assert_eq!(keys.get(1).unwrap().key, "real");
        assert_eq!(keys.get(999).unwrap().key, "shadow");
        assert_eq!(keys.get(2), None, "the shadow table is bounded too");
        assert_eq!(keys.len(), 3);
    }

    // A key denied for real after only a shadow denied it moves to the
    // real table; a shadow denial of a key named there changes nothing.
    #[test]
    fn a_key_is_named_once() {
        let mut keys = DeniedKeys::new(2);
        keys.record_shadow(1, "", "a");
        keys.record(1, "", "a");
        keys.record_shadow(1, "", "a");
        assert_eq!(keys.len(), 1);
        for hash in 2..10 {
            keys.record_shadow(hash, "", "shadow");
        }
        assert_eq!(keys.get(1).unwrap().key, "a", "named as a real denial");
    }

    // A key under attack is recorded at every denial: each refresh is O(1),
    // and the queue of recency stays within twice the table.
    #[test]
    fn refreshing_is_bounded() {
        let mut keys = DeniedKeys::new(4);
        for hash in 1..=4 {
            keys.record(hash, "", "k");
        }
        for i in 0..10_000u64 {
            keys.record(1 + i % 3, "", "k");
            assert!(keys.real.order.len() <= 2 * 4 + 1);
        }
        keys.record(5, "", "k");
        assert_eq!(keys.get(4), None, "the one not refreshed goes first");
        assert_eq!(keys.len(), 4);
    }

    #[test]
    fn a_long_class_is_cut_too() {
        let mut keys = DeniedKeys::new(1);
        keys.record(1, &"c".repeat(1000), "k");
        let named = keys.get(1).unwrap();
        assert_eq!(named.class.len(), MAX_LEN);
        assert!(!named.truncated, "the flag is the key's");
    }
}
