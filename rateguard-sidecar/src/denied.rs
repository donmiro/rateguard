//! The keys behind the hashes of [`Stats::most_denied`](rateguard::Stats::most_denied).

/// A denied key's class and string, by the hash this node checks it under,
/// for naming the entries of `/v1/stats` `most_denied`. Clients in other
/// languages cannot compute a hash, and with a key salt nobody outside the
/// node can.
///
/// Bounded: past `capacity` the entry recorded or refreshed longest ago
/// goes first.
pub struct DeniedKeys {
    capacity: usize,
    named: std::collections::HashMap<u64, Named>,
    // Oldest first.
    order: std::collections::VecDeque<u64>,
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

/// The longest key or class kept, in bytes: the table must not grow with
/// long keys.
pub const MAX_LEN: usize = 256;

impl DeniedKeys {
    /// An empty table of at most `capacity` keys.
    pub fn new(capacity: usize) -> Self {
        DeniedKeys {
            capacity,
            named: std::collections::HashMap::with_capacity(capacity),
            order: std::collections::VecDeque::with_capacity(capacity),
        }
    }

    /// Remembers that `hash` is `key` of `class`, as its newest entry.
    pub fn record(&mut self, hash: u64, class: &str, key: &str) {
        if self.capacity == 0 {
            return;
        }
        if self.named.contains_key(&hash) {
            // A key under attack is denied over and over: when it is
            // already the newest, nothing moves.
            if self.order.back() != Some(&hash)
                && let Some(at) = self.order.iter().position(|&h| h == hash)
            {
                self.order.remove(at);
                self.order.push_back(hash);
            }
            return;
        }
        if self.named.len() >= self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.named.remove(&oldest);
        }
        let (key, truncated) = cut(key);
        // A class this node does not know is taken as sent, so it is cut as
        // a key is; a class configured is far shorter.
        let (class, _) = cut(class);
        self.named.insert(
            hash,
            Named {
                class: class.to_owned(),
                key: key.to_owned(),
                truncated,
            },
        );
        self.order.push_back(hash);
    }

    /// What `hash` stands for, if it is here.
    pub fn get(&self, hash: u64) -> Option<&Named> {
        self.named.get(&hash)
    }

    /// How many keys are here.
    pub fn len(&self) -> usize {
        self.named.len()
    }

    /// Whether no key is here.
    pub fn is_empty(&self) -> bool {
        self.named.is_empty()
    }
}

impl Default for DeniedKeys {
    /// The table [`App::new`](crate::App::new) gives a sidecar: 1024 keys.
    fn default() -> Self {
        DeniedKeys::new(1024)
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

    #[test]
    fn a_long_class_is_cut_too() {
        let mut keys = DeniedKeys::new(1);
        keys.record(1, &"c".repeat(1000), "k");
        let named = keys.get(1).unwrap();
        assert_eq!(named.class.len(), MAX_LEN);
        assert!(!named.truncated, "the flag is the key's");
    }
}
