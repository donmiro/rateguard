//! Limits of single keys: the exceptions to what a key's class says.
//!
//! Read in the same two places as the classes, and no others: by the
//! background task once a tick, for each slot, and on the request path when
//! a key takes a slot of its own for the first time, and then only if there
//! is any exception at all. In the steady state the request path never
//! comes here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use crate::config::Error;
use crate::limit::Limit;

/// The most key limits one node keeps, whatever `max_key_limits` says.
pub(crate) const MAX_KEY_LIMITS: usize = 1 << 20;

/// The limit of one key, set with
/// [`Guard::set_key_limit`](crate::Guard::set_key_limit): its own numbers,
/// or those of a class.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum KeyLimit {
    /// This limit, whatever the key's class says. The class is then not
    /// looked up: checks in a class this node does not know are not
    /// counted in [`Stats::unknown_class`](crate::Stats::unknown_class).
    Limit(Limit),
    /// The limit of the class of this name, as it is at each moment: when
    /// the class changes, the keys given its limit follow. A class this
    /// node does not know gives the default limit, and the key's checks are
    /// counted in [`Stats::unknown_class`](crate::Stats::unknown_class).
    Class(String),
}
impl KeyLimit {
    /// The limit of the class `name`, followed as the class changes.
    pub fn class(name: impl Into<String>) -> Self {
        KeyLimit::Class(name.into())
    }
}
impl From<Limit> for KeyLimit {
    fn from(limit: Limit) -> Self {
        KeyLimit::Limit(limit)
    }
}

/// What the table holds for a key: its class and key as given, for reading
/// back, and its limit.
pub(crate) type Entry = (String, String, KeyLimit);

/// The key limits of one node, as a map by the salted hash of the key: the
/// hash the key table and the driver already have.
#[derive(Debug)]
pub(crate) struct KeyLimits {
    max: usize,
    /// How many there are, for the request path to skip the lock when there
    /// are none.
    count: AtomicUsize,
    /// Held only to swap the pointer, never while the map is copied: a
    /// new key reads it while any limit exists, and a reader waits behind a
    /// waiting writer.
    table: RwLock<Arc<HashMap<u64, Entry>>>,
    /// One change at a time, held while the copy is made and edited, so
    /// that two changes do not each edit their own copy and one get lost.
    writer: Mutex<()>,
}
impl KeyLimits {
    /// `max` is checked by the builder.
    pub fn new(max: usize) -> Self {
        Self {
            max,
            count: AtomicUsize::new(0),
            table: RwLock::default(),
            writer: Mutex::new(()),
        }
    }

    /// Whether there is none, the usual case.
    pub fn is_empty(&self) -> bool {
        self.count.load(Ordering::Relaxed) == 0
    }

    /// The key limits as of now.
    pub fn snapshot(&self) -> Arc<HashMap<u64, Entry>> {
        self.table
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Runs `read` on the key limits as of now, without taking a copy of
    /// the pointer.
    pub fn with<T>(&self, read: impl FnOnce(&HashMap<u64, Entry>) -> T) -> T {
        read(&self.table.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub fn set(&self, hash: u64, entry: Entry) -> Result<(), Error> {
        check(&entry.2)?;
        self.change(|table| {
            if !table.contains_key(&hash) && table.len() == self.max {
                return Err(Error::TooManyKeyLimits(self.max));
            }
            table.insert(hash, entry);
            Ok(())
        })
    }

    pub fn remove(&self, hash: u64) -> bool {
        let _writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let current = self.snapshot();
        // Nothing to remove, nothing to copy or swap: the usual case of a
        // client clearing a key it never limited.
        if !current.contains_key(&hash) {
            return false;
        }
        let mut table = HashMap::clone(&current);
        table.remove(&hash);
        self.swap(table);
        true
    }

    /// Every key limit at once: those left out are removed. All or nothing;
    /// a key given twice takes its last limit.
    pub fn replace(&self, entries: impl IntoIterator<Item = (u64, Entry)>) -> Result<(), Error> {
        let mut named = HashMap::new();
        for (hash, entry) in entries {
            check(&entry.2)?;
            named.insert(hash, entry);
            if named.len() > self.max {
                return Err(Error::TooManyKeyLimits(self.max));
            }
        }
        // Nothing of the old map is kept, so nothing of it is copied.
        let _writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        self.swap(named);
        Ok(())
    }

    /// The key limits, in no particular order.
    pub fn list(&self) -> Vec<Entry> {
        self.snapshot().values().cloned().collect()
    }

    // A new map, swapped in whole: readers see the old one or the new. The
    // copy, as long as the map is big, is made under `writer` alone; the
    // readers' lock is taken for the swap only, so a new key's check waits
    // a pointer's store at most, whatever the number of limits. One limit
    // at a time still copies the whole map, so a big set goes in at once,
    // with `replace`, which copies nothing.
    fn change(
        &self,
        edit: impl FnOnce(&mut HashMap<u64, Entry>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let _writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut table = HashMap::clone(&self.snapshot());
        edit(&mut table)?;
        self.swap(table);
        Ok(())
    }

    // Called holding `writer`. The old map is dropped after the readers'
    // lock is let go: freeing a big one takes as long as copying it.
    fn swap(&self, table: HashMap<u64, Entry>) {
        self.count.store(table.len(), Ordering::Relaxed);
        let table = Arc::new(table);
        let old = std::mem::replace(
            &mut *self.table.write().unwrap_or_else(PoisonError::into_inner),
            table,
        );
        drop(old);
    }
}

// A limit of a key is checked when set, so that the tick has none to
// refuse; a class is only looked up by name, whenever it is read.
fn check(limit: &KeyLimit) -> Result<(), Error> {
    match limit {
        KeyLimit::Limit(limit) => limit.to_core().map(|_| ()),
        KeyLimit::Class(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, limit: impl Into<KeyLimit>) -> Entry {
        (String::new(), key.to_owned(), limit.into())
    }

    #[test]
    fn the_count_follows_the_table() {
        let limits = KeyLimits::new(2);
        assert!(limits.is_empty());
        limits.set(1, entry("a", Limit::per_second(5))).unwrap();
        limits.set(2, entry("b", KeyLimit::class("pro"))).unwrap();
        assert!(!limits.is_empty());
        assert!(matches!(
            limits.set(3, entry("c", Limit::per_second(5))),
            Err(Error::TooManyKeyLimits(2))
        ));
        limits.set(1, entry("a", Limit::per_second(6))).unwrap();
        assert!(limits.remove(1));
        assert!(!limits.remove(1));
        assert!(limits.remove(2));
        assert!(limits.is_empty());
    }

    // Removing a key that has no limit copies nothing and swaps nothing:
    // a client cleaning up after itself must not stall new keys.
    #[test]
    fn removing_an_absent_key_leaves_the_table_as_it_is() {
        let limits = KeyLimits::new(2);
        limits.set(1, entry("a", Limit::per_second(5))).unwrap();
        let before = limits.snapshot();
        assert!(!limits.remove(2));
        assert!(Arc::ptr_eq(&before, &limits.snapshot()), "swapped");
        assert!(limits.remove(1));
        assert!(!Arc::ptr_eq(&before, &limits.snapshot()));
    }

    // Writers wait for one another, not for a copy under the readers'
    // lock: many at once each land, none lost.
    #[test]
    fn concurrent_writers_lose_nothing() {
        let limits = KeyLimits::new(1000);
        std::thread::scope(|scope| {
            for t in 0..8u64 {
                let limits = &limits;
                scope.spawn(move || {
                    for i in 0..100 {
                        let hash = t * 100 + i;
                        limits.set(hash, entry("k", Limit::per_second(5))).unwrap();
                    }
                });
            }
        });
        assert_eq!(limits.list().len(), 800);
        assert!(!limits.is_empty());
    }

    #[test]
    fn a_limit_out_of_range_is_refused_when_set() {
        let limits = KeyLimits::new(2);
        assert!(matches!(
            limits.set(1, entry("a", Limit::per_second(0))),
            Err(Error::ZeroLimit)
        ));
        assert!(matches!(
            limits.set(1, entry("a", Limit::per_second(5).burst(1 << 24))),
            Err(Error::TooLargeBurst(_))
        ));
        assert!(limits.is_empty());
    }

    #[test]
    fn a_refused_replace_changes_nothing() {
        let limits = KeyLimits::new(1);
        limits.set(1, entry("a", Limit::per_second(5))).unwrap();
        let refused = limits.replace([
            (2, entry("b", Limit::per_second(5))),
            (3, entry("c", Limit::per_second(5))),
        ]);
        assert!(matches!(refused, Err(Error::TooManyKeyLimits(1))));
        assert_eq!(limits.list(), [entry("a", Limit::per_second(5))]);
        limits
            .replace([
                (2, entry("b", Limit::per_second(5))),
                (2, entry("b", Limit::per_second(7))),
            ])
            .unwrap();
        assert_eq!(limits.list(), [entry("b", Limit::per_second(7))]);
    }
}
