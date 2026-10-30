//! The classes of limits of one node: the default class and the named ones.
//!
//! Read in two places: by the background task once a tick, and on the
//! request path when a key takes a slot of its own for the first time, to
//! give the slot its class's quota. The default class's limit is two atomics,
//! so a flood of new keys in the default class, the usual flood, takes no
//! lock; a new key in a named class takes a read lock, once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use rateguard_core::gcra::Quota;
use rateguard_core::limiter::Limit as CoreLimit;

use crate::config::Error;
use crate::key::class_hash;
use crate::key_limits::{Entry, KeyLimit};
use crate::limit::Limit;

/// The most classes one node keeps.
pub(crate) const MAX_CLASSES: usize = 4096;

/// Named classes checked, the one rule for a builder and for
/// `set_classes`: names not empty, limits in range, no more than `max`; a
/// name given twice takes its last limit.
pub(crate) fn checked<S: Into<String>>(
    classes: impl IntoIterator<Item = (S, Limit)>,
    max: usize,
) -> Result<Vec<(String, Limit, CoreLimit)>, Error> {
    let mut named: HashMap<u64, (String, Limit, CoreLimit)> = HashMap::new();
    for (name, limit) in classes {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::EmptyClassName);
        }
        let core = limit.to_core()?;
        named.insert(class_hash(&name), (name, limit, core));
        if named.len() > max {
            return Err(Error::TooManyClasses(max));
        }
    }
    Ok(named.into_values().collect())
}

/// The limit of one slot as the tick or a new key finds it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Resolved {
    pub limit: CoreLimit,
    /// Whether the class the limit came from exists: an unknown one took
    /// the default.
    pub known: bool,
    /// The shadow of the class whose limit it is, if that class has one;
    /// never for a key with a limit of its own.
    pub shadow: Option<CoreLimit>,
}

/// The quota of a shadow next to the quota `main` a slot was just given for
/// `limit`: `main` scaled by the ratio of the two limits, with the shadow's
/// burst. The node's own share and every hold on it, `joining`, `learning`,
/// `without_quorum`, are factors of `main`, and so they act on the shadow
/// alike. See [`Guard::set_shadow`](crate::Guard::set_shadow) for the side
/// this errs on.
pub(crate) fn shadow_quota(
    main: Quota,
    limit: CoreLimit,
    shadow: Option<CoreLimit>,
) -> Option<Quota> {
    shadow.map(|shadow| {
        Quota::per_second(main.rate_per_sec() * shadow.rate / limit.rate, shadow.burst)
    })
}

/// The classes as of one moment.
#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub default: CoreLimit,
    /// The default as given, for reading back.
    default_given: Limit,
    /// Each class as given, for reading back, and as the core takes it.
    named: HashMap<u64, (String, Limit, CoreLimit)>,
    /// The shadows by the hash of their class, 0 for the default; as
    /// given, and as the core takes them. Only of classes in `named`, or
    /// the default.
    shadows: HashMap<u64, (Limit, CoreLimit)>,
}
impl Table {
    /// The limit of the class with this hash, and whether there is such a
    /// class: an unknown one, a typo or a class since removed, takes the
    /// default.
    pub fn lookup(&self, class: u64) -> (CoreLimit, bool) {
        if class == 0 {
            return (self.default, true);
        }
        match self.named.get(&class) {
            Some(&(_, _, limit)) => (limit, true),
            None => (self.default, false),
        }
    }

    /// The limit of a slot of the class `class` holding the key `key`: its
    /// key limit if it has one, else its class's. A key given a class takes
    /// that class's limit as it is in this table.
    ///
    /// The shadow is that of the class the limit came from: a key given a
    /// class follows that class's shadow, one given a limit of its own has
    /// none, and one held to the default for want of its class follows the
    /// default's.
    pub fn resolve(&self, overrides: &HashMap<u64, Entry>, class: u64, key: u64) -> Resolved {
        let class = match overrides.get(&key) {
            Some((_, _, KeyLimit::Limit(limit))) => match limit.to_core() {
                Ok(limit) => {
                    return Resolved {
                        limit,
                        known: true,
                        shadow: None,
                    };
                }
                // Checked when set; the class's, were it ever not.
                Err(_) => class,
            },
            Some((_, _, KeyLimit::Class(name))) => class_hash(name),
            None => class,
        };
        let (limit, known) = self.lookup(class);
        let shadow_of = if known { class } else { 0 };
        Resolved {
            limit,
            known,
            shadow: self.shadows.get(&shadow_of).map(|&(_, core)| core),
        }
    }

    /// What [`resolve`](Table::resolve) says, with the limits as given
    /// rather than as the core takes them: the limit of a slot and its
    /// shadow, for reading back.
    pub fn resolve_given(
        &self,
        overrides: &HashMap<u64, Entry>,
        class: u64,
        key: u64,
    ) -> (Limit, Option<Limit>) {
        let class = match overrides.get(&key) {
            Some((_, _, KeyLimit::Limit(limit))) if limit.to_core().is_ok() => {
                return (*limit, None);
            }
            Some((_, _, KeyLimit::Class(name))) => class_hash(name),
            _ => class,
        };
        let (limit, shadow_of) = match self.named.get(&class) {
            Some(&(_, given, _)) => (given, class),
            None => (self.default_given, 0),
        };
        (limit, self.shadows.get(&shadow_of).map(|&(given, _)| given))
    }

    /// Whether any class has a shadow.
    pub fn has_shadows(&self) -> bool {
        !self.shadows.is_empty()
    }

    /// The hash of the class `name`, `""` for the default, if there is such
    /// a class.
    fn class_named(&self, name: &str) -> Result<u64, Error> {
        if name.is_empty() {
            return Ok(0);
        }
        let class = class_hash(name);
        if self.named.contains_key(&class) {
            Ok(class)
        } else {
            Err(Error::UnknownClass(name.to_owned()))
        }
    }

    /// The strictest of every class's rate and every class's burst: the
    /// quota a slot no key holds yet carries, so that a key whose class is
    /// not yet set errs on the strict side.
    ///
    /// The limits of single keys are not in it, on purpose: one penalty,
    /// "this abuser gets 1 a second", would lower the cold quota of every
    /// new key of the node. A key with a limit stricter than any class may
    /// thus pass at a looser quota: a check on another thread in the
    /// nanoseconds between the key's taking a slot and the slot getting its
    /// own quota, at this one; a key whose first check finds no key limit
    /// at all as the first is being set, at its class's until the next
    /// tick; and a key in the overflow slot, while there.
    pub fn strictest(&self) -> CoreLimit {
        self.named
            .values()
            .fold(self.default, |strictest, &(_, _, limit)| CoreLimit {
                rate: strictest.rate.min(limit.rate),
                burst: strictest.burst.min(limit.burst),
            })
    }
}

#[derive(Debug)]
pub(crate) struct Classes {
    max: usize,
    /// The default class's limit: the rate as the bits of an `f64`, and the
    /// burst. Two atomics, not one packed word, for a rate takes 64 bits
    /// now. A `set_limit` racing a first request of a new key may hand that
    /// key a mix of the old and the new limit, the rate of one and the
    /// burst of the other, until the next tick gives it its own. A flood of
    /// new keys takes no lock, which is the point.
    default_rate: AtomicU64,
    default_burst: AtomicU32,
    /// Whether any class has a shadow: when none has, the usual case, a new
    /// key of the default class still takes no lock.
    has_shadows: AtomicBool,
    table: RwLock<Arc<Table>>,
    /// Held by whoever changes the table, from the copy to the swap, so that
    /// two changes do not lose one another's edit while the readers' lock is
    /// free.
    writer: Mutex<()>,
}
impl Classes {
    /// Checked by the builder: names not empty, no more than `max`.
    pub fn new(
        given: Limit,
        default: CoreLimit,
        named: Vec<(String, Limit, CoreLimit)>,
        max: usize,
    ) -> Self {
        let named = named
            .into_iter()
            .map(|(name, given, limit)| (class_hash(&name), (name, given, limit)))
            .collect();
        Self {
            max,
            default_rate: AtomicU64::new(default.rate.to_bits()),
            default_burst: AtomicU32::new(default.burst),
            has_shadows: AtomicBool::new(false),
            table: RwLock::new(Arc::new(Table {
                default,
                default_given: given,
                named,
                shadows: HashMap::new(),
            })),
            writer: Mutex::new(()),
        }
    }

    /// The classes as of now.
    pub fn snapshot(&self) -> Arc<Table> {
        self.table
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The limit of the class with this hash, for a key taking its first
    /// slot; see [`Table::lookup`].
    pub fn limit_of(&self, class: u64) -> CoreLimit {
        if class == 0 {
            return CoreLimit {
                rate: f64::from_bits(self.default_rate.load(Ordering::Relaxed)),
                burst: self.default_burst.load(Ordering::Relaxed),
            };
        }
        self.table
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .lookup(class)
            .0
    }

    /// The limit of a key taking its first slot, and its shadow: its key
    /// limit, if it has one, else its class's, as
    /// [`limit_of`](Classes::limit_of) says, without a lock for the default
    /// class while no class has a shadow. The key limits come as they are
    /// in the map the caller holds.
    pub fn limit_for(
        &self,
        overrides: &HashMap<u64, Entry>,
        class: u64,
        key: u64,
    ) -> (CoreLimit, Option<CoreLimit>) {
        if self.has_shadows() || overrides.contains_key(&key) {
            let resolved = self.snapshot().resolve(overrides, class, key);
            (resolved.limit, resolved.shadow)
        } else {
            (self.limit_of(class), None)
        }
    }

    /// Whether any class has a shadow, without a lock.
    pub fn has_shadows(&self) -> bool {
        self.has_shadows.load(Ordering::Relaxed)
    }

    pub fn set(&self, name: &str, limit: Limit) -> Result<(), Error> {
        if name.is_empty() {
            return Err(Error::EmptyClassName);
        }
        let core = limit.to_core()?;
        self.change(|table| {
            let hash = class_hash(name);
            if !table.named.contains_key(&hash) && table.named.len() == self.max {
                return Err(Error::TooManyClasses(self.max));
            }
            table.named.insert(hash, (name.to_owned(), limit, core));
            Ok(())
        })
    }

    /// Removes the class `name`, and its shadow with it.
    pub fn remove(&self, name: &str) -> bool {
        let mut removed = false;
        let _ = self.change(|table| {
            let hash = class_hash(name);
            removed = table.named.remove(&hash).is_some();
            // Only a named class's: `""` hashes to 0, the default's.
            if removed {
                table.shadows.remove(&hash);
            }
            Ok(())
        });
        removed
    }

    pub fn set_default(&self, limit: Limit) -> Result<(), Error> {
        let given = limit;
        let limit = limit.to_core()?;
        self.change(|table| {
            table.default = limit;
            table.default_given = given;
            Ok(())
        })
    }

    /// Every named class at once: those left out are removed, with their
    /// shadows. All or nothing.
    pub fn replace<S: Into<String>>(
        &self,
        classes: impl IntoIterator<Item = (S, Limit)>,
    ) -> Result<(), Error> {
        let named: HashMap<_, _> = checked(classes, self.max)?
            .into_iter()
            .map(|(name, given, limit)| (class_hash(&name), (name, given, limit)))
            .collect();
        self.change(|table| {
            table
                .shadows
                .retain(|class, _| *class == 0 || named.contains_key(class));
            table.named = named;
            Ok(())
        })
    }

    /// Gives the class `name`, `""` for the default, the shadow `limit`, or
    /// takes its shadow away with `None`.
    pub fn set_shadow(&self, name: &str, limit: Option<Limit>) -> Result<(), Error> {
        let limit = limit
            .map(|limit| Ok((limit, limit.to_core()?)))
            .transpose()?;
        self.change(|table| {
            let class = table.class_named(name)?;
            match limit {
                Some(limit) => table.shadows.insert(class, limit),
                None => table.shadows.remove(&class),
            };
            Ok(())
        })
    }

    /// Every shadow at once: those left out are removed. All or nothing; a
    /// class given twice takes its last shadow.
    pub fn replace_shadows<S: Into<String>>(
        &self,
        shadows: impl IntoIterator<Item = (S, Limit)>,
    ) -> Result<(), Error> {
        let shadows: Vec<(String, Limit, CoreLimit)> = shadows
            .into_iter()
            .map(|(name, limit)| Ok((name.into(), limit, limit.to_core()?)))
            .collect::<Result<_, Error>>()?;
        self.change(|table| {
            let mut all = HashMap::new();
            for (name, limit, core) in shadows {
                all.insert(table.class_named(&name)?, (limit, core));
            }
            table.shadows = all;
            Ok(())
        })
    }

    /// The shadows by class name, `""` for the default, in no particular
    /// order.
    pub fn shadows(&self) -> Vec<(String, Limit)> {
        let table = self.snapshot();
        table
            .shadows
            .iter()
            .map(|(&class, &(limit, _))| {
                let name = match table.named.get(&class) {
                    Some((name, _, _)) => name.clone(),
                    None => String::new(),
                };
                (name, limit)
            })
            .collect()
    }

    /// The named classes, in no particular order.
    pub fn list(&self) -> Vec<(String, Limit)> {
        self.snapshot()
            .named
            .values()
            .map(|(name, limit, _)| (name.clone(), *limit))
            .collect()
    }

    // A new table, swapped in whole: readers see the old one or the new. The
    // copy, as long as the table is big, is made under `writer` alone; the
    // readers' lock is taken for the swap only, so a new key's lookup waits
    // a pointer's store at most.
    fn change(&self, edit: impl FnOnce(&mut Table) -> Result<(), Error>) -> Result<(), Error> {
        let _writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut table = Table::clone(&self.snapshot());
        edit(&mut table)?;
        let new = Arc::new(table);
        // The atomics are stored under the write lock, with the swap, so that
        // a reader taking the lock after it sees a table no older than them.
        // A reader that takes no lock may see them a moment before or after
        // the table, which the atomics' own mix of rate and burst already
        // allows.
        let old = {
            let mut current = self.table.write().unwrap_or_else(PoisonError::into_inner);
            self.default_rate
                .store(new.default.rate.to_bits(), Ordering::Relaxed);
            self.default_burst
                .store(new.default.burst, Ordering::Relaxed);
            self.has_shadows.store(new.has_shadows(), Ordering::Relaxed);
            std::mem::replace(&mut *current, new)
        };
        // Dropped after the lock is let go: freeing a big table takes as long
        // as copying it.
        drop(old);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The rule of which limit a slot takes is written twice, once for the
    // core and once for reading back: the two agree on every case.
    #[test]
    fn resolve_and_resolve_given_agree() {
        let core = |limit: Limit| limit.to_core().unwrap();
        let default = Limit::per_second(100);
        let pro = Limit::per_minute(600).burst(20);
        let free = Limit::per_hour(50);
        let classes = Classes::new(
            default,
            core(default),
            vec![
                ("pro".to_owned(), pro, core(pro)),
                ("free".to_owned(), free, core(free)),
            ],
            MAX_CLASSES,
        );
        classes
            .set_shadow("pro", Some(Limit::per_second(5)))
            .unwrap();
        classes.set_shadow("", Some(Limit::per_second(50))).unwrap();
        let own = Limit::per_second(7);
        let overrides: HashMap<u64, Entry> = [
            (1, KeyLimit::from(own)),
            (2, KeyLimit::class("pro")),
            (3, KeyLimit::class("free")),
            (4, KeyLimit::class("gone")),
        ]
        .into_iter()
        .map(|(key, limit)| (key, (String::new(), key.to_string(), limit)))
        .collect();
        let table = classes.snapshot();
        for class in ["", "pro", "free", "gone"].map(class_hash) {
            // Key 0 has no limit of its own.
            for key in 0..=4 {
                let resolved = table.resolve(&overrides, class, key);
                let (limit, shadow) = table.resolve_given(&overrides, class, key);
                let case = format!("class {class:x}, key {key}");
                assert_eq!(resolved.limit, core(limit), "{case}");
                assert_eq!(resolved.shadow, shadow.map(core), "{case}");
            }
        }
    }

    #[test]
    fn a_class_change_does_not_hold_readers_for_the_copy() {
        let named = (0..4096)
            .map(|i| {
                let limit = Limit::per_second(10);
                (format!("class-{i}"), limit, limit.to_core().unwrap())
            })
            .collect();
        let default = Limit::per_second(100);
        let classes = Classes::new(default, default.to_core().unwrap(), named, 8192);
        let mut ran = false;
        classes
            .change(|table| {
                // The edit runs after the copy is built: a reader, or the
                // lock itself, must not wait for it.
                ran = true;
                assert!(classes.table.try_read().is_ok(), "readers are locked out");
                assert!(classes.table.try_write().is_ok(), "the lock is held");
                assert_eq!(table.named.len(), 4096);
                Ok(())
            })
            .unwrap();
        assert!(ran);
        // A change taking the writer lock is not reentrant for another one,
        // yet readers still pass while one is in progress.
        classes
            .change(|_| {
                assert!(classes.writer.try_lock().is_err());
                let _ = classes.limit_of(class_hash("class-7"));
                Ok(())
            })
            .unwrap();
    }
}
