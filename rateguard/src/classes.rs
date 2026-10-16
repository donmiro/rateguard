//! The classes of limits of one node: the default class and the named ones.
//!
//! Read in two places: by the background task once a tick, and on the
//! request path when a key takes a slot of its own for the first time, to
//! give the slot its class's quota. The default class's limit is an atomic,
//! so a flood of new keys in the default class, the usual flood, takes no
//! lock; a new key in a named class takes a read lock, once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use rateguard_core::limiter::Limit as CoreLimit;

use crate::config::Error;
use crate::key::class_hash;
use crate::limit::Limit;

/// The most classes one node keeps.
pub(crate) const MAX_CLASSES: usize = 4096;

/// Named classes checked, the one rule for a builder and for
/// `set_classes`: names not empty, limits in range, no more than `max`; a
/// name given twice takes its last limit.
pub(crate) fn checked<S: Into<String>>(
    classes: impl IntoIterator<Item = (S, Limit)>,
    max: usize,
) -> Result<Vec<(String, CoreLimit)>, Error> {
    let mut named: HashMap<u64, (String, CoreLimit)> = HashMap::new();
    for (name, limit) in classes {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::EmptyClassName);
        }
        let limit = limit.to_core()?;
        named.insert(class_hash(&name), (name, limit));
        if named.len() > max {
            return Err(Error::TooManyClasses(max));
        }
    }
    Ok(named.into_values().collect())
}

/// The classes as of one moment.
#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub default: CoreLimit,
    named: HashMap<u64, (String, CoreLimit)>,
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
            Some(&(_, limit)) => (limit, true),
            None => (self.default, false),
        }
    }

    /// The strictest of every class's rate and every class's burst: the
    /// quota a slot no key holds yet carries, so that a key whose class is
    /// not yet set errs on the strict side.
    pub fn strictest(&self) -> CoreLimit {
        self.named
            .values()
            .fold(self.default, |strictest, &(_, limit)| CoreLimit {
                per_second: strictest.per_second.min(limit.per_second),
                burst: strictest.burst.min(limit.burst),
            })
    }
}

#[derive(Debug)]
pub(crate) struct Classes {
    max: usize,
    /// The default class's limit, packed: rate above, burst below.
    default: AtomicU64,
    table: RwLock<Arc<Table>>,
}
impl Classes {
    /// Checked by the builder: names not empty, no more than `max`.
    pub fn new(default: CoreLimit, named: Vec<(String, CoreLimit)>, max: usize) -> Self {
        let named = named
            .into_iter()
            .map(|(name, limit)| (class_hash(&name), (name, limit)))
            .collect();
        Self {
            max,
            default: AtomicU64::new(pack(default)),
            table: RwLock::new(Arc::new(Table { default, named })),
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
            return unpack(self.default.load(Ordering::Relaxed));
        }
        self.table
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .lookup(class)
            .0
    }

    pub fn set(&self, name: &str, limit: Limit) -> Result<(), Error> {
        if name.is_empty() {
            return Err(Error::EmptyClassName);
        }
        let limit = limit.to_core()?;
        self.change(|table| {
            let hash = class_hash(name);
            if !table.named.contains_key(&hash) && table.named.len() == self.max {
                return Err(Error::TooManyClasses(self.max));
            }
            table.named.insert(hash, (name.to_owned(), limit));
            Ok(())
        })
    }

    pub fn remove(&self, name: &str) -> bool {
        let mut removed = false;
        let _ = self.change(|table| {
            removed = table.named.remove(&class_hash(name)).is_some();
            Ok(())
        });
        removed
    }

    pub fn set_default(&self, limit: Limit) -> Result<(), Error> {
        let limit = limit.to_core()?;
        self.change(|table| {
            table.default = limit;
            Ok(())
        })
    }

    /// Every named class at once: those left out are removed. All or
    /// nothing.
    pub fn replace<S: Into<String>>(
        &self,
        classes: impl IntoIterator<Item = (S, Limit)>,
    ) -> Result<(), Error> {
        let named = checked(classes, self.max)?
            .into_iter()
            .map(|(name, limit)| (class_hash(&name), (name, limit)))
            .collect();
        self.change(|table| {
            table.named = named;
            Ok(())
        })
    }

    /// The named classes, in no particular order.
    pub fn list(&self) -> Vec<(String, Limit)> {
        self.snapshot()
            .named
            .values()
            .map(|(name, limit)| (name.clone(), Limit::from_core(*limit)))
            .collect()
    }

    // A new table, swapped in whole: readers see the old one or the new.
    fn change(&self, edit: impl FnOnce(&mut Table) -> Result<(), Error>) -> Result<(), Error> {
        let mut current = self.table.write().unwrap_or_else(PoisonError::into_inner);
        let mut table = Table::clone(&current);
        edit(&mut table)?;
        self.default.store(pack(table.default), Ordering::Relaxed);
        *current = Arc::new(table);
        Ok(())
    }
}

fn pack(limit: CoreLimit) -> u64 {
    (limit.per_second as u64) << 32 | limit.burst as u64
}

fn unpack(packed: u64) -> CoreLimit {
    CoreLimit {
        per_second: (packed >> 32) as u32,
        burst: packed as u32,
    }
}
