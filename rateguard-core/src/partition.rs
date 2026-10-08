//! What a node does when its cluster shrinks: the CAP trade-off as a
//! setting (spec §5.1).
//!
//! When the network splits into k groups, each group sees only itself and,
//! left alone, hands out the whole limit: up to k·R in all. No node can
//! tell a split from a death, so the choice is made in advance.

use std::collections::VecDeque;

use crate::gcra::Nanos;

const ONE_SEC: Nanos = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// What a node does when its cluster shrinks; see the variants.
pub enum PartitionPolicy {
    /// Shares follow the surviving cluster at once. Up to k·R during a
    /// split; nothing is ever left unused.
    Optimistic,
    /// For this long after the cluster shrinks, shares are computed as if
    /// it had not: the cluster size is held at its recent maximum, and the
    /// demand of the peers that went away is still counted. About R for
    /// that long, then k·R; after a real failure the limit is underused for
    /// that long. Most apparent splits are a GC pause or a restart lasting
    /// seconds, which is why this is the default.
    HoldDown(Nanos),
    /// A node that cannot see a majority of the cluster it knows drops every
    /// key, hot or cold, to the floor `R·β/N`. R plus the minority's floors
    /// during a split; the minority all but stops serving.
    Quorum,
}
impl Default for PartitionPolicy {
    fn default() -> Self {
        PartitionPolicy::HoldDown(10 * ONE_SEC)
    }
}

/// Whether `alive` nodes, this one included, are a majority of the `known`
/// ones, the dead included.
pub fn has_quorum(alive: usize, known: usize) -> bool {
    2 * alive > known
}

/// The cluster size over a sliding window: the largest one seen in the last
/// `window`. A drop shows only once it has lasted the whole window; a rise
/// shows at once.
#[derive(Debug, Clone)]
pub struct SizeHistory {
    window: Nanos,
    samples: VecDeque<(Nanos, usize)>,
}
impl SizeHistory {
    /// An empty history over `window`.
    pub fn new(window: Nanos) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
        }
    }

    /// Records the size seen at `now` and returns the largest one of the
    /// window that ends there.
    pub fn record(&mut self, now: Nanos, size: usize) -> usize {
        // A monotonic queue: sizes strictly falling from front to back, so
        // the front is the largest. A sample no larger than a newer one can
        // never be the maximum again.
        while self.samples.back().is_some_and(|&(_, s)| s <= size) {
            self.samples.pop_back();
        }
        self.samples.push_back((now, size));
        while self.samples.len() > 1
            && self
                .samples
                .front()
                .is_some_and(|&(at, _)| at.saturating_add(self.window) <= now)
        {
            self.samples.pop_front();
        }
        self.samples.front().expect("just pushed").1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_majority_is_strictly_more_than_half() {
        assert!(has_quorum(3, 5));
        assert!(!has_quorum(2, 5));
        assert!(has_quorum(2, 3));
        assert!(!has_quorum(2, 4), "half is not a majority");
        assert!(has_quorum(1, 1), "alone in a cluster of one");
    }

    #[test]
    fn a_drop_shows_only_after_the_window() {
        let mut h = SizeHistory::new(10 * ONE_SEC);
        assert_eq!(h.record(0, 5), 5);
        assert_eq!(h.record(ONE_SEC, 3), 5);
        assert_eq!(h.record(10 * ONE_SEC - 1, 3), 5);
        assert_eq!(h.record(10 * ONE_SEC, 3), 3);
    }

    #[test]
    fn a_rise_shows_at_once() {
        let mut h = SizeHistory::new(10 * ONE_SEC);
        h.record(0, 3);
        assert_eq!(h.record(ONE_SEC, 5), 5);
    }

    #[test]
    fn with_no_window_the_size_is_the_current_one() {
        let mut h = SizeHistory::new(0);
        h.record(0, 5);
        assert_eq!(h.record(1, 3), 3);
    }

    #[test]
    fn the_history_keeps_no_more_than_the_window() {
        let mut h = SizeHistory::new(ONE_SEC);
        for k in 0..1000 {
            h.record(k * ONE_SEC / 10, 5);
        }
        assert!(h.samples.len() <= 11, "{}", h.samples.len());
    }
}
