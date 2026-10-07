//! What the peers told us about their demand: one entry per peer and key.
//!
//! Reports carry absolute values, so applying one is idempotent and the
//! latest round of each peer wins: a lost report costs a round of accuracy,
//! a duplicated or reordered one costs nothing. Only direct reports are
//! kept, a peer speaking for itself; see spec §10.8 for why demand is not
//! relayed.

use std::collections::HashMap;

use rateguard_proto::DemandReport;

use crate::boundary::PeerId;
use crate::gcra::Nanos;

/// The latest a peer said about one key.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Heard {
    /// Attempts per second at the peer.
    pub demand: f32,
    /// The key is hot at the peer by its own demand.
    pub primary: bool,
    /// The peer's round the report was made in.
    pub round: u16,
    pub heard_at: Nanos,
}

/// Whether round `a` comes after round `b`. Rounds wrap, so they compare
/// as serial numbers (RFC 1982): `a` is newer if it is less than half the
/// circle ahead of `b`.
pub fn round_is_newer(a: u16, b: u16) -> bool {
    a != b && a.wrapping_sub(b) < 0x8000
}

#[derive(Debug, Clone)]
pub struct PeerDemand {
    entries: HashMap<(PeerId, u64), Heard>,
    reorder_window: Nanos,
}
impl PeerDemand {
    /// `reorder_window` is how long round numbers are trusted to order the
    /// reports of one peer. A report that arrives later than that after the
    /// last one is taken whatever its round: the peer may have restarted
    /// and counted from 0 again.
    pub fn new(reorder_window: Nanos) -> Self {
        Self {
            entries: HashMap::new(),
            reorder_window,
        }
    }

    /// Applies a report `from` a peer. A report the peer made about someone
    /// else is ignored.
    pub fn apply(&mut self, from: PeerId, report: &DemandReport, now: Nanos) {
        if report.origin != from.get() {
            return;
        }
        for key in &report.keys {
            let heard = Heard {
                demand: key.demand,
                primary: key.primary,
                round: report.round,
                heard_at: now,
            };
            match self.entries.get_mut(&(from, key.key_hash)) {
                Some(old)
                    if round_is_newer(report.round, old.round)
                        || now >= old.heard_at + self.reorder_window =>
                {
                    *old = heard;
                }
                Some(_) => {}
                None => {
                    self.entries.insert((from, key.key_hash), heard);
                }
            }
        }
    }

    pub fn get(&self, peer: PeerId, key: u64) -> Option<&Heard> {
        self.entries.get(&(peer, key))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rateguard_proto::KeyDemand;

    const PERIOD: Nanos = 200_000_000;
    const PEER: PeerId = PeerId::new(1);
    const KEY: u64 = 42;

    fn report(round: u16, demand: f32) -> DemandReport {
        DemandReport {
            origin: PEER.get(),
            round,
            keys: vec![KeyDemand {
                key_hash: KEY,
                demand,
                primary: true,
            }],
        }
    }

    fn demand(table: &PeerDemand) -> Option<f32> {
        table.get(PEER, KEY).map(|heard| heard.demand)
    }

    #[test]
    fn rounds_compare_across_the_wrap() {
        assert!(round_is_newer(1, 0));
        assert!(!round_is_newer(0, 1));
        assert!(!round_is_newer(7, 7), "a round is not newer than itself");
        assert!(round_is_newer(0, u16::MAX), "0 follows 65535");
        assert!(round_is_newer(100, u16::MAX - 100));
        assert!(!round_is_newer(u16::MAX, 0));
    }

    #[test]
    fn a_report_is_remembered_per_peer_and_key() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report(3, 120.0), 0);
        assert_eq!(
            t.get(PEER, KEY),
            Some(&Heard {
                demand: 120.0,
                primary: true,
                round: 3,
                heard_at: 0,
            })
        );
        assert_eq!(t.get(PeerId::new(2), KEY), None);
        assert_eq!(t.get(PEER, KEY + 1), None);
    }

    #[test]
    fn a_newer_round_replaces_an_older_one() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report(3, 120.0), 0);
        t.apply(PEER, &report(4, 80.0), 1);
        assert_eq!(demand(&t), Some(80.0));
    }

    #[test]
    fn a_late_report_does_not_undo_a_newer_one() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(3, 120.0), 1);
        assert_eq!(demand(&t), Some(80.0));
    }

    #[test]
    fn a_duplicate_changes_nothing() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(4, 80.0), 10);
        assert_eq!(t.get(PEER, KEY).unwrap().heard_at, 0);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn a_restarted_peer_is_believed_once_the_window_has_passed() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report(500, 80.0), 0);
        t.apply(PEER, &report(0, 30.0), PERIOD - 1);
        assert_eq!(
            demand(&t),
            Some(80.0),
            "within the window round 0 is late news"
        );
        t.apply(PEER, &report(1, 30.0), PERIOD);
        assert_eq!(demand(&t), Some(30.0), "after it, the peer started over");
    }

    #[test]
    fn a_report_about_someone_else_is_ignored() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PeerId::new(2), &report(3, 120.0), 0);
        assert!(t.is_empty());
    }
}
