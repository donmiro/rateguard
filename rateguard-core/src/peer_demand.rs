//! What the peers told us about their demand: the latest snapshot of each.
//!
//! A peer reports first hand, on every message it sends, the complete list
//! of its hot keys; see spec §10.8 for why demand is not relayed. Each
//! report is therefore a snapshot of absolute values: a newer one replaces
//! the older one whole, so a key that cooled at the peer is gone as soon as
//! the next report arrives, and a message with no report at all means the
//! peer has no hot key left. A lost report costs a round of accuracy, a
//! duplicated or reordered one costs nothing.
//!
//! Memory is bounded by the peers times the keys one report can carry: a
//! peer that leaves the cluster is forgotten, and so is one not heard from
//! for [`stale_after_rounds`].

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

/// How many protocol periods a peer's snapshot outlives the last message
/// from it: max(10, 3·N).
///
/// Two nodes talk directly only when one probes the other, so the gaps grow
/// with the cluster. Measured in the simulator over 5 seeds and 300 s:
/// without loss the longest gap is 1.84·N rounds at N = 50; at 30% loss it
/// is 3.18·N, and a gap passed 3·N once in 1.3 million. The floor covers
/// small clusters, where loss weighs more than N.
pub fn stale_after_rounds(cluster_size: usize) -> u64 {
    (3 * cluster_size as u64).max(10)
}

#[derive(Debug, Clone)]
struct Snapshot {
    round: u16,
    heard_at: Nanos,
    keys: HashMap<u64, (f32, bool)>,
}

#[derive(Debug, Clone)]
pub struct PeerDemand {
    peers: HashMap<PeerId, Snapshot>,
    reorder_window: Nanos,
}
impl PeerDemand {
    /// `reorder_window` is how long round numbers are trusted to order the
    /// reports of one peer. A report that arrives later than that after the
    /// last one is taken whatever its round: the peer may have restarted
    /// and counted from 0 again.
    pub fn new(reorder_window: Nanos) -> Self {
        Self {
            peers: HashMap::new(),
            reorder_window,
        }
    }

    /// Applies a report `from` a peer. A report the peer made about someone
    /// else is ignored.
    pub fn apply(&mut self, from: PeerId, report: &DemandReport, now: Nanos) {
        if report.origin != from.get() {
            return;
        }
        if let Some(old) = self.peers.get(&from)
            && !round_is_newer(report.round, old.round)
            && now < old.heard_at + self.reorder_window
        {
            return;
        }
        let keys = report
            .keys
            .iter()
            .map(|key| (key.key_hash, (key.demand, key.primary)))
            .collect();
        self.peers.insert(
            from,
            Snapshot {
                round: report.round,
                heard_at: now,
                keys,
            },
        );
    }

    /// Drops everything heard from `peer`: it left the cluster, or it sent
    /// a message with no report, which means it has no hot key.
    pub fn forget(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
    }

    /// Drops the snapshots last heard `max_age` ago or earlier.
    pub fn expire(&mut self, now: Nanos, max_age: Nanos) {
        self.peers
            .retain(|_, snapshot| now < snapshot.heard_at + max_age);
    }

    pub fn get(&self, peer: PeerId, key: u64) -> Option<Heard> {
        let snapshot = self.peers.get(&peer)?;
        let &(demand, primary) = snapshot.keys.get(&key)?;
        Some(Heard {
            demand,
            primary,
            round: snapshot.round,
            heard_at: snapshot.heard_at,
        })
    }

    /// The demand all peers reported for `key`, summed.
    pub fn total(&self, key: u64) -> f64 {
        self.peers
            .values()
            .filter_map(|snapshot| snapshot.keys.get(&key))
            .map(|&(demand, _)| demand as f64)
            .sum()
    }

    /// Whether some peer holds `key` hot by its own demand. Only such news
    /// makes the key hot here (see [`hot_set`](crate::hot_set)).
    pub fn hot_elsewhere(&self, key: u64) -> bool {
        self.peers
            .values()
            .any(|snapshot| snapshot.keys.get(&key).is_some_and(|&(_, primary)| primary))
    }

    /// How many keys are known over all peers.
    pub fn len(&self) -> usize {
        self.peers
            .values()
            .map(|snapshot| snapshot.keys.len())
            .sum()
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

    fn report_of(round: u16, keys: &[(u64, f32)]) -> DemandReport {
        DemandReport {
            origin: PEER.get(),
            round,
            keys: keys
                .iter()
                .map(|&(key_hash, demand)| KeyDemand {
                    key_hash,
                    demand,
                    primary: true,
                })
                .collect(),
        }
    }

    fn report(round: u16, demand: f32) -> DemandReport {
        report_of(round, &[(KEY, demand)])
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
    fn the_staleness_threshold_grows_with_the_cluster() {
        assert_eq!(stale_after_rounds(1), 10);
        assert_eq!(stale_after_rounds(3), 10);
        assert_eq!(stale_after_rounds(4), 12);
        assert_eq!(stale_after_rounds(50), 150);
    }

    #[test]
    fn a_report_is_remembered_per_peer_and_key() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report(3, 120.0), 0);
        assert_eq!(
            t.get(PEER, KEY),
            Some(Heard {
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
    fn a_key_missing_from_a_newer_report_has_cooled() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report_of(3, &[(KEY, 120.0), (KEY + 1, 90.0)]), 0);
        t.apply(PEER, &report_of(4, &[(KEY + 1, 95.0)]), 1);
        assert_eq!(demand(&t), None);
        assert_eq!(t.get(PEER, KEY + 1).map(|heard| heard.demand), Some(95.0));
        assert_eq!(t.len(), 1);
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

    #[test]
    fn a_forgotten_peer_is_gone_and_others_stay() {
        let mut t = PeerDemand::new(PERIOD);
        let other = PeerId::new(2);
        t.apply(PEER, &report(3, 120.0), 0);
        t.apply(
            other,
            &DemandReport {
                origin: other.get(),
                ..report(3, 60.0)
            },
            0,
        );
        t.forget(PEER);
        assert_eq!(demand(&t), None);
        assert_eq!(t.get(other, KEY).map(|heard| heard.demand), Some(60.0));
    }

    #[test]
    fn the_total_adds_up_what_every_peer_reported() {
        let mut t = PeerDemand::new(PERIOD);
        let other = PeerId::new(2);
        t.apply(PEER, &report_of(3, &[(KEY, 120.0), (KEY + 1, 5.0)]), 0);
        t.apply(
            other,
            &DemandReport {
                origin: other.get(),
                ..report(9, 60.0)
            },
            0,
        );
        assert_eq!(t.total(KEY), 180.0);
        assert_eq!(t.total(KEY + 1), 5.0);
        assert_eq!(t.total(KEY + 2), 0.0);
    }

    #[test]
    fn a_key_is_hot_elsewhere_only_if_a_peer_holds_it_primary() {
        let mut t = PeerDemand::new(PERIOD);
        let mut secondary = report(3, 120.0);
        secondary.keys[0].primary = false;
        t.apply(PEER, &secondary, 0);
        assert!(!t.hot_elsewhere(KEY), "secondary news is no news");

        t.apply(PEER, &report(4, 120.0), 1);
        assert!(t.hot_elsewhere(KEY));
        assert!(!t.hot_elsewhere(KEY + 1));
    }

    #[test]
    fn a_snapshot_expires_once_it_is_max_age_old() {
        let mut t = PeerDemand::new(PERIOD);
        t.apply(PEER, &report(3, 120.0), PERIOD);
        t.expire(PERIOD + 10 * PERIOD - 1, 10 * PERIOD);
        assert_eq!(demand(&t), Some(120.0));
        t.expire(PERIOD + 10 * PERIOD, 10 * PERIOD);
        assert!(t.is_empty());
    }
}
