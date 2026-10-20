//! What the peers told us about their demand: the latest snapshot of each.
//!
//! A peer reports first hand, on every message it sends, the complete list
//! of its hot keys; see spec §10.8 for why demand is not relayed. Each
//! report is therefore a snapshot of absolute values: a newer one replaces
//! the older one whole, so a key that cooled at the peer is gone as soon as
//! the next report arrives, and an empty report means the peer has no hot
//! key left. Rounds order the reports of one epoch, a run of the peer from
//! start to restart. A lost report costs a round of accuracy, a duplicated
//! or reordered one costs nothing.
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
    /// When this node took the report in.
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

/// How many past epochs of a peer are remembered. A datagram outlives a
/// restart or two at most; four leaves room for a flapping peer without
/// letting the memory per peer grow.
const PAST_EPOCHS: usize = 4;

#[derive(Debug, Clone)]
struct Snapshot {
    epoch: u32,
    /// The epochs the peer ran before this one, newest first, as far as we
    /// saw it change. A late or replayed report of one of them must not
    /// replace the current snapshot.
    past_epochs: Vec<u32>,
    round: u16,
    heard_at: Nanos,
    keys: HashMap<u64, (f32, bool)>,
    /// The peer left, but its demand still counts until then.
    held_until: Option<Nanos>,
}

#[derive(Debug, Clone, Default)]
/// The latest demand snapshot of each peer, bounded in size.
pub struct PeerDemand {
    peers: HashMap<PeerId, Snapshot>,
}
impl PeerDemand {
    /// No peer heard from yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a report `from` a peer. A report the peer made about someone
    /// else is ignored, and so is one no newer than what the peer already
    /// said in the same epoch, however late it comes. A report of a new
    /// epoch is taken whatever its round: the peer restarted. One of
    /// the last few epochs before the current one is a straggler and is
    /// ignored.
    pub fn apply(&mut self, from: PeerId, report: &DemandReport, now: Nanos) {
        if report.origin != from.get() {
            return;
        }
        let mut past_epochs = Vec::new();
        if let Some(old) = self.peers.get(&from) {
            if old.past_epochs.contains(&report.epoch) {
                return;
            }
            if old.epoch == report.epoch {
                if !round_is_newer(report.round, old.round) {
                    return;
                }
                past_epochs = old.past_epochs.clone();
            } else {
                past_epochs.push(old.epoch);
                past_epochs.extend(old.past_epochs.iter().take(PAST_EPOCHS - 1));
            }
        }
        let keys = report
            .keys
            .iter()
            .map(|key| (key.key_hash, (key.demand, key.primary)))
            .collect();
        self.peers.insert(
            from,
            Snapshot {
                epoch: report.epoch,
                past_epochs,
                round: report.round,
                heard_at: now,
                keys,
                held_until: None,
            },
        );
    }

    /// Drops everything heard from `peer`: it left the cluster.
    pub fn forget(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
    }

    /// The peer left the cluster. With a zero `hold` it is forgotten at
    /// once; otherwise its demand keeps counting for `hold`, as
    /// [`PartitionPolicy::HoldDown`](crate::partition::PartitionPolicy)
    /// wants, unless it reports again first.
    pub fn leave(&mut self, peer: PeerId, now: Nanos, hold: Nanos) {
        if hold == 0 {
            self.forget(peer);
        } else if let Some(snapshot) = self.peers.get_mut(&peer) {
            snapshot.held_until = Some(now.saturating_add(hold));
        }
    }

    /// Drops the snapshots last heard `max_age` ago or earlier, and the
    /// held ones whose hold has ended.
    pub fn expire(&mut self, now: Nanos, max_age: Nanos) {
        self.peers.retain(|_, snapshot| match snapshot.held_until {
            Some(until) => now < until,
            None => now < snapshot.heard_at.saturating_add(max_age),
        });
    }

    /// What `peer` last said about `key`, if anything.
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

    /// Whether no peer reported any key.
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
    const EPOCH: u32 = 7;

    fn report_of(round: u16, keys: &[(u64, f32)]) -> DemandReport {
        DemandReport {
            origin: PEER.get(),
            epoch: EPOCH,
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
        let mut t = PeerDemand::new();
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
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(3, 120.0), 0);
        t.apply(PEER, &report(4, 80.0), 1);
        assert_eq!(demand(&t), Some(80.0));
    }

    #[test]
    fn a_key_missing_from_a_newer_report_has_cooled() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report_of(3, &[(KEY, 120.0), (KEY + 1, 90.0)]), 0);
        t.apply(PEER, &report_of(4, &[(KEY + 1, 95.0)]), 1);
        assert_eq!(demand(&t), None);
        assert_eq!(t.get(PEER, KEY + 1).map(|heard| heard.demand), Some(95.0));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn a_late_report_does_not_undo_a_newer_one() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(3, 120.0), 1);
        assert_eq!(demand(&t), Some(80.0));
    }

    #[test]
    fn a_duplicate_changes_nothing() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(4, 80.0), 10);
        assert_eq!(t.get(PEER, KEY).unwrap().heard_at, 0);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn a_late_report_does_not_undo_a_newer_one_however_late() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(3, 120.0), 10 * PERIOD);
        assert_eq!(demand(&t), Some(80.0));
    }

    #[test]
    fn a_restarted_peer_is_believed_at_once() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(500, 80.0), 0);
        let restarted = DemandReport {
            epoch: EPOCH + 1,
            ..report(0, 30.0)
        };
        t.apply(PEER, &restarted, 1);
        assert_eq!(demand(&t), Some(30.0));
    }

    #[test]
    fn a_late_report_from_before_a_restart_is_ignored() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report_of(5, &[(KEY, 80.0)]), 0);
        let restarted = DemandReport {
            epoch: EPOCH + 1,
            ..report_of(1, &[(KEY + 1, 30.0)])
        };
        t.apply(PEER, &restarted, 1);
        t.apply(PEER, &report_of(6, &[(KEY, 120.0)]), 2);
        assert_eq!(demand(&t), None, "the old epoch's keys must not return");
        assert_eq!(t.get(PEER, KEY + 1).map(|heard| heard.demand), Some(30.0));

        let again = DemandReport {
            epoch: EPOCH + 2,
            ..report_of(1, &[(KEY, 50.0)])
        };
        t.apply(PEER, &again, 3);
        assert_eq!(demand(&t), Some(50.0), "a third epoch is a new restart");
    }

    fn in_epoch(epoch: u32, round: u16, key: u64, demand: f32) -> DemandReport {
        DemandReport {
            epoch,
            ..report_of(round, &[(key, demand)])
        }
    }

    #[test]
    fn stragglers_of_every_recent_epoch_are_ignored() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &in_epoch(1, 5, 1, 10.0), 0);
        t.apply(PEER, &in_epoch(2, 1, 2, 20.0), 1);
        t.apply(PEER, &in_epoch(3, 1, 3, 30.0), 2);
        t.apply(PEER, &in_epoch(1, 6, 1, 11.0), 3);
        t.apply(PEER, &in_epoch(2, 2, 2, 21.0), 4);
        assert_eq!(t.get(PEER, 3).map(|heard| heard.demand), Some(30.0));
        assert!(t.get(PEER, 1).is_none() && t.get(PEER, 2).is_none());
        t.apply(PEER, &in_epoch(3, 2, 3, 31.0), 5);
        assert_eq!(t.get(PEER, 3).map(|heard| heard.demand), Some(31.0));
    }

    #[test]
    fn a_straggler_after_a_same_epoch_update_is_still_ignored() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &in_epoch(1, 5, 1, 10.0), 0);
        t.apply(PEER, &in_epoch(2, 1, 2, 20.0), 1);
        t.apply(PEER, &in_epoch(2, 2, 2, 22.0), 2);
        t.apply(PEER, &in_epoch(1, 6, 1, 11.0), 3);
        assert_eq!(t.get(PEER, 2).map(|heard| heard.demand), Some(22.0));
        assert!(t.get(PEER, 1).is_none());
    }

    #[test]
    fn a_report_about_someone_else_is_ignored() {
        let mut t = PeerDemand::new();
        t.apply(PeerId::new(2), &report(3, 120.0), 0);
        assert!(t.is_empty());
    }

    #[test]
    fn a_forgotten_peer_is_gone_and_others_stay() {
        let mut t = PeerDemand::new();
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
        let mut t = PeerDemand::new();
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
        let mut t = PeerDemand::new();
        let mut secondary = report(3, 120.0);
        secondary.keys[0].primary = false;
        t.apply(PEER, &secondary, 0);
        assert!(!t.hot_elsewhere(KEY), "secondary news is no news");

        t.apply(PEER, &report(4, 120.0), 1);
        assert!(t.hot_elsewhere(KEY));
        assert!(!t.hot_elsewhere(KEY + 1));
    }

    #[test]
    fn a_peer_that_leaves_with_no_hold_is_forgotten() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(3, 120.0), 0);
        t.leave(PEER, 1, 0);
        assert!(t.is_empty());
    }

    #[test]
    fn a_held_peer_outlives_staleness_until_the_hold_ends() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(3, 120.0), 0);
        t.leave(PEER, PERIOD, 50 * PERIOD);

        t.expire(51 * PERIOD - 1, 10 * PERIOD);
        assert_eq!(demand(&t), Some(120.0), "still counted while held");
        t.expire(51 * PERIOD, 10 * PERIOD);
        assert!(t.is_empty());
    }

    #[test]
    fn a_held_peer_that_reports_again_is_back() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(3, 120.0), 0);
        t.leave(PEER, PERIOD, 50 * PERIOD);
        t.apply(PEER, &report(4, 90.0), 2 * PERIOD);

        t.expire(12 * PERIOD, 10 * PERIOD);
        assert!(t.is_empty(), "back to the ordinary staleness rule");
    }

    #[test]
    fn a_snapshot_expires_once_it_is_max_age_old() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(3, 120.0), PERIOD);
        t.expire(PERIOD + 10 * PERIOD - 1, 10 * PERIOD);
        assert_eq!(demand(&t), Some(120.0));
        t.expire(PERIOD + 10 * PERIOD, 10 * PERIOD);
        assert!(t.is_empty());
    }
}
