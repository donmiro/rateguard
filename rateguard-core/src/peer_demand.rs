//! What the peers told us about their demand: the latest snapshot of each,
//! and whatever else they were heard to say within the stale window.
//!
//! A peer reports first hand, on every message it sends, the complete list
//! of its hot keys; see spec §10.8 for why demand is not relayed. Each
//! report is therefore a snapshot of absolute values: a newer one replaces
//! the older one whole, so a key that cooled at the peer is gone as soon as
//! the next report arrives, and an empty report means the peer has no hot
//! key left. Rounds order the reports of one epoch, a run of the peer from
//! start to restart.
//!
//! Neither rounds nor epochs tell a report from a copy recorded on the
//! wire and sent again, signed or not: a round recorded half a circle ago
//! looks newer, and an epoch this node never saw looks like a restart. So
//! no report is thrown away unread. One that is not taken, being no newer
//! than the snapshot or of a past epoch, is contested: for the stale window
//! it still counts, per key, at the maximum with the snapshot. So does a
//! snapshot replaced by a new epoch, or by a jump of rounds no live peer
//! makes. A replay can then make this node stricter, and it never locks
//! the peer's live reports out: they land among the contested ones and
//! count. It makes the node looser only if recorded within a quarter
//! circle of a whole one ago (2.7 to 3.6 h at 200 ms a round, or a
//! multiple), and only until the peer's next report. A lost report costs a
//! round of accuracy and an exact duplicate costs nothing; a reordered one
//! may make the node stricter for one stale window, never looser.
//!
//! Memory is bounded by the peers times the keys one report can carry, and
//! four times that for the contested ones: a peer that leaves the cluster
//! is forgotten, and so is all it said once [`stale_after_rounds`] passed.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use rateguard_proto::{DemandReport, MAX_DEMAND_KEYS};

use crate::boundary::PeerId;
use crate::gcra::Nanos;

/// What a peer said about one key: the most it said within the stale
/// window, from the report taken or one contested.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Heard {
    /// Attempts per second at the peer.
    pub demand: f32,
    /// The key is hot at the peer by its own demand, in any report that
    /// still counts.
    pub primary: bool,
    /// The peer's round of the report the demand comes from.
    pub round: u16,
    /// When this node took that report in; for a contested key, the latest
    /// of the reports merged into it, which its stale window counts from.
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

/// How many contested keys are kept per peer. Past that, the one heard
/// longest ago goes: a replay flood costs memory only up to here.
const MAX_CONTESTED: usize = 4 * MAX_DEMAND_KEYS;

/// A jump of rounds between two snapshots no live peer makes. Two snapshots
/// of one peer are at most a stale window apart, or the older one would
/// have expired, and the window is shorter than this below 5462 nodes. A
/// round this far ahead is more likely one recorded over half a circle ago.
/// In a larger cluster a genuine jump this long only makes the node stricter
/// for one window.
const IMPLAUSIBLE_JUMP: u16 = 0x4000;

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
}

/// What reports not taken said about one key, at the maximum.
#[derive(Debug, Clone, Copy)]
struct Contested {
    demand: f32,
    primary: bool,
    round: u16,
    heard_at: Nanos,
}

#[derive(Debug, Clone, Default)]
struct Peer {
    /// The latest report taken. It goes once stale, while contested
    /// entries heard later still count.
    snapshot: Option<Snapshot>,
    contested: HashMap<u64, Contested>,
    /// The peer left, but all it said still counts until then.
    held_until: Option<Nanos>,
}
impl Peer {
    // Merges what a report said into the contested entries, per key at the
    // maximum, stamped `heard_at`. A key the report leaves out is not news
    // here: a report not taken may only raise the peer.
    fn contest(&mut self, keys: &HashMap<u64, (f32, bool)>, round: u16, heard_at: Nanos) {
        for (&key, &(demand, primary)) in keys {
            match self.contested.entry(key) {
                Entry::Occupied(mut entry) => {
                    let entry = entry.get_mut();
                    if demand > entry.demand {
                        entry.demand = demand;
                        entry.round = round;
                    }
                    entry.primary |= primary;
                    entry.heard_at = entry.heard_at.max(heard_at);
                }
                Entry::Vacant(entry) => {
                    entry.insert(Contested {
                        demand,
                        primary,
                        round,
                        heard_at,
                    });
                }
            }
        }
        while self.contested.len() > MAX_CONTESTED {
            let oldest = self
                .contested
                .iter()
                .min_by_key(|&(&key, entry)| (entry.heard_at, key))
                .map(|(&key, _)| key)
                .expect("over the cap, so not empty");
            self.contested.remove(&oldest);
        }
    }

    // The snapshot's word on `key` and the contested one, whichever is
    // higher; the key counts as primary if either holds it so.
    fn heard(&self, key: u64) -> Option<Heard> {
        let taken = self.snapshot.as_ref().and_then(|snapshot| {
            let &(demand, primary) = snapshot.keys.get(&key)?;
            Some(Heard {
                demand,
                primary,
                round: snapshot.round,
                heard_at: snapshot.heard_at,
            })
        });
        let contested = self.contested.get(&key).map(|entry| Heard {
            demand: entry.demand,
            primary: entry.primary,
            round: entry.round,
            heard_at: entry.heard_at,
        });
        match (taken, contested) {
            (Some(taken), Some(contested)) => {
                let higher = if contested.demand > taken.demand {
                    contested
                } else {
                    taken
                };
                Some(Heard {
                    primary: taken.primary || contested.primary,
                    ..higher
                })
            }
            (taken, contested) => taken.or(contested),
        }
    }

    fn len(&self) -> usize {
        let taken = self.snapshot.as_ref().map(|snapshot| &snapshot.keys);
        let only_contested = self
            .contested
            .keys()
            .filter(|key| !taken.is_some_and(|keys| keys.contains_key(key)))
            .count();
        taken.map_or(0, HashMap::len) + only_contested
    }
}

#[derive(Debug, Clone, Default)]
/// The latest demand snapshot of each peer, and the reports of it not
/// taken but still counted, bounded in size.
pub struct PeerDemand {
    peers: HashMap<PeerId, Peer>,
}
impl PeerDemand {
    /// No peer heard from yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a report `from` a peer. A report the peer made about someone
    /// else is ignored. One newer than what the peer already said in the
    /// same epoch replaces it, and so does one of a new epoch whatever its
    /// round: the peer restarted. Any other report, no newer in the same
    /// epoch or of one of the last few epochs before it, is contested: it
    /// counts at the maximum with the snapshot until it is stale, but
    /// replaces nothing. An exact duplicate of the snapshot changes nothing.
    pub fn apply(&mut self, from: PeerId, report: &DemandReport, now: Nanos) {
        if report.origin != from.get() {
            return;
        }
        let keys: HashMap<u64, (f32, bool)> = report
            .keys
            .iter()
            .map(|key| (key.key_hash, (key.demand, key.primary)))
            .collect();
        let peer = self.peers.entry(from).or_default();
        let mut past_epochs = Vec::new();
        let mut keep_replaced = false;
        if let Some(old) = &peer.snapshot {
            let same_epoch = old.epoch == report.epoch;
            let taken = !old.past_epochs.contains(&report.epoch)
                && (!same_epoch || round_is_newer(report.round, old.round));
            if !taken {
                // A peer sends the same report on every message of a
                // round: that is no conflict.
                let duplicate = same_epoch && old.round == report.round && old.keys == keys;
                if !duplicate {
                    peer.contest(&keys, report.round, now);
                }
                return;
            }
            if same_epoch {
                past_epochs = old.past_epochs.clone();
                keep_replaced = report.round.wrapping_sub(old.round) >= IMPLAUSIBLE_JUMP;
            } else {
                past_epochs.push(old.epoch);
                past_epochs.extend(old.past_epochs.iter().take(PAST_EPOCHS - 1));
                // A restart, or a replay of a run we never saw: the old
                // run counts until its window ends, either way.
                keep_replaced = true;
            }
        }
        let taken = Snapshot {
            epoch: report.epoch,
            past_epochs,
            round: report.round,
            heard_at: now,
            keys,
        };
        if let Some(replaced) = peer.snapshot.replace(taken)
            && keep_replaced
        {
            peer.contest(&replaced.keys, replaced.round, replaced.heard_at);
        }
        peer.held_until = None;
    }

    /// Drops everything heard from `peer`, contested or not: it left the
    /// cluster.
    pub fn forget(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
    }

    /// The peer left the cluster. With a zero `hold` it is forgotten at
    /// once; otherwise all it said, contested or not, keeps counting for
    /// `hold`, as
    /// [`PartitionPolicy::HoldDown`](crate::partition::PartitionPolicy)
    /// wants, unless a report of it is taken first.
    pub fn leave(&mut self, peer: PeerId, now: Nanos, hold: Nanos) {
        if hold == 0 {
            self.forget(peer);
        } else if let Some(heard) = self.peers.get_mut(&peer) {
            heard.held_until = Some(now.saturating_add(hold));
        }
    }

    /// Drops the snapshots and contested entries last heard `max_age` ago
    /// or earlier, and everything of the held peers whose hold has ended.
    pub fn expire(&mut self, now: Nanos, max_age: Nanos) {
        let fresh = |heard_at: Nanos| now < heard_at.saturating_add(max_age);
        self.peers.retain(|_, peer| {
            if let Some(until) = peer.held_until {
                return now < until;
            }
            if peer
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| !fresh(snapshot.heard_at))
            {
                peer.snapshot = None;
            }
            peer.contested.retain(|_, entry| fresh(entry.heard_at));
            peer.snapshot.is_some() || !peer.contested.is_empty()
        });
    }

    /// The most `peer` said about `key` within the stale window, if
    /// anything.
    pub fn get(&self, peer: PeerId, key: u64) -> Option<Heard> {
        self.peers.get(&peer)?.heard(key)
    }

    /// The demand all peers reported for `key`, each at the most it said
    /// within the stale window, summed.
    pub fn total(&self, key: u64) -> f64 {
        self.peers
            .values()
            .filter_map(|peer| peer.heard(key))
            .map(|heard| heard.demand as f64)
            .sum()
    }

    /// Whether some peer holds `key` hot by its own demand, in any report
    /// of it that still counts. Only such news makes the key hot here (see
    /// [`hot_set`](crate::hot_set)).
    pub fn hot_elsewhere(&self, key: u64) -> bool {
        self.peers
            .values()
            .any(|peer| peer.heard(key).is_some_and(|heard| heard.primary))
    }

    /// How many keys are known over all peers.
    pub fn len(&self) -> usize {
        self.peers.values().map(Peer::len).sum()
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
    // The stale window of a small cluster, as the node passes it to expire.
    const WINDOW: Nanos = 10 * PERIOD;
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

    // The round of the report taken: what a late one must not replace.
    fn taken_round(table: &PeerDemand) -> Option<u16> {
        table.peers[&PEER]
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.round)
    }

    #[test]
    fn a_late_report_does_not_replace_a_newer_one() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report_of(4, &[(KEY, 80.0)]), 0);
        t.apply(PEER, &report_of(3, &[(KEY + 1, 120.0)]), 1);
        assert_eq!(taken_round(&t), Some(4));
        assert_eq!(demand(&t), Some(80.0), "the late report took nothing away");
        t.apply(PEER, &report_of(5, &[(KEY, 70.0)]), 2);
        assert_eq!(demand(&t), Some(70.0), "the next one is taken as before");
    }

    #[test]
    fn a_duplicate_changes_nothing() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(4, 80.0), 10);
        assert_eq!(t.get(PEER, KEY).unwrap().heard_at, 0);
        assert_eq!(t.len(), 1);
        assert!(t.peers[&PEER].contested.is_empty(), "no conflict");
    }

    #[test]
    fn a_late_report_does_not_replace_a_newer_one_however_late() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report_of(4, &[(KEY, 80.0)]), 0);
        t.apply(PEER, &report_of(3, &[(KEY + 1, 120.0)]), 10 * PERIOD);
        assert_eq!(taken_round(&t), Some(4));
        assert_eq!(demand(&t), Some(80.0));
    }

    #[test]
    fn a_restarted_peer_is_taken_at_once() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report_of(500, &[(KEY, 80.0)]), 0);
        let restarted = DemandReport {
            epoch: EPOCH + 1,
            ..report_of(0, &[(KEY + 1, 30.0)])
        };
        t.apply(PEER, &restarted, 1);
        assert_eq!(taken_round(&t), Some(0), "round 0 of the new run");
        assert_eq!(t.get(PEER, KEY + 1).map(|heard| heard.demand), Some(30.0));
        assert_eq!(demand(&t), Some(80.0), "the old run counts for its window");
        t.expire(WINDOW, WINDOW);
        assert_eq!(demand(&t), None);
        assert_eq!(t.get(PEER, KEY + 1).map(|heard| heard.demand), Some(30.0));
    }

    #[test]
    fn a_late_report_from_before_a_restart_replaces_nothing() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report_of(5, &[(KEY, 80.0)]), 0);
        let restarted = DemandReport {
            epoch: EPOCH + 1,
            ..report_of(1, &[(KEY + 1, 30.0)])
        };
        t.apply(PEER, &restarted, 1);
        t.apply(PEER, &report_of(6, &[(KEY, 120.0)]), 2);
        assert_eq!(taken_round(&t), Some(1), "the old epoch is not back");
        assert_eq!(t.get(PEER, KEY + 1).map(|heard| heard.demand), Some(30.0));
        assert_eq!(
            demand(&t),
            Some(120.0),
            "but its word counts for the window"
        );
        t.expire(2 + WINDOW, WINDOW);
        assert_eq!(demand(&t), None);

        let again = DemandReport {
            epoch: EPOCH + 2,
            ..report_of(1, &[(KEY, 50.0)])
        };
        t.apply(PEER, &again, 2 + WINDOW);
        assert_eq!(demand(&t), Some(50.0), "a third epoch is a new restart");
    }

    fn in_epoch(epoch: u32, round: u16, key: u64, demand: f32) -> DemandReport {
        DemandReport {
            epoch,
            ..report_of(round, &[(key, demand)])
        }
    }

    fn taken_epoch(table: &PeerDemand) -> Option<u32> {
        table.peers[&PEER]
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.epoch)
    }

    #[test]
    fn stragglers_of_every_recent_epoch_replace_nothing() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &in_epoch(1, 5, 1, 10.0), 0);
        t.apply(PEER, &in_epoch(2, 1, 2, 20.0), 1);
        t.apply(PEER, &in_epoch(3, 1, 3, 30.0), 2);
        t.apply(PEER, &in_epoch(1, 6, 1, 11.0), 3);
        t.apply(PEER, &in_epoch(2, 2, 2, 21.0), 4);
        assert_eq!((taken_epoch(&t), taken_round(&t)), (Some(3), Some(1)));
        assert_eq!(t.get(PEER, 3).map(|heard| heard.demand), Some(30.0));
        let contested = |key| t.get(PEER, key).map(|heard| heard.demand);
        assert_eq!((contested(1), contested(2)), (Some(11.0), Some(21.0)));
        t.apply(PEER, &in_epoch(3, 2, 3, 31.0), 5);
        assert_eq!(t.get(PEER, 3).map(|heard| heard.demand), Some(31.0));
    }

    #[test]
    fn a_straggler_after_a_same_epoch_update_still_replaces_nothing() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &in_epoch(1, 5, 1, 10.0), 0);
        t.apply(PEER, &in_epoch(2, 1, 2, 20.0), 1);
        t.apply(PEER, &in_epoch(2, 2, 2, 22.0), 2);
        t.apply(PEER, &in_epoch(1, 6, 1, 11.0), 3);
        assert_eq!((taken_epoch(&t), taken_round(&t)), (Some(2), Some(2)));
        assert_eq!(t.get(PEER, 2).map(|heard| heard.demand), Some(22.0));
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

    #[test]
    fn a_report_two_hours_old_does_not_lower_the_peer() {
        let mut t = PeerDemand::new();
        // Recorded at round 100, then sent again when the peer is half a
        // circle of rounds further, ~1.8 h later: it looks newer.
        let recorded = report(100, 900.0);
        t.apply(PEER, &recorded, 100 * PERIOD);
        for round in 101..=32_900u16 {
            let at = round as Nanos * PERIOD;
            t.apply(PEER, &report(round, 5.0), at);
            t.expire(at, WINDOW);
        }
        assert_eq!(t.total(KEY), 5.0);

        let replayed_at = 32_900 * PERIOD + 1;
        t.apply(PEER, &recorded, replayed_at);
        assert!(t.total(KEY) >= 5.0, "a replay may only raise the peer");
        for round in 32_901..=32_950u16 {
            let at = round as Nanos * PERIOD;
            t.apply(PEER, &report(round, 5.0), at);
            assert!(t.total(KEY) >= 5.0, "round {round}: {}", t.total(KEY));
            t.expire(at, WINDOW);
            assert!(t.total(KEY) >= 5.0, "round {round}: {}", t.total(KEY));
        }
        assert_eq!(t.total(KEY), 5.0, "the replay is past the stale window");

        t.apply(PEER, &report(32_951, 7.0), 32_951 * PERIOD);
        assert_eq!(t.total(KEY), 7.0, "the live reports still count");
    }

    #[test]
    fn a_report_of_an_unseen_epoch_does_not_lock_out_the_live_one() {
        let mut t = PeerDemand::new();
        let (old, live) = (11, 22);
        t.apply(PEER, &in_epoch(live, 100, KEY, 10.0), 0);
        let replayed_at = 1;
        t.apply(PEER, &in_epoch(old, 7, KEY, 5000.0), replayed_at);
        for round in 101..200u16 {
            let at = (round - 100) as Nanos * PERIOD;
            t.apply(PEER, &in_epoch(live, round, KEY, 10.0), at);
            t.expire(at, WINDOW);
            assert!(t.total(KEY) >= 10.0, "round {round}: {}", t.total(KEY));
            if at >= replayed_at + WINDOW {
                assert_eq!(t.total(KEY), 10.0, "round {round}: the replay is stale");
            }
        }
        assert_eq!(t.total(KEY), 10.0);
    }

    #[test]
    fn a_replayed_empty_report_never_lowers_a_peer() {
        let mut t = PeerDemand::new();
        let heard_at = 32_900 * PERIOD;
        t.apply(PEER, &report(32_900, 300.0), heard_at);
        // An empty report recorded half a circle of rounds ago.
        t.apply(PEER, &report_of(100, &[]), heard_at + 1);
        for step in 0..WINDOW / PERIOD {
            let at = heard_at + step * PERIOD;
            t.expire(at, WINDOW);
            assert_eq!(t.total(KEY), 300.0, "{step} rounds after the live report");
        }
        t.expire(heard_at + WINDOW, WINDOW);
        assert_eq!(t.total(KEY), 0.0, "the live report is stale too by now");
    }

    #[test]
    fn a_duplicate_is_not_a_conflict() {
        // What a 0.7.0 peer does all the time: the same report on every
        // message of a round.
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(4, 80.0), 10);
        assert_eq!(t.total(KEY), 80.0);
        t.apply(PEER, &report(5, 20.0), PERIOD);
        assert_eq!(t.total(KEY), 20.0, "the duplicate left nothing behind");
    }

    #[test]
    fn a_late_report_counts_at_the_maximum_for_the_stale_window() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(3, 120.0), PERIOD);
        assert_eq!(t.total(KEY), 120.0, "the stricter of the two");
        t.apply(PEER, &report(5, 70.0), 2 * PERIOD);
        assert_eq!(t.total(KEY), 120.0);
        t.expire(PERIOD + WINDOW - 1, WINDOW);
        assert_eq!(t.total(KEY), 120.0);
        t.expire(PERIOD + WINDOW, WINDOW);
        assert_eq!(t.total(KEY), 70.0);
    }

    #[test]
    fn a_restarted_peer_counts_its_new_demand_at_once_if_higher() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &in_epoch(1, 500, KEY, 100.0), 0);
        t.apply(PEER, &in_epoch(2, 0, KEY, 400.0), PERIOD);
        assert_eq!(t.total(KEY), 400.0, "the new run counts at once");
        t.apply(PEER, &in_epoch(2, 1, KEY, 50.0), 2 * PERIOD);
        assert_eq!(t.total(KEY), 100.0, "the old run until its window ends");
        t.expire(WINDOW - 1, WINDOW);
        assert_eq!(t.total(KEY), 100.0);
        t.expire(WINDOW, WINDOW);
        assert_eq!(t.total(KEY), 50.0);
    }

    #[test]
    fn a_leave_releases_contested_demand_too() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(3, 120.0), 1);
        t.leave(PEER, 2, 0);
        assert_eq!(t.total(KEY), 0.0);
        assert!(t.is_empty());

        t.apply(PEER, &report(4, 80.0), 3);
        t.apply(PEER, &report(3, 120.0), 4);
        t.forget(PEER);
        assert_eq!(t.total(KEY), 0.0);
        assert!(t.is_empty());
    }

    #[test]
    fn a_held_peer_holds_its_contested_demand_until_the_hold_ends() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report(4, 80.0), 0);
        t.apply(PEER, &report(3, 120.0), 1);
        t.leave(PEER, PERIOD, 50 * PERIOD);
        t.expire(51 * PERIOD - 1, WINDOW);
        assert_eq!(t.total(KEY), 120.0, "held with the snapshot");
        t.expire(51 * PERIOD, WINDOW);
        assert_eq!(t.total(KEY), 0.0);
        assert!(t.is_empty());
    }

    #[test]
    fn a_contested_key_is_hot_elsewhere_if_held_primary() {
        let mut t = PeerDemand::new();
        let mut secondary = report(4, 80.0);
        secondary.keys[0].primary = false;
        t.apply(PEER, &secondary, 0);
        t.apply(PEER, &report(3, 10.0), 1);
        assert!(t.hot_elsewhere(KEY), "the stricter reading counts");
        let heard = t.get(PEER, KEY).unwrap();
        assert_eq!((heard.demand, heard.primary, heard.round), (80.0, true, 4));
    }

    #[test]
    fn contested_entries_are_bounded_per_peer() {
        let mut t = PeerDemand::new();
        t.apply(PEER, &report_of(1000, &[]), 0);
        let cap = 4 * rateguard_proto::MAX_DEMAND_KEYS as u64;
        for key in 0..cap + 10 {
            t.apply(PEER, &report_of(999, &[(key, 1.0)]), 1 + key);
        }
        assert_eq!(t.len() as u64, cap);
        assert_eq!(t.total(0), 0.0, "the entry heard longest ago went first");
        assert_eq!(t.total(cap + 9), 1.0);
    }
}
