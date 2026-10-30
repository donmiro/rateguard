//! SWIM's member table: who is in the cluster, in which state and
//! incarnation. A pure state machine, with no network at all.
//!
//! News about a member is an [`Update`], and newer news wins by
//! [`Update::supersedes`]: a higher incarnation beats anything, and within
//! one incarnation `Dead` beats `Suspect` beats `Alive`. Only the member
//! itself raises its incarnation, to refute news about itself.
//!
//! Two departures from the SWIM paper, both on purpose:
//!
//! - `Dead` is not final. A higher incarnation brings a member back, so a
//!   node restarted under the same ID, or one that was only paused, can
//!   rejoin.
//! - A stale accusation is answered too. A node that hears it is suspected
//!   or dead in an older incarnation repeats its current `Alive`: the
//!   accuser missed the refutation, and nothing else would tell it.
//!
//! A suspect still counts toward the cluster size, so a GC pause does not
//! reshuffle everyone's shares. Only the dead leave
//! [`peers`](Membership::peers).

use rateguard_proto::{Address, Status, Update};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};

use crate::boundary::PeerId;
use crate::gcra::Nanos;
use crate::membership::{Change, Membership};

/// What applying a piece of news did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Applied {
    /// Old or repeated news: nothing changed.
    Ignored,
    /// The news was new and is now in the table; pass it on. `from` is the
    /// member's previous status, `None` for a stranger.
    Accepted {
        /// The member's previous status; `None` for a stranger.
        from: Option<Status>,
    },
    /// The news was about this node and wrong. Spread the update instead:
    /// it is this node's current `Alive`, at a higher incarnation if that
    /// was needed to win.
    Refuted(Update),
    /// News of a stranger, refused because the table is full: nothing
    /// changed, and the news is not passed on. See
    /// [`MemberTable::set_max`].
    Refused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Member {
    addr: Address,
    incarnation: u32,
    status: Status,
    since: Nanos,
    /// When it last became live.
    joined: Nanos,
    /// Dead because it said it was leaving, not because it stopped
    /// answering.
    departed: bool,
}
impl Member {
    fn update(&self, peer: PeerId) -> Update {
        Update {
            member: peer.get(),
            addr: self.addr,
            incarnation: self.incarnation,
            status: self.status,
        }
    }
}

/// One node's view of the cluster.
#[derive(Debug, Clone)]
pub struct MemberTable {
    local: PeerId,
    addr: Address,
    incarnation: u32,
    members: BTreeMap<PeerId, Member>,
    live: Vec<PeerId>,
    changes: Vec<Change>,
    max: usize,
    refused: u64,
}
impl MemberTable {
    /// A table holding only the node itself, `Alive` at incarnation 0, listening at `addr`.
    pub fn new(local: PeerId, addr: Address) -> Self {
        Self {
            local,
            addr,
            incarnation: 0,
            members: BTreeMap::new(),
            live: Vec::new(),
            changes: Vec::new(),
            max: usize::MAX,
            refused: 0,
        }
    }

    /// Holds the table to `max` members, the dead not yet forgotten and
    /// this node included: news of a stranger is refused while it is full.
    /// Unbounded unless set. The members already known are never evicted,
    /// a lower cap included, and news of them is taken as ever; only
    /// newcomers wait, until a forgotten member frees a slot.
    ///
    /// # Panics
    ///
    /// If `max` is 0: the table always holds this node.
    pub fn set_max(&mut self, max: usize) {
        assert!(max >= 1, "max_members must be at least 1: the node itself");
        self.max = max;
    }

    /// The cap set with [`set_max`](MemberTable::set_max).
    pub fn max(&self) -> usize {
        self.max
    }

    /// How many times news of a stranger was refused for a full table.
    pub fn refused(&self) -> u64 {
        self.refused
    }

    /// The members known, alive, suspect or dead and not yet forgotten,
    /// this node included.
    pub fn known(&self) -> usize {
        self.members.len() + 1
    }

    /// This node's own incarnation. Only it can raise it, and only to
    /// refute news about itself.
    pub fn incarnation(&self) -> u32 {
        self.incarnation
    }

    /// The member's status, or `None` for a stranger or a forgotten member.
    pub fn status(&self, peer: PeerId) -> Option<Status> {
        if peer == self.local {
            return Some(Status::Alive);
        }
        self.members.get(&peer).map(|member| member.status)
    }

    /// What this node would say about `peer`: the record to gossip, or to
    /// compare news against. `None` for a stranger.
    /// Where `peer` listens, this node included.
    pub fn address(&self, peer: PeerId) -> Option<Address> {
        if peer == self.local {
            return Some(self.addr);
        }
        self.members.get(&peer).map(|member| member.addr)
    }

    /// The latest news about the member, as it would be gossiped.
    pub fn update_about(&self, peer: PeerId) -> Option<Update> {
        if peer == self.local {
            return Some(self.own_update());
        }
        self.members.get(&peer).map(|member| member.update(peer))
    }

    /// Applies news heard at `now`. News about this node itself is never
    /// taken as fact: it is refuted or ignored.
    pub fn apply(&mut self, update: Update, now: Nanos) -> Applied {
        let peer = PeerId::new(update.member);
        if peer == self.local {
            return self.refute(update);
        }

        let from = self.members.get(&peer).copied();
        if let Some(current) = from
            && !update.supersedes(&current.update(peer))
        {
            return Applied::Ignored;
        }
        // A stranger is refused, not anyone evicted for it: with gossip
        // unsigned, anyone may name members by the thousand, and a table
        // that took them all would grow without bound.
        if from.is_none() && self.known() >= self.max {
            self.refused += 1;
            return Applied::Refused;
        }

        let was_live = from.is_some_and(|member| member.status != Status::Dead);
        let is_live = update.status != Status::Dead;
        self.members.insert(
            peer,
            Member {
                addr: update.addr,
                incarnation: update.incarnation,
                status: update.status,
                since: now,
                joined: match from {
                    Some(member) if was_live || !is_live => member.joined,
                    _ => now,
                },
                departed: false,
            },
        );

        match (was_live, is_live) {
            (false, true) => {
                let at = self
                    .live
                    .binary_search(&peer)
                    .expect_err("a peer outside the cluster is not among the live ones");
                self.live.insert(at, peer);
                self.changes.push(Change::Joined(peer));
            }
            (true, false) => {
                let at = self
                    .live
                    .binary_search(&peer)
                    .expect("a live peer is among the live ones");
                self.live.remove(at);
                self.changes.push(Change::Left(peer));
            }
            _ => {}
        }

        Applied::Accepted {
            from: from.map(|member| member.status),
        }
    }

    /// Marks an alive member as suspect after a failed probe and returns the
    /// news to gossip. `None` if the member was not alive: a suspicion is
    /// never renewed, its timer runs from the first one.
    pub fn suspect(&mut self, peer: PeerId, now: Nanos) -> Option<Update> {
        assert_ne!(peer, self.local, "a node never suspects itself");

        let member = self.members.get(&peer)?;
        if member.status != Status::Alive {
            return None;
        }
        let update = Update {
            status: Status::Suspect,
            ..member.update(peer)
        };
        self.apply(update, now);
        Some(update)
    }

    /// Marks a dead member as departed: it said it was leaving, so it is no
    /// longer serving, unlike one that only stopped answering. Whether it
    /// was dead and not departed already; news that brings it back clears
    /// the mark.
    pub fn depart(&mut self, peer: PeerId) -> bool {
        match self.members.get_mut(&peer) {
            Some(member) if member.status == Status::Dead && !member.departed => {
                member.departed = true;
                true
            }
            _ => false,
        }
    }

    /// Whether the member is dead because it said it was leaving.
    pub fn has_departed(&self, peer: PeerId) -> bool {
        self.members
            .get(&peer)
            .is_some_and(|member| member.departed)
    }

    /// When the member last became live; `None` for a stranger, a
    /// forgotten member, or this node.
    pub fn joined_at(&self, peer: PeerId) -> Option<Nanos> {
        self.members.get(&peer).map(|member| member.joined)
    }

    /// The record of the first member after `after` in the order of IDs,
    /// from the first with `None`, that is not dead: a walk over the live
    /// and the suspect that needs no more than the place it stopped at.
    pub fn next_live_after(&self, after: Option<PeerId>) -> Option<Update> {
        let rest = match after {
            Some(after) => self.members.range((Excluded(after), Unbounded)),
            None => self.members.range(..),
        };
        rest.filter(|(_, member)| member.status != Status::Dead)
            .map(|(&peer, member)| member.update(peer))
            .next()
    }

    /// Members known to be dead and not yet forgotten.
    pub fn dead(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.members
            .iter()
            .filter(|(_, member)| member.status == Status::Dead)
            .map(|(&peer, _)| peer)
    }

    /// Drops the members dead for at least `ttl` and returns how many. A
    /// forgotten member is a stranger again: late news about it is accepted
    /// as new. That is the price of a table bounded in size.
    pub fn forget_dead(&mut self, now: Nanos, ttl: Nanos) -> usize {
        let before = self.members.len();
        self.members.retain(|_, member| {
            member.status != Status::Dead || now.saturating_sub(member.since) < ttl
        });
        before - self.members.len()
    }

    /// Declares dead every suspect suspected for at least `timeout` and
    /// returns the news to gossip.
    pub fn expire_suspects(&mut self, now: Nanos, timeout: Nanos) -> Vec<Update> {
        assert!(
            timeout > 0,
            "a zero suspicion timeout is no suspicion at all"
        );

        let expired: Vec<Update> = self
            .members
            .iter()
            .filter(|(_, member)| {
                member.status == Status::Suspect && now.saturating_sub(member.since) >= timeout
            })
            .map(|(&peer, member)| Update {
                status: Status::Dead,
                ..member.update(peer)
            })
            .collect();

        for &update in &expired {
            self.apply(update, now);
        }
        expired
    }

    fn own_update(&self) -> Update {
        Update {
            member: self.local.get(),
            addr: self.addr,
            incarnation: self.incarnation,
            status: Status::Alive,
        }
    }

    // A stale accusation is answered too, with the current incarnation: the
    // accuser missed our refutation, and once the news has left every gossip
    // buffer nothing else would ever tell it.
    fn refute(&mut self, news: Update) -> Applied {
        if !news.supersedes(&self.own_update()) {
            return match news.status {
                Status::Alive => Applied::Ignored,
                Status::Suspect | Status::Dead => Applied::Refuted(self.own_update()),
            };
        }
        // Only a corrupt datagram reaches u32::MAX; a panic on it would let one packet kill the node.
        let Some(incarnation) = news.incarnation.checked_add(1) else {
            return Applied::Ignored;
        };
        self.incarnation = incarnation;
        Applied::Refuted(self.own_update())
    }
}
impl Membership for MemberTable {
    fn local(&self) -> PeerId {
        self.local
    }

    fn peers(&self) -> &[PeerId] {
        &self.live
    }

    fn drain_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::membership::check_contract;
    use Status::{Alive, Dead, Suspect};

    const ONE_SEC: Nanos = 1_000_000_000;
    const TIMEOUT: Nanos = 5 * ONE_SEC;

    fn id(raw: u64) -> PeerId {
        PeerId::new(raw)
    }

    fn addr(member: u64) -> Address {
        Address::V4([10, 0, 0, member as u8], 7946)
    }

    fn news(member: u64, incarnation: u32, status: Status) -> Update {
        Update {
            member,
            addr: addr(member),
            incarnation,
            status,
        }
    }

    fn table() -> MemberTable {
        MemberTable::new(id(0), addr(0))
    }

    #[test]
    fn a_member_is_known_with_its_address() {
        let mut t = table();
        t.apply(news(1, 0, Alive), 0);
        assert_eq!(t.address(id(1)), Some(addr(1)));
        assert_eq!(t.address(id(0)), Some(addr(0)), "itself too");
        assert_eq!(t.address(id(9)), None);
        assert_eq!(t.update_about(id(0)).unwrap().addr, addr(0));
    }

    #[test]
    fn only_the_dead_depart_and_coming_back_undoes_it() {
        let mut t = table();
        t.apply(news(1, 0, Alive), 0);
        assert!(!t.depart(id(1)), "alive");
        assert!(!t.depart(id(9)), "a stranger");
        t.apply(news(1, 0, Dead), ONE_SEC);
        assert!(t.depart(id(1)));
        assert!(!t.depart(id(1)), "once");
        assert!(t.has_departed(id(1)));
        t.apply(news(1, 1, Alive), 2 * ONE_SEC);
        assert!(!t.has_departed(id(1)));
    }

    #[test]
    fn a_member_joins_when_it_becomes_live() {
        let mut t = table();
        t.apply(news(1, 0, Alive), ONE_SEC);
        t.apply(news(1, 0, Suspect), 2 * ONE_SEC);
        t.apply(news(1, 1, Alive), 3 * ONE_SEC);
        assert_eq!(t.joined_at(id(1)), Some(ONE_SEC), "a refutation is no join");
        t.apply(news(1, 1, Dead), 4 * ONE_SEC);
        assert_eq!(t.joined_at(id(1)), Some(ONE_SEC), "nor is a death");
        t.apply(news(1, 2, Alive), 5 * ONE_SEC);
        assert_eq!(t.joined_at(id(1)), Some(5 * ONE_SEC));
    }

    #[test]
    fn a_fresh_table_knows_only_itself() {
        let mut t = table();
        assert_eq!(t.cluster_size(), 1);
        assert_eq!(t.status(id(0)), Some(Alive));
        assert_eq!(t.status(id(1)), None);
        assert_eq!(t.update_about(id(0)), Some(news(0, 0, Alive)));
        assert!(t.drain_changes().is_empty());
        check_contract(&t).unwrap();
    }

    #[test]
    fn hearing_of_a_peer_brings_it_into_the_cluster() {
        let mut t = table();
        assert_eq!(
            t.apply(news(1, 0, Alive), 0),
            Applied::Accepted { from: None }
        );
        assert_eq!(t.peers(), [id(1)]);
        assert_eq!(t.drain_changes(), [Change::Joined(id(1))]);
    }

    #[test]
    fn old_and_repeated_news_is_ignored() {
        let mut t = table();
        t.apply(news(1, 3, Alive), 0);

        assert_eq!(t.apply(news(1, 3, Alive), 0), Applied::Ignored, "an echo");
        assert_eq!(t.apply(news(1, 2, Suspect), 0), Applied::Ignored, "stale");
        assert_eq!(t.update_about(id(1)), Some(news(1, 3, Alive)));
    }

    #[test]
    fn a_suspect_still_counts_toward_the_cluster() {
        let mut t = table();
        t.apply(news(1, 0, Alive), 0);
        t.drain_changes();

        assert_eq!(t.suspect(id(1), ONE_SEC), Some(news(1, 0, Suspect)));
        assert_eq!(t.status(id(1)), Some(Suspect));
        assert_eq!(t.cluster_size(), 2);
        assert!(
            t.drain_changes().is_empty(),
            "a pause must not reshuffle everyone's shares"
        );
    }

    #[test]
    fn only_a_higher_incarnation_clears_a_suspicion() {
        let mut t = table();
        t.apply(news(1, 4, Suspect), 0);

        assert_eq!(t.apply(news(1, 4, Alive), 0), Applied::Ignored);
        assert_eq!(t.status(id(1)), Some(Suspect));

        assert_eq!(
            t.apply(news(1, 5, Alive), 0),
            Applied::Accepted {
                from: Some(Suspect)
            }
        );
        assert_eq!(t.status(id(1)), Some(Alive));
    }

    #[test]
    fn only_an_alive_member_can_be_suspected() {
        let mut t = table();
        assert_eq!(t.suspect(id(1), 0), None, "a stranger");

        t.apply(news(1, 0, Suspect), 0);
        assert_eq!(t.suspect(id(1), ONE_SEC), None, "already suspected");

        t.apply(news(2, 0, Dead), 0);
        assert_eq!(t.suspect(id(2), 0), None, "already dead");
    }

    #[test]
    #[should_panic(expected = "never suspects itself")]
    fn a_node_cannot_suspect_itself() {
        table().suspect(id(0), 0);
    }

    #[test]
    fn an_unrefuted_suspicion_becomes_death_after_the_timeout() {
        let mut t = table();
        t.apply(news(1, 2, Alive), 0);
        t.apply(news(2, 0, Alive), 0);
        t.suspect(id(1), ONE_SEC);
        t.drain_changes();

        assert!(t.expire_suspects(ONE_SEC + TIMEOUT - 1, TIMEOUT).is_empty());
        assert_eq!(
            t.expire_suspects(ONE_SEC + TIMEOUT, TIMEOUT),
            [news(1, 2, Dead)],
            "death keeps the incarnation it was suspected in"
        );
        assert_eq!(t.peers(), [id(2)]);
        assert_eq!(t.drain_changes(), [Change::Left(id(1))]);
        assert!(
            t.expire_suspects(100 * TIMEOUT, TIMEOUT).is_empty(),
            "an alive member never expires"
        );
    }

    #[test]
    fn a_fresher_suspicion_restarts_the_timer() {
        let mut t = table();
        t.apply(news(1, 0, Alive), 0);
        t.suspect(id(1), 0);
        t.apply(news(1, 1, Suspect), 3 * ONE_SEC);

        assert!(t.expire_suspects(TIMEOUT, TIMEOUT).is_empty());
        assert_eq!(
            t.expire_suspects(3 * ONE_SEC + TIMEOUT, TIMEOUT),
            [news(1, 1, Dead)]
        );
    }

    #[test]
    fn the_dead_stay_dead_until_a_higher_incarnation() {
        let mut t = table();
        t.apply(news(1, 2, Alive), 0);
        t.apply(news(1, 2, Dead), 0);
        t.drain_changes();

        assert_eq!(
            t.apply(news(1, 2, Alive), ONE_SEC),
            Applied::Ignored,
            "a late echo"
        );
        assert_eq!(
            t.apply(news(1, 3, Alive), ONE_SEC),
            Applied::Accepted { from: Some(Dead) },
            "a restart under the same ID"
        );
        assert_eq!(t.drain_changes(), [Change::Joined(id(1))]);
    }

    #[test]
    fn a_stranger_heard_dead_leaves_a_tombstone_not_a_departure() {
        let mut t = table();
        t.apply(news(1, 2, Dead), 0);

        assert!(t.peers().is_empty());
        assert!(
            t.drain_changes().is_empty(),
            "it never was in peers(), so nothing has left"
        );
        assert_eq!(t.apply(news(1, 2, Alive), 0), Applied::Ignored);
    }

    #[test]
    fn a_suspicion_about_ourselves_is_refuted_with_a_higher_incarnation() {
        let mut t = table();
        assert_eq!(
            t.apply(news(0, 0, Suspect), 0),
            Applied::Refuted(news(0, 1, Alive))
        );
        assert_eq!(t.incarnation(), 1);
        assert_eq!(t.status(id(0)), Some(Alive));
        assert_eq!(t.update_about(id(0)), Some(news(0, 1, Alive)));
    }

    #[test]
    fn being_declared_dead_is_refuted_too() {
        let mut t = table();
        assert_eq!(
            t.apply(news(0, 0, Dead), 0),
            Applied::Refuted(news(0, 1, Alive)),
            "a node back from a pause must be able to rejoin"
        );
    }

    #[test]
    fn a_refutation_outbids_the_news_not_our_own_count() {
        let mut t = table();
        assert_eq!(
            t.apply(news(0, 7, Suspect), 0),
            Applied::Refuted(news(0, 8, Alive)),
            "after a restart the cluster remembers incarnation 7, so 1 would lose"
        );
        assert_eq!(t.incarnation(), 8);
    }

    #[test]
    fn a_higher_alive_about_ourselves_is_refuted_as_well() {
        let mut t = table();
        assert_eq!(
            t.apply(news(0, 3, Alive), 0),
            Applied::Refuted(news(0, 4, Alive)),
            "a previous life of ours must not outrank the current one"
        );
    }

    #[test]
    fn a_stale_accusation_is_answered_without_a_new_incarnation() {
        let mut t = table();
        t.apply(news(0, 4, Suspect), 0);
        assert_eq!(t.incarnation(), 5);

        assert_eq!(
            t.apply(news(0, 4, Suspect), 0),
            Applied::Refuted(news(0, 5, Alive)),
            "re-gossiped: the accuser has not heard the refutation yet"
        );
        assert_eq!(
            t.apply(news(0, 2, Dead), 0),
            Applied::Refuted(news(0, 5, Alive))
        );
        assert_eq!(t.incarnation(), 5, "a stale accusation must not bump it");
    }

    #[test]
    fn echoes_and_old_alives_about_ourselves_are_ignored() {
        let mut t = table();
        t.apply(news(0, 4, Suspect), 0);

        assert_eq!(t.apply(news(0, 5, Alive), 0), Applied::Ignored, "an echo");
        assert_eq!(
            t.apply(news(0, 3, Alive), 0),
            Applied::Ignored,
            "an old life accuses no one"
        );
        assert_eq!(t.incarnation(), 5);
    }

    #[test]
    fn a_refutation_leaves_the_peers_alone() {
        let mut t = table();
        t.apply(news(1, 0, Alive), 0);
        t.drain_changes();

        t.apply(news(0, 0, Suspect), 0);
        assert_eq!(t.peers(), [id(1)]);
        assert_eq!(t.cluster_size(), 2);
        assert!(t.drain_changes().is_empty());
        check_contract(&t).unwrap();
    }

    #[test]
    fn a_refutation_always_supersedes_what_it_refutes() {
        for incarnation in [0, 1, 41, u32::MAX - 1] {
            for status in [Alive, Suspect, Dead] {
                let mut t = MemberTable::new(id(0), addr(0));
                let claim = news(0, incarnation, status);
                if let Applied::Refuted(answer) = t.apply(claim, 0) {
                    assert!(answer.supersedes(&claim), "{answer:?} vs {claim:?}");
                } else {
                    assert_eq!((incarnation, status), (0, Alive), "only our own echo");
                }
            }
        }
    }

    #[test]
    fn an_incarnation_that_cannot_be_outbid_is_ignored_not_a_panic() {
        let mut t = table();
        assert_eq!(t.apply(news(0, u32::MAX, Dead), 0), Applied::Ignored);
        assert_eq!(t.incarnation(), 0);
    }

    #[test]
    fn a_suspected_node_clears_its_name_across_two_tables() {
        let mut a = MemberTable::new(id(1), addr(1));
        let mut b = MemberTable::new(id(2), addr(2));
        a.apply(b.update_about(id(2)).unwrap(), 0);

        let suspicion = a.suspect(id(2), ONE_SEC).unwrap();
        let Applied::Refuted(answer) = b.apply(suspicion, ONE_SEC) else {
            panic!("b must refute the suspicion about itself");
        };

        assert_eq!(
            a.apply(answer, 2 * ONE_SEC),
            Applied::Accepted {
                from: Some(Suspect)
            }
        );
        assert_eq!(a.status(id(2)), Some(Alive));
        assert!(
            a.expire_suspects(100 * TIMEOUT, TIMEOUT).is_empty(),
            "a refuted suspicion must not expire into death"
        );
    }

    #[test]
    fn only_the_long_dead_are_forgotten() {
        let mut t = table();
        t.apply(news(1, 0, Dead), 0);
        t.apply(news(2, 0, Suspect), 0);
        t.apply(news(3, 0, Alive), 0);
        t.apply(news(4, 0, Dead), ONE_SEC);
        assert!(t.dead().eq([id(1), id(4)]));

        assert_eq!(t.forget_dead(TIMEOUT - 1, TIMEOUT), 0);
        assert_eq!(t.forget_dead(TIMEOUT, TIMEOUT), 1);
        assert_eq!(t.status(id(1)), None);
        assert_eq!(t.status(id(4)), Some(Dead), "died a second later");
        assert_eq!(t.status(id(2)), Some(Suspect));
        assert_eq!(t.status(id(3)), Some(Alive));
        assert!(t.dead().eq([id(4)]));
    }

    #[test]
    fn a_forgotten_member_is_a_stranger_again() {
        let mut t = table();
        t.apply(news(1, 3, Dead), 0);
        t.forget_dead(TIMEOUT, TIMEOUT);

        assert_eq!(
            t.apply(news(1, 3, Alive), TIMEOUT),
            Applied::Accepted { from: None },
            "the price of a bounded table: a late echo is news again"
        );
        assert_eq!(t.peers(), [id(1)]);
    }

    #[test]
    fn replaying_the_changes_rebuilds_the_peers() {
        let mut t = table();
        let mut log = Vec::new();
        let step = |t: &mut MemberTable, log: &mut Vec<Change>| {
            check_contract(t).unwrap();
            log.extend(t.drain_changes());
        };

        t.apply(news(1, 0, Alive), 0);
        step(&mut t, &mut log);
        t.apply(news(2, 0, Alive), 0);
        t.apply(news(3, 0, Dead), 0);
        t.apply(news(3, 0, Dead), 0);
        step(&mut t, &mut log);
        t.suspect(id(1), ONE_SEC);
        step(&mut t, &mut log);
        t.expire_suspects(ONE_SEC + TIMEOUT, TIMEOUT);
        step(&mut t, &mut log);
        t.apply(news(1, 1, Alive), 10 * ONE_SEC);
        step(&mut t, &mut log);

        assert_eq!(
            log,
            [
                Change::Joined(id(1)),
                Change::Joined(id(2)),
                Change::Left(id(1)),
                Change::Joined(id(1)),
            ]
        );

        let mut rebuilt = BTreeSet::new();
        for change in log {
            match change {
                Change::Joined(peer) => assert!(rebuilt.insert(peer)),
                Change::Left(peer) => assert!(rebuilt.remove(&peer)),
            }
        }
        assert!(rebuilt.iter().copied().eq(t.peers().iter().copied()));
    }

    #[test]
    fn a_full_table_refuses_strangers_but_not_news_of_its_members() {
        let mut t = table();
        t.set_max(3);
        t.apply(news(1, 0, Alive), 0);
        t.apply(news(2, 0, Dead), 0);
        assert_eq!(t.known(), 3, "the dead count too");
        assert_eq!(t.apply(news(3, 0, Alive), 0), Applied::Refused);
        assert_eq!(t.apply(news(4, 0, Dead), 0), Applied::Refused);
        assert_eq!(t.refused(), 2);
        assert_eq!(t.status(id(3)), None);
        assert_eq!(t.known(), 3);
        assert!(
            t.drain_changes()
                .iter()
                .all(|c| !matches!(c, Change::Joined(p) if *p == id(3)))
        );

        assert!(matches!(
            t.apply(news(2, 1, Alive), ONE_SEC),
            Applied::Accepted { .. }
        ));
        assert!(t.suspect(id(1), ONE_SEC).is_some());
        assert!(matches!(
            t.apply(news(0, 0, Suspect), ONE_SEC),
            Applied::Refuted(_)
        ));
        assert_eq!(t.refused(), 2, "only strangers are refused");

        t.apply(news(2, 1, Dead), 2 * ONE_SEC);
        assert_eq!(t.forget_dead(2 * ONE_SEC + TIMEOUT, TIMEOUT), 1);
        assert!(matches!(
            t.apply(news(3, 0, Alive), 3 * ONE_SEC),
            Applied::Accepted { from: None }
        ));
        assert_eq!(t.known(), 3);
        check_contract(&t).unwrap();
    }

    #[test]
    fn a_table_is_unbounded_unless_told() {
        let mut t = table();
        for member in 1..2000 {
            t.apply(news(member, 0, Alive), 0);
        }
        assert_eq!(t.known(), 2000);
        assert_eq!(t.refused(), 0);
    }
}
