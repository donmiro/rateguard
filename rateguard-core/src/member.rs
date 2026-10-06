use rateguard_proto::{Status, Update};
use std::collections::BTreeMap;

use crate::boundary::PeerId;
use crate::gcra::Nanos;
use crate::membership::{Change, Membership};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    Ignored,
    Accepted { from: Option<Status> },
    Refuted(Update),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Member {
    incarnation: u32,
    status: Status,
    since: Nanos,
}
impl Member {
    fn update(&self, peer: PeerId) -> Update {
        Update {
            member: peer.get(),
            incarnation: self.incarnation,
            status: self.status,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemberTable {
    local: PeerId,
    incarnation: u32,
    members: BTreeMap<PeerId, Member>,
    live: Vec<PeerId>,
    changes: Vec<Change>,
}
impl MemberTable {
    pub fn new(local: PeerId) -> Self {
        Self {
            local,
            incarnation: 0,
            members: BTreeMap::new(),
            live: Vec::new(),
            changes: Vec::new(),
        }
    }

    pub fn incarnation(&self) -> u32 {
        self.incarnation
    }

    pub fn status(&self, peer: PeerId) -> Option<Status> {
        if peer == self.local {
            return Some(Status::Alive);
        }
        self.members.get(&peer).map(|member| member.status)
    }

    pub fn update_about(&self, peer: PeerId) -> Option<Update> {
        if peer == self.local {
            return Some(self.own_update());
        }
        self.members.get(&peer).map(|member| member.update(peer))
    }

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

        self.members.insert(
            peer,
            Member {
                incarnation: update.incarnation,
                status: update.status,
                since: now,
            },
        );

        let was_live = from.is_some_and(|member| member.status != Status::Dead);
        let is_live = update.status != Status::Dead;
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

    pub fn dead(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.members
            .iter()
            .filter(|(_, member)| member.status == Status::Dead)
            .map(|(&peer, _)| peer)
    }

    pub fn forget_dead(&mut self, now: Nanos, ttl: Nanos) -> usize {
        let before = self.members.len();
        self.members.retain(|_, member| {
            member.status != Status::Dead || now.saturating_sub(member.since) < ttl
        });
        before - self.members.len()
    }

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

    fn news(member: u64, incarnation: u32, status: Status) -> Update {
        Update {
            member,
            incarnation,
            status,
        }
    }

    fn table() -> MemberTable {
        MemberTable::new(id(0))
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
                let mut t = MemberTable::new(id(0));
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
        let mut a = MemberTable::new(id(1));
        let mut b = MemberTable::new(id(2));
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
}
