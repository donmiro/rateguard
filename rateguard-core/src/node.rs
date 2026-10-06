use rateguard_proto::{self as proto, Message, Status, Update};

use crate::{
    boundary::{Action, Event, PeerId},
    gcra::{Decision, Nanos},
    limiter::{Config, Limiter},
    member::MemberTable,
    membership::{Change, Membership},
    rng::Rng,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Probe {
    target: PeerId,
    seq: u32,
}

pub struct Node {
    limiter: Limiter,
    members: MemberTable,
    rng: Rng,
    order: Vec<PeerId>,
    next: usize,
    seq: u32,
    probe: Option<Probe>,
    last_now: Nanos,
    actions: Vec<Action>,
}
impl Node {
    pub fn new(config: Config, local: PeerId, seed: u64) -> Self {
        Self {
            limiter: Limiter::new(config),
            members: MemberTable::new(local),
            rng: Rng::new(seed),
            order: Vec::new(),
            next: 0,
            seq: 0,
            probe: None,
            last_now: 0,
            actions: Vec::new(),
        }
    }

    pub fn introduce(&mut self, peer: PeerId) {
        assert_ne!(peer, self.members.local(), "a node is not its own peer");
        self.members.apply(
            Update {
                member: peer.get(),
                incarnation: 0,
                status: Status::Alive,
            },
            self.last_now,
        );
    }

    pub fn cluster_size(&self) -> usize {
        self.members.cluster_size()
    }

    pub fn members(&self) -> &MemberTable {
        &self.members
    }

    pub fn awaiting_ack(&self) -> Option<PeerId> {
        self.probe.map(|probe| probe.target)
    }

    pub fn limiter(&self) -> &Limiter {
        &self.limiter
    }

    pub fn check(&mut self, key: u64, now: Nanos) -> Decision {
        self.advance(now);
        self.limiter.check(key, now, self.members.cluster_size())
    }

    pub fn handle(&mut self, event: Event<'_>, now: Nanos) -> &[Action] {
        self.advance(now);
        self.actions.clear();

        match event {
            Event::Tick => {
                self.limiter.tick(now, self.members.cluster_size());
                self.probe_next();
            }
            Event::MessageReceived { from, bytes } => self.receive(from, bytes),
        }

        &self.actions
    }

    fn probe_next(&mut self) {
        self.follow_changes();
        self.probe = None;

        let Some(target) = self.next_target() else {
            return;
        };
        self.seq = self.seq.wrapping_add(1);
        self.probe = Some(Probe {
            target,
            seq: self.seq,
        });
        self.send(
            target,
            &Message::Ping {
                seq: self.seq,
                updates: Vec::new(),
            },
        );
    }

    // SWIM inserts a newcomer at a random place among the peers still to be
    // probed this round, so it is probed within the round it joined.
    fn follow_changes(&mut self) {
        for change in self.members.drain_changes() {
            if let Change::Joined(peer) = change
                && !self.order[self.next..].contains(&peer)
            {
                let remaining = (self.order.len() - self.next) as u64;
                let at = self.next + self.rng.up_to(remaining) as usize;
                self.order.insert(at, peer);
            }
        }
    }

    fn next_target(&mut self) -> Option<PeerId> {
        if let Some(target) = self.take_live() {
            return Some(target);
        }
        self.order.clear();
        self.order.extend_from_slice(self.members.peers());
        self.rng.shuffle(&mut self.order);
        self.next = 0;
        self.take_live()
    }

    fn take_live(&mut self) -> Option<PeerId> {
        while let Some(&peer) = self.order.get(self.next) {
            self.next += 1;
            if self.members.peers().binary_search(&peer).is_ok() {
                return Some(peer);
            }
        }
        None
    }

    fn receive(&mut self, from: PeerId, bytes: &[u8]) {
        let Ok(message) = proto::decode(bytes) else {
            return;
        };
        match message {
            Message::Ping { seq, .. } => self.send(
                from,
                &Message::Ack {
                    seq,
                    updates: Vec::new(),
                },
            ),
            Message::Ack { seq, .. } => {
                if self.probe == Some(Probe { target: from, seq }) {
                    self.probe = None;
                }
            }
            Message::PingReq { .. } => {}
        }
    }

    fn send(&mut self, peer: PeerId, message: &Message) {
        let bytes = proto::encode(message).expect("a message without updates always fits");
        self.actions.push(Action::SendTo { peer, bytes });
    }

    fn advance(&mut self, now: Nanos) {
        assert!(
            now >= self.last_now,
            "time must not run backwards: {now} < {}",
            self.last_now
        );
        self.last_now = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    const ONE_SEC: Nanos = 1_000_000_000;
    const KEY: u64 = 42;
    const LOCAL: PeerId = PeerId::new(0);

    fn config() -> Config {
        Config {
            limit_per_sec: 1000,
            burst: 10,
            alpha: 0.5,
            cooldown: 5 * ONE_SEC,
            hot_set_size: 4,
            max_tracked_keys: 8,
            demand_time_constant: ONE_SEC,
        }
    }

    fn node() -> Node {
        Node::new(config(), LOCAL, 1)
    }

    fn node_with(peers: impl IntoIterator<Item = u64>) -> Node {
        let mut n = node();
        for peer in peers {
            n.introduce(PeerId::new(peer));
        }
        n
    }

    fn measured_interval(node: &mut Node, now: Nanos) -> Nanos {
        for _ in 0..config().burst {
            assert_eq!(node.check(KEY, now), Decision::Allow);
        }
        let Decision::Deny { retry_at } = node.check(KEY, now) else {
            panic!("burst must be exhausted");
        };
        retry_at - now
    }

    fn sent(actions: &[Action]) -> Vec<(PeerId, Message)> {
        actions
            .iter()
            .map(|Action::SendTo { peer, bytes }| (*peer, proto::decode(bytes).unwrap()))
            .collect()
    }

    fn deliver(node: &mut Node, from: u64, message: Message, now: Nanos) -> Vec<(PeerId, Message)> {
        let bytes = proto::encode(&message).unwrap();
        sent(node.handle(
            Event::MessageReceived {
                from: PeerId::new(from),
                bytes: &bytes,
            },
            now,
        ))
    }

    fn ping_once(node: &mut Node, now: Nanos) -> (PeerId, u32) {
        match sent(node.handle(Event::Tick, now)).as_slice() {
            [(peer, Message::Ping { seq, updates })] => {
                assert!(updates.is_empty(), "nothing to piggyback yet");
                (*peer, *seq)
            }
            other => panic!("a tick must send exactly one PING, sent {other:?}"),
        }
    }

    #[test]
    fn a_freash_node_is_a_cluster_of_one() {
        assert_eq!(node().cluster_size(), 1);
    }

    #[test]
    fn check_delegates_to_the_limiter_at_the_cold_share() {
        assert_eq!(measured_interval(&mut node(), 0), 2_000_000);
    }

    #[test]
    fn the_cluster_size_comes_from_the_members() {
        let mut n = node_with([1]);
        assert_eq!(n.cluster_size(), 2);
        assert_eq!(measured_interval(&mut n, 0), 4_000_000);
    }

    #[test]
    #[should_panic(expected = "not its own peer")]
    fn a_node_cannot_introduce_itself() {
        node().introduce(LOCAL);
    }

    #[test]
    fn a_tick_drives_promotion() {
        let mut n = node();
        for _ in 0..5000 {
            n.check(KEY, 0);
        }

        assert_eq!(
            measured_interval(&mut n, 100_000_000),
            2_000_000,
            "promotion belongs to the tick, not the check()"
        );

        n.handle(Event::Tick, ONE_SEC);
        assert_eq!(
            measured_interval(&mut n, 2 * ONE_SEC),
            1_000_000,
            "a hot key gets the full share, not the alphs one"
        );
    }

    #[test]
    fn a_lone_node_pings_nobody() {
        let mut n = node();
        assert!(n.handle(Event::Tick, 0).is_empty());
        assert_eq!(n.awaiting_ack(), None);
    }

    #[test]
    fn a_tick_pings_one_peer_and_waits_for_it() {
        let mut n = node_with([1, 2]);
        let (target, _) = ping_once(&mut n, 0);
        assert!([PeerId::new(1), PeerId::new(2)].contains(&target));
        assert_eq!(n.awaiting_ack(), Some(target));
    }

    #[test]
    fn every_peer_is_probed_once_per_round() {
        let peers = [1, 2, 3, 4, 5];
        let mut n = node_with(peers);
        let expected: BTreeSet<PeerId> = peers.into_iter().map(PeerId::new).collect();

        let mut rounds = BTreeSet::new();
        for round in 0..20 {
            let order: Vec<PeerId> = (0..peers.len())
                .map(|i| ping_once(&mut n, (round * peers.len() + i) as Nanos).0)
                .collect();
            assert_eq!(
                order.iter().copied().collect::<BTreeSet<_>>(),
                expected,
                "round {round} went {order:?}"
            );
            rounds.insert(order);
        }
        assert!(
            rounds.len() > 1,
            "the order is reshuffled between rounds, or failures correlate"
        );
    }

    #[test]
    fn a_newcomer_is_probed_within_the_current_round() {
        let mut n = node_with([1, 2, 3]);
        let first = ping_once(&mut n, 0).0;
        n.introduce(PeerId::new(9));

        let rest: Vec<PeerId> = (1..4).map(|t| ping_once(&mut n, t).0).collect();
        assert!(rest.contains(&PeerId::new(9)), "{rest:?}");
        assert!(
            !rest.contains(&first),
            "the round must not restart: {rest:?}"
        );
    }

    #[test]
    fn a_ping_is_answered_with_an_ack_of_the_same_seq() {
        let mut n = node();
        let ping = Message::Ping {
            seq: 77,
            updates: Vec::new(),
        };
        assert_eq!(
            deliver(&mut n, 5, ping, 0),
            [(
                PeerId::new(5),
                Message::Ack {
                    seq: 77,
                    updates: Vec::new()
                }
            )],
            "liveness is answered even to a stranger"
        );
    }

    #[test]
    fn the_matching_ack_ends_the_wait() {
        let mut n = node_with([1]);
        let (target, seq) = ping_once(&mut n, 0);

        let ack = Message::Ack {
            seq,
            updates: Vec::new(),
        };
        assert!(deliver(&mut n, target.get(), ack, 1).is_empty());
        assert_eq!(n.awaiting_ack(), None);
    }

    #[test]
    fn a_stray_ack_does_not_end_the_wait() {
        let mut n = node_with([1, 2]);
        let (target, seq) = ping_once(&mut n, 0);
        let other = if target == PeerId::new(1) { 2 } else { 1 };
        let ack = |seq| Message::Ack {
            seq,
            updates: Vec::new(),
        };

        deliver(&mut n, other, ack(seq), 1);
        assert_eq!(n.awaiting_ack(), Some(target), "an ACK from someone else");
        deliver(&mut n, target.get(), ack(seq.wrapping_sub(1)), 1);
        assert_eq!(n.awaiting_ack(), Some(target), "an ACK to an older PING");
    }

    #[test]
    fn a_late_ack_does_not_end_the_next_wait() {
        let mut n = node_with([1]);
        let (_, old) = ping_once(&mut n, 0);
        let (target, new) = ping_once(&mut n, 1);
        assert_ne!(old, new, "every PING has its own seq");

        let late = Message::Ack {
            seq: old,
            updates: Vec::new(),
        };
        deliver(&mut n, target.get(), late, 2);
        assert_eq!(n.awaiting_ack(), Some(target));
    }

    #[test]
    fn the_next_tick_gives_up_on_an_unanswered_ping() {
        let mut n = node_with([1, 2]);
        let (first, _) = ping_once(&mut n, 0);
        let (second, _) = ping_once(&mut n, 1);
        assert_ne!(first, second);
        assert_eq!(n.awaiting_ack(), Some(second));
    }

    #[test]
    fn garbage_is_dropped_without_a_reply() {
        let mut n = node_with([7]);
        let bytes = [0xde, 0xad, 0xbe, 0xef];

        assert!(
            n.handle(
                Event::MessageReceived {
                    from: PeerId::new(7),
                    bytes: &bytes
                },
                0
            )
            .is_empty()
        );
    }

    #[test]
    fn the_same_seed_probes_in_the_same_order() {
        let order = |seed| {
            let mut n = Node::new(config(), LOCAL, seed);
            for peer in 1..=6 {
                n.introduce(PeerId::new(peer));
            }
            (0..12).map(|t| ping_once(&mut n, t).0).collect::<Vec<_>>()
        };
        assert_eq!(order(5), order(5));
        assert_ne!(order(5), order(6));
    }

    #[test]
    #[should_panic(expected = "time must not run backwards")]
    fn an_event_may_not_arrive_in_the_past() {
        let mut n = node();
        n.handle(Event::Tick, ONE_SEC);
        n.handle(Event::Tick, 0);
    }

    #[test]
    #[should_panic(expected = "time must not run backwards")]
    fn a_request_may_not_arrive_in_the_past() {
        let mut n = node();
        n.handle(Event::Tick, ONE_SEC);
        n.check(KEY, 0);
    }
}
