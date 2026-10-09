//! The background task: the node, the socket and the ticker.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use rateguard_core::boundary::{Action, Event, PeerId};
use rateguard_core::node::Node;
use rateguard_proto::Address;
use tokio::sync::mpsc;

use crate::guard::Shared;
use crate::transport::Transport;

/// The sender of a datagram: the member of its first record, which every
/// message opens with (spec §3). Not the socket address, which a NAT on the
/// way may have rewritten.
pub(crate) fn sender(bytes: &[u8]) -> Option<PeerId> {
    let message = rateguard_proto::decode(bytes).ok()?;
    message
        .updates()
        .first()
        .map(|update| PeerId::new(update.member))
}

/// Runs the node until the last `Guard` is gone; it notices on the next
/// tick.
pub(crate) async fn run<T: Transport>(
    mut node: Node,
    transport: Arc<T>,
    mut names: Names,
    shared: Weak<Shared>,
    mut ticker: tokio::time::Interval,
) {
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Sending is given one tick: a transport that never completes a send
    // must not freeze the node, nor keep it alive once the last Guard is
    // gone.
    let budget = ticker.period();
    let mut buf = vec![0u8; rateguard_proto::MAX_DATAGRAM];
    loop {
        // The shared state is held only while the node works, never across
        // an await: the last Guard dropped must be noticed.
        let actions = tokio::select! {
            received = transport.recv_from(&mut buf) => {
                let Some(shared) = shared.upgrade() else { return };
                match received {
                    Ok((len, _)) => match sender(&buf[..len]) {
                        Some(from) => {
                            let event = Event::MessageReceived { from, bytes: &buf[..len] };
                            node.handle(event, shared.now()).to_vec()
                        }
                        None => Vec::new(),
                    },
                    Err(_) => Vec::new(),
                }
            }
            _ = ticker.tick() => {
                let Some(shared) = shared.upgrade() else { return };
                sync(&mut node, &shared)
            }
            found = names.next() => {
                names.update(&mut node, found);
                Vec::new()
            }
        };
        send(&node, &*transport, actions, budget).await;
    }
}

/// How often the names among the seeds are looked up again once the node
/// has peers. While it has none, every reconnect interval: a fleet started
/// all at once may find nobody behind the name at first.
const RESOLVE_EVERY: Duration = Duration::from_secs(30);

/// How many lookups in a row must find this node alone behind its names
/// before it takes the fleet to be itself alone: about 4 s at the default
/// reconnect interval. A fleet whose replicas become ready further apart
/// than that runs over the limit until they find each other.
const ALONE_AFTER: u32 = 3;

/// What each name among the seeds resolved to, in order, `None` where the
/// lookup failed.
type Found = Vec<Option<Vec<SocketAddr>>>;

/// Looks the names among the seeds up, now and then again, until the node
/// is gone. On its own task: a lookup may take seconds, the node's rounds
/// may not.
pub(crate) async fn resolve<T: Transport>(
    transport: Arc<T>,
    names: Vec<(String, u16)>,
    shared: Weak<Shared>,
    retry: Duration,
    found: mpsc::Sender<Found>,
) {
    loop {
        let lookups = async {
            let mut all = Vec::with_capacity(names.len());
            for (host, port) in &names {
                all.push(transport.resolve(host, *port).await.ok());
            }
            all
        };
        // The node may stop meanwhile; the transport must go with it.
        let all = tokio::select! {
            all = lookups => all,
            _ = found.closed() => return,
        };
        if found.send(all).await.is_err() {
            return;
        }
        let Some(alone) = shared
            .upgrade()
            .map(|shared| shared.cluster_size.load(Ordering::Relaxed) <= 1)
        else {
            return;
        };
        let wait = if alone { retry } else { RESOLVE_EVERY };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = found.closed() => return,
        }
    }
}

/// The seeds that come from names: added as their addresses appear,
/// removed as they go, so that the seed list follows the fleet rather than
/// grow with every instance it ever had.
#[derive(Debug)]
pub(crate) struct Names {
    local: PeerId,
    lookups: Option<mpsc::Receiver<Found>>,
    /// The last addresses of each name, kept through a failed lookup.
    last: Vec<Vec<SocketAddr>>,
    /// Seeds given as addresses: never removed.
    kept: BTreeSet<PeerId>,
    /// Seeds added from names.
    added: BTreeSet<PeerId>,
    /// Lookups in a row that found this node and nobody else.
    alone: u32,
}
impl Names {
    pub fn new(local: PeerId) -> Self {
        Self {
            local,
            lookups: None,
            last: Vec::new(),
            kept: BTreeSet::new(),
            added: BTreeSet::new(),
            alone: 0,
        }
    }

    /// A seed given as an address, which no lookup removes.
    pub fn keep(&mut self, seed: PeerId) {
        self.kept.insert(seed);
    }

    pub fn listen(&mut self, lookups: mpsc::Receiver<Found>) {
        self.lookups = Some(lookups);
    }

    // The next round of lookups; never, if there are no names. Cancel-safe.
    async fn next(&mut self) -> Found {
        if let Some(lookups) = &mut self.lookups
            && let Some(found) = lookups.recv().await
        {
            return found;
        }
        self.lookups = None;
        std::future::pending().await
    }

    fn update(&mut self, node: &mut Node, found: Found) {
        // A replica running alone finds itself behind its name and nobody
        // else, and would wait for peers for good. Nobody at all, or a
        // failed lookup, is no such sign: a fleet started at once may not
        // be listed yet.
        let only_me = found.iter().all(|found| {
            found.as_ref().is_some_and(|found| {
                found
                    .iter()
                    .all(|&address| crate::key::peer_id(address.into()) == self.local)
            })
        }) && found.iter().flatten().any(|found| !found.is_empty());
        self.alone = if only_me {
            self.alone.saturating_add(1)
        } else {
            0
        };
        if self.alone == ALONE_AFTER {
            node.expect_peers(false);
        }

        self.last.resize(found.len(), Vec::new());
        for (last, found) in self.last.iter_mut().zip(found) {
            if let Some(found) = found {
                *last = found;
            }
        }
        let mut current = BTreeSet::new();
        for &address in self.last.iter().flatten() {
            let address = Address::from(address);
            let id = crate::key::peer_id(address);
            // A name for the whole fleet resolves to this node too.
            if id == self.local || self.kept.contains(&id) {
                continue;
            }
            if current.insert(id) && !self.added.contains(&id) {
                node.add_seed(id, address);
            }
        }
        for &gone in self.added.difference(&current) {
            node.remove_seed(gone);
        }
        self.added = current;
    }
}

/// Waits for the background task. If it panicked, a bug, the node is gone
/// from its peers' view, and they hand its share out among themselves:
/// kept at its last quotas, it would admit that share a second time. It
/// drops every key to the floor `R × β / N` instead, the part of the limit
/// a cluster leaves to each member, and says it no longer runs.
pub(crate) async fn watch(
    task: tokio::task::JoinHandle<()>,
    shared: Weak<Shared>,
    config: rateguard_core::limiter::Config,
) {
    let Err(error) = task.await else { return };
    let Some(shared) = shared.upgrade() else {
        return;
    };
    if error.is_panic() {
        let n = shared.cluster_size.load(Ordering::Relaxed).max(1);
        let floor = config.limit_per_sec as f64 * config.floor_factor / n as f64;
        let quota = rateguard_core::gcra::Quota::per_second(floor, config.burst);
        let now = shared.now();
        shared
            .table
            .for_each(|_, slot| slot.gcra.set_quota(quota, now));
        shared
            .table
            .for_each_free(|slot| slot.gcra.set_quota(quota, now));
        shared.table.overflow().gcra.set_quota(quota, now);
    }
    shared.running.store(false, Ordering::Relaxed);
}

// One tick: attempts in, the core's round, quotas out (spec §5.3).
fn sync(node: &mut Node, shared: &Arc<Shared>) -> Vec<Action> {
    let now = shared.now();
    shared.table.for_each(|key, slot| {
        let n = slot.attempts.swap(0, crate::sync::Ordering::Relaxed);
        if n > 0 {
            node.record_attempts(key, n, now);
        }
    });
    let actions = node.handle(Event::Tick, now).to_vec();

    let new_key = node.new_key_quota();
    shared.table.for_each(|key, slot| match node.quota(key) {
        Some(quota) => slot.gcra.set_quota(quota, now),
        None if !slot.gcra.has_debt(now) => {
            shared.table.release(slot, key, new_key);
        }
        None => {}
    });
    shared
        .table
        .for_each_free(|slot| slot.gcra.set_quota(new_key, now));
    shared.table.overflow().gcra.set_quota(new_key, now);
    shared
        .cluster_size
        .store(node.cluster_size(), Ordering::Relaxed);
    actions
}

// The whole batch gets the budget, not each datagram: three PING-REQs on a
// hung transport would hold the node three ticks. A lost datagram, or one
// that took too long to leave, is the protocol's normal case.
async fn send<T: Transport>(node: &Node, transport: &T, actions: Vec<Action>, budget: Duration) {
    let batch = async {
        for Action::SendTo { peer, bytes } in actions {
            if let Some(address) = node.address(peer) {
                let _ = transport.send_to(&bytes, address.into()).await;
            }
        }
    };
    let _ = tokio::time::timeout(budget, batch).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rateguard_proto::{Address, Message, Status, Update, encode};

    struct HungSend;
    impl Transport for HungSend {
        async fn send_to(&self, _: &[u8], _: std::net::SocketAddr) -> std::io::Result<usize> {
            std::future::pending().await
        }
        async fn recv_from(&self, _: &mut [u8]) -> std::io::Result<(usize, std::net::SocketAddr)> {
            std::future::pending().await
        }
    }

    // A probe gone unanswered sends three PING-REQs in one tick: on a hung
    // transport they must cost the node one tick, not three.
    #[tokio::test(start_paused = true)]
    async fn a_hung_transport_costs_one_budget_per_batch() {
        use rateguard_core::limiter::Config;
        use rateguard_core::node::SwimConfig;

        let config = Config {
            limit_per_sec: 100,
            burst: 5,
            alpha: 0.5,
            floor_factor: 0.05,
            cooldown: 5_000_000_000,
            hot_set_size: 4,
            max_tracked_keys: 8,
            demand_time_constant: 1_000_000_000,
        };
        let me = Address::V4([10, 0, 0, 1], 7946);
        let mut node = Node::new(config, SwimConfig::default(), PeerId::new(1), me, 1);
        let actions: Vec<Action> = (2..5)
            .map(|peer| {
                node.introduce(PeerId::new(peer), Address::V4([10, 0, 0, peer as u8], 7946));
                Action::SendTo {
                    peer: PeerId::new(peer),
                    bytes: vec![1],
                }
            })
            .collect();

        let budget = Duration::from_millis(50);
        let start = tokio::time::Instant::now();
        send(&node, &HungSend, actions, budget).await;
        assert_eq!(start.elapsed(), budget);
    }

    fn node_at(host: u8) -> Node {
        use rateguard_core::limiter::Config;
        use rateguard_core::node::SwimConfig;

        let config = Config {
            limit_per_sec: 100,
            burst: 5,
            alpha: 0.5,
            floor_factor: 0.05,
            cooldown: 5_000_000_000,
            hot_set_size: 4,
            max_tracked_keys: 8,
            demand_time_constant: 1_000_000_000,
        };
        let me = Address::V4([10, 0, 0, host], 7946);
        Node::new(
            config,
            SwimConfig::default(),
            crate::key::peer_id(me),
            me,
            1,
        )
    }

    fn at(host: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, host], 7946))
    }

    fn id(host: u8) -> PeerId {
        crate::key::peer_id(Address::from(at(host)))
    }

    #[test]
    fn seeds_from_names_follow_the_lookups() {
        let mut node = node_at(1);
        let mut names = Names::new(id(1));
        node.add_seed(id(9), at(9).into());
        names.keep(id(9));

        // Two names; the first resolves to this node too.
        names.update(&mut node, vec![Some(vec![at(1), at(2)]), Some(vec![at(3)])]);
        for host in [2, 3, 9] {
            assert_eq!(node.address(id(host)), Some(at(host).into()), "{host}");
        }

        // 2 is gone, 4 has come, the second lookup failed: 3 is kept.
        names.update(&mut node, vec![Some(vec![at(4)]), None]);
        assert_eq!(node.address(id(2)), None);
        assert_eq!(node.address(id(3)), Some(at(3).into()));
        assert_eq!(node.address(id(4)), Some(at(4).into()));

        // A seed given as an address outlives a name that resolved to it.
        names.update(&mut node, vec![Some(vec![at(9)]), Some(Vec::new())]);
        names.update(&mut node, vec![Some(Vec::new()), Some(Vec::new())]);
        assert_eq!(node.address(id(9)), Some(at(9).into()));
        for host in [3, 4] {
            assert_eq!(node.address(id(host)), None, "{host}");
        }
    }

    #[test]
    fn a_node_alone_behind_its_name_stops_waiting_for_peers() {
        // What a node that takes itself for the whole fleet starts keys at.
        let alone = node_at(1).new_key_quota();
        let mut node = node_at(1);
        node.expect_peers(true);
        let mut names = Names::new(id(1));

        // Nobody listed yet, a failed lookup, another replica: none counts,
        // and the last starts the count again.
        for found in [
            vec![Some(vec![at(1)])],
            vec![Some(vec![at(1)])],
            vec![Some(Vec::new())],
            vec![None],
            vec![Some(vec![at(1), at(2)])],
            vec![Some(vec![at(1)])],
            vec![Some(vec![at(1)])],
        ] {
            names.update(&mut node, found);
            assert_ne!(node.new_key_quota(), alone, "still waiting");
        }
        names.update(&mut node, vec![Some(vec![at(1)])]);
        assert_eq!(node.new_key_quota(), alone, "a fleet of one");
    }

    #[test]
    fn the_sender_is_the_first_record_not_the_socket_address() {
        let me = Address::V4([10, 0, 0, 9], 7946);
        let bytes = encode(&Message::Ping {
            seq: 1,
            updates: vec![Update {
                member: 77,
                addr: me,
                incarnation: 0,
                status: Status::Alive,
            }],
            demand: Vec::new(),
        })
        .unwrap();
        assert_eq!(sender(&bytes).map(|peer| peer.get()), Some(77));

        let empty = encode(&Message::Ping {
            seq: 1,
            updates: Vec::new(),
            demand: Vec::new(),
        })
        .unwrap();
        assert_eq!(sender(&empty), None);
        assert_eq!(sender(&[0xde, 0xad]), None);
    }
}
