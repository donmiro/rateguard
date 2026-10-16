//! The background task: the node, the socket and the ticker.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use rateguard_core::boundary::{Action, Event, PeerId};
use rateguard_core::node::Node;
use rateguard_proto::Address;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

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

/// Runs the node until it is asked to leave or the last `Guard` is gone,
/// then leaves the cluster.
pub(crate) async fn run<T: Transport>(
    mut node: Node,
    transport: Arc<T>,
    mut names: Names,
    shared: Weak<Shared>,
    mut ticker: tokio::time::Interval,
    mut leave: mpsc::Receiver<oneshot::Sender<()>>,
) {
    let Some(epoch) = shared.upgrade().map(|shared| shared.epoch) else {
        return;
    };
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Sending is given one tick: a transport that never completes a send
    // must not freeze the node, nor keep it alive once the last Guard is
    // gone.
    let budget = ticker.period();
    let mut buf = vec![0u8; rateguard_proto::MAX_DATAGRAM];
    let mut done = None;
    loop {
        // The shared state is held only while the node works, never across
        // an await: the last Guard dropped must be noticed.
        let actions = tokio::select! {
            received = transport.recv_from(&mut buf) => {
                let Some(shared) = shared.upgrade() else { break };
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
                let Some(shared) = shared.upgrade() else { break };
                sync(&mut node, &shared)
            }
            found = names.next() => {
                names.update(&mut node, found);
                Vec::new()
            }
            // None: the last Guard is gone.
            asked = leave.recv() => {
                done = asked;
                break;
            }
        };
        send(&node, &*transport, actions, budget).await;
    }

    // The quotas drop to the floor before the peers hear: from the moment
    // they do, they no longer count this node.
    if let Some(shared) = shared.upgrade() {
        hold_at_floor(&shared, node.limiter().config());
        shared.running.store(false, Ordering::Relaxed);
    }
    let now = epoch.elapsed().as_nanos() as u64;
    let actions = node.leave(now).to_vec();
    send(&node, &*transport, actions, budget).await;
    if let Some(done) = done {
        let _ = done.send(());
    }
}

/// How often the names among the seeds are looked up again once the node
/// has peers. While it has none, every reconnect interval: a fleet started
/// all at once may find nobody behind the name at first.
const RESOLVE_EVERY: Duration = Duration::from_secs(30);

/// The most time a round waits for a name: the names are looked up all at
/// once, so a stalled lookup holds a round up by this much at most, and a
/// timeout keeps that name's last addresses, just like a failed lookup.
/// Well above the system resolver's own 5 s retry, so that a lookup slow
/// for a lost packet still resolves rather than time out every round.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(20);

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
        let mut pending = JoinSet::new();
        for (i, (host, port)) in names.iter().cloned().enumerate() {
            let transport = transport.clone();
            pending.spawn(async move {
                let lookup = tokio::time::timeout(RESOLVE_TIMEOUT, transport.resolve(&host, port));
                (i, lookup.await.ok().and_then(Result::ok))
            });
        }
        let lookups = async {
            let mut all = vec![None; names.len()];
            while let Some(done) = pending.join_next().await {
                if let Ok((i, addrs)) = done {
                    all[i] = addrs;
                }
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
        hold_at_floor(&shared, &config);
    }
    shared.running.store(false, Ordering::Relaxed);
}

// Every key to the floor `R × β / N`, the part of the limit a cluster
// leaves to each member: for a node its peers no longer count.
fn hold_at_floor(shared: &Shared, config: &rateguard_core::limiter::Config) {
    let n = shared.cluster_size.load(Ordering::Relaxed).max(1);
    let floor = |limit: rateguard_core::limiter::Limit| {
        let rate = limit.per_second as f64 * config.floor_factor / n as f64;
        rateguard_core::gcra::Quota::per_second(rate, limit.burst)
    };
    let classes = shared.classes.snapshot();
    let strictest = floor(classes.strictest());
    // Keys not seen yet too: a slot taken from now on is sized from these.
    let factor = config.floor_factor / n as f64;
    shared
        .fresh_factor
        .store(factor.to_bits(), Ordering::Relaxed);
    shared.free_quota.store(strictest.pack(), Ordering::Relaxed);
    let now = shared.now();
    shared.table.for_each(|_, slot| {
        let (limit, _) = classes.lookup(slot.class.load(Ordering::Relaxed));
        slot.gcra.set_quota(floor(limit), now);
    });
    shared
        .table
        .for_each_free(|slot| slot.gcra.set_quota(strictest, now));
    shared.table.overflow().gcra.set_quota(strictest, now);
}

// One tick: attempts in, the core's round, quotas out (spec §5.3).
fn sync(node: &mut Node, shared: &Arc<Shared>) -> Vec<Action> {
    let now = shared.now();
    let classes = shared.classes.snapshot();
    let mut counts = Counts::default();
    shared.table.for_each(|key, slot| {
        let (n, d) = slot.take_counts();
        let (limit, known) = classes.lookup(slot.class.load(Ordering::Relaxed));
        if n > 0 {
            node.record(key, n, d, limit, now);
        }
        counts.attempts += n;
        counts.denied += d;
        if !known {
            counts.unknown_class += n;
        }
    });
    // Not a key: counted, but no demand of anyone's.
    let (overflowed, d) = shared.table.overflow().take_counts();
    counts.attempts += overflowed;
    counts.denied += d;
    counts.overflowed = overflowed;
    let actions = node.handle(Event::Tick, now).to_vec();
    report(node, shared, counts);

    // A slot nobody holds carries the strictest new key's quota of any
    // class; whoever takes it gives it its own class's.
    let new_key = node.new_key_quota_in(classes.strictest());
    shared
        .fresh_factor
        .store(node.new_key_factor().to_bits(), Ordering::Relaxed);
    shared.free_quota.store(new_key.pack(), Ordering::Relaxed);
    // A slot the node knows nothing of is freed, unless a check has come
    // since the counts were taken: a key that took its slot meanwhile, in
    // the middle of this tick, would lose it under its first check.
    shared.table.for_each(|key, slot| match node.quota(key) {
        Some(quota) => slot.gcra.set_quota(quota, now),
        None if !slot.gcra.has_debt(now) && !slot.has_counts() => {
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

// One tick's checks, from the key table.
#[derive(Debug, Default)]
struct Counts {
    attempts: u64,
    denied: u64,
    overflowed: u64,
    unknown_class: u64,
}

// This tick's counts and the node's state into the report for
// Guard::stats.
fn report(node: &Node, shared: &Shared, counts: Counts) {
    use rateguard_core::membership::Membership;
    use rateguard_proto::Status;

    let members = node.members();
    let suspects = members
        .peers()
        .iter()
        .filter(|&&peer| members.status(peer) == Some(Status::Suspect))
        .count();
    let held = node.held();
    let mut report = shared.report();
    report.attempts += counts.attempts;
    report.last_round = node.last_round();
    let stats = &mut report.stats;
    stats.denied += counts.denied;
    stats.overflowed += counts.overflowed;
    stats.unknown_class += counts.unknown_class;
    stats.most_denied = node
        .most_denied(10)
        .into_iter()
        .map(|(key_hash, denied_per_sec)| crate::stats::DeniedKey {
            key_hash,
            denied_per_sec,
        })
        .collect();
    stats.cluster_size = node.cluster_size();
    stats.counted_size = node.counted_size();
    stats.suspects = suspects;
    stats.tracked_keys = node.limiter().tracked_keys();
    stats.hot_keys = node.limiter().hot_keys();
    stats.joining = held.joining;
    stats.learning = held.learning;
    stats.without_quorum = held.without_quorum;

    #[cfg(feature = "metrics")]
    {
        let mut stats = stats.clone();
        stats.admitted = report.attempts.saturating_sub(stats.denied);
        stats.dry_run = shared.dry_run.load(Ordering::Relaxed);
        drop(report);
        crate::metrics::publish(&stats, &shared.metrics_label);
    }
}

// The whole batch gets the budget, not each datagram: three PING-REQs on a
// hung transport would hold the node three ticks. A lost datagram, or one
// that took too long to leave, is the protocol's normal case.
async fn send<T: Transport>(node: &Node, transport: &T, actions: Vec<Action>, budget: Duration) {
    let batch = async {
        for action in actions {
            // Sending is the only action this runtime knows.
            let Action::SendTo { peer, bytes } = action else {
                continue;
            };
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

    struct SlowLookup {
        calls: std::sync::atomic::AtomicUsize,
    }
    impl Transport for SlowLookup {
        async fn send_to(&self, _: &[u8], _: SocketAddr) -> std::io::Result<usize> {
            std::future::pending().await
        }
        async fn recv_from(&self, _: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            std::future::pending().await
        }
        async fn resolve(&self, host: &str, _: u16) -> std::io::Result<Vec<SocketAddr>> {
            match host {
                "slow.svc" if self.calls.fetch_add(1, Ordering::Relaxed) == 0 => Ok(vec![at(2)]),
                "slow.svc" | "dead.svc" => std::future::pending().await,
                "working.svc" => Ok(vec![at(3)]),
                _ => panic!("unexpected name: {host}"),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_lookup_preserves_old_seeds_and_does_not_block_other_names() {
        let guard = crate::Guard::builder()
            .bind("10.0.0.1:7946")
            .limit(100)
            .spawn_on(HungSend)
            .unwrap();
        let (found, mut lookups) = mpsc::channel(1);
        let task = tokio::spawn(resolve(
            Arc::new(SlowLookup {
                calls: std::sync::atomic::AtomicUsize::new(0),
            }),
            vec![
                ("slow.svc".into(), 7946),
                ("dead.svc".into(), 7946),
                ("working.svc".into(), 7946),
            ],
            Arc::downgrade(&guard.shared),
            Duration::from_secs(2),
            found,
        ));
        let mut node = node_at(1);
        node.expect_peers(true);
        let mut names = Names::new(id(1));
        let first = lookups.recv().await.unwrap();
        assert_eq!(first, vec![Some(vec![at(2)]), None, Some(vec![at(3)])]);
        names.update(&mut node, first);

        // Each later round must finish despite two names hanging, and
        // within one timeout, not one per hung name. The first name's last
        // address remains a seed, and the working name is still looked up;
        // a timeout is not an empty DNS answer.
        for _ in 0..2 {
            let round = RESOLVE_TIMEOUT + Duration::from_secs(3);
            let next = tokio::time::timeout(round, lookups.recv())
                .await
                .expect("hung lookups must not hold the round past one timeout")
                .unwrap();
            assert_eq!(next, vec![None, None, Some(vec![at(3)])]);
            names.update(&mut node, next);
            for host in [2, 3] {
                assert_eq!(node.address(id(host)), Some(at(host).into()));
            }
        }

        drop(lookups);
        task.await.unwrap();
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
