// The runtime's wiring in turmoil: real tasks, sockets and timers over a
// simulated network. The logic is the simulator's business; these only
// check that the pieces are connected the right way round.
#![cfg(feature = "turmoil")]

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rateguard::{GossipAuth, Guard, PartitionPolicy, RejectedDatagrams, Secret, Transport};
use turmoil::net::UdpSocket;

const PORT: u16 = 7946;
const LIMIT: u32 = 1000;
const BURST: u32 = 10;
const CEILING: usize = (LIMIT + 3 * BURST) as usize;

type Log = Arc<Mutex<Vec<(u64, usize)>>>;

// Rewrites the source of every datagram received, the way a NAT on the way
// would: the node must still know who sent it.
struct Rewriting(UdpSocket);
impl Transport for Rewriting {
    fn send_to(
        &self,
        bytes: &[u8],
        to: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send {
        self.0.send_to(bytes, to)
    }
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (len, _) = self.0.recv_from(buf).await?;
        Ok((len, "10.9.9.9:1".parse().unwrap()))
    }
}

// A node serving 400 attempts a second on one key, logging how many it
// admitted in each simulated second.
fn node(
    sim: &mut turmoil::Sim<'_>,
    name: &'static str,
    seed: &'static str,
    log: Log,
    rewrite: bool,
) {
    sim.host(name, move || {
        let log = log.clone();
        async move {
            let guard = spawn(name, seed, rewrite, GossipAuth::Off, Vec::new()).await?;
            serve(guard, log).await
        }
    });
}

// A host's Guard, joining through `seed`, under a hold longer than any run
// here: whatever share a node frees, it frees by leaving.
async fn spawn(
    name: &'static str,
    seed: &'static str,
    rewrite: bool,
    mode: GossipAuth,
    secrets: Vec<Secret>,
) -> turmoil::Result<Guard> {
    let me = SocketAddr::new(turmoil::lookup(name), PORT);
    let seed = SocketAddr::new(turmoil::lookup(seed), PORT);
    let socket = UdpSocket::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT)).await?;
    let builder = Guard::builder()
        .bind(format!("0.0.0.0:{PORT}"))
        .advertise(me.to_string())
        .seeds([seed.to_string()])
        .limit(LIMIT)
        .burst(BURST)
        .partition_policy(PartitionPolicy::HoldDown(Duration::from_secs(60)))
        .gossip_auth(mode, secrets);
    Ok(if rewrite {
        builder.spawn_on(Rewriting(socket))?
    } else {
        builder.spawn_on(socket)?
    })
}

async fn serve(guard: Guard, log: Log) -> turmoil::Result {
    let mut ticker = tokio::time::interval(Duration::from_micros(2500));
    loop {
        ticker.tick().await;
        if guard.check("k").is_allowed() {
            // Simulation time, not the host's: a restarted host starts its
            // own clock again.
            let second = turmoil::sim_elapsed().unwrap().as_secs();
            let mut log = log.lock().unwrap();
            match log.iter_mut().find(|(s, _)| *s == second) {
                Some((_, n)) => *n += 1,
                None => log.push((second, 1)),
            }
        }
    }
}

// A headless service: its name resolves to the hosts listed in it, the way
// Kubernetes lists the pods that are ready.
type Service = Arc<Mutex<Vec<&'static str>>>;

struct Listed(UdpSocket, Service);
impl Transport for Listed {
    fn send_to(
        &self,
        bytes: &[u8],
        to: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send {
        self.0.send_to(bytes, to)
    }
    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send {
        self.0.recv_from(buf)
    }
    async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        assert_eq!(host, "gossip.svc");
        let listed = self.1.lock().unwrap().clone();
        Ok(listed
            .into_iter()
            .map(|host| SocketAddr::new(turmoil::lookup(host), port))
            .collect())
    }
}

fn cluster(duration: Duration, rewrite_c: bool) -> (turmoil::Sim<'static>, Vec<Log>) {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(duration)
        .build();
    let logs: Vec<Log> = (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
    node(&mut sim, "a", "a", logs[0].clone(), false);
    node(&mut sim, "b", "a", logs[1].clone(), false);
    node(&mut sim, "c", "a", logs[2].clone(), rewrite_c);
    (sim, logs)
}

fn total(logs: &[Log], second: u64) -> usize {
    logs.iter()
        .map(|log| {
            log.lock()
                .unwrap()
                .iter()
                .find(|(s, _)| *s == second)
                .map_or(0, |(_, n)| *n)
        })
        .sum()
}

fn assert_shared(logs: &[Log], seconds: std::ops::Range<u64>) {
    for second in seconds {
        let total = total(logs, second);
        assert!(
            (900..=CEILING).contains(&total),
            "second {second}: {total} admitted, the limit is {LIMIT}"
        );
    }
}

fn wait(sim: &mut turmoil::Sim<'_>, until: Duration) {
    sim.client("clock", async move {
        tokio::time::sleep(until).await;
        Ok(())
    });
}

#[test]
fn three_nodes_share_one_limit() {
    let (mut sim, logs) = cluster(Duration::from_secs(40), false);
    wait(&mut sim, Duration::from_secs(30));
    sim.run().unwrap();
    assert_shared(&logs, 20..29);
}

// Hosts behind one name, all started at once and listed only once ready,
// three seconds later: every first lookup finds nobody.
fn listed(names: &[&'static str]) -> (turmoil::Sim<'static>, Vec<Log>) {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(40))
        .build();
    let service = Service::default();
    let logs: Vec<Log> = names
        .iter()
        .map(|_| Arc::new(Mutex::new(Vec::new())))
        .collect();
    for (&name, log) in names.iter().zip(&logs) {
        let (log, service) = (log.clone(), service.clone());
        sim.host(name, move || {
            let (log, service) = (log.clone(), service.clone());
            async move {
                let me = SocketAddr::new(turmoil::lookup(name), PORT);
                let socket = UdpSocket::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT)).await?;
                let guard = Guard::builder()
                    .bind(format!("0.0.0.0:{PORT}"))
                    .advertise(me.to_string())
                    .seeds([format!("gossip.svc:{PORT}")])
                    .limit(LIMIT)
                    .burst(BURST)
                    .partition_policy(PartitionPolicy::HoldDown(Duration::from_secs(60)))
                    .spawn_on(Listed(socket, service.clone()))?;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    service.lock().unwrap().push(name);
                });
                serve(guard, log).await
            }
        });
    }
    wait(&mut sim, Duration::from_secs(30));
    (sim, logs)
}

// Only looking again brings the fleet together; never over meanwhile, not
// even before anyone is listed: a node with nobody behind its name yet is
// waiting to join, not a cluster of one.
#[test]
fn a_fleet_started_at_once_finds_itself_by_name() {
    let (mut sim, logs) = listed(&["a", "b", "c"]);
    sim.run().unwrap();
    for second in 0..29 {
        let total = total(&logs, second);
        assert!(total <= CEILING, "second {second}: {total} admitted");
    }
    assert_shared(&logs, 20..29);
}

// One replica, its name resolving to itself alone: a fleet of one, which
// gets the whole limit, not a node waiting at the floor for peers.
#[test]
fn a_single_replica_behind_a_name_gets_the_whole_limit() {
    let (mut sim, logs) = listed(&["a"]);
    sim.run().unwrap();
    for second in 15..29 {
        let total = total(&logs, second);
        assert!(total >= 390, "second {second}: {total} admitted of 400");
    }
}

#[test]
fn a_rewritten_source_address_breaks_nothing() {
    let (mut sim, logs) = cluster(Duration::from_secs(40), true);
    wait(&mut sim, Duration::from_secs(30));
    sim.run().unwrap();
    assert_shared(&logs, 20..29);
}

#[test]
fn a_split_under_hold_down_keeps_the_limit() {
    let (mut sim, logs) = cluster(Duration::from_secs(60), false);
    sim.client("splitter", async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        turmoil::partition("c", "a");
        turmoil::partition("c", "b");
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    });
    sim.run().unwrap();
    for second in 15..44 {
        let total = total(&logs, second);
        assert!(total <= CEILING, "second {second}: {total} admitted");
    }
}

#[test]
fn a_restarted_node_comes_back() {
    let (mut sim, logs) = cluster(Duration::from_secs(60), false);
    let mut bounced = false;
    while sim.elapsed() < Duration::from_secs(45) {
        sim.step().unwrap();
        if !bounced && sim.elapsed() >= Duration::from_secs(15) {
            sim.bounce("b");
            bounced = true;
        }
    }
    assert_shared(&logs, 35..44);
}

#[test]
fn garbage_on_the_port_changes_nothing() {
    let (mut sim, logs) = cluster(Duration::from_secs(40), false);
    sim.client("scanner", async {
        let socket = UdpSocket::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9999)).await?;
        let target = SocketAddr::new(turmoil::lookup("a"), PORT);
        for i in 0..3000u32 {
            socket.send_to(&i.to_le_bytes().repeat(37), target).await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    });
    sim.run().unwrap();
    assert_shared(&logs, 20..29);
}

// Under a 60 s hold a node that only stopped would leave the other two at
// two thirds of the limit for the whole run, 666 a second; one that leaves
// lets them have all they ask for, 800, at once.
fn leaving(how: impl Fn(Guard) -> std::pin::Pin<Box<dyn Future<Output = ()>>> + 'static) {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(40))
        .build();
    let logs: Vec<Log> = (0..2).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
    node(&mut sim, "a", "a", logs[0].clone(), false);
    node(&mut sim, "b", "a", logs[1].clone(), false);
    let how = std::rc::Rc::new(how);
    sim.host("c", move || {
        let how = how.clone();
        async move {
            let guard = spawn("c", "a", false, GossipAuth::Off, Vec::new()).await?;
            let log = Arc::new(Mutex::new(Vec::new()));
            let _ = tokio::time::timeout(Duration::from_secs(15), serve(guard.clone(), log)).await;
            how(guard).await;
            std::future::pending().await
        }
    });
    wait(&mut sim, Duration::from_secs(30));
    sim.run().unwrap();
    for second in 17..29 {
        let total = total(&logs, second);
        assert!(
            (790..=800).contains(&total),
            "second {second}: {total} admitted of the 800 asked for"
        );
    }
}

#[test]
fn a_node_that_leaves_hands_its_share_over_at_once() {
    leaving(|guard| {
        Box::pin(async move {
            guard.leave().await;
            assert!(!guard.is_running());
            guard.leave().await;
        })
    });
}

#[test]
fn dropping_the_last_guard_leaves_too() {
    leaving(|guard| Box::pin(async move { drop(guard) }));
}

const A: [u8; 32] = [0xA1; 32];
const B: [u8; 32] = [0xB2; 32];

const NAMES: [&str; 3] = ["a", "b", "c"];
// The cluster has formed and shares the limit well before the first step.
const FIRST_STEP: u64 = 10;
// A step goes over the nodes two seconds apart, and the next one starts
// once the last node has taken it.
const STEP: u64 = 2 * NAMES.len() as u64;

type Seen = Arc<Mutex<Vec<(&'static str, u64, usize, RejectedDatagrams)>>>;

// A rolling update, the way an operator would run it (§3 of the design):
// the cluster starts in `initial`, then takes each of `steps` in turn, node
// by node. Every node notes how many it sees before each switch it makes,
// and once more after the last; the whole run, the three must share one
// limit and never split. A suspect counts toward the size for 6 s, longer
// than a step leaves two nodes apart, so the size alone would miss a node
// deaf to another for a step: none may have refused a single datagram.
fn rolled_out(initial: (GossipAuth, &[[u8; 32]]), steps: &[(GossipAuth, &[[u8; 32]])]) {
    let secrets = |bytes: &[[u8; 32]]| -> Vec<Secret> {
        bytes
            .iter()
            .map(|bytes| Secret::new(bytes.to_vec()).unwrap())
            .collect()
    };
    let initial = (initial.0, secrets(initial.1));
    let steps: Vec<(GossipAuth, Vec<Secret>)> = steps
        .iter()
        .map(|&(mode, bytes)| (mode, secrets(bytes)))
        .collect();
    let end = FIRST_STEP + STEP * steps.len() as u64;
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(end + 20))
        .build();
    let logs: Vec<Log> = (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
    let seen = Seen::default();
    for (i, (&name, log)) in NAMES.iter().zip(&logs).enumerate() {
        let (log, seen, initial, steps) =
            (log.clone(), seen.clone(), initial.clone(), steps.clone());
        sim.host(name, move || {
            let (log, seen, initial, steps) =
                (log.clone(), seen.clone(), initial.clone(), steps.clone());
            async move {
                let guard = spawn(name, "a", false, initial.0, initial.1).await?;
                let switching = guard.clone();
                tokio::spawn(async move {
                    let start = tokio::time::Instant::now();
                    let note = |guard: &Guard| {
                        let second = turmoil::sim_elapsed().unwrap().as_secs();
                        let rejected = guard.stats().rejected_datagrams;
                        seen.lock()
                            .unwrap()
                            .push((name, second, guard.cluster_size(), rejected));
                    };
                    for (step, (mode, secrets)) in steps.into_iter().enumerate() {
                        let at = FIRST_STEP + STEP * step as u64 + 2 * i as u64;
                        tokio::time::sleep_until(start + Duration::from_secs(at)).await;
                        note(&switching);
                        switching.set_gossip_auth(mode, secrets).unwrap();
                    }
                    tokio::time::sleep_until(start + Duration::from_secs(end + 5)).await;
                    note(&switching);
                });
                serve(guard, log).await
            }
        });
    }
    wait(&mut sim, Duration::from_secs(end + 10));
    sim.run().unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), NAMES.len() * (steps.len() + 1), "{seen:?}");
    for &(name, second, size, rejected) in seen.iter() {
        assert_eq!(size, 3, "{name} saw {size} at second {second}: {seen:?}");
        assert_eq!(
            rejected,
            RejectedDatagrams::default(),
            "{name} refused datagrams by second {second}"
        );
    }
    assert_shared(&logs, FIRST_STEP..end + 10);
}

// From 0.5's wire to signed gossip and back again: each pair of
// neighbouring modes hears each other, so the cluster never splits.
#[test]
fn signing_is_switched_on_and_off_without_a_split() {
    rolled_out(
        (GossipAuth::Off, &[]),
        &[
            (GossipAuth::Transitional, &[A]),
            (GossipAuth::Required, &[A]),
            (GossipAuth::Transitional, &[A]),
            (GossipAuth::Off, &[]),
        ],
    );
}

// A to B in three steps: a node signing with either is heard by the nodes
// one step before or after it.
#[test]
fn a_secret_is_rotated_without_a_split() {
    rolled_out(
        (GossipAuth::Required, &[A]),
        &[
            (GossipAuth::Required, &[A, B]),
            (GossipAuth::Required, &[B, A]),
            (GossipAuth::Required, &[B]),
        ],
    );
}
