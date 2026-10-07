// The runtime's wiring in turmoil: real tasks, sockets and timers over a
// simulated network. The logic is the simulator's business; these only
// check that the pieces are connected the right way round.
#![cfg(feature = "turmoil")]

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rateguard::{Guard, PartitionPolicy, Transport};
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
            let me = SocketAddr::new(turmoil::lookup(name), PORT);
            let seed = SocketAddr::new(turmoil::lookup(seed), PORT);
            let socket = UdpSocket::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT)).await?;
            let builder = Guard::builder()
                .bind(format!("0.0.0.0:{PORT}"))
                .advertise(me.to_string())
                .seeds([seed.to_string()])
                .limit(LIMIT)
                .burst(BURST)
                .partition_policy(PartitionPolicy::HoldDown(Duration::from_secs(60)));
            let guard = if rewrite {
                builder.spawn_on(Rewriting(socket))?
            } else {
                builder.spawn_on(socket)?
            };
            let mut ticker = tokio::time::interval(Duration::from_micros(2500));
            loop {
                ticker.tick().await;
                if guard.check("k").is_allowed() {
                    // Simulation time, not the host's: a restarted host
                    // starts its own clock again.
                    let second = turmoil::sim_elapsed().unwrap().as_secs();
                    let mut log = log.lock().unwrap();
                    match log.iter_mut().find(|(s, _)| *s == second) {
                        Some((_, n)) => *n += 1,
                        None => log.push((second, 1)),
                    }
                }
            }
        }
    });
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
