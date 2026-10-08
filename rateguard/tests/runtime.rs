// The runtime on a real UDP socket: one node, two nodes, and the request
// path cut off from a network that never answers.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use rateguard::{Decision, Error, Guard, Transport};

fn free_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn alone(limit: u32, burst: u32) -> Guard {
    Guard::builder()
        .bind(format!("127.0.0.1:{}", free_port()))
        .limit(limit)
        .burst(burst)
        .spawn()
        .unwrap()
}

#[tokio::test]
async fn a_lone_node_admits_one_burst_at_once() {
    let guard = alone(100, 5);
    let allowed = (0..1000).filter(|_| guard.check("k").is_allowed()).count();
    assert_eq!(allowed, 5);
    let Decision::Deny { retry_after } = guard.check("k") else {
        panic!("over the burst");
    };
    assert!(retry_after > Duration::ZERO);
}

#[tokio::test]
async fn keys_are_limited_apart_the_empty_one_and_a_long_one_included() {
    let guard = alone(100, 2);
    let long = "k".repeat(10_000);
    for key in ["", "a", long.as_str()] {
        let allowed = (0..10).filter(|_| guard.check(key).is_allowed()).count();
        assert_eq!(allowed, 2, "{key:.8}");
    }
}

// A guard in a log line must not dump its key table: 8192 slots by
// default, two million at most.
#[tokio::test]
async fn a_guard_debugs_in_a_line() {
    let guard = alone(100, 5);
    let debug = format!("{guard:?}");
    assert!(debug.len() < 200, "{} bytes: {debug:.200}", debug.len());
}

#[test]
fn check_works_from_a_thread_outside_the_runtime() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let guard = runtime.block_on(async { alone(100, 3) });
    let allowed =
        std::thread::spawn(move || (0..10).filter(|_| guard.check("k").is_allowed()).count())
            .join()
            .unwrap();
    assert_eq!(allowed, 3);
}

#[test]
fn spawning_outside_a_runtime_is_an_error_not_a_panic() {
    let result = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(10)
        .spawn()
        .map(drop);
    assert!(matches!(result, Err(Error::NoRuntime)), "{result:?}");
}

#[tokio::test]
async fn dropping_the_last_guard_stops_the_node_and_frees_the_port() {
    let port = free_port();
    let guard = Guard::builder()
        .bind(format!("127.0.0.1:{port}"))
        .limit(10)
        .spawn()
        .unwrap();
    let clone = guard.clone();
    drop(guard);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        std::net::UdpSocket::bind(("127.0.0.1", port)).is_err(),
        "a clone keeps it alive"
    );
    drop(clone);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok());
}

// Guarantee 1: the request path does not depend on the network.
struct Hung;
impl Transport for Hung {
    fn send_to(&self, _: &[u8], _: SocketAddr) -> impl Future<Output = io::Result<usize>> + Send {
        std::future::pending()
    }
    fn recv_from(
        &self,
        _: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send {
        std::future::pending()
    }
}

#[tokio::test]
async fn a_hung_network_does_not_touch_the_request_path() {
    let guard = Guard::builder()
        .bind("10.0.0.1:7946")
        .seeds(["10.0.0.2:7946"])
        .limit(100)
        .burst(4)
        .spawn_on(Hung)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let allowed = (0..100).filter(|_| guard.check("k").is_allowed()).count();
    assert_eq!(allowed, 4);
}

// The background task dying, whatever kills it: here the transport
// panics; a bug in the core would do the same.
// A deadline, not a sleep: the node drops a pending receive every tick.
struct Panicking(tokio::time::Instant);
impl Transport for Panicking {
    fn send_to(&self, _: &[u8], _: SocketAddr) -> impl Future<Output = io::Result<usize>> + Send {
        std::future::pending()
    }
    async fn recv_from(&self, _: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        tokio::time::sleep_until(self.0).await;
        panic!("the background task dies");
    }
}

// Its peers take a dead node's share for themselves; were it to go on at
// its last quotas, the cluster would admit that share twice. So it drops
// to the floor, R × β / N, and says it no longer runs.
#[tokio::test]
async fn a_node_whose_background_task_died_falls_back_to_the_floor() {
    let guard = Guard::builder()
        .bind("10.0.0.1:7946")
        .limit(1000)
        .burst(1)
        .spawn_on(Panicking(
            tokio::time::Instant::now() + Duration::from_millis(300),
        ))
        .unwrap();
    assert!(guard.is_running());
    let retry_after = |guard: &Guard| {
        let _ = guard.check("k");
        match guard.check("k") {
            Decision::Deny { retry_after } => retry_after,
            Decision::Allow => panic!("a burst of one"),
        }
    };
    assert!(
        retry_after(&guard) <= Duration::from_millis(3),
        "a new key, alone: the cold α × R, one every 2 ms"
    );

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(!guard.is_running());
    tokio::time::sleep(Duration::from_millis(30)).await;
    // 1000 × 0.05 / 1 = 50 a second: one every 20 ms.
    assert!(retry_after(&guard) > Duration::from_millis(15));
}

#[tokio::test]
async fn two_nodes_on_loopback_find_each_other() {
    let (pa, pb) = (free_port(), free_port());
    let a = Guard::builder()
        .bind(format!("127.0.0.1:{pa}"))
        .limit(100)
        .spawn()
        .unwrap();
    let b = Guard::builder()
        .bind(format!("127.0.0.1:{pb}"))
        .seeds([format!("127.0.0.1:{pa}")])
        .limit(100)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!((a.cluster_size(), b.cluster_size()), (2, 2));
}

// Bound to port 0, a node advertises the port the OS picked, not 0:
// otherwise its peers would answer into the void.
#[tokio::test]
async fn a_node_bound_to_port_zero_advertises_the_port_it_got() {
    let seed_port = free_port();
    let seed = Guard::builder()
        .bind(format!("127.0.0.1:{seed_port}"))
        .limit(100)
        .spawn()
        .unwrap();
    let joiner = Guard::builder()
        .bind("127.0.0.1:0")
        .seeds([format!("127.0.0.1:{seed_port}")])
        .limit(100)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!((seed.cluster_size(), joiner.cluster_size()), (2, 2));
}

#[test]
fn a_runtime_without_timers_is_an_error_not_a_silently_dead_node() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        Guard::builder()
            .bind("127.0.0.1:0")
            .limit(10)
            .spawn()
            .map(drop)
    });
    assert!(matches!(result, Err(Error::NoTimer)), "{result:?}");
}

#[test]
fn a_period_too_short_to_tick_is_an_error() {
    let result = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(10)
        .protocol_period(Duration::from_nanos(3))
        .spawn()
        .map(drop);
    assert!(matches!(result, Err(Error::ZeroPeriod)), "{result:?}");
}

#[test]
fn a_runtime_without_io_is_an_error_not_a_panic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        Guard::builder()
            .bind("127.0.0.1:0")
            .limit(10)
            .spawn()
            .map(drop)
    });
    assert!(matches!(result, Err(Error::NoIo)), "{result:?}");
}

// A transport whose sends never complete, and which says when it is gone.
struct Stuck(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Transport for Stuck {
    fn send_to(&self, _: &[u8], _: SocketAddr) -> impl Future<Output = io::Result<usize>> + Send {
        std::future::pending()
    }
    fn recv_from(
        &self,
        _: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send {
        std::future::pending()
    }
}
impl Drop for Stuck {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_node_stuck_sending_still_stops_with_its_last_guard() {
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = Guard::builder()
        .bind("10.0.0.1:7946")
        .seeds(["10.0.0.2:7946"])
        .limit(100)
        .spawn_on(Stuck(dropped.clone()))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(guard);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        dropped.load(std::sync::atomic::Ordering::SeqCst),
        "the node and its transport are gone"
    );
}

// Spec §6: many threads, many keys, and no key admitted more than its rate
// over the run plus one burst, however the threads interleave.
#[test]
fn no_key_gets_more_than_its_rate_and_a_burst_under_contention() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (limit, burst, keys) = (100, 5, 32);
    let guard = runtime.block_on(async {
        Guard::builder()
            .bind("127.0.0.1:0")
            .limit(limit)
            .burst(burst)
            .spawn()
            .unwrap()
    });
    let admitted: Vec<AtomicU64> = (0..keys).map(|_| AtomicU64::new(0)).collect();
    let names: Vec<String> = (0..keys).map(|k| format!("key-{k}")).collect();

    let start = std::time::Instant::now();
    let run = Duration::from_millis(1500);
    std::thread::scope(|s| {
        for t in 0..8 {
            let (guard, admitted, names) = (&guard, &admitted, &names);
            s.spawn(move || {
                let mut k = t;
                while start.elapsed() < run {
                    k = (k + 1) % keys;
                    if guard.check(&names[k]).is_allowed() {
                        admitted[k].fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let elapsed = start.elapsed().as_secs_f64();

    let ceiling = (limit as f64 * elapsed).ceil() as u64 + burst as u64;
    for (k, count) in admitted.iter().enumerate() {
        let count = count.load(Ordering::Relaxed);
        assert!(
            count <= ceiling,
            "key {k}: {count} admitted, at most {ceiling}"
        );
        assert!(count > 0, "key {k} admitted nothing");
    }
}
