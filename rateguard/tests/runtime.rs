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
