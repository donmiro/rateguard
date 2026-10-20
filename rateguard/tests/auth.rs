// Signed gossip on real UDP sockets: who hears whom in each mode, what is
// refused and counted, and the secret kept out of sight.

use std::time::Duration;

use rateguard::{GossipAuth, Guard, Secret};

fn free_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

const A: [u8; 32] = [0xA1; 32];
const B: [u8; 32] = [0xB2; 32];

fn secret(bytes: &[u8]) -> Vec<Secret> {
    vec![Secret::new(bytes.to_vec()).unwrap()]
}

// A node on `port` joining through `seeds`, in `mode` with `secrets`.
fn node(port: u16, seeds: &[u16], mode: GossipAuth, secrets: Vec<Secret>) -> Guard {
    Guard::builder()
        .bind(format!("127.0.0.1:{port}"))
        .seeds(seeds.iter().map(|seed| format!("127.0.0.1:{seed}")))
        .limit(100)
        .gossip_auth(mode, secrets)
        .spawn()
        .unwrap()
}

// As in tests/runtime.rs: wait for the sizes, and name them if they never
// come.
async fn until_each_sees(guards: &[&Guard], size: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let sizes: Vec<usize> = guards.iter().map(|guard| guard.cluster_size()).collect();
        if sizes.iter().all(|&seen| seen == size) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster sizes {sizes:?} after 10 s, {size} expected at each"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn nodes_with_one_secret_form_a_cluster() {
    let (pa, pb) = (free_port(), free_port());
    let a = node(pa, &[], GossipAuth::Required, secret(&A));
    let b = node(pb, &[pa], GossipAuth::Required, secret(&A));
    until_each_sees(&[&a, &b], 2).await;
    for guard in [&a, &b] {
        let stats = guard.stats();
        assert_eq!(stats.gossip_auth, GossipAuth::Required);
        assert_eq!(stats.rejected_datagrams.bad_tag, 0);
        assert_eq!(stats.rejected_datagrams.unsigned, 0);
    }
}

#[tokio::test]
async fn a_node_with_another_secret_is_kept_out() {
    // Each seeds the other, so that each sends to the other and each
    // refuses what it gets.
    let (pa, pb) = (free_port(), free_port());
    let a = node(pa, &[pb], GossipAuth::Required, secret(&A));
    let b = node(pb, &[pa], GossipAuth::Required, secret(&B));
    tokio::time::sleep(Duration::from_secs(5)).await;
    for guard in [&a, &b] {
        assert_eq!(guard.cluster_size(), 1);
        let rejected = guard.stats().rejected_datagrams;
        assert!(rejected.bad_tag > 0, "{rejected:?}");
    }
}

#[tokio::test]
async fn an_unsigned_node_is_kept_out_of_a_required_one() {
    let (pr, po) = (free_port(), free_port());
    let required = node(pr, &[], GossipAuth::Required, secret(&A));
    let off = node(po, &[pr], GossipAuth::Off, Vec::new());
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(required.cluster_size(), 1);
    assert_eq!(off.cluster_size(), 1);
    let rejected = required.stats().rejected_datagrams;
    assert!(rejected.unsigned > 0, "{rejected:?}");
    assert_eq!(off.stats().gossip_auth, GossipAuth::Off);
}

#[tokio::test]
async fn transitional_hears_both() {
    let (off, transitional, required) = (free_port(), free_port(), free_port());
    let t = node(transitional, &[], GossipAuth::Transitional, secret(&A));
    let o = node(off, &[transitional], GossipAuth::Off, Vec::new());
    let _r = node(required, &[transitional], GossipAuth::Required, secret(&A));
    // The Off node opens signed frames unchecked, so it hears both too.
    until_each_sees(&[&t, &o], 3).await;
    assert_eq!(o.stats().rejected_datagrams, Default::default());
}

#[tokio::test]
async fn set_gossip_auth_refuses_a_mode_without_secrets() {
    let guard = node(free_port(), &[], GossipAuth::Off, Vec::new());
    let result = guard.set_gossip_auth(GossipAuth::Transitional, Vec::new());
    assert!(
        matches!(result, Err(rateguard::Error::GossipAuth(_))),
        "{result:?}"
    );
    assert!(!result.unwrap_err().to_string().is_empty());
    assert_eq!(guard.stats().gossip_auth, GossipAuth::Off);

    guard
        .set_gossip_auth(GossipAuth::Required, secret(&A))
        .unwrap();
    assert_eq!(guard.stats().gossip_auth, GossipAuth::Required);
    let result = guard.set_gossip_auth(GossipAuth::Required, Vec::new());
    assert!(result.is_err());
    assert_eq!(
        guard.stats().gossip_auth,
        GossipAuth::Required,
        "a refused setting leaves the old one"
    );
}

// A secret changed on the fly is used from the next datagram on: two
// nodes kept apart by their secrets join once one takes the other's.
#[tokio::test]
async fn a_secret_changed_on_the_fly_lets_a_node_in() {
    let (pa, pb) = (free_port(), free_port());
    let a = node(pa, &[pb], GossipAuth::Required, secret(&A));
    let b = node(pb, &[pa], GossipAuth::Required, secret(&B));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!((a.cluster_size(), b.cluster_size()), (1, 1));
    assert!(b.check("k").is_allowed(), "checks go on meanwhile");
    b.set_gossip_auth(GossipAuth::Required, secret(&A)).unwrap();
    until_each_sees(&[&a, &b], 2).await;
}

#[tokio::test]
async fn garbage_is_counted_malformed() {
    let port = free_port();
    let guard = node(port, &[], GossipAuth::Off, Vec::new());
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    // The marker of a frame and nothing else; then no message at all.
    socket.send_to(&[0xA6], ("127.0.0.1", port)).unwrap();
    socket.send_to(&[0xff; 40], ("127.0.0.1", port)).unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let stats = guard.stats();
        if stats.rejected_datagrams.malformed == 2 {
            assert!(stats.running);
            assert_eq!(stats.rejected_datagrams.unsigned, 0);
            assert_eq!(stats.rejected_datagrams.bad_tag, 0);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{:?}",
            stats.rejected_datagrams
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(guard.is_running());
}

#[test]
fn builder_debug_shows_no_secret() {
    // Bytes that read as text, so that a leak shows in every form.
    let bytes = b"hunter2-hunter2-hunter2-hunter2-!".to_vec();
    let builder = Guard::builder()
        .bind("127.0.0.1:7946")
        .limit(100)
        .gossip_auth(GossipAuth::Required, secret(&bytes));
    let debug = format!("{builder:?} {builder:#?}");
    assert!(!debug.contains("hunter"), "{debug}");
    assert!(!debug.contains("68756e746572"), "{debug}");
    assert!(!debug.contains("104, 117"), "{debug}");
    assert!(!debug.contains("104,\n"), "{debug}");
}

#[test]
fn a_mode_without_secrets_is_refused_at_spawn() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(async {
        Guard::builder()
            .bind("127.0.0.1:0")
            .limit(10)
            .gossip_auth(GossipAuth::Required, Vec::new())
            .spawn()
            .map(drop)
    });
    assert!(
        matches!(result, Err(rateguard::Error::GossipAuth(_))),
        "{result:?}"
    );
}
