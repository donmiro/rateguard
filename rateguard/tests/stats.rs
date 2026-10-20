// What a node tells about itself: Guard::stats, Guard::share and the
// dry run, on real UDP sockets.

use std::time::Duration;

use rateguard::{Decision, Guard, Stats, key_hash};

fn free_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn builder(limit: u32, burst: u32) -> rateguard::Builder {
    Guard::builder()
        .bind("127.0.0.1:0")
        .limit(limit)
        .burst(burst)
}

// Stats are as of the background task's last tick: wait for them to show
// what the test is after, and name what they showed if they never do.
async fn until(guard: &Guard, what: impl Fn(&Stats) -> bool) -> Stats {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let stats = guard.stats();
        if what(&stats) {
            return stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never came, last seen: {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn checks(guard: &Guard, key: &str, n: usize) -> usize {
    (0..n).filter(|_| guard.check(key).is_allowed()).count()
}

#[tokio::test]
async fn stats_count_what_was_admitted_and_what_denied() {
    let guard = builder(100, 5).spawn().unwrap();
    assert_eq!(checks(&guard, "k", 8), 5);
    let stats = until(&guard, |s| s.admitted + s.denied == 8).await;
    assert_eq!((stats.admitted, stats.denied), (5, 3));
    assert!(!stats.dry_run);
}

#[tokio::test]
async fn a_fresh_node_has_counted_nothing() {
    let guard = builder(100, 5).spawn().unwrap();
    let stats = guard.stats();
    assert_eq!((stats.admitted, stats.denied, stats.overflowed), (0, 0, 0));
    assert!(stats.running);
}

#[tokio::test]
async fn checks_crowded_into_the_overflow_slot_are_counted_apart() {
    // One bucket of slots for the whole table: most of 200 keys overflow.
    let guard = builder(100, 5).hot_keys(1).tracked_keys(1).spawn().unwrap();
    for i in 0..200 {
        let _ = guard.check(&format!("k{i}"));
    }
    let stats = until(&guard, |s| s.admitted + s.denied == 200).await;
    assert!(stats.overflowed >= 150, "{stats:?}");
}

#[tokio::test]
async fn a_lone_node_tells_its_cluster_and_keys() {
    let guard = builder(100, 5).spawn().unwrap();
    // One check, before the first tick: its attempt reaches the node in the
    // tick of the first round, and the key must not be dropped with it.
    let _ = guard.check("k");
    let stats = until(&guard, |s| s.tracked_keys == 1).await;
    assert_eq!((stats.cluster_size, stats.counted_size), (1, 1));
    assert_eq!((stats.suspects, stats.hot_keys), (0, 0));
    assert!(!stats.joining && !stats.learning && !stats.without_quorum);
    let since = stats.since_last_round.expect("rounds run");
    assert!(since < Duration::from_secs(1), "{since:?}");
}

#[tokio::test]
async fn a_node_with_a_seed_nobody_answers_says_it_waits_to_join() {
    let guard = builder(100, 5)
        .seeds([format!("127.0.0.1:{}", free_port())])
        .spawn()
        .unwrap();
    until(&guard, |s| s.joining).await;
}

#[tokio::test]
async fn two_nodes_count_each_other() {
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
    // The floor lifts at the round after the news, not with it.
    for guard in [&a, &b] {
        let free = |s: &Stats| s.cluster_size == 2 && !s.joining && !s.learning;
        let stats = until(guard, free).await;
        assert_eq!((stats.counted_size, stats.suspects), (2, 0));
    }
}

#[tokio::test]
async fn a_node_that_left_says_it_no_longer_runs() {
    let guard = builder(100, 5).spawn().unwrap();
    guard.leave().await;
    assert!(!guard.stats().running);
}

#[tokio::test]
async fn share_tells_the_rate_a_key_gets_here() {
    let guard = builder(100, 5).spawn().unwrap();
    assert_eq!(guard.share("k"), None, "no state yet");
    let _ = guard.check("k");
    // Alone, a cold key gets α × R = 50 a second.
    assert_eq!(guard.share("k"), Some(50.0));
    assert_eq!(guard.share("other"), None, "looking does not take a slot");
    assert_eq!(guard.share("other"), None);
}

#[tokio::test]
async fn a_dry_run_admits_what_it_would_deny_and_counts_it() {
    let guard = builder(100, 5).dry_run(true).spawn().unwrap();
    assert_eq!(checks(&guard, "k", 8), 8);
    let stats = until(&guard, |s| s.admitted + s.denied == 8).await;
    assert_eq!((stats.admitted, stats.denied), (5, 3));
    assert!(stats.dry_run);
}

#[tokio::test]
async fn a_dry_run_can_be_ended_on_the_fly_by_any_clone() {
    let guard = builder(100, 5).dry_run(true).spawn().unwrap();
    let clone = guard.clone();
    assert_eq!(checks(&guard, "k", 5), 5, "the burst");
    assert!(guard.check("k").is_allowed(), "would be denied");
    clone.set_dry_run(false);
    assert!(matches!(guard.check("k"), Decision::Deny { .. }));
    clone.set_dry_run(true);
    assert!(guard.check("k").is_allowed());
}

// A node that has left is no longer counted by its peers: a key it has
// never seen must start at the floor too, not at a share nobody counts.
#[tokio::test]
async fn after_leaving_a_new_key_starts_at_the_floor() {
    let guard = builder(1000, 5).spawn().unwrap();
    guard.leave().await;
    let _ = guard.check("new");
    // R × β / 1: 1000 × 0.05.
    assert_eq!(guard.share("new"), Some(50.0));
}

// A salted node's hashes are its own: the free key_hash cannot name its
// keys, Guard::key_hash_in can.
#[tokio::test]
async fn most_denied_matches_guard_key_hash_in_with_a_salt() {
    let guard = builder(100, 5).key_salt([7; 16]).spawn().unwrap();
    assert_eq!(checks(&guard, "busy", 30), 5);
    let stats = until(&guard, |s| !s.most_denied.is_empty()).await;
    assert_eq!(stats.most_denied[0].key_hash, guard.key_hash_in("", "busy"));
    assert_ne!(stats.most_denied[0].key_hash, key_hash("busy"));
}

// Without a salt the node's hashes are the free functions'.
#[tokio::test]
async fn an_unsalted_guard_hashes_as_key_hash_in() {
    let guard = builder(100, 5).spawn().unwrap();
    assert_eq!(guard.key_hash_in("", "busy"), key_hash("busy"));
    assert_eq!(
        guard.key_hash_in("pro", "busy"),
        rateguard::key_hash_in("pro", "busy")
    );
}
