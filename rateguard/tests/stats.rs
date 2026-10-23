// What a node tells about itself: Guard::stats, Guard::share and the
// dry run, on real UDP sockets.

use std::time::Duration;

use rateguard::{Decision, Error, Guard, KeyLimit, Limit, Stats, key_hash};

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
    assert!(!stats.joining && !stats.learning && !stats.without_quorum && !stats.members_full);
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

#[tokio::test]
async fn most_denied_tells_requests_too() {
    let guard = builder(100, 5).spawn().unwrap();
    assert_eq!(checks(&guard, "busy", 30), 5);
    let stats = until(&guard, |s| !s.most_denied.is_empty()).await;
    let busy = &stats.most_denied[0];
    assert!(busy.denied_per_sec > 0.0);
    // 30 asked, 25 denied: the denials are the larger part of the requests.
    assert!(busy.requests_per_sec > busy.denied_per_sec, "{busy:?}");
    assert!(
        busy.requests_per_sec < busy.denied_per_sec * 2.0,
        "{busy:?}"
    );
    assert_eq!(busy.shadow_denied_per_sec, 0.0);
}

#[tokio::test]
async fn most_denied_takes_k() {
    let guard = builder(100, 1).most_denied(20).spawn().unwrap();
    for i in 0..30 {
        // The more a key asks, the more it is denied.
        let _ = checks(&guard, &format!("key{i}"), 5 + i);
    }
    let stats = until(&guard, |s| s.most_denied.len() == 20).await;
    let rates: Vec<f64> = stats.most_denied.iter().map(|d| d.denied_per_sec).collect();
    assert!(rates.windows(2).all(|w| w[0] >= w[1]), "{rates:?}");
    assert_eq!(guard.stats().most_denied.len(), 20);

    let none = builder(100, 1).most_denied(0).spawn().unwrap();
    let _ = checks(&none, "k", 10);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(none.stats().most_denied.is_empty());
    assert!(builder(100, 1).most_denied(1024).spawn().is_ok());
    assert!(matches!(
        builder(100, 1).most_denied(1025).spawn(),
        Err(Error::TooManyMostDenied(1025))
    ));
}

#[tokio::test]
async fn most_shadow_denied_lists_the_shadowed_keys() {
    let guard = builder(1000, 100).spawn().unwrap();
    guard
        .set_shadow("", Some(Limit::per_second(1).burst(1)))
        .unwrap();
    assert_eq!(checks(&guard, "shadowed", 20), 20);
    let stats = until(&guard, |s| !s.most_shadow_denied.is_empty()).await;
    assert_eq!(stats.most_shadow_denied.len(), 1);
    let key = &stats.most_shadow_denied[0];
    assert_eq!(key.key_hash, guard.key_hash_in("", "shadowed"));
    assert!(key.shadow_denied_per_sec > 0.0);
    assert!(key.requests_per_sec > 0.0);
    assert!(stats.most_denied.is_empty(), "the limit denied nothing");
}

#[tokio::test]
async fn key_stats_answers_for_a_tracked_key_only() {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .default_limit(Limit::per_minute(600))
        .spawn()
        .unwrap();
    assert!(guard.key_stats("", "never").await.is_none());
    for _ in 0..50 {
        let _ = guard.check("seen");
    }
    let mut stats = None;
    for _ in 0..500 {
        stats = guard.key_stats("", "seen").await;
        // Tracked from the tick that takes its attempts, rated from the
        // next, which has a time to measure them over.
        if stats.as_ref().is_some_and(|s| s.denied_per_sec > 0.0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let stats = stats.expect("tracked after a tick");
    assert_eq!(stats.limit, Limit::per_minute(600));
    assert_eq!(stats.shadow, None);
    assert!(stats.requests_per_sec > 0.0);
    assert!(stats.denied_per_sec > 0.0, "burst 30, then denials");
    assert_eq!(stats.share, guard.share("seen").unwrap());
    assert!(guard.key_stats("", "never").await.is_none());

    // An override's limit, and a class's shadow, as given.
    guard
        .set_key_limit("", "seen", KeyLimit::Limit(Limit::per_hour(100)))
        .unwrap();
    assert_eq!(
        guard.key_stats("", "seen").await.unwrap().limit,
        Limit::per_hour(100)
    );
    guard.remove_key_limit("", "seen");
    guard.set_shadow("", Some(Limit::per_second(3))).unwrap();
    let stats = guard.key_stats("", "seen").await.unwrap();
    assert_eq!(stats.shadow, Some(Limit::per_second(3)));
}

#[tokio::test]
async fn key_stats_gives_up_with_the_task() {
    let guard = builder(100, 5).spawn().unwrap();
    let _ = guard.check("k");
    guard.leave().await;
    let answered = tokio::time::timeout(Duration::from_secs(5), guard.key_stats("", "k")).await;
    assert_eq!(answered, Ok(None), "no task left to ask");
}

#[tokio::test]
async fn an_alive_of_its_own_it_did_not_make_is_an_identity_conflict() {
    // A peer, played by hand: the node's seed, so the node writes first and
    // says who it is.
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let at = peer.local_addr().unwrap();
    let guard = builder(100, 5).seeds([at.to_string()]).spawn().unwrap();
    assert_eq!(guard.stats().identity_conflicts, 0);
    let mut buf = [0u8; 1500];
    let (len, from) = tokio::time::timeout(Duration::from_secs(10), peer.recv_from(&mut buf))
        .await
        .expect("the node knocks on its seed")
        .unwrap();
    let knock = rateguard_proto::decode(&buf[..len]).unwrap();
    let own = knock.updates()[0];

    // The peer holds the node alive above its own incarnation, as when a
    // second instance runs under its address.
    let ahead = rateguard_proto::Update {
        incarnation: own.incarnation + 5,
        ..own
    };
    let reply = rateguard_proto::Message::Ping {
        seq: 1,
        updates: vec![
            rateguard_proto::Update {
                member: id_of(at.into()),
                addr: at.into(),
                incarnation: 0,
                status: rateguard_proto::Status::Alive,
            },
            ahead,
        ],
        demand: Vec::new(),
    };
    let bytes = rateguard_proto::encode(&reply).unwrap();
    peer.send_to(&bytes, from).await.unwrap();
    let stats = until(&guard, |s| s.identity_conflicts > 0).await;
    assert_eq!(stats.identity_conflicts, 1);
}

// A peer's ID as every node derives it from its address: anything else is
// refused as malformed.
fn id_of(addr: rateguard_proto::Address) -> u64 {
    xxhash_rust::xxh3::xxh3_64(&postcard::to_allocvec(&addr).unwrap())
}

#[tokio::test]
async fn a_stranger_over_the_member_cap_is_refused_and_counted() {
    let port = free_port();
    let guard = Guard::builder()
        .bind(format!("127.0.0.1:{port}"))
        .limit(100)
        .max_members(1)
        .spawn()
        .unwrap();
    assert_eq!(guard.stats().members_refused, 0);
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let me = rateguard_proto::Address::from(socket.local_addr().unwrap());
    let ping = rateguard_proto::Message::Ping {
        seq: 1,
        updates: vec![rateguard_proto::Update {
            member: id_of(me),
            addr: me,
            incarnation: 0,
            status: rateguard_proto::Status::Alive,
        }],
        demand: Vec::new(),
    };
    let bytes = rateguard_proto::encode(&ping).unwrap();
    socket.send_to(&bytes, ("127.0.0.1", port)).unwrap();
    let stats = until(&guard, |s| s.members_refused > 0).await;
    assert_eq!(stats.members_refused, 1);
    assert_eq!(stats.cluster_size, 1);
    // A newcomer this instance cannot count: it shares as if its table
    // were full of live members, and holds every key at that floor.
    assert!(stats.members_full);
    assert_eq!(
        stats.rejected_datagrams.malformed, 0,
        "refused, not malformed"
    );
}
