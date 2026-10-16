// Classes of limits in one Guard: check_in, classes changed on the fly,
// and the keys denied most. On real UDP sockets, one node alone: a key
// gets its class's burst at once, and its cold share, α × R, afterwards.
// The rates are low, a few dozen a second, so that what a key earns back
// while a test checks it, slowly in a debug build on a busy machine, is
// next to nothing next to its burst.

use std::time::Duration;

use rateguard::{Error, Guard, Limit, Stats, key_hash, key_hash_in};

// The default class at 40 a second with a burst of 50, "pro" at 10 with a
// burst of 5.
fn guard() -> Guard {
    Guard::builder()
        .bind("127.0.0.1:0")
        .limit(40)
        .burst(50)
        .class("pro", Limit::per_second(10).burst(5))
        .spawn()
        .unwrap()
}

fn admitted_in(guard: &Guard, class: &str, key: &str, n: usize) -> usize {
    (0..n)
        .filter(|_| guard.check_in(class, key).is_allowed())
        .count()
}

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

// Waits until `what` holds of the guard, checking every 20 ms.
async fn eventually(what: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !what() {
        assert!(tokio::time::Instant::now() < deadline, "never came");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn the_default_class_hashes_keys_as_before() {
    assert_eq!(key_hash_in("", "tenant:42"), key_hash("tenant:42"));
    assert_ne!(key_hash_in("pro", "tenant:42"), key_hash("tenant:42"));
    assert_ne!(
        key_hash_in("pro", "tenant:42"),
        key_hash_in("free", "tenant:42")
    );
}

#[test]
fn a_limit_takes_a_twentieth_as_its_burst_unless_told() {
    assert_eq!(Limit::per_second(1000).burst_size(), 50);
    assert_eq!(Limit::per_second(10).burst_size(), 1);
    assert_eq!(Limit::per_second(10).burst(7).burst_size(), 7);
    assert_eq!(Limit::per_second(10).rate(), 10);
    assert_eq!(
        Limit::per_second(100),
        Limit::per_second(100).burst(5),
        "the default burst is a burst like any"
    );
}

#[tokio::test]
async fn classes_tell_the_limits_they_were_given() {
    let guard = guard();
    guard.set_class("team", Limit::per_second(30)).unwrap();
    let mut classes = guard.classes();
    classes.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        classes,
        [
            ("pro".to_owned(), Limit::per_second(10).burst(5)),
            ("team".to_owned(), Limit::per_second(30)),
        ]
    );
}

#[tokio::test]
async fn a_key_in_a_class_gets_the_class_s_burst() {
    let guard = guard();
    assert_eq!(admitted_in(&guard, "pro", "k", 20), 5);
    assert_eq!(admitted_in(&guard, "", "k", 100), 50, "the default class");
}

#[tokio::test]
async fn one_key_in_two_classes_is_two_keys() {
    let guard = guard();
    assert_eq!(admitted_in(&guard, "pro", "k", 20), 5);
    assert_eq!(admitted_in(&guard, "", "k", 20), 20, "untouched by pro");
}

#[tokio::test]
async fn a_key_settles_at_the_cold_share_of_its_class() {
    let guard = guard();
    let _ = guard.check_in("pro", "k");
    // Alone, a cold key gets α × R: 5 of pro's 10.
    eventually(|| guard.share_in("pro", "k") == Some(5.0)).await;
    assert_eq!(guard.share("k"), None, "no such key in the default class");
}

#[tokio::test]
async fn an_unknown_class_takes_the_default_limit_and_is_counted() {
    let guard = guard();
    assert_eq!(
        admitted_in(&guard, "porr", "k", 100),
        50,
        "the default burst"
    );
    let stats = until(&guard, |s| s.unknown_class > 0).await;
    assert_eq!(stats.unknown_class, 100);
}

#[tokio::test]
async fn a_class_set_on_the_fly_applies_to_new_keys_at_once() {
    let guard = guard();
    let clone = guard.clone();
    clone
        .set_class("enterprise", Limit::per_second(20).burst(30))
        .unwrap();
    assert_eq!(admitted_in(&guard, "enterprise", "k", 100), 30);
    clone
        .set_class("pro", Limit::per_second(10).burst(9))
        .unwrap();
    assert_eq!(admitted_in(&guard, "pro", "new", 20), 9);
}

#[tokio::test]
async fn a_class_changed_on_the_fly_reaches_known_keys_at_the_next_tick() {
    let guard = guard();
    let _ = guard.check_in("pro", "k");
    eventually(|| guard.share_in("pro", "k") == Some(5.0)).await;
    guard.set_class("pro", Limit::per_second(40)).unwrap();
    eventually(|| guard.share_in("pro", "k") == Some(20.0)).await;
}

#[tokio::test]
async fn a_removed_class_falls_back_to_the_default() {
    let guard = guard();
    guard.remove_class("pro");
    assert_eq!(admitted_in(&guard, "pro", "k", 100), 50);
    assert!(guard.classes().is_empty());
}

#[tokio::test]
async fn the_default_limit_changes_on_the_fly() {
    let guard = guard();
    guard.set_limit(Limit::per_second(40).burst(8)).unwrap();
    assert_eq!(admitted_in(&guard, "", "k", 100), 8);
}

#[tokio::test]
async fn set_classes_replaces_every_named_class() {
    let guard = guard();
    guard
        .set_classes([
            ("free", Limit::per_second(10)),
            ("team", Limit::per_second(500)),
        ])
        .unwrap();
    let mut names: Vec<String> = guard.classes().into_iter().map(|(name, _)| name).collect();
    names.sort();
    assert_eq!(names, ["free", "team"], "pro is gone");
}

#[tokio::test]
async fn classes_that_cannot_be_are_refused() {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(100)
        .max_classes(2)
        .spawn()
        .unwrap();
    assert!(matches!(
        guard.set_class("", Limit::per_second(5)),
        Err(Error::EmptyClassName)
    ));
    assert!(matches!(
        guard.set_class("x", Limit::per_second(0)),
        Err(Error::ZeroLimit)
    ));
    assert!(matches!(
        guard.set_class("x", Limit::per_second(5).burst(1 << 24)),
        Err(Error::TooLargeBurst(_))
    ));
    guard.set_class("a", Limit::per_second(5)).unwrap();
    guard.set_class("b", Limit::per_second(5)).unwrap();
    assert!(matches!(
        guard.set_class("c", Limit::per_second(5)),
        Err(Error::TooManyClasses(2))
    ));
    guard.set_class("a", Limit::per_second(6)).unwrap();
    assert!(
        guard
            .set_classes([
                ("a", Limit::per_second(5)),
                ("b", Limit::per_second(5)),
                ("c", Limit::per_second(5)),
            ])
            .is_err()
    );
    assert_eq!(
        guard.classes().len(),
        2,
        "a refused set_classes changes nothing"
    );
}

#[tokio::test]
async fn a_builder_refuses_classes_that_cannot_be() {
    let builder = || Guard::builder().bind("127.0.0.1:0").limit(100);
    assert!(matches!(
        builder().class("", Limit::per_second(5)).spawn(),
        Err(Error::EmptyClassName)
    ));
    assert!(matches!(
        builder()
            .max_classes(1)
            .class("a", Limit::per_second(5))
            .class("b", Limit::per_second(5))
            .spawn(),
        Err(Error::TooManyClasses(1))
    ));
    assert!(matches!(
        builder().max_classes(5000).spawn(),
        Err(Error::TooManyClasses(5000))
    ));
}

#[tokio::test]
async fn the_keys_denied_most_are_listed_by_hash() {
    let guard = guard();
    for _ in 0..30 {
        let _ = guard.check_in("pro", "busy");
        let _ = guard.check_in("pro", "busier");
        let _ = guard.check_in("pro", "busier");
    }
    let stats = until(&guard, |s| s.most_denied.len() == 2).await;
    let hashes: Vec<u64> = stats.most_denied.iter().map(|d| d.key_hash).collect();
    assert_eq!(
        hashes,
        [key_hash_in("pro", "busier"), key_hash_in("pro", "busy")]
    );
    assert!(stats.most_denied[0].denied_per_sec > stats.most_denied[1].denied_per_sec);
}
