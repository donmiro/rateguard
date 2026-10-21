// Classes of limits in one Guard: check_in, classes changed on the fly,
// and the keys denied most. On real UDP sockets, one node alone: a key
// gets its class's burst at once, and its cold share, α × R, afterwards.
// The rates are low, a few dozen a second, so that what a key earns back
// while a test checks it, slowly in a debug build on a busy machine, is
// next to nothing next to its burst.

use std::time::Duration;

use rateguard::{Error, Guard, KeyLimit, Limit, Stats, key_hash, key_hash_in};

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
    assert_eq!(Limit::per_second(10).rate_per_sec(), 10.0);
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

#[test]
fn a_limit_reads_back_as_given() {
    assert_ne!(Limit::per_minute(60), Limit::per_second(1));
    assert_eq!(Limit::per_minute(60).rate_per_sec(), 1.0);
    assert_eq!(Limit::per_minute(90).count(), 90);
}

#[tokio::test]
async fn a_class_reads_back_as_given() {
    let guard = guard();
    guard.set_class("m", Limit::per_minute(90)).unwrap();
    assert!(
        guard
            .classes()
            .contains(&("m".to_owned(), Limit::per_minute(90)))
    );
}

#[test]
fn default_bursts_are_a_twentieth_of_the_count() {
    assert_eq!(Limit::per_minute(10).burst_size(), 1);
    assert_eq!(Limit::per_minute(1000).burst_size(), 50);
    assert_eq!(Limit::per_hour(100).period(), Duration::from_secs(3600));
    assert_eq!(Limit::per_minute(60).period(), Duration::from_secs(60));
}

#[test]
#[allow(deprecated)]
fn the_old_rate_rounds_up() {
    assert_eq!(Limit::per_minute(90).rate(), 2);
    assert_eq!(Limit::per_second(10).rate(), 10);
}

// A burst set on the builder applies on top of the limit, in either order.
#[tokio::test]
async fn the_builders_burst_applies_on_top_of_a_default_limit_in_any_order() {
    for guard in [
        Guard::builder()
            .bind("127.0.0.1:0")
            .default_limit(Limit::per_minute(600))
            .burst(7),
        Guard::builder()
            .bind("127.0.0.1:0")
            .burst(7)
            .default_limit(Limit::per_minute(600)),
    ] {
        let guard = guard.spawn().unwrap();
        assert_eq!(admitted_in(&guard, "", "k", 100), 7);
    }
}

// 600 a minute is 10 a second, a burst of 30. A lone node is the whole
// fleet and gets all of R, so it admits the burst, then 10 a second.
#[tokio::test(start_paused = true)]
async fn per_minute_holds_its_rate_on_one_node() {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .default_limit(Limit::per_minute(600))
        .spawn()
        .unwrap();
    let mut admitted = admitted_in(&guard, "", "k", 100);
    assert_eq!(admitted, 30, "the burst of a twentieth of 600");
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        admitted += admitted_in(&guard, "", "k", 1);
    }
    assert!(
        (30 + 100 - 3..=30 + 100 + 3).contains(&admitted),
        "{admitted} admitted: 30 and 10 a second for 10 s"
    );
}

// A guard whose default class is 10 a second with a burst of 5, for the
// key limits' tests.
fn small_guard() -> Guard {
    Guard::builder()
        .bind("127.0.0.1:0")
        .limit(10)
        .burst(5)
        .class("pro", Limit::per_second(10).burst(5))
        .spawn()
        .unwrap()
}

#[tokio::test]
async fn a_key_limit_raises_one_key_only() {
    let guard = small_guard();
    let _ = guard.check_in("", "vip");
    let _ = guard.check_in("", "other");
    // Alone, a cold key gets α × R: 5 of the default's 10.
    eventually(|| guard.share("vip") == Some(5.0) && guard.share("other") == Some(5.0)).await;
    guard
        .set_key_limit("", "vip", Limit::per_second(1000).burst(200))
        .unwrap();
    eventually(|| guard.share("vip") == Some(500.0)).await;
    assert_eq!(guard.share("other"), Some(5.0), "the others are untouched");
    assert!(admitted_in(&guard, "", "vip", 150) > 100);
    assert!(admitted_in(&guard, "", "other", 100) <= 10);
}

// On a paused clock and at a slow rate: no time the test does not ask for
// passes between the burst and the checks, however slow the machine.
#[tokio::test(start_paused = true)]
async fn a_key_limit_keeps_the_key_s_debt() {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .default_limit(Limit::per_minute(60).burst(5))
        .spawn()
        .unwrap();
    assert_eq!(admitted_in(&guard, "", "k", 20), 5);
    let before = guard.share("k").unwrap();
    guard
        .set_key_limit("", "k", Limit::per_minute(120).burst(5))
        .unwrap();
    assert!(
        !guard.check("k").is_allowed(),
        "the debt is carried, no fresh burst"
    );
    // Past a few ticks, the key holds its new limit and still its debt, in
    // requests: at a share of a request a second or less, then two, 300 ms
    // repay under the slot it is short of. The same burst: a bigger one
    // would admit the difference at once, rightly.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(guard.share("k").unwrap() > before, "the new limit applies");
    assert!(
        !guard.check("k").is_allowed(),
        "the debt is carried, no fresh burst"
    );
}

#[tokio::test]
async fn a_key_given_a_class_follows_its_changes() {
    let guard = small_guard();
    guard
        .set_key_limit("free", "t7", KeyLimit::class("pro"))
        .unwrap();
    let _ = guard.check_in("free", "t7");
    eventually(|| guard.share_in("free", "t7") == Some(5.0)).await;
    guard.set_class("pro", Limit::per_second(40)).unwrap();
    eventually(|| guard.share_in("free", "t7") == Some(20.0)).await;
}

#[tokio::test]
async fn a_key_given_a_missing_class_takes_the_default_and_is_counted() {
    let guard = small_guard();
    guard
        .set_key_limit("", "k", KeyLimit::class("nope"))
        .unwrap();
    assert_eq!(admitted_in(&guard, "", "k", 100), 5, "the default burst");
    let stats = until(&guard, |s| s.unknown_class > 0).await;
    assert_eq!(stats.unknown_class, 100);
}

#[tokio::test]
async fn set_key_limits_is_all_or_nothing() {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(100)
        .max_key_limits(2)
        .spawn()
        .unwrap();
    guard.set_key_limit("", "a", Limit::per_second(5)).unwrap();
    let before = guard.key_limits();
    let result = guard.set_key_limits([
        ("", "x", Limit::per_second(5)),
        ("", "y", Limit::per_second(5)),
        ("", "z", Limit::per_second(5)),
    ]);
    assert!(matches!(result, Err(Error::TooManyKeyLimits(2))));
    assert_eq!(guard.key_limits(), before);
    assert!(matches!(
        guard.set_key_limits([("", "x", Limit::per_second(0))]),
        Err(Error::ZeroLimit)
    ));
    assert_eq!(guard.key_limits(), before);
    guard
        .set_key_limits([("", "x", Limit::per_second(5))])
        .unwrap();
    assert_eq!(
        guard.key_limits(),
        [(
            "".to_owned(),
            "x".to_owned(),
            KeyLimit::Limit(Limit::per_second(5))
        )],
        "replaces every override"
    );
    guard.set_key_limit("", "b", Limit::per_second(5)).unwrap();
    assert!(matches!(
        guard.set_key_limit("", "c", Limit::per_second(5)),
        Err(Error::TooManyKeyLimits(2))
    ));
    guard.set_key_limit("", "b", Limit::per_second(6)).unwrap();
    assert!(guard.remove_key_limit("", "b"));
    assert!(!guard.remove_key_limit("", "b"));
    assert!(matches!(
        Guard::builder()
            .bind("127.0.0.1:0")
            .limit(100)
            .max_key_limits((1 << 20) + 1)
            .spawn(),
        Err(Error::TooLargeMaxKeyLimits(n)) if n == (1 << 20) + 1
    ));
}

#[tokio::test]
async fn a_new_key_with_a_key_limit_starts_at_it() {
    let guard = small_guard();
    guard
        .set_key_limit("pro", "vip", Limit::per_second(100).burst(30))
        .unwrap();
    assert_eq!(admitted_in(&guard, "pro", "vip", 100), 30);
    assert_eq!(admitted_in(&guard, "pro", "plain", 100), 5);
}

#[tokio::test]
async fn a_removed_key_limit_returns_the_key_to_its_class() {
    let guard = small_guard();
    guard
        .set_key_limit("", "k", Limit::per_second(1000))
        .unwrap();
    let _ = guard.check("k");
    eventually(|| guard.share("k") == Some(500.0)).await;
    assert!(guard.remove_key_limit("", "k"));
    eventually(|| guard.share("k") == Some(5.0)).await;
}

// The same requests at the same moments, on two guards alike but for a
// shadow: every decision the same, and only the shadowed one counting
// shadow denials. On paused time, every check lands 5 ms off a tick, so
// both guards have ticked alike before it.
#[tokio::test(start_paused = true)]
async fn a_shadow_never_changes_the_decision() {
    let spawn = || {
        Guard::builder()
            .bind("127.0.0.1:0")
            .limit(40)
            .burst(10)
            .class("free", Limit::per_second(20).burst(5))
            .class("pro", Limit::per_second(10).burst(5))
            .spawn()
            .unwrap()
    };
    let (plain, shadowed) = (spawn(), spawn());
    shadowed
        .set_shadow("free", Some(Limit::per_second(1)))
        .unwrap();

    let classes = ["free", "free", "", "pro"];
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) as usize
    };
    let (mut ours, mut theirs) = (Vec::new(), Vec::new());
    tokio::time::sleep(Duration::from_millis(5)).await;
    for _ in 0..300 {
        for _ in 0..next() % 8 {
            let class = classes[next() % classes.len()];
            let key = format!("k{}", next() % 5);
            ours.push(plain.check_in(class, &key));
            theirs.push(shadowed.check_in(class, &key));
        }
        tokio::time::sleep(Duration::from_millis(10 * (next() % 5) as u64)).await;
    }
    assert_eq!(ours, theirs);
    assert!(ours.iter().any(|d| !d.is_allowed()), "some were denied");

    let stats = until(&shadowed, |s| s.shadow_denied > 0).await;
    assert!(stats.shadow_denied > 0);
    assert_eq!(plain.stats().shadow_denied, 0);
}

// One node, a class at 100 a second with a shadow at 10, and another guard
// with the class at 10, both offered one request every `every` for 5 s:
// the shadow denials of the first, and the denials of the second.
async fn shadowed_and_enforced(every: Duration) -> (f64, f64) {
    let spawn = |limit: Limit| {
        Guard::builder()
            .bind("127.0.0.1:0")
            .limit(1000)
            .class("c", limit)
            .spawn()
            .unwrap()
    };
    let shadowed = spawn(Limit::per_second(100));
    shadowed
        .set_shadow("c", Some(Limit::per_second(10)))
        .unwrap();
    let enforced = spawn(Limit::per_second(10));
    // Off the 50 ms ticks by 5 ms, as every step below keeps it.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        let _ = shadowed.check_in("c", "k");
        let _ = enforced.check_in("c", "k");
        tokio::time::sleep(every).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let shadow = shadowed.stats().shadow_denied as f64;
    let denied = enforced.stats().denied as f64;
    assert!(denied > 0.0);
    (shadow, denied)
}

// Offered 62.5 a second, the key is hot under either limit, α × R = 50 or
// 5 alone: the shadow denies what the class at 10 does.
#[tokio::test(start_paused = true)]
async fn the_shadow_counts_what_its_limit_would_deny() {
    let (shadow, denied) = shadowed_and_enforced(Duration::from_millis(16)).await;
    assert!(
        (shadow / denied - 1.0).abs() <= 0.1,
        "{shadow} shadow denials, {denied} denials"
    );
}

// Offered 50 a second, the key is cold under the class's 100, α × R being
// 50, and hot under the shadow's 10: the strict side of `set_shadow`. The
// shadow holds it to the cold share of 10, 5 a second, where the class at
// 10 gives it all 10: up to 5 × 5 s = 25 more denials, never fewer.
#[tokio::test(start_paused = true)]
async fn a_shadow_errs_strict_for_a_key_hot_under_it_alone() {
    let (shadow, denied) = shadowed_and_enforced(Duration::from_millis(20)).await;
    assert!(
        denied <= shadow && shadow <= denied + 25.0 + 2.0,
        "{shadow} shadow denials, {denied} denials"
    );
}

#[tokio::test]
async fn a_shadow_goes_with_its_class() {
    let guard = guard();
    guard.set_class("free", Limit::per_second(20)).unwrap();
    guard.set_shadow("pro", Some(Limit::per_second(5))).unwrap();
    guard
        .set_shadow("free", Some(Limit::per_second(5)))
        .unwrap();
    guard.set_shadow("", Some(Limit::per_second(20))).unwrap();
    let mut shadows = guard.shadows();
    shadows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        shadows,
        [
            ("".to_owned(), Limit::per_second(20)),
            ("free".to_owned(), Limit::per_second(5)),
            ("pro".to_owned(), Limit::per_second(5)),
        ]
    );

    assert!(guard.remove_class("free"));
    guard
        .set_classes([("team", Limit::per_second(500))])
        .unwrap();
    assert_eq!(
        guard.shadows(),
        [("".to_owned(), Limit::per_second(20))],
        "the default class is never removed"
    );
    assert!(!guard.remove_class(""), "no class named \"\"");
    assert_eq!(guard.shadows().len(), 1, "the default's shadow stays");
    guard.set_shadow("", None).unwrap();
    assert!(guard.shadows().is_empty());
}

#[tokio::test]
async fn a_shadow_on_a_missing_class_is_refused() {
    let guard = guard();
    assert!(matches!(
        guard.set_shadow("nope", Some(Limit::per_second(5))),
        Err(Error::UnknownClass(name)) if name == "nope"
    ));
    assert!(matches!(
        guard.set_shadow("pro", Some(Limit::per_second(0))),
        Err(Error::ZeroLimit)
    ));
    guard.set_shadow("pro", Some(Limit::per_second(5))).unwrap();
    let before = guard.shadows();
    assert!(matches!(
        guard.set_shadows([
            ("", Limit::per_second(5)),
            ("nope", Limit::per_second(5))
        ]),
        Err(Error::UnknownClass(name)) if name == "nope"
    ));
    assert_eq!(guard.shadows(), before, "all or nothing");
    guard.set_shadows([("", Limit::per_second(7))]).unwrap();
    assert_eq!(
        guard.shadows(),
        [("".to_owned(), Limit::per_second(7))],
        "replaces every shadow"
    );
}

// The class at 10 a second, a burst of 5; the shadow at 1, a burst of 1.
#[tokio::test]
async fn check_in_traced_tells_the_shadow() {
    let guard = guard();
    guard.set_shadow("pro", Some(Limit::per_second(1))).unwrap();
    let first = guard.check_in_traced("pro", "k");
    assert_eq!(first.decision, rateguard::Decision::Allow);
    assert_eq!(first.shadow, Some(rateguard::Decision::Allow));
    let second = guard.check_in_traced("pro", "k");
    assert_eq!(second.decision, rateguard::Decision::Allow);
    assert!(matches!(
        second.shadow,
        Some(rateguard::Decision::Deny { .. })
    ));
    assert_eq!(
        guard.check_traced("k").shadow,
        None,
        "the default class has no shadow"
    );
}
