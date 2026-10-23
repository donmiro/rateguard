// A key the node tracks but nobody checks takes a changed limit at the
// next tick, not at its next request: the core learns a key's limit from
// `record`, and a tick without attempts used to skip it. One node alone on
// a real socket, a default class at 40 a second with a burst of 50.

use std::time::Duration;

use rateguard::{Guard, Limit};

fn guard() -> Guard {
    Guard::builder()
        .bind("127.0.0.1:0")
        .limit(40)
        .burst(50)
        .class("pro", Limit::per_second(40).burst(50))
        .spawn()
        .unwrap()
}

// Long enough for a few ticks, so the key is tracked by the core.
async fn a_few_ticks() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

fn tight() -> Limit {
    Limit::per_second(1).burst(1)
}

#[tokio::test]
async fn an_idle_known_key_takes_its_key_limit_at_the_next_tick() {
    let guard = guard();
    let _ = guard.check("abuser");
    a_few_ticks().await;
    guard.set_key_limit("", "abuser", tight()).unwrap();
    // Many ticks and no request: "a known one from the next tick".
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let admitted = (0..50)
        .filter(|_| guard.check("abuser").is_allowed())
        .count();
    assert!(admitted <= 1, "{admitted} of 50 admitted");
}

#[tokio::test]
async fn an_idle_known_key_takes_a_lowered_class_limit_at_the_next_tick() {
    let guard = guard();
    let _ = guard.check_in("pro", "k");
    a_few_ticks().await;
    guard.set_class("pro", tight()).unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let admitted = (0..50)
        .filter(|_| guard.check_in("pro", "k").is_allowed())
        .count();
    assert!(admitted <= 1, "{admitted} of 50 admitted");
}

#[tokio::test]
async fn an_idle_known_key_takes_a_lowered_default_limit_at_the_next_tick() {
    let guard = guard();
    let _ = guard.check("k");
    a_few_ticks().await;
    guard.set_limit(tight()).unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let admitted = (0..50).filter(|_| guard.check("k").is_allowed()).count();
    assert!(admitted <= 1, "{admitted} of 50 admitted");
}

#[tokio::test]
async fn an_idle_key_whose_class_is_removed_takes_the_default() {
    let guard = guard();
    // The default is lowered, so that "pro" removed is told from "pro" kept.
    guard.set_limit(tight()).unwrap();
    let _ = guard.check_in("pro", "k");
    a_few_ticks().await;
    let before = guard.stats().unknown_class;
    guard.set_classes(Vec::<(String, Limit)>::new()).unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let admitted = (0..50)
        .filter(|_| guard.check_in("pro", "k").is_allowed())
        .count();
    // Counted by the next tick after the checks.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = guard.stats().unknown_class;
    assert!(after > before, "unknown_class stayed at {before}");
    assert!(admitted <= 1, "{admitted} of 50 admitted under the default");
}
