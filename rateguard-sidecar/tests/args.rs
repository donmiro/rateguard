// The sidecar's flags: what they parse to, and what they refuse.

use std::time::Duration;

use clap::Parser;
use rateguard::{Limit, PartitionPolicy};
use rateguard_sidecar::args::{Args, ClassSpec, Policy};

fn parse(flags: &[&str]) -> Result<Args, clap::Error> {
    Args::try_parse_from(std::iter::once("rateguard-sidecar").chain(flags.iter().copied()))
}

#[test]
fn a_class_is_a_name_a_rate_and_maybe_a_burst() {
    assert_eq!(
        "pro=1000/50".parse::<ClassSpec>().unwrap(),
        ClassSpec("pro".into(), Limit::per_second(1000).burst(50))
    );
    assert_eq!(
        "free=10".parse::<ClassSpec>().unwrap(),
        ClassSpec("free".into(), Limit::per_second(10))
    );
    for bad in ["pro", "=10", "pro=", "pro=ten", "pro=10/", "pro=10/x"] {
        assert!(bad.parse::<ClassSpec>().is_err(), "{bad}");
    }
}

#[test]
fn a_partition_policy_is_named_as_in_the_library() {
    assert_eq!(
        "optimistic".parse::<Policy>().unwrap().0,
        PartitionPolicy::Optimistic
    );
    assert_eq!(
        "quorum".parse::<Policy>().unwrap().0,
        PartitionPolicy::Quorum
    );
    assert_eq!(
        "hold-down:30s".parse::<Policy>().unwrap().0,
        PartitionPolicy::HoldDown(Duration::from_secs(30))
    );
    assert_eq!(
        "hold-down:500ms".parse::<Policy>().unwrap().0,
        PartitionPolicy::HoldDown(Duration::from_millis(500))
    );
    for bad in ["hold-down", "hold-down:", "hold-down:30", "pessimistic"] {
        assert!(bad.parse::<Policy>().is_err(), "{bad}");
    }
}

#[test]
fn the_defaults_listen_on_loopback_and_gossip_on_7946() {
    let args = parse(&["--limit", "100", "--advertise", "10.0.0.3:7946"]).unwrap();
    assert_eq!(args.listen, "127.0.0.1:7947".parse().unwrap());
    assert_eq!(args.bind, "0.0.0.0:7946");
    assert!(!args.admin && !args.dry_run);
    assert_eq!(args.metrics_listen, None);
}

#[test]
fn a_limit_is_required() {
    assert!(parse(&["--advertise", "10.0.0.3:7946"]).is_err());
}

#[test]
fn seeds_and_classes_come_as_lists() {
    let args = parse(&[
        "--limit",
        "100",
        "--bind",
        "127.0.0.1:0",
        "--seeds",
        "10.0.0.1:7946,rateguard.svc:7946",
        "--class",
        "pro=1000/50",
        "--class",
        "free=10",
    ])
    .unwrap();
    assert_eq!(args.seeds, ["10.0.0.1:7946", "rateguard.svc:7946"]);
    assert_eq!(args.classes.len(), 2);
}

#[tokio::test]
async fn the_flags_make_a_guard() {
    let args = parse(&[
        "--limit",
        "100",
        "--burst",
        "7",
        "--bind",
        "127.0.0.1:0",
        "--class",
        "pro=1000/50",
        "--partition-policy",
        "quorum",
        "--dry-run",
    ])
    .unwrap();
    let guard = args.builder().spawn().unwrap();
    assert_eq!(
        guard.classes(),
        [("pro".to_owned(), Limit::per_second(1000).burst(50))]
    );
    assert!(guard.stats().dry_run);
    let admitted = (0..20).filter(|_| guard.check("k").is_allowed()).count();
    assert_eq!(admitted, 20, "a dry run admits all");
}

#[tokio::test]
async fn flags_the_library_refuses_are_refused_at_start() {
    let args = parse(&["--limit", "0", "--bind", "127.0.0.1:0"]).unwrap();
    assert!(matches!(
        args.builder().spawn(),
        Err(rateguard::Error::ZeroLimit)
    ));
}

#[test]
fn health_and_metrics_may_listen_apart() {
    let args = parse(&["--limit", "1", "--metrics-listen", "0.0.0.0:9090"]).unwrap();
    assert_eq!(args.metrics_listen, Some("0.0.0.0:9090".parse().unwrap()));
}
