// The sidecar's flags: what they parse to, and what they refuse.

use std::time::Duration;

use clap::Parser;
use rateguard::{GossipAuth, Guard, Limit, PartitionPolicy};
use rateguard_sidecar::args::{Args, ClassSpec, LimitSpec, Policy};

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
fn a_limit_takes_a_unit() {
    let args = parse(&["--limit", "1000/min"]).unwrap();
    assert_eq!(args.limit.limit, Limit::per_minute(1000));
    assert!(!args.limit.has_burst);
    let args = parse(&["--limit", "5000/h/10"]).unwrap();
    assert_eq!(args.limit.limit, Limit::per_hour(5000).burst(10));
    assert!(args.limit.has_burst);
    assert_eq!(
        parse(&["--limit", "100/5"]).unwrap().limit.limit,
        Limit::per_second(100).burst(5)
    );
    assert_eq!(
        "pro=5000/h/10".parse::<ClassSpec>().unwrap(),
        ClassSpec("pro".into(), Limit::per_hour(5000).burst(10))
    );
    assert_eq!(
        "x=100/5".parse::<ClassSpec>().unwrap(),
        ClassSpec("x".into(), Limit::per_second(100).burst(5))
    );
    assert_eq!(
        "x=1000/min/50".parse::<ClassSpec>().unwrap(),
        ClassSpec("x".into(), Limit::per_minute(1000).burst(50))
    );
    let error = "x=1/day".parse::<ClassSpec>().unwrap_err();
    assert!(error.contains("x=1/day"), "{error}");
    for bad in ["1/day", "1/min/x", "1/min/5/6", "/min", "1/"] {
        assert!(bad.parse::<LimitSpec>().is_err(), "{bad}");
    }
    // Whether the text gave a burst, for `--burst` to fill in the rest.
    assert_eq!(
        "1000/min/50".parse::<LimitSpec>().unwrap(),
        LimitSpec {
            limit: Limit::per_minute(1000).burst(50),
            has_burst: true,
        }
    );
    assert_eq!(
        "1000/min".parse::<LimitSpec>().unwrap(),
        LimitSpec {
            limit: Limit::per_minute(1000),
            has_burst: false,
        }
    );
}

#[tokio::test]
async fn a_burst_flag_fills_in_only_where_the_limit_has_none() {
    // A slow limit earns nothing back while the test runs, so what a key
    // is admitted is its burst.
    let admitted = |limit: &'static str| {
        let args = parse(&["--limit", limit, "--burst", "7", "--bind", "127.0.0.1:0"]).unwrap();
        let guard = args.builder().unwrap().spawn().unwrap();
        (0..20).filter(|_| guard.check("k").is_allowed()).count()
    };
    assert_eq!(admitted("100/h"), 7);
    assert_eq!(admitted("100/h/3"), 3);
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
    let guard = args.builder().unwrap().spawn().unwrap();
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
        args.builder().unwrap().spawn(),
        Err(rateguard::Error::ZeroLimit)
    ));
}

#[test]
fn health_and_metrics_may_listen_apart() {
    let args = parse(&["--limit", "1", "--metrics-listen", "0.0.0.0:9090"]).unwrap();
    assert_eq!(args.metrics_listen, Some("0.0.0.0:9090".parse().unwrap()));
}

#[test]
fn most_denied_flag_and_env() {
    assert_eq!(parse(&["--limit", "1"]).unwrap().most_denied, None);
    let args = parse(&["--limit", "1", "--most-denied", "25"]).unwrap();
    assert_eq!(args.most_denied, Some(25));
    assert!(parse(&["--limit", "1", "--most-denied", "x"]).is_err());
}

// A file of its own per test, removed when dropped.
struct File(std::path::PathBuf);
impl File {
    fn with(name: &str, content: &[u8]) -> File {
        let path = std::env::temp_dir().join(format!(
            "rateguard-sidecar-args-{}-{name}",
            std::process::id()
        ));
        std::fs::write(&path, content).unwrap();
        File(path)
    }
    fn path(&self) -> &str {
        self.0.to_str().unwrap()
    }
}
impl Drop for File {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

// 32 bytes of 0xA1, in base64.
const SECRET: &str = "oaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaE=\n";

#[test]
fn the_auth_mode_defaults_to_required_with_a_secret_file_and_off_without() {
    let args = parse(&["--limit", "1"]).unwrap();
    assert_eq!(args.gossip_auth(), GossipAuth::Off);
    let args = parse(&["--limit", "1", "--gossip-secret-file", "/run/secrets"]).unwrap();
    assert_eq!(args.gossip_auth(), GossipAuth::Required);
    let args = parse(&[
        "--limit",
        "1",
        "--gossip-secret-file",
        "/run/secrets",
        "--gossip-auth",
        "transitional",
    ])
    .unwrap();
    assert_eq!(args.gossip_auth(), GossipAuth::Transitional);
    assert!(parse(&["--limit", "1", "--gossip-auth", "sometimes"]).is_err());
}

#[test]
fn transitional_without_a_secret_file_is_refused() {
    for mode in ["transitional", "required"] {
        let args = parse(&["--limit", "1", "--gossip-auth", mode]).unwrap();
        let error = args.builder().unwrap_err().to_string();
        assert!(error.contains("--gossip-secret-file"), "{error}");
    }
}

#[test]
fn off_with_a_secret_file_is_allowed() {
    let file = File::with("off", SECRET.as_bytes());
    let args = parse(&[
        "--limit",
        "1",
        "--gossip-secret-file",
        file.path(),
        "--gossip-auth",
        "off",
    ])
    .unwrap();
    assert_eq!(args.gossip_auth(), GossipAuth::Off);
    assert!(args.builder().is_ok());
}

#[tokio::test]
async fn a_secret_file_signs_the_gossip() {
    let file = File::with("signs", SECRET.as_bytes());
    let args = parse(&[
        "--limit",
        "1",
        "--bind",
        "127.0.0.1:0",
        "--gossip-secret-file",
        file.path(),
    ])
    .unwrap();
    let guard = args.builder().unwrap().spawn().unwrap();
    assert_eq!(guard.stats().gossip_auth, GossipAuth::Required);
}

#[test]
fn a_bad_secret_file_is_refused_by_name_without_its_content() {
    let file = File::with("bad", b"not*base64\n");
    let args = parse(&["--limit", "1", "--gossip-secret-file", file.path()]).unwrap();
    let error = args.builder().unwrap_err().to_string();
    assert!(error.contains("--gossip-secret-file"), "{error}");
    assert!(error.contains("line 1") && !error.contains('*'), "{error}");
    let missing = parse(&[
        "--limit",
        "1",
        "--gossip-secret-file",
        "/nonexistent/rateguard",
    ])
    .unwrap();
    let error = missing.builder().unwrap_err().to_string();
    assert!(error.contains("--gossip-secret-file"), "{error}");
}

#[test]
fn an_empty_secret_file_is_refused_when_signing() {
    let file = File::with("empty", b"\n\n");
    let args = parse(&["--limit", "1", "--gossip-secret-file", file.path()]).unwrap();
    let error = args.builder().unwrap_err().to_string();
    assert!(error.contains("--gossip-secret-file"), "{error}");
}

#[test]
fn a_salt_file_shorter_than_16_bytes_is_refused() {
    let file = File::with("short-salt", b"fifteen bytes!!");
    let args = parse(&["--limit", "1", "--key-salt-file", file.path()]).unwrap();
    let error = args.builder().unwrap_err().to_string();
    assert!(error.contains("--key-salt-file"), "{error}");
    assert!(
        error.contains("15") && !error.contains("fifteen"),
        "{error}"
    );
    let missing = parse(&["--limit", "1", "--key-salt-file", "/nonexistent/rateguard"]).unwrap();
    let error = missing.builder().unwrap_err().to_string();
    assert!(error.contains("--key-salt-file"), "{error}");
}

// Taken as it is, newline and all: 15 bytes and a newline are 16.
#[tokio::test]
async fn a_salt_file_is_taken_as_it_is() {
    let file = File::with("salt", b"fifteen bytes!!\n");
    let args = parse(&[
        "--limit",
        "1",
        "--bind",
        "127.0.0.1:0",
        "--key-salt-file",
        file.path(),
    ])
    .unwrap();
    let guard = args.builder().unwrap().spawn().unwrap();
    let salted = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(1)
        .key_salt(&b"fifteen bytes!!\n"[..])
        .spawn()
        .unwrap();
    assert_eq!(guard.key_hash_in("", "k"), salted.key_hash_in("", "k"));
    assert_ne!(guard.key_hash_in("", "k"), rateguard::key_hash_in("", "k"));
}
