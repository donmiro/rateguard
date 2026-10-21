// The flags as variables, as a Kubernetes manifest sets them. A test
// binary of its own, with one test: it changes the process's environment.

use clap::Parser;
use rateguard_sidecar::args::Args;

fn with(vars: &[(&str, &str)]) -> Result<Args, clap::Error> {
    for (name, value) in vars {
        // One test in this binary: nothing else reads the environment.
        unsafe { std::env::set_var(name, value) };
    }
    let args = Args::try_parse_from(["rateguard-sidecar"]);
    for (name, _) in vars {
        unsafe { std::env::remove_var(name) };
    }
    args
}

#[test]
fn switches_take_the_usual_ways_of_saying_yes_and_no() {
    for yes in ["1", "true", "yes", "on"] {
        let args = with(&[
            ("RATEGUARD_LIMIT", "10"),
            ("RATEGUARD_ADMIN", yes),
            ("RATEGUARD_DRY_RUN", yes),
        ])
        .unwrap_or_else(|e| panic!("{yes}: {e}"));
        assert!(args.admin && args.dry_run, "{yes}");
    }
    for no in ["0", "false", "no", "off"] {
        let args = with(&[
            ("RATEGUARD_LIMIT", "10"),
            ("RATEGUARD_ADMIN", no),
            ("RATEGUARD_DRY_RUN", no),
        ])
        .unwrap_or_else(|e| panic!("{no}: {e}"));
        assert!(!args.admin && !args.dry_run, "{no}");
    }
    let args = with(&[
        ("RATEGUARD_LIMIT", "10"),
        ("RATEGUARD_SEEDS", "a:7946,b:7946"),
        ("RATEGUARD_CLASSES", "pro=100/5,free=10"),
        ("RATEGUARD_DRAIN", "3s"),
    ])
    .unwrap();
    assert_eq!(args.seeds, ["a:7946", "b:7946"]);
    assert_eq!(args.classes.len(), 2);
    assert_eq!(args.drain.0, std::time::Duration::from_secs(3));

    let args = with(&[("RATEGUARD_LIMIT", "10"), ("RATEGUARD_MOST_DENIED", "40")]).unwrap();
    assert_eq!(args.most_denied, Some(40));

    // Paths only: a secret's value is never a flag or a variable, so that
    // `ps` and /proc/*/cmdline do not show it.
    let args = with(&[
        ("RATEGUARD_LIMIT", "10"),
        ("RATEGUARD_GOSSIP_SECRET_FILE", "/run/secrets/gossip"),
        ("RATEGUARD_GOSSIP_AUTH", "transitional"),
        ("RATEGUARD_KEY_SALT_FILE", "/run/secrets/salt"),
    ])
    .unwrap();
    assert_eq!(
        args.gossip_secret_file.as_deref(),
        Some(std::path::Path::new("/run/secrets/gossip"))
    );
    assert_eq!(args.gossip_auth(), rateguard::GossipAuth::Transitional);
    assert_eq!(
        args.key_salt_file.as_deref(),
        Some(std::path::Path::new("/run/secrets/salt"))
    );
}
