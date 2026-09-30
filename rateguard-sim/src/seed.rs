use crate::rng::Rng;
use std::collections::hash_map::RandomState;
use std::env::{self, VarError};
use std::hash::{BuildHasher, Hasher};
use std::thread;

pub const SEED_VAR: &str = "RATEGUARD_SEED";

pub fn seeded(scenario: impl FnOnce(u64)) {
    let seed = pinned().unwrap_or_else(fresh);
    let _report = Report(seed);
    scenario(seed);
}

pub fn each_seed(count: usize, mut scenario: impl FnMut(u64)) {
    for seed in plan(pinned(), count, fresh()) {
        let _report = Report(seed);
        scenario(seed);
    }
}

pub fn pinned() -> Option<u64> {
    match env::var(SEED_VAR) {
        Ok(raw) => Some(parse(&raw)),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(raw)) => panic!("{SEED_VAR} must be a u64, got {raw:?}"),
    }
}

fn parse(raw: &str) -> u64 {
    raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "{SEED_VAR} must be a u64, got {raw:?}: a mistyped seed would replay some other run"
        );
    })
}

fn fresh() -> u64 {
    RandomState::new().build_hasher().finish()
}

fn plan(pinned: Option<u64>, count: usize, base: u64) -> Vec<u64> {
    assert!(count > 0, "a sweep over no seeds tests nothing");
    match pinned {
        Some(seed) => vec![seed],
        None => {
            let mut rng = Rng::new(base);
            (0..count).map(|_| rng.next_u64()).collect()
        }
    }
}

fn replay_hint(seed: u64, test: Option<&str>) -> String {
    match test {
        Some(name) if name != "main" => format!(
            "seed {seed} failed, replay with: {SEED_VAR}={seed} cargo test -p rateguard-sim -- {name} --exact"
        ),
        _ => format!("seed {seed} failed, reply with: {SEED_VAR}={seed} cargo test"),
    }
}

struct Report(u64);
impl Drop for Report {
    fn drop(&mut self) {
        if thread::panicking() {
            eprintln!("{}", replay_hint(self.0, thread::current().name()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn a_decimal_seed_is_parsed() {
        assert_eq!(parse("8371"), 8371);
        assert_eq!(parse(" 8371\n"), 8371);
        assert_eq!(parse(&u64::MAX.to_string()), u64::MAX);
    }

    #[test]
    #[should_panic(expected = "RATEGUARD_SEED must be a u64")]
    fn a_mistyped_seed_is_an_error_not_a_fresh_run() {
        parse("83a1");
    }

    #[test]
    fn a_pinned_seed_replaces_the_whole_sweep() {
        assert_eq!(plan(Some(8371), 50, 1), vec![8371]);
    }

    #[test]
    fn a_sweep_is_distinct_and_reproducible_from_its_base() {
        let sweep = plan(None, 50, 1);
        assert_eq!(sweep, plan(None, 50, 1));
        assert_eq!(sweep.iter().collect::<HashSet<_>>().len(), 50);
    }

    #[test]
    fn fresh_seeds_differ_between_calls() {
        assert_ne!(fresh(), fresh());
    }

    #[test]
    fn the_hint_names_the_seed_and_the_test() {
        let hint = replay_hint(8371, Some("sim::tests::partition"));
        assert!(hint.contains("RATEGUARD_SEED=8371"), "{hint}");
        assert!(hint.contains("-- sim::tests::partition --exact"), "{hint}");

        let single_threaded = replay_hint(8371, Some("main"));
        assert!(!single_threaded.contains("--exact"), "{single_threaded}");
    }

    #[test]
    fn every_seed_of_the_sweep_is_run() {
        let mut seen = Vec::new();
        each_seed(5, |seed| seen.push(seed));
        let expected = if pinned().is_some() { 1 } else { 5 };
        assert_eq!(seen.len(), expected);
    }

    #[test]
    #[should_panic(expected = "boom")]
    fn a_failing_seed_still_fails_the_test() {
        seeded(|_| panic!("boom"));
    }
}
