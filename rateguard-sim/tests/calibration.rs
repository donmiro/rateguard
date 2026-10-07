// A sweep over α and β, to choose them by measurement (spec §10.3). Not a
// test of correctness: it prints a table and asserts nothing. Run it with
//
//     cargo test --release -p rateguard-sim --test calibration -- --ignored --nocapture

use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::Config;
use rateguard_sim::link::PerfectLink;
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const R: f64 = 1000.0;
const KEY: u64 = 42;

fn config(alpha: f64, beta: f64) -> Config {
    Config {
        limit_per_sec: R as u32,
        burst: 10,
        alpha,
        floor_factor: beta,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 4,
        max_tracked_keys: 8,
        demand_time_constant: ONE_SEC,
    }
}

fn sim(nodes: usize, alpha: f64, beta: f64) -> Sim<PerfectLink> {
    let mut s = Sim::new(nodes, config(alpha, beta), PerfectLink::new(ONE_MS));
    s.remove_invariant("admission-window");
    s
}

fn admitted(s: &Sim<PerfectLink>, from: Nanos, until: Nanos) -> f64 {
    let n = s
        .admissions()
        .iter()
        .filter(|a| a.decision == Decision::Allow && (from..until).contains(&a.at))
        .count();
    n as f64 / ((until - from) as f64 / ONE_SEC as f64)
}

// The largest one-second window in [from, until), sliding by 100 ms.
fn peak(s: &Sim<PerfectLink>, from: Nanos, until: Nanos) -> f64 {
    let mut t = from;
    let mut most: f64 = 0.0;
    while t + ONE_SEC <= until {
        most = most.max(admitted(s, t, t + ONE_SEC));
        t += ONE_SEC / 10;
    }
    most
}

// How long after `from` the one-second window first reaches 90% of R.
fn time_to_90(s: &Sim<PerfectLink>, from: Nanos, until: Nanos) -> Option<f64> {
    let mut t = from;
    while t + ONE_SEC <= until {
        if admitted(s, t, t + ONE_SEC) >= 0.9 * R {
            return Some((t + ONE_SEC - from) as f64 / ONE_SEC as f64);
        }
        t += ONE_SEC / 10;
    }
    None
}

struct Row {
    /// One node takes all the traffic, the others none: what the idle
    /// nodes' floors cost.
    concentrated: f64,
    /// Every node busy, one far more than the rest.
    skewed: f64,
    /// Every node busy from a cold start: seconds to 90% of R.
    cold_start: Option<f64>,
    /// A key turning hot on 3 nodes at once mid-run: admitted per second
    /// over its first 2 s, and seconds to 90% of R.
    promotion: f64,
    promotion_to_90: Option<f64>,
    /// The largest one-second window anywhere, over R.
    peak: f64,
}

fn measure(nodes: usize, alpha: f64, beta: f64) -> Row {
    let mut peaks = Vec::new();

    let mut s = sim(nodes, alpha, beta);
    s.schedule_request_stream(0, KEY, 2000, 0, 30 * ONE_SEC);
    s.run_until(30 * ONE_SEC);
    let concentrated = admitted(&s, 20 * ONE_SEC, 30 * ONE_SEC);
    peaks.push(peak(&s, 0, 30 * ONE_SEC));

    let mut s = sim(nodes, alpha, beta);
    s.schedule_request_stream(0, KEY, 1200, 0, 30 * ONE_SEC);
    for node in 1..nodes {
        s.schedule_request_stream(node, KEY, 150, 0, 30 * ONE_SEC);
    }
    s.run_until(30 * ONE_SEC);
    let skewed = admitted(&s, 20 * ONE_SEC, 30 * ONE_SEC);
    peaks.push(peak(&s, 0, 30 * ONE_SEC));

    let mut s = sim(nodes, alpha, beta);
    for node in 0..nodes {
        s.schedule_request_stream(node, KEY, (2.0 * R / nodes as f64) as u32, 0, 20 * ONE_SEC);
    }
    s.run_until(20 * ONE_SEC);
    let cold_start = time_to_90(&s, 0, 20 * ONE_SEC);
    peaks.push(peak(&s, 0, 20 * ONE_SEC));

    let mut s = sim(nodes, alpha, beta);
    let start = 10 * ONE_SEC;
    for node in [0, 1, 2] {
        s.schedule_request_stream(node, KEY, 700, start, start + 20 * ONE_SEC);
    }
    s.run_until(start + 20 * ONE_SEC);
    let promotion = admitted(&s, start, start + 2 * ONE_SEC);
    let promotion_to_90 = time_to_90(&s, start, start + 20 * ONE_SEC);
    peaks.push(peak(&s, start, start + 20 * ONE_SEC));

    Row {
        concentrated,
        skewed,
        cold_start,
        promotion,
        promotion_to_90,
        peak: peaks.into_iter().fold(0.0, f64::max) / R,
    }
}

fn secs(t: Option<f64>) -> String {
    t.map_or("never".into(), |t| format!("{t:.1} s"))
}

#[test]
#[ignore = "a measurement, run by hand"]
fn sweep() {
    for nodes in [5, 20] {
        println!("\nN = {nodes}, R = {R}");
        println!(
            "{:>5} {:>5} | {:>12} {:>7} | {:>10} | {:>14} {:>9} | {:>6}",
            "α", "β", "concentrated", "skewed", "cold start", "promotion 2 s", "to 90%", "peak"
        );
        for alpha in [0.25, 0.5, 0.75] {
            for beta in [0.02, 0.05, 0.1, 0.2] {
                let r = measure(nodes, alpha, beta);
                println!(
                    "{alpha:>5} {beta:>5} | {:>12.0} {:>7.0} | {:>10} | {:>14.0} {:>9} | {:>6.3}",
                    r.concentrated,
                    r.skewed,
                    secs(r.cold_start),
                    r.promotion,
                    secs(r.promotion_to_90),
                    r.peak
                );
            }
        }
    }
}
