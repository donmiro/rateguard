//! Three instances sharing one limit, in one process:
//!
//!     cargo run -p rateguard --release --example cluster
//!
//! One key, a limit of 1000 a second across the fleet, and a load balancer
//! that is far from fair: 1500 requests a second land on A, 300 on B, 200 on
//! C. Every second the example prints what each instance admitted and what
//! the fleet did. The first seconds sit at the floor while the nodes learn
//! each other's demand; then the shares follow the demand and the fleet
//! holds the limit. At 12 s C leaves, and A and B take its share at once.

use std::time::Duration;

use rateguard::Guard;

const KEY: &str = "api:tenant-42";
const LIMIT: u32 = 1_000;
const PORTS: [u16; 3] = [7951, 7952, 7953];
const NAMES: [&str; 3] = ["A", "B", "C"];
// Requests a second offered to each instance.
const OFFERED: [u32; 3] = [1_500, 300, 200];
const TICK: Duration = Duration::from_millis(10);
const C_LEAVES_AT: u64 = 12;
const SECONDS: u64 = 18;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut guards = Vec::new();
    for port in PORTS {
        let seeds = PORTS
            .iter()
            .filter(|&&p| p != port)
            .map(|p| format!("127.0.0.1:{p}"));
        guards.push(
            Guard::builder()
                .bind(format!("127.0.0.1:{port}"))
                .seeds(seeds)
                .limit(LIMIT)
                .spawn()?,
        );
    }

    // Each instance's traffic, spread evenly over the second.
    let load: Vec<_> = guards
        .iter()
        .zip(OFFERED)
        .map(|(guard, offered)| {
            let guard = guard.clone();
            let per_tick = offered / (Duration::from_secs(1).as_millis() / TICK.as_millis()) as u32;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(TICK);
                loop {
                    tick.tick().await;
                    // A service would serve or refuse here; the example
                    // only counts, through stats().
                    for _ in 0..per_tick {
                        let _ = guard.check(KEY);
                    }
                }
            })
        })
        .collect();

    println!(
        "limit {LIMIT}/s on one key across the fleet; offered {} + {} + {} = {}/s\n",
        OFFERED[0],
        OFFERED[1],
        OFFERED[2],
        OFFERED.iter().sum::<u32>()
    );
    println!(
        "{:>4}  {:>6}  {:>6}  {:>6}  {:>6}  {:>5}",
        "t", NAMES[0], NAMES[1], NAMES[2], "fleet", "nodes"
    );

    let mut last: Vec<u64> = guards.iter().map(|guard| guard.stats().admitted).collect();
    let mut gone = [false; 3];
    let mut second = tokio::time::interval(Duration::from_secs(1));
    second.tick().await;
    for t in 1..=SECONDS {
        second.tick().await;
        let mut row = String::new();
        let mut fleet = 0;
        for (i, guard) in guards.iter().enumerate() {
            if gone[i] {
                row.push_str(&format!("  {:>6}", "-"));
                continue;
            }
            let admitted = guard.stats().admitted;
            fleet += admitted - last[i];
            row.push_str(&format!("  {:>6}", admitted - last[i]));
            last[i] = admitted;
        }
        println!("{t:>3}s{row}  {fleet:>6}  {:>5}", guards[0].cluster_size());

        if t == C_LEAVES_AT {
            load[2].abort();
            guards[2].leave().await;
            gone[2] = true;
            println!("      C leaves");
        }
    }
    Ok(())
}
