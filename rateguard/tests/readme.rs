// The README's examples, so that they cannot go stale unnoticed: the
// getting-started one compiles as written, and runs on loopback.

use std::time::Duration;

use rateguard::{Decision, Guard, PartitionPolicy};

fn serve() {}
fn reject(_retry_after: Duration) {}

// As in the README, word for word between the markers; never run: the
// addresses are someone's fleet.
#[allow(dead_code)]
async fn getting_started() -> Result<(), rateguard::Error> {
    // README: begin
    let guard = Guard::builder()
        .bind("0.0.0.0:7946")
        .advertise("10.0.0.3:7946")
        .seeds(["10.0.0.1:7946", "10.0.0.2:7946"])
        .limit(1_000)
        .burst(50)
        .partition_policy(PartitionPolicy::HoldDown(Duration::from_secs(30)))
        .spawn()?;

    match guard.check("api:tenant-42") {
        Decision::Allow => serve(),
        Decision::Deny { retry_after } => reject(retry_after),
    }
    // README: end
    Ok(())
}

#[tokio::test]
async fn getting_started_runs_on_loopback() {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(1_000)
        .burst(50)
        .partition_policy(PartitionPolicy::HoldDown(Duration::from_secs(30)))
        .spawn()
        .unwrap();
    let allowed = (0..100)
        .filter(|_| guard.check("api:tenant-42").is_allowed())
        .count();
    assert_eq!(allowed, 50);
}
