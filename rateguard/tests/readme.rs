// The README's examples, so that they cannot go stale unnoticed: the
// getting-started one compiles as written, and runs on loopback.

use std::time::Duration;

use rateguard::{Decision, Guard, PartitionPolicy};

fn serve(_request: ()) {}
fn reject(_retry_after: Duration) {}

// As in the README, word for word between the markers; never run: the
// addresses are someone's fleet.
#[allow(dead_code)]
async fn getting_started() -> Result<(), rateguard::Error> {
    let request = ();
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
        Decision::Allow => serve(request),
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

// Every snippet between the markers, in the sources that compile and run
// it, must stand in the README as it is there, indentation aside.
#[test]
fn the_readme_says_what_the_tested_examples_do() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    // A packaged crate has no workspace around it; nothing to compare.
    let Ok(readme) = std::fs::read_to_string(root.join("README.md")) else {
        return;
    };
    for source in [
        "rateguard/tests/readme.rs",
        "rateguard/examples/axum.rs",
        "rateguard-core/tests/readme.rs",
    ] {
        let text = std::fs::read_to_string(root.join(source)).unwrap();
        let begin = text.find("// README: begin\n").expect(source) + "// README: begin\n".len();
        let end = text.find("// README: end").expect(source);
        // Up to the end of the last line: the marker's own indentation is
        // not part of the snippet.
        let body = &text[begin..end];
        let body = &body[..body.rfind('\n').map_or(0, |i| i + 1)];
        let lines: Vec<&str> = body.lines().collect();
        let indent = lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.len() - line.trim_start().len())
            .min()
            .unwrap_or(0);
        let snippet: String = lines
            .iter()
            .map(|line| format!("{}\n", line.get(indent..).unwrap_or("")))
            .collect();
        assert!(
            readme.contains(&snippet),
            "{source}: the README no longer shows\n{snippet}"
        );
    }
}
