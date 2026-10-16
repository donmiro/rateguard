// The `metrics` feature: what the background task publishes, read back
// through a debugging recorder. A test binary of its own, as the recorder
// is global.

use std::time::Duration;

use metrics::{SharedString, Unit};
use metrics_util::CompositeKey;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rateguard::Guard;

type Snapshot = Vec<(CompositeKey, Option<Unit>, Option<SharedString>, DebugValue)>;

// The value of one metric of this guard in a snapshot, if published. A
// snapshot zeroes what it reads, and the next tick publishes it again: so
// one snapshot, then every lookup in it.
fn value(snapshot: &Snapshot, name: &str, labels: &[(&str, &str)], guard: &str) -> Option<f64> {
    snapshot
        .iter()
        .find(|(key, ..)| {
            let key = key.key();
            let has = |k: &str, v: &str| key.labels().any(|l| l.key() == k && l.value() == v);
            key.name() == name && has("guard", guard) && labels.iter().all(|&(k, v)| has(k, v))
        })
        .map(|(.., value)| match value {
            DebugValue::Counter(n) => *n as f64,
            DebugValue::Gauge(g) => g.into_inner(),
            DebugValue::Histogram(_) => unreachable!("no histograms"),
        })
}

#[tokio::test]
async fn the_background_task_publishes_counts_and_state() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();

    let port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let me = format!("127.0.0.1:{port}");
    let guard = Guard::builder()
        .bind(&me)
        .limit(100)
        .burst(5)
        .spawn()
        .unwrap();
    let admitted = (0..8).filter(|_| guard.check("k").is_allowed()).count();
    assert_eq!(admitted, 5);

    let wanted = [
        ("rateguard_checks_total", vec![("decision", "allow")], 5.0),
        ("rateguard_checks_total", vec![("decision", "deny")], 3.0),
        ("rateguard_overflowed_total", vec![], 0.0),
        ("rateguard_cluster_size", vec![], 1.0),
        ("rateguard_counted_size", vec![], 1.0),
        ("rateguard_suspects", vec![], 0.0),
        ("rateguard_hot_keys", vec![], 0.0),
        ("rateguard_held", vec![("reason", "joining")], 0.0),
        ("rateguard_held", vec![("reason", "learning")], 0.0),
        ("rateguard_held", vec![("reason", "without_quorum")], 0.0),
        ("rateguard_dry_run", vec![], 0.0),
    ];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = snapshotter.snapshot().into_vec();
        let seen: Vec<Option<f64>> = wanted
            .iter()
            .map(|(name, labels, _)| value(&snapshot, name, labels, &me))
            .collect();
        if wanted
            .iter()
            .zip(&seen)
            .all(|((.., want), seen)| *seen == Some(*want))
        {
            assert!(value(&snapshot, "rateguard_tracked_keys", &[], &me).is_some());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never came: {:?}",
            wanted
                .iter()
                .map(|(n, l, _)| (n, l))
                .zip(seen)
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
