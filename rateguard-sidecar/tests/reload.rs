// The file of gossip secrets reread while the node runs: a rotation picked
// up, a broken file leaving the secrets in force alone.

use std::path::PathBuf;
use std::time::Duration;

use rateguard::{GossipAuth, Guard, Secret};
use rateguard_sidecar::reload_secrets;

fn free_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// 32 bytes of 0xA1 and of 0xB2, raw and in base64.
const A: [u8; 32] = [0xA1; 32];
const B: [u8; 32] = [0xB2; 32];
const A64: &str = "oaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaE=";
const B64: &str = "srKysrKysrKysrKysrKysrKysrKysrKysrKysrKysrI=";

fn secrets(list: &[[u8; 32]]) -> Vec<Secret> {
    list.iter()
        .map(|bytes| Secret::new(bytes.to_vec()).unwrap())
        .collect()
}

// A file of its own, removed when dropped.
struct File(PathBuf);
impl Drop for File {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

// Rewritten through a rename, as Kubernetes swaps a mounted Secret: a
// reader never sees half a file.
fn write(path: &PathBuf, content: &str) {
    let next = path.with_extension("next");
    std::fs::write(&next, content).unwrap();
    std::fs::rename(&next, path).unwrap();
}

#[tokio::test]
async fn a_replaced_secret_file_is_picked_up() {
    let file =
        File(std::env::temp_dir().join(format!("rateguard-sidecar-reload-{}", std::process::id())));
    write(&file.0, &format!("{A64}\n"));
    let (pa, pb) = (free_port(), free_port());
    let a = Guard::builder()
        .bind(format!("127.0.0.1:{pa}"))
        .limit(100)
        .gossip_auth(GossipAuth::Required, secrets(&[A]))
        .spawn()
        .unwrap();
    tokio::spawn(reload_secrets(
        a.clone(),
        file.0.clone(),
        GossipAuth::Required,
        Duration::from_millis(50),
    ));

    // The rotation's first step: B added, A still signing.
    write(&file.0, &format!("{A64}\n{B64}\n"));
    tokio::time::sleep(Duration::from_millis(500)).await;
    // A node a step further, signing with B: heard only by a node that
    // picked B up.
    let b = Guard::builder()
        .bind(format!("127.0.0.1:{pb}"))
        .seeds([format!("127.0.0.1:{pa}")])
        .limit(100)
        .gossip_auth(GossipAuth::Required, secrets(&[B, A]))
        .spawn()
        .unwrap();
    until_both_see_two(&a, &b).await;
    assert_eq!(a.stats().rejected_datagrams.bad_tag, 0);

    // Garbage: [A, B] stays in force, and B's datagrams keep passing.
    write(&file.0, "this is not base64\n");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = a.stats().rejected_datagrams.bad_tag;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(before, 0);
    assert_eq!(a.stats().rejected_datagrams.bad_tag, 0);
    assert_eq!(a.cluster_size(), 2);
}

async fn until_both_see_two(a: &Guard, b: &Guard) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if a.cluster_size() == 2 && b.cluster_size() == 2 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster sizes {} and {} after 10 s; {:?}",
            a.cluster_size(),
            b.cluster_size(),
            a.stats().rejected_datagrams
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// Each failed reread counted, for an alert: the warning on stderr is easy
// to miss. On a runtime of this thread, so that the recorder set for it
// sees the task.
#[test]
fn a_failed_reload_is_counted() {
    let file = File(std::env::temp_dir().join(format!(
        "rateguard-sidecar-reload-count-{}",
        std::process::id()
    )));
    write(&file.0, "this is not base64\n");
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let guard = Guard::builder()
                .bind("127.0.0.1:0")
                .limit(100)
                .gossip_auth(GossipAuth::Required, secrets(&[A]))
                .spawn()
                .unwrap();
            let reload = reload_secrets(
                guard.clone(),
                file.0.clone(),
                GossipAuth::Required,
                Duration::from_millis(50),
            );
            let _ = tokio::time::timeout(Duration::from_millis(400), reload).await;
            assert_eq!(guard.stats().gossip_auth, GossipAuth::Required);
        })
    });
    let rendered = handle.render();
    let count: u64 = rendered
        .lines()
        .find_map(|line| line.strip_prefix("rateguard_gossip_secret_reload_errors_total "))
        .unwrap_or_else(|| panic!("{rendered}"))
        .parse()
        .unwrap();
    assert!(count >= 3, "{rendered}");
}
