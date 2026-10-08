// The request path, measured: README reports ~25 ns and spec §6.1 sets a
// ceiling of 100 ns, whatever the state of the cluster (guarantee 1).
//
//     cargo bench -p rateguard --bench check

use std::future::Future;
use std::hint::black_box;
use std::io;
use std::net::SocketAddr;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use rateguard::{Guard, Transport};

// A network that never answers: the cluster as good as gone.
struct Hung;
impl Transport for Hung {
    fn send_to(&self, _: &[u8], _: SocketAddr) -> impl Future<Output = io::Result<usize>> + Send {
        std::future::pending()
    }
    fn recv_from(
        &self,
        _: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send {
        std::future::pending()
    }
}

// A guard with a limit high enough that the measured path is the common
// one, an admission; its background task runs on `runtime`.
fn guard(runtime: &tokio::runtime::Runtime, hung: bool) -> Guard {
    runtime.block_on(async {
        let builder = Guard::builder()
            .bind("127.0.0.1:0")
            .limit(u32::MAX)
            .tracked_keys(4096);
        if hung {
            builder
                .bind("10.0.0.1:7946")
                .seeds(["10.0.0.2:7946", "10.0.0.3:7946"])
                .spawn_on(Hung)
                .unwrap()
        } else {
            builder.spawn().unwrap()
        }
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

fn single_thread(c: &mut Criterion) {
    let rt = runtime();
    let lone = guard(&rt, false);
    let hung = guard(&rt, true);
    let keys: Vec<String> = (0..4096).map(|i| format!("tenant:{i}")).collect();

    let mut group = c.benchmark_group("check");
    group.bench_function("one hot key", |b| {
        b.iter(|| black_box(lone.check(black_box("api:tenant-42"))))
    });
    let mut i = 0;
    group.bench_function("4096 keys in turn", |b| {
        b.iter(|| {
            i = (i + 1) % keys.len();
            black_box(lone.check(black_box(&keys[i])))
        })
    });
    group.bench_function("one hot key, network hung", |b| {
        b.iter(|| black_box(hung.check(black_box("api:tenant-42"))))
    });
    group.finish();
}

// Many threads at once, per check: one key they all fight over, and one
// key each.
fn contended(c: &mut Criterion) {
    let rt = runtime();
    let guard = guard(&rt, false);
    let mut group = c.benchmark_group("check, 8 threads");
    for (name, shared) in [("one key for all", true), ("a key each", false)] {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let threads = 8;
                let barrier = Barrier::new(threads);
                let total: Duration = std::thread::scope(|s| {
                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let (guard, barrier) = (&guard, &barrier);
                            s.spawn(move || {
                                let key = if shared {
                                    "shared".to_string()
                                } else {
                                    format!("own:{t}")
                                };
                                barrier.wait();
                                let start = Instant::now();
                                for _ in 0..iters {
                                    let _ = black_box(guard.check(&key));
                                }
                                start.elapsed()
                            })
                        })
                        .collect();
                    handles.into_iter().map(|h| h.join().unwrap()).sum()
                });
                total / threads as u32
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(3));
    targets = single_thread, contended
}
criterion_main!(benches);
