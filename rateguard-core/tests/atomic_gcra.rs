// AtomicGcra from many threads. Out here rather than next to it: the core's
// sources must not spawn anything, tests included (see sans_io.rs).

use rateguard_core::gcra::{AtomicGcra, Decision, Quota};

#[test]
fn threads_hammering_one_key_get_exactly_the_burst() {
    let g = std::sync::Arc::new(AtomicGcra::new(Quota::new(100, 10)));
    let allowed: usize = (0..8)
        .map(|_| {
            let g = g.clone();
            std::thread::spawn(move || {
                (0..10_000)
                    .filter(|_| g.check(1_000) == Decision::Allow)
                    .count()
            })
        })
        .map(|t| t.join().unwrap())
        .sum();
    assert_eq!(
        allowed, 10,
        "one burst at one instant, however many threads"
    );
}
