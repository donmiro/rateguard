// The README's example of the core on its own, word for word between the
// markers, so that it cannot go stale unnoticed. The test in the rateguard
// crate checks that the README still says the same.

use rateguard_proto::Address;

fn serve() {}
fn reject(_retry_at: u64) {}
fn send(_peer: rateguard_core::boundary::PeerId, _bytes: &[u8]) {}

#[test]
fn without_the_network() {
    let my_address = Address::V4([10, 0, 0, 1], 7946);
    let seed_address = Address::V4([10, 0, 0, 2], 7946);
    let rng_seed = 7;
    let key_hash = 42;
    let now_nanos = 0;
    let from = rateguard_core::boundary::PeerId::new(0);
    let datagram: Vec<u8> = Vec::new();

    // README: begin
    use rateguard_core::{
        boundary::{Action, Event, PeerId},
        gcra::Decision,
        limiter::Config,
        node::{Node, SwimConfig},
    };

    const ONE_SEC: u64 = 1_000_000_000;

    let mut node = Node::new(
        Config {
            limit_per_sec: 1_000,
            burst: 10,
            alpha: 0.5,
            floor_factor: 0.05,
            cooldown: 5 * ONE_SEC,
            hot_set_size: 64,
            max_tracked_keys: 4_096,
            demand_time_constant: ONE_SEC,
        },
        SwimConfig::default(),
        PeerId::new(1), // this node
        my_address,     // a rateguard_proto::Address, gossiped so others can reach it
        rng_seed,       // drives its random choices: same seed, same run
    );
    node.add_seed(PeerId::new(0), seed_address); // N comes from membership

    // Send each datagram to node.address(peer); a received datagram's sender is
    // the member of its first record, see the rateguard crate.

    // Keys are 64-bit hashes; the original string never leaves your process.
    match node.check(key_hash, now_nanos) {
        Decision::Allow => serve(),
        Decision::Deny { retry_at } => reject(retry_at),
    }

    // From your own timer (several times per protocol period) and socket.
    for Action::SendTo { peer, bytes } in node.handle(Event::Tick, now_nanos) {
        send(*peer, bytes);
    }
    node.handle(
        Event::MessageReceived {
            from,
            bytes: &datagram,
        },
        now_nanos,
    );
    // README: end
}
