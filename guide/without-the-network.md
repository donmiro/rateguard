# Without the network

`rateguard-core` is the whole protocol without the I/O: enforcement and
membership, no sockets, no tokio, time passed in as a parameter. Bring your
own UDP socket and timer, feed it events, and send the datagrams it returns.

```rust
use rateguard_core::{
    boundary::{Action, Event, PeerId},
    gcra::Decision,
    limiter::{Config, Limit},
    node::{Node, SwimConfig},
};

const ONE_SEC: u64 = 1_000_000_000;

let mut node = Node::new(
    Config {
        limit: Limit {
            rate: 1_000.0,
            burst: 10,
        },
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
for action in node.handle(Event::Tick, now_nanos) {
    if let Action::SendTo { peer, bytes } = action {
        send(*peer, bytes);
    }
}
node.handle(
    Event::MessageReceived {
        from,
        bytes: &datagram,
    },
    now_nanos,
);
```
