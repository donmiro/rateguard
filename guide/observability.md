# Observability

`guard.stats()` tells what one instance did and how it sees the cluster, as
of the last tick of its background task, 50 ms ago at most. It takes a lock
the request path never takes.

```rust
let stats = guard.stats();
// Since the instance started.
println!("{} admitted, {} denied", stats.admitted, stats.denied);
```

| Field | What a dashboard learns from it |
|---|---|
| `admitted`, `denied` | What this instance decided; in a dry run, `denied` counts what it would have denied |
| `overflowed` | Checks on keys with no place of their own in the key table: if it grows, raise `tracked_keys` |
| `cluster_size`, `counted_size` | Members seen, and the N the limit is shared over. `counted_size` stays higher under `HoldDown` while a lost member is held, and is at least `max_members` while `members_full`; that is a floor on this node's shares, not a guarantee for the fleet, see `max_members(n)` |
| `suspects` | Members not heard from lately; many of them, a network in trouble |
| `joining`, `learning`, `without_quorum`, `members_full` | Why every key is held at the floor `R × β / N`, if one is true: no peer reached yet, peers not yet told this node's demand, no majority under `Quorum`, or a newcomer refused for a full member table in the last 10 minutes (N is then at least `max_members`) |
| `tracked_keys`, `hot_keys` | Keys with state here, and keys coordinated with the peers |
| `since_last_round` | Far over the protocol period, the background task is stuck |
| `running`, `dry_run` | Whether the node still runs, and whether this is a dry run |
| `most_denied` | The keys denied most lately (ten, or `most_denied(k)`), with their `denied_per_sec`, `requests_per_sec` and `shadow_denied_per_sec`, all over the same ten seconds or so: who is being limited, and what share of its requests. By hash: match them with `guard.key_hash_in(class, key)` |
| `shadow_denied`, `most_shadow_denied` | What shadow limits would have denied, in all, and by key, sorted by it: who a new limit would limit |
| `unknown_class` | Checks in a class this instance does not know, held to the default limit |
| `identity_conflicts` | Times this instance had to outbid an `Alive` record of itself it did not make. A restart quicker than its peers noticed adds one; steady growth means two instances advertise one address |
| `members_refused` | News of members not seen before, refused because the member table was full (`max_members(n)`). Steady growth means a fleet bigger than the cap, or a stranger flooding unsigned gossip with made-up members |
| `gossip_auth`, `rejected_datagrams` | How this instance signs its gossip, and the datagrams it refused, by reason: `unsigned`, `bad_tag`, `malformed` (no message, or a sender under an ID its address does not give), and `mismatched_records`: not datagrams but records after the sender's, under an ID their address does not give, dropped from a datagram otherwise taken. A growing `bad_tag`, secrets that do not match the peers'; a growing `unsigned`, a peer not signing yet, or a stranger |

`guard.share(key)` is the rate one key is admitted at on this instance now:
its share of the limit, before the burst; `share_in(class, key)` the same in a
class.

For one key, ask the background task:

```rust
if let Some(k) = guard.key_stats("pro", "tenant:42").await {
    let (requests, denied) = (k.requests_per_sec, k.denied_per_sec);
    println!("{requests:.0} requests a second, {denied:.0} denied");
}
```

A `KeyStats` has `requests_per_sec`, `denied_per_sec`, `shadow_denied_per_sec`,
`share`, the `limit` the key is held to, its own or its class's, and its
`shadow`. `None` means this instance keeps no state for the key, and asking
does not make it. Every number is this instance's alone, averaged over the
last ten seconds or so; to get the fleet's, add them up.

With the `metrics` feature the background task publishes the same, labelled
`guard` with the address the instance advertises:
`rateguard_checks_total{decision="allow"|"deny"}`,
`rateguard_overflowed_total`, `rateguard_unknown_class_total`, and the gauges
`rateguard_cluster_size`, `rateguard_counted_size`, `rateguard_suspects`,
`rateguard_tracked_keys`, `rateguard_hot_keys`,
`rateguard_held{reason="joining"|"learning"|"without_quorum"|"members_full"}`,
`rateguard_dry_run`, the counters `rateguard_shadow_denied_total`,
`rateguard_identity_conflicts_total`, `rateguard_members_refused_total` and
`rateguard_rejected_datagrams_total{reason="unsigned"|"bad_tag"|"malformed"|"mismatched_records"}`
and `rateguard_gossip_auth{mode="off"|"transitional"|"required"}`, 1 for the
instance's mode: everything but `most_denied` and `most_shadow_denied`, whose
key hashes as labels would make a series per key. Install any `metrics`
recorder, Prometheus or another; nothing is published on the request path.
