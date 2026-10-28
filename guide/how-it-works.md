# How it works

The design, what it costs, what it guarantees, and how close it comes, measured.

## Three layers

```
   request ──► [ enforcement ]   local, ~25 ns, exact, no I/O
                     ▲              GCRA over the share this node holds
                     │ "your share: 340 rps"
              [ allocation  ]   over the network, every 200 ms, approximate
                     ▲              demand gossip, shares recomputed
                     │
              [ membership  ]   SWIM: who is alive
```

The idea the whole design rests on: **only allocation is distributed and
eventually consistent.** Enforcement is always local, synchronous and
deterministic.

Three consequences follow, and they are the reason for the split:

- `check()` is an ordinary synchronous function — no executor, no `.await`, no
  lock shared with a network task.
- Clock skew between machines cannot produce a wrong decision. Nodes exchange
  demand estimates, never timestamps that must agree.
- The system is testable in a single-threaded simulator on virtual time.
  Partitions, packet loss and five-second GC pauses are ordinary test cases, and
  there is no `sleep` anywhere in the suite.

## Shares follow demand

Splitting the limit evenly, `R/N` per node, breaks as soon as your load balancer
is uneven: one node starts rejecting while its neighbours sit below their share.

Instead, each node gossips the demand it observes and computes its own share as
`R × (my_demand / total_demand)`. Shares converge on the real traffic
distribution. Absolute values are sent rather than deltas, so a lost datagram
costs 200 milliseconds of accuracy rather than corrupting a counter permanently.

A floor of `R × β / N` is always reserved for every live node, so a node with no
traffic yet can still admit its first requests.

## Only hot keys are coordinated

You may have ten million keys; about seventy fit in a UDP datagram. But
coordination is only needed for keys that are approaching their limit:

- **cold keys** run locally on `R/N × α` (α = 0.5) and generate no network traffic
- **hot keys** join the gossip set and receive a demand-proportional share

Keys are promoted on crossing the threshold and demoted after they cool down.
The bound is provable: traffic that is never coordinated cannot exceed `R × α` —
half the limit at the default α.

## Network partitions

When the network splits into `k` groups, each group believes it is the whole
cluster. No design can avoid that; `rateguard` makes it an explicit choice.

| Policy | Overshoot | Cost |
|---|---|---|
| `Optimistic` | up to `k × R` | none, maximum availability |
| `HoldDown(d)` **(default)** | ≈ `R` for `d`, then `k × R` | the limit is underused for `d` after a real failure |
| `Quorum` | `R + ε` | the minority side stops serving almost entirely |

`HoldDown` is the default because most apparent partitions are a GC pause or a
pod restart lasting seconds, and holding the limit steady through those is nearly
always what you want.

Under `Quorum`, a node that cannot tell a split from a death keeps counting
the members it lost, however long ago. So never remove half the cluster or
more at once: the nodes left would take it for a split and stay at the floor.
Scale down by less than half at a time, and let ten minutes pass between
steps, the time a dead member takes to be forgotten.

## Performance

`check()`, measured with `criterion` on an Apple M4 Pro
(`cargo bench -p rateguard --bench check`):

| Case | Per check |
|---|---|
| One hot key, one thread | 26 ns |
| 4096 keys in turn | 33 ns |
| One hot key, the network hung (the cluster as good as gone) | 25 ns |
| 8 threads, a key each | 27 ns |
| 8 threads on one key, back to back | 1.4–2.1 µs |

The last line is the one cost to know. An exact limit means every check on a key
writes the same word, so cores hitting one key at once queue for it: about 6
million checks a second on a single key per instance. Far beyond what one tenant
sends to one instance in practice, but not free.

## Guarantees

1. `check()` performs no I/O and takes no lock shared with a network task. Its
   latency is independent of cluster state, including a fully collapsed one.
2. A healthy cluster in steady state admits between `R × (1 − ε)` and `R`,
   as long as it fits in `max_members(n)`; over it `R` is not guaranteed,
   see [Security](security.md).
3. Under partition, the bounds above, according to the selected policy.
4. Memory is bounded by configuration: the coordination state is
   `O(hot_keys × N)`, and the per-key table is allocated once: two 64-byte
   slots per tracked key, one cache line each, rounded up to a power of two
   (512 KB at the default 4096).
5. Bandwidth in a healthy cluster is two datagrams per node per protocol
   period: the node's own probe and, on average, one answer to a probe of it.

## Accuracy

Every scenario below runs in the simulator on the settings `rateguard` ships
with: five instances, a limit of 1000 per second on one key, `β = 0.05`,
64 hot keys, a burst of 50 on each instance. The table is generated, not
written: `cargo run -p rateguard-sim --release --bin accuracy -- --write`,
and CI fails if it falls out of date.

<!-- accuracy: begin -->
| Scenario | Design bound | Admitted, steady | Peak second | Seconds over 1.01 R | Settles in |
|---|---|---|---|---|---|
| Even demand, 5 × 400 | R | 1.00 R | 1.00 R | 0 | 3 s |
| Skewed demand, 1200 + 4 × 150 | R | 1.00 R | 1.00 R | 0 | 3 s |
| Demand on 1 of 5 nodes | ≥ R × (1 − 4β/5) = 0.96 R | 0.96 R | 0.96 R | 0 | 2 s |
| Skewed, 30% packet loss | R | 1.00 R | 1.00 R | 0 | 2 s |
| Skewed, 20% duplicated, half delayed up to 6 periods | R | 1.00 R | 1.00 R | 0 | 2 s |
| 3/2 split, Optimistic | up to k × R = 2 R | 1.80 R | 1.80 R | 37 | 4 s |
| 3/2 split, HoldDown(10 s) | ≈ R for 10 s, then up to 2 R | 1.80 R | 1.80 R | 24 | 18 s |
| 3/2 split, HoldDown(60 s), longer than the split | ≈ R | 1.00 R | 1.00 R | 0 | 0 s |
| 3/2 split, Quorum | R + the minority's floors = 1.02 R | 1.02 R | 1.02 R | 33 | 4 s |
| One-way link, 0 → 1 lost | R | 1.00 R | 1.00 R | 0 | 0 s |
| GC pause, 5 s on node 4 (the other four) | their shares unchanged | 0.80 R | 0.80 R | 0 | 9 s |
| Rolling restart, one node every 8 s | ≤ R, no second without service | 1.00 R | 1.00 R | 0 | 2 s |
| Node 4 crashes for good (the other four) | R once the suspicion and the hold are over | 1.00 R | 1.00 R | 0 | 17 s |
| Node 4 leaves for good (the other four) | R at once | 1.00 R | 1.00 R | 0 | 1 s |
| Traffic jumps 100×, cold to hot | R | 1.00 R | 1.00 R | 0 | 3 s |
| A node joins 10 s late | R | 1.00 R | 1.01 R | 0 | 1 s |
| Limit lowered to R/2, one node a second | ≤ R in passing, R/2 after | 0.50 R | 1.00 R | 0 | 3 s |
| Limit raised from R/2 to R, one node a second | ≤ R, then R | 1.00 R | 1.00 R | 0 | 3 s |

Shadow at R/5, skewed demand on R: shadow denials vs. enforced, 1600.0 vs. 1600.0 a second (+0.0%).
<!-- accuracy: end -->

*Admitted, steady* is the rate once the scenario has settled, over a window
after its event; *peak second* the busiest second of the whole run;
*settles in* the time from the event until every second stays within 5% of
the steady rate. Bursts let an instance admit a little over its share now and
then, which is why only seconds above 1.01 R are counted as over.

Two rows deserve a word. Under `Quorum` the minority keeps its floors, which
is the `ε` of `R + ε`: 2% here. And after a GC pause the paused instance
works through the requests that queued up meanwhile, its demand estimate
jumps, and the others give way for a few seconds: the cluster dips *under*
the limit, never over it.
