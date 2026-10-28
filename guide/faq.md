# FAQ

**Is there a coordinator, leader, or master node?** No. Every node runs the same
code and reaches its own conclusions. There is nothing to elect and nothing whose
loss stops the cluster.

**What happens when a node dies?** Membership notices within a few protocol
periods and the remaining nodes redistribute its share among themselves. Until
that happens, the fleet is under the limit rather than over it.

**What happens when a node restarts?** It rejoins through its seeds. Until it
has, and until its peers have heard its demand, every key it serves is held at
a floor, `R × β` split over the members it knows of (under `Optimistic`,
before it rejoins, at the cold-key share instead). A restart does not hand it a fresh full quota.

**And when a node stops on purpose, in a rolling deploy?** If it calls
`guard.leave()`, or drops its last `Guard` while the runtime still runs, it
tells its peers it is going, and they take its share within a round: a deploy
runs at the limit throughout. Requests it still serves afterwards get the
floor `R × β / N`, which its peers no longer count, so draining ones admit a
little over the limit. An instance that just stops looks like a crash: its
peers notice within the suspicion timeout (six seconds), and under the
default `HoldDown` keep its share out of use for the hold, ten seconds, so
the fleet runs a little under the limit meanwhile, never over it. Instances
of a version from before `leave` do not understand the news and treat the
departure as a crash.

**How do I health-check it?** `guard.is_running()` turns false if the
background task has died, which is a bug; the instance then holds every key at
the floor `R × β / N` rather than at shares its peers no longer count.
`guard.cluster_size()` tells you how many members it sees, itself included, and
`guard.stats()` the rest, see [Observability](observability.md).

**Does clock skew matter?** No. Nodes never compare timestamps with each other;
each uses only its own monotonic clock, and what travels between them are demand
estimates.

**How much traffic does gossip generate?** In a healthy cluster, two datagrams
per node per protocol period (200 ms by default), each capped at a 1400-byte
MTU. A fifty-node cluster exchanges at most about 700 KB/s in total, no matter
how much traffic it serves. Failures add indirect probes for as long as they
last.

**Can I use it from services that aren't in Rust?** Yes, through
`rateguard-sidecar` and the clients in `sdk/`: see
[From other languages](https://github.com/donmiro/rateguard#from-other-languages). A check then costs a round
trip on loopback, a fraction of a millisecond, rather than 25 ns.
