# Security

By default the gossip protocol trusts its network. Datagrams are neither
encrypted nor authenticated: anyone who can reach the UDP port can join the
cluster, announce demand that shifts the shares, or declare members dead or
gone. Keep the port on a private network, or behind a firewall that lets in
only your own instances, and sign the gossip.

Two values only the cluster's nodes know close what is open:

- **A gossip secret**, the same on every node. Each datagram carries an
  HMAC-SHA256 tag made with it, cut to 16 bytes: 18 bytes more per
  datagram. Under `GossipAuth::Required` a node drops every datagram
  without a valid tag, so a stranger can neither join nor forge news of
  members or demand.
- **A key salt**, against chosen keys. Where clients pick the keys, an IP
  address or a user name, one of them could search for a string that
  hashes like someone else's key and spend that key's limit. With a salt,
  keys are hashed with SipHash-1-3 keyed by it rather than xxh3, and
  nobody without the salt can.

```rust
use rateguard::{GossipAuth, Secret};

let secret = Secret::new(std::fs::read("/etc/rateguard/gossip-secret")?)?;
let guard = Guard::builder()
    .bind("0.0.0.0:7946")
    .advertise("10.0.0.3:7946")
    .seeds(["10.0.0.1:7946", "10.0.0.2:7946"])
    .limit(1_000)
    .gossip_auth(GossipAuth::Required, vec![secret])
    .key_salt(std::fs::read("/etc/rateguard/key-salt")?)
    .spawn()?;
```

A secret is at least 32 bytes and a salt at least 16, random ones, such as
`openssl rand 32 > gossip-secret` and `openssl rand 16 > key-salt`. Neither
shows in `Debug`, errors, stats or metrics. The sidecar takes both from
files, a Kubernetes Secret: see its
[README](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar#signed-gossip).
Its secret file holds each secret as a line of base64, not raw bytes: in a
fleet of Rust nodes and sidecars, give `Secret::new` the decoded line, or
the secrets differ and the cluster splits.

What this does not protect against:

- **Eavesdropping.** Nothing is encrypted: whoever sees the traffic reads
  the nodes' addresses, the hashes of keys and their rates.
- **Replay.** A datagram recorded on the way can be sent again, and is
  taken. News of a member loses to newer news, by incarnation, but a
  recorded leave can remove a node again after it restarts, until it
  refutes: that is open (spec §9.1, item 37). A replayed demand report
  makes a node only stricter, never looser, with one exception: a recording
  0.75 to 1 of a round circle old (the round is a `u16`, so about 2.7 to
  3.6 hours at the default 200 ms period) can lower a peer's demand for one
  gap between its reports, about N/2 rounds. Closing that takes a wider
  round on the wire. A reordered datagram may make a node stricter for one
  stale window. A ping's reply must match the probe in flight, and a
  probe's sequence number starts at random, so a recorded reply cannot
  match a probe of a later run. Guarding against replay in full would take
  synchronised clocks, or counters per sender that survive restarts, for no
  gain yet.
- **A node gone rogue.** There is one secret per cluster, and whoever holds
  it can speak for any node.
- **Salts that differ between nodes.** Nothing detects them; see below.

**Without signing**, two checks still bound what a stranger can do. Every
member in a datagram must go by the ID its address gives, as every node
derives its own: a datagram whose sender does not is refused as
`malformed`, and any later record that does not is dropped alone, counted
in `mismatched_records`, so that a peer passing on a forger's news is still
heard. That stops IDs detached from their addresses. It does not stop a stranger from giving a
real member's address, whose ID anyone can compute; keeping out that
impersonation takes signing. And an instance keeps at most
`max_members(n)` members, 8192 by default: news of a newcomer is refused
while its table is full, and counted in `members_refused`. Size it for the
fleet plus every address that churned in the last 10 minutes, as the dead
keep their slots that long: about twice the fleet for rolling deploys that
give pods new IPs; the default covers fleets up to about 4000 instances
under rolling deploys. **Over the cap the limit is not guaranteed.** The
instances that refused a newcomer count the fleet short, and some can
admit more than their share. While its table refuses newcomers, an
instance cannot count them, so it shares the limit as if `max_members`
nodes were up and holds every key at the floor `R × β / max_members`
(`members_full`, with `members_refused` and
`rateguard_held{reason="members_full"}` to show it). That softens an
overflow but does not close it, for two reasons. The refused instance
itself never holds: its own table is not full, so it shares the limit over
the small view it has. And an instance that heard of the newcomer only
second-hand lifts its hold after `tombstone_ttl` while the overflow lasts,
because the gossip about the refused one has run out of retransmits. In
the simulator, ten instances with a cap of 4 admitted 1501 a second
against a bound of 1300, and a fleet over its cap went up to 1.3 times
the limit. Treat any growth
of `members_refused` in a legitimate fleet as a reason to raise the cap
now. That cap bounds memory, not availability. A stranger can still fill the table with
made-up members at addresses of its choosing, and every key falls to that
floor; real newcomers wait for a slot until the made-up ones are found dead
and forgotten. Only signing keeps strangers out. Steady growth of
`members_refused` means either a fleet bigger than the cap or such a
flood.

**Turning signing on** in a running cluster takes three rolling updates,
each on every node before the next starts:

1. Every node on 0.6 with no secret, `GossipAuth::Off`. On the wire that is
   0.5, so old and new nodes run side by side. Go on only once no 0.5 node
   is left: 0.5 cannot read a signed datagram.
2. The secret, in `GossipAuth::Transitional`: such a node signs, and takes
   signed datagrams with a valid tag and unsigned ones; a node still `Off`
   takes signed ones without checking them. Old and new hear each other.
   For the minutes of a rollout only: a forgery passes by going unsigned.
3. `GossipAuth::Required`.

**Turning signing off** walks the same steps backwards: `Transitional` on
every node first, then `Off`. Never go from `Required` straight to `Off`:
for the length of that rollout the `Off` nodes send unsigned datagrams,
which the `Required` ones refuse as `unsigned`, and the cluster splits.

**Rotating** from secret A to secret B, in the same way:

1. `[A, B]`: A signs, either verifies.
2. `[B, A]`: B signs, either verifies.
3. `[B]`.

To undo a rotation, walk its steps backwards in the same way.

A node at step 1 and one at step 3 at once do not hear each other, and the
cluster splits: start a step only when the one before has reached every
node. A node whose secrets match nobody's hears nobody, and shows it in
`stats().rejected_datagrams` and in `rateguard_rejected_datagrams_total`:
`bad_tag` grows on secrets that do not match, `unsigned` under `Required`
on a peer not signing yet, still `Off`, or on a stranger. `guard.set_gossip_auth(mode,
secrets)` takes a step with no restart; the sidecar rereads its secret
file.

**The salt** is set when the cluster is created, and is not rotated with
the secret. Nodes exchange demand by the hash of each key: while some hash
with one salt and others with another, or none, they do not know each
other's keys, and each takes its share as if the others' demand were for
other keys, up to N times the limit for N nodes. Nothing tells: the gossip
carries no trace of the salt. So set or change a salt by restarting every
node at once, never by a rolling update, which would admit up to that much
for as long as it lasts. With a salt, match the hashes of `most_denied` with
`guard.key_hash_in(class, key)`, which knows it, not the free function
`key_hash_in`.
