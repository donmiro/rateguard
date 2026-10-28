# The rateguard guide

The [README](../README.md) gets one guard running. This is the rest.

- [Using rateguard](usage.md): keys, classes of limits, limits per minute
  and hour, a key's own limit, shadow limits, a dry run, axum, and every
  option.
- [Observability](observability.md): `stats()`, one key's counters, and the
  metrics.
- [How it works](how-it-works.md): the three layers, shares that follow
  demand, hot keys, network partitions, performance, the guarantees, and the
  accuracy report.
- [Security](security.md): signed gossip, the key salt, turning signing on
  and rotating secrets, and what is not protected.
- [Without the network](without-the-network.md): `rateguard-core`, the
  protocol with no I/O, for your own sockets and timers.
- [FAQ](faq.md).

For services in other languages, the
[sidecar's README](../rateguard-sidecar/README.md) and the clients in
[`sdk/`](../sdk). The API is on [docs.rs](https://docs.rs/rateguard).
