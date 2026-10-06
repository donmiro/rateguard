//! A deterministic simulator for rateguard: whole clusters of the real core
//! in one thread, on virtual time.
//!
//! A run is a pure function of its schedule and its seeds, so every failure
//! replays (see [`seed`]). The network is a [`link::Link`]; nodes can be
//! paused and restarted ([`sim::Sim::pause`], [`sim::Sim::schedule_restart`]);
//! [`invariant`]s are checked after every event.

pub mod invariant;
pub mod link;
pub mod seed;
pub mod sim;
