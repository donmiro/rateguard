//! The sans-I/O core of rateguard: every decision, none of the I/O.
//!
//! Nothing here opens a socket, reads a clock or spawns a thread. Time is a
//! parameter of every entry point, and the network is a list of
//! [`Action`](boundary::Action)s for the caller to carry out. The same code
//! therefore runs in the real runtime and, deterministically, in the
//! simulator.
//!
//! The layers, bottom up:
//!
//! - enforcement: [`gcra`], [`demand`] and [`hot_set`], put together by
//!   [`limiter`];
//! - membership: [`member`], SWIM's member table, behind the
//!   [`Membership`](membership::Membership) trait;
//! - [`node`]: one cluster member, driving both through events.

pub mod boundary;
pub mod demand;
pub mod gcra;
pub(crate) mod gossip;
pub mod hot_set;
pub mod limiter;
pub mod member;
pub mod membership;
pub mod node;
pub mod rng;
