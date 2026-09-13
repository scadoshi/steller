//! diprotodon, a minimal Redis-compatible KV server.
//!
//! Speaks RESP over TCP, so real `redis-cli` clients work. In-memory store with disk
//! durability, lazy and active expiry, graceful shutdown.
//!
//! ## Layout
//!
//! Ports and adapters. The dependency arrow points inward: adapters depend on the domain,
//! never the reverse.
//!
//! - [`domain`] holds the core: `Cache`, `Entry`, `Command`, the `Service` orchestrator,
//!   and the `ports` the adapters plug into. Nothing here knows about RESP, TCP, or files.
//! - [`inbound`] is the driving adapter: accept loop plus per-connection session.
//! - [`outbound`] is the driven adapter: the persister behind the `CacheRepository` port.
//! - [`resp`] is the wire codec. Both other layers use it, so it sits beside them rather
//!   than inside either.
//!
//! `src/main.rs` just calls [`inbound::server::Server::run`].

pub mod domain;
pub mod inbound;
pub mod outbound;
pub mod resp;

#[cfg(test)]
pub mod test_support;
