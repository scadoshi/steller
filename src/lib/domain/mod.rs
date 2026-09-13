//! Domain types. Wire- and storage-agnostic.
//!
//! - [`cache`] is the Arc-shared KV store with TTL and lazy plus active expiry. Purely
//!   in-memory; durability lives behind the persister.
//! - [`command`] is the parsed [`command::Command`] enum, produced by the RESP layer and
//!   consumed by the service.
//! - [`ports`] holds the trait boundaries the adapters plug into.
//! - [`service`] composes a cache with a repository to execute commands and log them.
//! - [`time`] owns the clock and the seconds-to-millis conversion.
//!
//! Nothing here knows about TCP, RESP, or files. That boundary is what lets the wire
//! format or the persistence strategy change without touching the core.

pub mod cache;
pub mod channels;
pub mod command;
pub mod ports;
pub mod service;
pub mod time;
