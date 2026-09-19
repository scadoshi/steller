//! Domain types. Nothing here knows about TCP, RESP, or files.
//!
//! [`cache`] is the Arc-shared KV store, in-memory only; durability lives behind the
//! persister. [`command`] is the parsed [`command::Command`], produced by the RESP layer
//! and consumed by [`service`], which composes a cache with a repository to execute
//! commands and log them. [`ports`] holds the traits the adapters plug into, and [`time`]
//! owns the clock and the seconds-to-millis conversion.

pub mod cache;
pub mod channels;
pub mod command;
pub mod ports;
pub mod service;
pub mod time;
