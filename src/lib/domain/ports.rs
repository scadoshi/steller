//! The trait boundaries the rest of the system plugs into.
//!
//! Both traits are defined here, in the domain, which is what points the dependency arrow
//! inward. The domain names the capabilities it needs; the adapters satisfy them.
//!
//! [`CacheRepository`] is the driven (outbound) port, implemented by the persistence
//! adapter and called by the service. [`CacheService`] is the driving (inbound) port,
//! implemented by [`Service`](crate::domain::service::Service) and called by the session.
//!
//! The error types are domain-owned too. Adapters map their concrete failures into this
//! vocabulary at the boundary, via the `From` impls in the outbound layer, so the domain
//! never names an outbound error type.

use crate::domain::{
    cache::{Cache, CacheError},
    channels::ChannelsError,
    command::{
        cache::{CacheCommand, write::WriteCommand},
        outcome::CommandOutcome,
    },
};
use thiserror::Error;

/// Failure crossing the persistence boundary, in terms the domain can react to rather
/// than the adapter's own taxonomy.
///
/// One opaque catch-all for now. The domain doesn't branch on persistence failure modes
/// yet, so every adapter error boxes into it. Variants like `Unavailable` or `Corrupt`
/// get carved out when the service needs to decide on one, not before.
#[derive(Debug, Error)]
pub enum RepositoryError {
    /// Opaque catch-all. The adapter boxes any error it has no domain meaning for into
    /// here; the domain treats it as "persistence failed" without inspecting the cause.
    #[error(transparent)]
    Generic(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Outbound persistence port. The service depends on this, not on any concrete persister.
///
/// The `Clone + Send + Sync + 'static` bounds let one repository handle be shared across
/// every session thread and live for the whole process.
pub trait CacheRepository: Clone + Send + Sync + 'static {
    /// Durably log one state-mutating command.
    fn append(&self, command: WriteCommand) -> Result<(), RepositoryError>;
    /// Snapshot the cache and compact the log.
    fn snapshot(&self, cache: &Cache) -> Result<(), RepositoryError>;
}

/// Failure from the service layer, covering everything a command touches.
///
/// `execute` only hits the cache, so it can only fail with [`Cache`](ServiceError::Cache).
/// `execute_logged` also appends, so it can additionally fail with
/// [`Repository`](ServiceError::Repository). The `#[from]` conversions let `?` lift either
/// at the call site.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// The cache operation failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// Logging the mutation failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    /// The subscription registry failed.
    #[error(transparent)]
    Channels(#[from] ChannelsError),
    /// Anything without a more specific service meaning.
    #[error(transparent)]
    Generic(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Inbound command port. Implemented by [`Service`](crate::domain::service::Service) and
/// called by the session, which supplies the parsed
/// [`Command`](crate::domain::command::Command) and renders the returned
/// [`CommandOutcome`] as a RESP reply.
pub trait CacheService: Clone + Send + Sync + 'static {
    /// Run a command against the cache only, with no persistence. This is what replay
    /// uses, where re-logging would duplicate the log.
    fn execute(&self, command: &CacheCommand) -> Result<CommandOutcome, ServiceError>;
    /// Run a command and, if it mutates state, append it to the log. The live client path.
    fn execute_logged(&self, command: CacheCommand) -> Result<CommandOutcome, ServiceError>;
}
