//! The commands that change state, and therefore the ones that get logged.
//!
//! Every deadline here is an absolute [`Milliseconds`] timestamp. The relative forms
//! (`EXPIRE`, `SET … EX`, `SET … PX`) are converted at parse time and have no
//! representation in this module. That is deliberate: a relative TTL that cannot be built
//! cannot be written to the AOF, which is what keeps replay time-invariant.

use crate::domain::time::Milliseconds;

/// A command that changes state. The executor applies it to the cache and then appends it
/// to the AOF, and replay feeds the same values back through the cache.
#[derive(Debug, Clone, PartialEq)]
#[expect(
    missing_docs,
    reason = "each key and value field is the key or value its command names"
)]
pub enum WriteCommand {
    /// `SET`, with any `EX`, `PX`, or `EXAT` option already turned into an absolute
    /// deadline.
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        /// Absolute deadline, or `None` for no expiry. A plain `SET` clears any TTL the
        /// key already had, which is what Redis does.
        expires_at: Option<Milliseconds>,
    },
    /// `DEL` of a single key.
    Delete { key: Vec<u8> },
    /// `EXPIRE`, `EXPIREAT`, or `PEXPIREAT`. All three land here with an absolute deadline.
    ExpireAt {
        key: Vec<u8>,
        /// Absolute UNIX millisecond deadline. One already past deletes the key.
        expires_at: Milliseconds,
    },
    /// `PERSIST`: drop the key's TTL and keep its value.
    Persist { key: Vec<u8> },
}

impl WriteCommand {
    /// A `SET` of `key` to `value`, with an optional absolute deadline.
    pub fn set(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        expires_at: Option<Milliseconds>,
    ) -> Self {
        Self::Set {
            key: key.into(),
            value: value.into(),
            expires_at,
        }
    }

    /// A `DEL` of `key`.
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete { key: key.into() }
    }

    /// A deadline of `expires_at`, in absolute UNIX milliseconds, on `key`.
    pub fn expire_at(key: impl Into<Vec<u8>>, expires_at: Milliseconds) -> Self {
        Self::ExpireAt {
            key: key.into(),
            expires_at,
        }
    }

    /// A `PERSIST` of `key`.
    pub fn persist(key: impl Into<Vec<u8>>) -> Self {
        Self::Persist { key: key.into() }
    }
}
