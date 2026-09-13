//! The commands that change state, and therefore the ones that get logged.
//!
//! Every deadline here is an absolute [`Milliseconds`] timestamp. The relative forms
//! (`EXPIRE`, `SET … EX`, `SET … PX`) are converted at parse time and have no
//! representation in this module. That is deliberate: a relative TTL that cannot be built
//! cannot be written to the AOF, which is what keeps replay time-invariant.

use crate::domain::time::Milliseconds;

#[derive(Debug, Clone, PartialEq)]
pub enum WriteCommand {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        /// Absolute deadline, or `None` for no expiry. A plain `SET` clears any TTL the
        /// key already had, which is what Redis does.
        expires_at: Option<Milliseconds>,
    },
    Delete {
        key: Vec<u8>,
    },
    ExpireAt {
        key: Vec<u8>,
        expires_at: Milliseconds,
    },
    Persist {
        key: Vec<u8>,
    },
}

impl WriteCommand {
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

    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete { key: key.into() }
    }

    pub fn expire_at(key: impl Into<Vec<u8>>, expires_at: Milliseconds) -> Self {
        Self::ExpireAt {
            key: key.into(),
            expires_at,
        }
    }

    pub fn persist(key: impl Into<Vec<u8>>) -> Self {
        Self::Persist { key: key.into() }
    }
}
