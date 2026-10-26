/// A command that reads one key without changing its value.
///
/// Reads still go through lazy expiry, so any of these can drop a key whose deadline has
/// passed.
#[derive(Debug, Clone, PartialEq)]
#[expect(missing_docs, reason = "each field is the key its command names")]
pub enum ReadCommand {
    /// `GET`: the value as a bulk string, or a null bulk on a miss.
    Get { key: Vec<u8> },
    /// `EXISTS` with a single key: `1` if it is present, `0` if not.
    Exists { key: Vec<u8> },
    /// `TTL`: whole seconds left, rounded up, or `-1` with no TTL and `-2` with no key.
    Ttl { key: Vec<u8> },
}

impl ReadCommand {
    /// A `GET` of `key`.
    pub fn get(key: impl Into<Vec<u8>>) -> Self {
        Self::Get { key: key.into() }
    }

    /// An `EXISTS` of `key`.
    pub fn exists(key: impl Into<Vec<u8>>) -> Self {
        Self::Exists { key: key.into() }
    }

    /// A `TTL` of `key`.
    pub fn ttl(key: impl Into<Vec<u8>>) -> Self {
        Self::Ttl { key: key.into() }
    }
}
