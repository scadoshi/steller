//! Outbound RESP: the [`Reply`] enum the server emits and the [`SimpleInner`] newtype
//! that guards RESP's "no CR/LF in simple frames" invariant at construction time.

use std::{
    io::Write,
    ops::{Deref, DerefMut},
};
use thiserror::Error;

/// Errors returned by [`SimpleInner::try_from`] when payload bytes contain a
/// forbidden character. Simple-string and simple-error frames must not carry `\r` or
/// `\n` because RESP uses those bytes as the frame terminator.
#[derive(Debug, Error)]
pub enum SimpleInnerError {
    #[error("must not contain a carriage return: (\"\\r\")")]
    IncludesCarriageReturn,
    #[error("must not contain a line feed (\"\\n\")")]
    IncludesLineFeed,
}

/// Validated payload for a RESP simple string or simple error.
///
/// Construction enforces the no-CR/LF rule, so once a `SimpleInner` exists it's
/// guaranteed safe to write between a sigil byte and a `\r\n` terminator. The serializer
/// in [`Reply::write_to`] relies on that.
///
/// Three constructors are exposed:
///
/// - [`SimpleInner::ok`] and [`SimpleInner::pong`], trusted constants for normal replies.
/// - [`SimpleInner::sanitized`], for arbitrary error message bytes. Strips `\r` and `\n`
///   defensively instead of returning a `Result` (errors crossing this boundary should
///   never themselves be a source of new errors).
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleInner(Vec<u8>);

impl TryFrom<&[u8]> for SimpleInner {
    type Error = SimpleInnerError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.contains(&b'\r') {
            return Err(SimpleInnerError::IncludesCarriageReturn);
        }
        if value.contains(&b'\n') {
            return Err(SimpleInnerError::IncludesLineFeed);
        }
        Ok(Self(value.to_vec()))
    }
}

impl SimpleInner {
    /// Borrow the validated payload bytes for serialization.
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Trusted constructor for the canonical `+OK\r\n` reply payload.
    pub fn ok() -> Self {
        Self(b"OK".to_vec())
    }

    /// Trusted constructor for the canonical `+PONG\r\n` reply payload.
    pub fn pong() -> Self {
        Self(b"PONG".to_vec())
    }

    /// Strip any `\r` or `\n` bytes from `bytes` and wrap the result. Use this when
    /// the payload comes from a `Display`-formatted error or any other source where
    /// CR/LF is plausible. Guarantees a valid frame without forcing the caller to
    /// handle a `Result`.
    pub fn sanitized(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes
            .into()
            .into_iter()
            .filter(|b| *b != b'\r' && *b != b'\n')
            .collect();
        Self(bytes)
    }
}

/// A RESP reply the server can write back to a client. Variants cover every reply
/// shape this server emits today.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// `+<payload>\r\n`, such as `+OK` or `+PONG`.
    SimpleString(SimpleInner),
    /// `-<payload>\r\n`, such as `-ERR unknown command`.
    SimpleError(SimpleInner),
    /// `$<len>\r\n<bytes>\r\n`, the binary-safe value reply. A GET hit, or PING with a message.
    BulkString(Vec<u8>),
    /// `$-1\r\n`, the RESP "no such key" sentinel GET returns on a miss.
    NullBulk,
    /// `:<n>\r\n`, used for boolean-as-int replies (EXISTS, DEL, EXPIRE, PERSIST) and
    /// for TTL's `-2`/`-1`/`n` ladder.
    Integer(i64),
    /// `*<len>\r\n` followed by each element serialized in turn. RESP arrays are
    /// heterogeneous, so elements are themselves [`Reply`]s. Used for pub/sub acks
    /// (`["subscribe", channel, count]`) and message pushes (`["message", channel, payload]`),
    /// and reused by MULTI/EXEC later.
    Array(Vec<Reply>),
}

impl Reply {
    /// Serialize this reply onto `w` as RESP bytes. Uses `write_all` so partial writes
    /// don't leave a half-formed frame on the wire; the caller is expected to flush
    /// after dispatching a reply (the session does this).
    pub fn write_to(&self, w: &mut impl Write) -> std::io::Result<()> {
        w.write_all(self.to_bytes().as_slice())
    }

    /// Serialize this reply to an owned RESP byte buffer. This is the canonical serializer;
    /// [`write_to`](Self::write_to) is a thin wrapper that streams these bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            Self::SimpleString(inner) => {
                b.extend_from_slice(b"+");
                b.extend_from_slice(inner.as_bytes());
                b.extend_from_slice(b"\r\n");
            }
            Self::SimpleError(inner) => {
                b.extend_from_slice(b"-");
                b.extend_from_slice(inner.as_bytes());
                b.extend_from_slice(b"\r\n");
            }
            Self::BulkString(bytes) => {
                b.extend_from_slice(b"$");
                b.extend_from_slice(bytes.len().to_string().as_bytes());
                b.extend_from_slice(b"\r\n");
                b.extend_from_slice(bytes);
                b.extend_from_slice(b"\r\n");
            }
            Self::NullBulk => {
                b.extend_from_slice(b"$-1\r\n");
            }
            Self::Integer(int) => {
                b.extend_from_slice(b":");
                b.extend_from_slice(int.to_string().as_bytes());
                b.extend_from_slice(b"\r\n");
            }
            Self::Array(replies) => {
                b.extend_from_slice(b"*");
                b.extend_from_slice(replies.len().to_string().as_bytes());
                b.extend_from_slice(b"\r\n");
                for reply in replies {
                    b.extend_from_slice(reply.to_bytes().as_slice());
                }
            }
        }
        b
    }
}

/// Several independent top-level RESP frames sent back to back, *not* a single array.
/// A command like `SUBSCRIBE a b` produces one frame per channel; this batches them so they
/// serialize concatenated, with no enclosing array header.
#[derive(Debug, Clone)]
pub struct Replies {
    pub inner: Vec<Reply>,
}

impl Replies {
    /// Stream every frame, in order, onto `buf`.
    pub fn write_to(&self, buf: &mut impl Write) -> std::io::Result<()> {
        for reply in &self.inner {
            reply.write_to(buf)?;
        }
        Ok(())
    }

    /// Serialize every frame into one flat buffer, concatenated with no wrapping header.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.inner.iter().flat_map(Reply::to_bytes).collect()
    }
}

impl Deref for Replies {
    type Target = Vec<Reply>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Replies {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn simple_inner_from_bytes_ok() {
        assert_eq!(
            SimpleInner::try_from(b"foo".as_slice()).unwrap(),
            SimpleInner(b"foo".to_vec())
        );
    }
    #[test]
    fn simple_inner_from_bytes_err_cr() {
        assert!(matches!(
            SimpleInner::try_from(b"foo\r".as_slice()),
            Err(SimpleInnerError::IncludesCarriageReturn)
        ));
    }
    #[test]
    fn simple_inner_from_bytes_err_lf() {
        assert!(matches!(
            SimpleInner::try_from(b"foo\n".as_slice()),
            Err(SimpleInnerError::IncludesLineFeed)
        ));
    }
    #[test]
    fn write_to_ok_simple_string() {
        let mut buf = Vec::new();
        Reply::SimpleString(SimpleInner::try_from(b"foo".as_slice()).unwrap())
            .write_to(&mut buf)
            .unwrap();
        assert_eq!(buf, b"+foo\r\n");
    }
    #[test]
    fn write_to_ok_simple_error() {
        let mut buf = Vec::new();
        Reply::SimpleError(SimpleInner::try_from(b"foo".as_slice()).unwrap())
            .write_to(&mut buf)
            .unwrap();
        assert_eq!(buf, b"-foo\r\n");
    }
    #[test]
    fn write_to_ok_bulk_string() {
        let mut buf = Vec::new();
        Reply::BulkString(b"foo\r\nbar".to_vec())
            .write_to(&mut buf)
            .unwrap();
        assert_eq!(buf, b"$8\r\nfoo\r\nbar\r\n");
    }
    #[test]
    fn write_to_ok_bulk_string_empty() {
        let mut buf = Vec::new();
        Reply::BulkString(b"".to_vec()).write_to(&mut buf).unwrap();
        assert_eq!(buf, b"$0\r\n\r\n");
    }
    #[test]
    fn write_to_ok_null_bulk() {
        let mut buf = Vec::new();
        Reply::NullBulk.write_to(&mut buf).unwrap();
        assert_eq!(buf, b"$-1\r\n");
    }
    #[test]
    fn write_to_ok_integer() {
        let mut buf = Vec::new();
        Reply::Integer(123).write_to(&mut buf).unwrap();
        assert_eq!(buf, b":123\r\n");
    }
    #[test]
    fn write_to_ok_integer_negative() {
        let mut buf = Vec::new();
        Reply::Integer(-123).write_to(&mut buf).unwrap();
        assert_eq!(buf, b":-123\r\n");
    }

    // ---------- Array ----------

    #[test]
    fn write_to_array_empty() {
        let mut buf = Vec::new();
        Reply::Array(vec![]).write_to(&mut buf).unwrap();
        assert_eq!(buf, b"*0\r\n");
    }

    #[test]
    fn write_to_array_mixed_elements() {
        // The subscribe ack shape: two bulk strings + an integer.
        let mut buf = Vec::new();
        Reply::Array(vec![
            Reply::BulkString(b"subscribe".to_vec()),
            Reply::BulkString(b"foo".to_vec()),
            Reply::Integer(1),
        ])
        .write_to(&mut buf)
        .unwrap();
        assert_eq!(buf, b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n");
    }

    #[test]
    fn write_to_array_nested() {
        let mut buf = Vec::new();
        Reply::Array(vec![
            Reply::Array(vec![Reply::Integer(1)]),
            Reply::Integer(2),
        ])
        .write_to(&mut buf)
        .unwrap();
        assert_eq!(buf, b"*2\r\n*1\r\n:1\r\n:2\r\n");
    }

    // The header-once bug lived here: `to_bytes` must agree with `write_to` byte-for-byte.
    #[test]
    fn to_bytes_array_matches_write_to() {
        let reply = Reply::Array(vec![
            Reply::BulkString(b"message".to_vec()),
            Reply::BulkString(b"foo".to_vec()),
            Reply::BulkString(b"bar".to_vec()),
        ]);
        let mut buf = Vec::new();
        reply.write_to(&mut buf).unwrap();
        assert_eq!(reply.to_bytes(), buf);
        assert_eq!(
            reply.to_bytes(),
            b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        );
    }

    #[test]
    fn to_bytes_array_empty_matches_write_to() {
        let reply = Reply::Array(vec![]);
        let mut buf = Vec::new();
        reply.write_to(&mut buf).unwrap();
        assert_eq!(reply.to_bytes(), buf);
        assert_eq!(reply.to_bytes(), b"*0\r\n");
    }

    // ---------- Replies (several top-level frames back to back, no outer wrapper) ----------

    #[test]
    fn replies_to_bytes_concatenates_frames_without_wrapper() {
        // SUBSCRIBE foo bar emits two *separate* arrays, not one array-of-arrays.
        let replies = Replies {
            inner: vec![
                Reply::Array(vec![
                    Reply::BulkString(b"subscribe".to_vec()),
                    Reply::BulkString(b"foo".to_vec()),
                    Reply::Integer(1),
                ]),
                Reply::Array(vec![
                    Reply::BulkString(b"subscribe".to_vec()),
                    Reply::BulkString(b"bar".to_vec()),
                    Reply::Integer(2),
                ]),
            ],
        };
        assert_eq!(
            replies.to_bytes(),
            b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n\
              *3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n"
        );
    }

    #[test]
    fn replies_to_bytes_empty_is_empty() {
        let replies = Replies { inner: vec![] };
        assert_eq!(replies.to_bytes(), b"");
    }

    #[test]
    fn replies_to_bytes_matches_write_to() {
        let replies = Replies {
            inner: vec![Reply::Integer(1), Reply::Integer(2)],
        };
        let mut buf = Vec::new();
        replies.write_to(&mut buf).unwrap();
        assert_eq!(replies.to_bytes(), buf);
    }
}
