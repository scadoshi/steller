//! RESP frame parser. Streams `&[u8]` into [`Frame`] values and signals to the caller
//! when more bytes are needed via [`FrameError::Incomplete`].
//!
//! Only two frame shapes are recognized: RESP arrays (`*`) and bulk strings (`$`).
//! That's the entire surface a client uses to send commands; outbound reply types
//! (simple strings, errors, integers, null-bulk) live in the outbound layer.

use crate::{domain::command::cache::write::WriteCommand, resp::crlf::Crlf};
use std::{io::Write, num::ParseIntError, str::Utf8Error};
use thiserror::Error;

type IoError = std::io::Error;

/// Failure parsing a frame.
#[derive(Debug, Error)]
pub enum FrameError {
    /// A bulk-string payload wasn't followed by `\r\n`.
    #[error("missing crlf terminator")]
    MissingTerminator,
    /// The header byte wasn't `*` or `$`.
    #[error("unknown sigil")]
    UnknownSigil,
    /// The header's length bytes didn't form a `usize`.
    #[error("failed to parse length: {0}")]
    InvalidLength(#[from] ParseLengthError),
    /// Not enough bytes yet. The caller reads more from the socket and retries.
    #[error("incomplete frame")]
    Incomplete,
    /// A CRLF-terminated header too short to hold a sigil plus a length.
    #[error("malformed value")]
    Malformed,
}

/// Why the length in a RESP header didn't parse.
#[derive(Debug, Error)]
pub enum ParseLengthError {
    /// The bytes weren't UTF-8.
    #[error(transparent)]
    Utf8(#[from] Utf8Error),
    /// The bytes were UTF-8 but not a `usize`.
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
}

/// A parsed RESP frame. Only the two shapes a client uses to send commands.
///
/// `BulkString` payloads are arbitrary bytes. The parser does not enforce UTF-8.
/// `Array` is recursive (an array of frames), but parsing iterates rather than
/// recurses, so deeply-nested arrays cannot blow the stack.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// `*<n>\r\n` followed by `n` frames.
    Array(Vec<Frame>),
    /// `$<len>\r\n<bytes>\r\n`. Length is taken from the header; payload is bytes-exact.
    BulkString(Vec<u8>),
}

impl Frame {
    /// Parse one frame from the front of `bytes`, returning it and the leftover slice.
    /// [`FrameError::Incomplete`] is the signal the session uses to read more from the
    /// socket before retrying.
    pub fn parse_one(bytes: &[u8]) -> Result<(Frame, &[u8]), FrameError> {
        let Some((header, bytes)) = bytes.split_crlf() else {
            return Err(FrameError::Incomplete);
        };
        let Some((sigil, len_bytes)) = header.split_first() else {
            return Err(FrameError::Malformed);
        };
        if len_bytes.is_empty() {
            return Err(FrameError::Malformed);
        }
        let len = std::str::from_utf8(len_bytes)
            .map_err(ParseLengthError::from)?
            .parse::<usize>()
            .map_err(ParseLengthError::from)?;
        match sigil {
            b'*' => Frame::parse_array(bytes, len),
            b'$' => Frame::parse_bulk_string(bytes, len),
            _ => Err(FrameError::UnknownSigil),
        }
    }

    /// Parse `len` consecutive frames from `bytes` into a [`Frame::Array`]. Iterative
    /// rather than recursive, so an array of N elements uses O(1) stack no matter how big
    /// N gets. That is what stops `MGET key1..key100000` blowing the stack.
    pub fn parse_array(bytes: &[u8], len: usize) -> Result<(Frame, &[u8]), FrameError> {
        let mut vec = Vec::new();
        let mut buf: &[u8] = bytes;
        for _ in 0..len {
            let (value, bytes) = Frame::parse_one(buf)?;
            vec.push(value);
            buf = bytes;
        }
        Ok((Frame::Array(vec), buf))
    }

    /// Parse exactly `len` payload bytes followed by `\r\n`.
    pub fn parse_bulk_string(bytes: &[u8], len: usize) -> Result<(Frame, &[u8]), FrameError> {
        let Some((payload, rest)) = bytes.split_at_checked(len) else {
            return Err(FrameError::MissingTerminator);
        };
        let Some(remaining) = rest.strip_prefix(b"\r\n") else {
            return Err(FrameError::MissingTerminator);
        };
        Ok((Frame::BulkString(payload.to_vec()), remaining))
    }

    pub fn write_to(&self, w: &mut impl Write) -> Result<(), IoError> {
        match self {
            Self::Array(frames) => {
                write!(w, "*{}", frames.len())?;
                w.write_all(b"\r\n")?;
                for f in frames {
                    f.write_to(w)?;
                }
            }
            Self::BulkString(str) => {
                write!(w, "${}", str.len())?;
                w.write_all(b"\r\n")?;
                w.write_all(str)?;
                w.write_all(b"\r\n")?;
            }
        }
        Ok(())
    }
}

/// Encode a mutation as the RESP bytes a client would have sent. This is what the AOF
/// stores, and it is why replay needs no decoder of its own.
///
/// Deadlines go out in the millisecond verbs, `PXAT` and `PEXPIREAT`, rather than the
/// second ones. Those are the forms the parser takes verbatim, so a logged command
/// round-trips through `Command::try_from` unchanged. Writing `EXPIREAT` here would hand
/// replay a millisecond value that the seconds arm would multiply a second time.
///
/// No clock is read here. Everything being encoded is already absolute.
impl From<WriteCommand> for Frame {
    fn from(value: WriteCommand) -> Self {
        match value {
            WriteCommand::Set {
                key,
                value,
                expires_at,
            } => {
                let mut parts = vec![
                    Frame::BulkString(b"SET".to_vec()),
                    Frame::BulkString(key),
                    Frame::BulkString(value),
                ];
                if let Some(expires_at) = expires_at {
                    parts.push(Frame::BulkString(b"PXAT".to_vec()));
                    parts.push(Frame::BulkString(expires_at.get().to_string().into_bytes()));
                }
                Frame::Array(parts)
            }
            WriteCommand::Delete { key } => Frame::Array(vec![
                Frame::BulkString(b"DEL".to_vec()),
                Frame::BulkString(key),
            ]),
            WriteCommand::ExpireAt { key, expires_at } => Frame::Array(vec![
                Frame::BulkString(b"PEXPIREAT".to_vec()),
                Frame::BulkString(key),
                Frame::BulkString(expires_at.get().to_string().into_bytes()),
            ]),
            WriteCommand::Persist { key } => Frame::Array(vec![
                Frame::BulkString(b"PERSIST".to_vec()),
                Frame::BulkString(key),
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_bulk_string_ok_basic() {
        assert_eq!(
            Frame::parse_bulk_string(b"foo\r\n", 3).unwrap(),
            (Frame::BulkString(b"foo".to_vec()), "".as_bytes())
        );
    }
    #[test]
    fn parse_bulk_string_ok_with_inner_terminator() {
        assert_eq!(
            Frame::parse_bulk_string(b"foo\r\nbar\r\n", 8).unwrap(),
            (Frame::BulkString(b"foo\r\nbar".to_vec()), "".as_bytes())
        );
    }
    #[test]
    fn parse_bulk_string_err_missing_terminator() {
        assert!(matches!(
            Frame::parse_bulk_string(b"foo", 1),
            Err(FrameError::MissingTerminator)
        ));
    }
    #[test]
    fn parse_array_ok_basic() {
        assert_eq!(
            Frame::parse_array(b"$3\r\nfoo\r\n$3\r\nbar\r\nfoo", 2).unwrap(),
            (
                Frame::Array(vec![
                    Frame::BulkString(b"foo".to_vec()),
                    Frame::BulkString(b"bar".to_vec()),
                ]),
                "foo".as_bytes()
            )
        );
    }
    #[test]
    fn parse_array_ok_empty() {
        assert_eq!(
            Frame::parse_array(b"", 0).unwrap(),
            (Frame::Array(vec![]), "".as_bytes())
        );
    }
    #[test]
    fn parse_one_ok_basic_bulk_string() {
        assert_eq!(
            Frame::parse_one(b"$3\r\nfoo\r\nbar").unwrap(),
            (Frame::BulkString(b"foo".to_vec()), "bar".as_bytes(),)
        );
    }
    #[test]
    fn parse_one_ok_basic_array() {
        assert_eq!(
            Frame::parse_one(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\nfoo").unwrap(),
            (
                Frame::Array(vec![
                    Frame::BulkString(b"foo".to_vec()),
                    Frame::BulkString(b"bar".to_vec())
                ]),
                "foo".as_bytes(),
            )
        );
    }
    #[test]
    fn parse_one_ok_empty_array() {
        assert_eq!(
            Frame::parse_one(b"*0\r\n").unwrap(),
            (Frame::Array(vec![]), "".as_bytes())
        );
    }
    #[test]
    fn parse_one_ok_nested_array() {
        assert_eq!(
            Frame::parse_one(b"*1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\nbaz").unwrap(),
            (
                Frame::Array(vec![Frame::Array(vec![
                    Frame::BulkString(b"foo".to_vec()),
                    Frame::BulkString(b"bar".to_vec())
                ])]),
                "baz".as_bytes()
            )
        );
    }
    #[test]
    fn parse_one_err_incomplete() {
        assert!(matches!(
            Frame::parse_one(b"foo"),
            Err(FrameError::Incomplete)
        ));
        assert!(matches!(Frame::parse_one(b""), Err(FrameError::Incomplete)));
    }
    #[test]
    fn parse_one_err_malformed() {
        assert!(matches!(
            Frame::parse_one(b"$\r\n"),
            Err(FrameError::Malformed)
        ));
    }
    #[test]
    fn parse_one_err_invalid_length() {
        assert!(matches!(
            Frame::parse_one(b"$\xFF\xFE\r\nbar"),
            Err(FrameError::InvalidLength(ParseLengthError::Utf8(_)))
        ));
        assert!(matches!(
            Frame::parse_one(b"$foo\r\nbar"),
            Err(FrameError::InvalidLength(ParseLengthError::ParseInt(_)))
        ));
    }
    #[test]
    fn parse_one_err_invalid_unknown_sigil() {
        assert!(matches!(
            Frame::parse_one(b"?2\r\n"),
            Err(FrameError::UnknownSigil)
        ));
    }
    #[test]
    fn parse_one_err_invalid_missing_terminator() {
        assert!(matches!(
            Frame::parse_one(b"$3\r\nfoo"),
            Err(FrameError::MissingTerminator)
        ));
    }

    #[test]
    fn write_to_bulk_string() {
        let mut buf = Vec::<u8>::new();
        Frame::BulkString(b"foo".to_vec())
            .write_to(&mut buf)
            .unwrap();
        assert_eq!(buf, b"$3\r\nfoo\r\n".to_vec(),);
    }

    #[test]
    fn to_bytes_array() {
        let mut buf = Vec::<u8>::new();
        Frame::Array(vec![
            Frame::BulkString(b"foo".to_vec()),
            Frame::BulkString(b"bar".to_vec()),
        ])
        .write_to(&mut buf)
        .unwrap();
        assert_eq!(buf, b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".to_vec(),);
    }
}
