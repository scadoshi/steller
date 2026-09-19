//! `Crlf`: byte-slice utilities for the `\r\n` terminator RESP uses between every
//! header and payload. Implemented for `[u8]` so callers can write `bytes.split_crlf()`
//! on any slice without ceremony.

/// Non-allocating inspection and splitting on the `\r\n` terminator.
pub trait Crlf {
    /// `true` if the slice starts with `\r\n`.
    fn is_crlf(&self) -> bool;

    /// Split on the first `\r\n`, dropping the terminator. `None` when there isn't one,
    /// which one layer up is the parser's "incomplete frame, read more" signal.
    fn split_crlf(&self) -> Option<(&[u8], &[u8])>;
}

impl Crlf for [u8] {
    fn is_crlf(&self) -> bool {
        if let (Some(first_byte), Some(second_byte)) = (self.first(), self.get(1)) {
            *first_byte == b'\r' && *second_byte == b'\n'
        } else {
            false
        }
    }

    fn split_crlf(&self) -> Option<(&[u8], &[u8])> {
        let p = self.windows(2).position(|w| w == b"\r\n")?;
        let (before, rest) = self.split_at_checked(p)?;
        let after = rest.get(2..)?;
        Some((before, after))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn is_clrf_true() {
        assert!(b"\r\n".is_crlf());
    }
    #[test]
    fn is_clrf_false() {
        assert!(!b"abc".is_crlf());
    }
    #[test]
    fn split_crlf_some() {
        assert_eq!(
            b"a\r\na".split_crlf(),
            Some(("a".as_bytes(), "a".as_bytes()))
        );
        assert_eq!(b"a\r\n".split_crlf(), Some(("a".as_bytes(), "".as_bytes())));
        assert_eq!(b"\r\na".split_crlf(), Some(("".as_bytes(), "a".as_bytes())));
        assert_eq!(b"\r\n".split_crlf(), Some(("".as_bytes(), "".as_bytes())));
    }
    #[test]
    fn split_crlf_none() {
        assert!(b"a".split_crlf().is_none());
        assert!(b"".split_crlf().is_none());
    }
}
