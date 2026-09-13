//! `Crlf`: byte-slice utilities for the `\r\n` terminator RESP uses between every
//! header and payload. Implemented for `[u8]` so callers can write `bytes.split_crlf()`
//! on any slice without ceremony.

/// Operations for inspecting and splitting on the RESP `\r\n` terminator.
///
/// Both methods are non-allocating and return slices that borrow from the receiver.
pub trait Crlf {
    /// `true` if the slice starts with exactly the two bytes `\r\n`.
    /// Returns `false` for shorter inputs or any other prefix.
    fn is_crlf(&self) -> bool;

    /// Split on the *first* `\r\n` in the slice. Returns `(before, after)` excluding
    /// the terminator itself.
    ///
    /// Returns `None` when no `\r\n` is found. That `None` is load-bearing one layer up:
    /// it is the parser's "incomplete frame, read more" signal.
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
        let p = self
            .windows(2)
            .position(|w| w[0] == b'\r' && w[1] == b'\n')?;
        Some((&self[..p], &self[p + 2..]))
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
