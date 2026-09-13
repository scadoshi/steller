//! Lifts a parsed RESP [`Frame`] into a domain [`Command`].
//!
//! A valid command on the wire is always an array of bulk strings: the first is the
//! verb (case-insensitive), the rest are arguments. Anything else is a
//! [`CommandFromFrameError::UnexpectedFrame`]: a bare bulk string, an array whose first
//! element is an array, an inner array where a key belongs. Verb-level errors (wrong arity,
//! non-numeric TTL, unknown verb) surface as [`CommandError`].

use crate::{
    domain::{
        command::{Command, CommandError},
        time::{Milliseconds, Seconds},
    },
    resp::frame::Frame,
};
use std::time::SystemTimeError;
use thiserror::Error;

/// Pull the next frame and require it to be a bulk string.
macro_rules! next_bulk {
    ($iter:expr) => {{
        let Some(frame) = $iter.next() else {
            return Err(CommandError::NotEnoughParts.into());
        };
        let Frame::BulkString(bytes) = frame else {
            return Err(CommandFromFrameError::UnexpectedFrame);
        };
        bytes
    }};
}

/// Parse a bulk string as a `u64`.
fn parse_u64(bytes: &[u8]) -> Result<u64, CommandFromFrameError> {
    Ok(std::str::from_utf8(bytes)
        .map_err(CommandError::from)?
        .parse::<u64>()
        .map_err(CommandError::from)?)
}

/// Errors produced while turning a [`Frame`] into a [`Command`].
#[derive(Debug, Error)]
pub enum CommandFromFrameError {
    /// Verb-level semantic error from the domain layer (wrong arity, unknown verb,
    /// numeric arg failed to parse, etc.).
    #[error(transparent)]
    CommandError(#[from] CommandError),
    /// The outer frame wasn't an array of bulk strings, or a nested frame appeared
    /// where the protocol requires a bulk string (e.g. an array passed as a key).
    #[error("unexpected value; command is made of an array of bulk strings")]
    UnexpectedFrame,
    #[error(transparent)]
    SystemTime(#[from] SystemTimeError),
}

impl TryFrom<Frame> for Command {
    type Error = CommandFromFrameError;
    fn try_from(value: Frame) -> Result<Self, Self::Error> {
        let Frame::Array(vec) = value else {
            return Err(CommandFromFrameError::UnexpectedFrame);
        };
        let mut iter = vec.into_iter();
        let Some(command_value) = iter.next() else {
            return Err(CommandError::NotEnoughParts.into());
        };
        match command_value {
            Frame::BulkString(command) => {
                let command = command.to_ascii_lowercase();
                match command.as_slice() {
                    b"get" => {
                        let key = iter.next().ok_or(CommandError::NotEnoughParts)?;
                        let Frame::BulkString(key) = key else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Self::get(key))
                    }
                    // SET key value [EX s | PX ms | EXAT unix-s | PXAT unix-ms]
                    //
                    // All four options land on the same absolute Milliseconds deadline, so
                    // nothing downstream can tell which one the client used. That is the
                    // point: the relative forms read the clock here, on the live path, and
                    // the AOF only ever sees an absolute value.
                    b"set" => {
                        let key = next_bulk!(iter);
                        let value = next_bulk!(iter);
                        let Some(option) = iter.next() else {
                            return Ok(Self::set(key, value, None));
                        };
                        let Frame::BulkString(option) = option else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        let option = option.to_ascii_lowercase();
                        // Reject an unknown keyword before demanding its argument, so
                        // `SET k v junk` is a syntax error rather than a missing-parts one.
                        if !matches!(option.as_slice(), b"ex" | b"px" | b"exat" | b"pxat") {
                            return Err(CommandError::Syntax.into());
                        }
                        let amount = parse_u64(&next_bulk!(iter))?;
                        let expires_at = match option.as_slice() {
                            b"ex" => {
                                Milliseconds::now()?.saturating_add(Seconds::new(amount).into())
                            }
                            b"px" => Milliseconds::now()?.saturating_add(Milliseconds::new(amount)),
                            b"exat" => Seconds::new(amount).into(),
                            _ => Milliseconds::new(amount),
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Self::set(key, value, Some(expires_at)))
                    }
                    b"del" => {
                        let key = iter.next().ok_or(CommandError::NotEnoughParts)?;
                        let Frame::BulkString(key) = key else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Self::delete(key))
                    }
                    b"ping" => {
                        let Some(message_frame) = iter.next() else {
                            return Ok(Command::ping(None));
                        };
                        let Frame::BulkString(message) = message_frame else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Command::ping(Some(message)))
                    }
                    b"exists" => {
                        let key = iter.next().ok_or(CommandError::NotEnoughParts)?;
                        let Frame::BulkString(key) = key else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Self::exists(key))
                    }
                    // Relative, so it reads the clock. Safe because WriteCommand has no
                    // relative variant: this can never be the form that reaches the AOF,
                    // and replay therefore never runs this arm.
                    b"expire" => {
                        let key = next_bulk!(iter);
                        let seconds = Seconds::new(parse_u64(&next_bulk!(iter))?);
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Self::expire_at(
                            key,
                            Milliseconds::now()?.saturating_add(seconds.into()),
                        ))
                    }
                    b"expireat" => {
                        let key = next_bulk!(iter);
                        let seconds = Seconds::new(parse_u64(&next_bulk!(iter))?);
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Self::expire_at(key, seconds.into()))
                    }
                    // The form the AOF speaks. Already in the storage unit, so it needs no
                    // conversion, which is exactly why encoding picks it over EXPIREAT.
                    b"pexpireat" => {
                        let key = next_bulk!(iter);
                        let millis = Milliseconds::new(parse_u64(&next_bulk!(iter))?);
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Self::expire_at(key, millis))
                    }
                    b"ttl" => {
                        let key = iter.next().ok_or(CommandError::NotEnoughParts)?;
                        let Frame::BulkString(key) = key else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Command::ttl(key))
                    }
                    b"persist" => {
                        let key = iter.next().ok_or(CommandError::NotEnoughParts)?;
                        let Frame::BulkString(key) = key else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Command::persist(key))
                    }
                    b"subscribe" => {
                        let mut channel_ids: Vec<Vec<u8>> = Vec::new();
                        for frame in iter {
                            let Frame::BulkString(channel) = frame else {
                                return Err(CommandFromFrameError::UnexpectedFrame);
                            };
                            channel_ids.push(channel);
                        }
                        if channel_ids.is_empty() {
                            return Err(CommandError::NotEnoughParts.into());
                        }
                        Ok(Command::subscribe(channel_ids))
                    }
                    b"unsubscribe" => {
                        let mut channel_ids: Vec<Vec<u8>> = Vec::new();
                        for frame in iter {
                            let Frame::BulkString(channel) = frame else {
                                return Err(CommandFromFrameError::UnexpectedFrame);
                            };
                            channel_ids.push(channel);
                        }
                        Ok(Command::unsubscribe(channel_ids))
                    }
                    b"publish" => {
                        let (Some(channel), Some(message)) = (iter.next(), iter.next()) else {
                            return Err(CommandError::NotEnoughParts.into());
                        };
                        let (Frame::BulkString(channel), Frame::BulkString(message)) =
                            (channel, message)
                        else {
                            return Err(CommandFromFrameError::UnexpectedFrame);
                        };
                        if iter.next().is_some() {
                            return Err(CommandError::TooManyParts.into());
                        }
                        Ok(Command::publish(channel, message))
                    }
                    _ => Err(CommandError::UnrecognizedCommand.into()),
                }
            }
            Frame::Array(_) => Err(CommandFromFrameError::UnexpectedFrame),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::command::cache::{CacheCommand, write::WriteCommand};

    // ---------- ok cases ----------

    #[test]
    fn try_from_frame_ok_get() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"get".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::get(b"foo")
        );
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"GET".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::get(b"foo")
        );
    }

    #[test]
    fn try_from_frame_ok_set() {
        for verb in [b"set".as_slice(), b"SET".as_slice()] {
            assert_eq!(
                Command::try_from(Frame::Array(vec![
                    Frame::BulkString(verb.to_vec()),
                    Frame::BulkString(b"foo".to_vec()),
                    Frame::BulkString(b"value".to_vec()),
                ]))
                .unwrap(),
                Command::set(b"foo", b"value", None)
            );
        }
    }

    // EXAT and PXAT are already absolute, so they land on an exact deadline with no clock
    // read involved.
    #[test]
    fn try_from_frame_ok_set_absolute_options() {
        for (option, amount, expected) in [
            (b"exat".as_slice(), "1700000000", 1_700_000_000_000u64),
            (b"EXAT".as_slice(), "1700000000", 1_700_000_000_000),
            (b"pxat".as_slice(), "1700000000123", 1_700_000_000_123),
            (b"PXAT".as_slice(), "1700000000123", 1_700_000_000_123),
        ] {
            assert_eq!(
                Command::try_from(Frame::Array(vec![
                    Frame::BulkString(b"set".to_vec()),
                    Frame::BulkString(b"foo".to_vec()),
                    Frame::BulkString(b"value".to_vec()),
                    Frame::BulkString(option.to_vec()),
                    Frame::BulkString(amount.as_bytes().to_vec()),
                ]))
                .unwrap(),
                Command::set(b"foo", b"value", Some(Milliseconds::new(expected))),
                "option {}",
                String::from_utf8_lossy(option)
            );
        }
    }

    // EX and PX are relative, so the deadline is bracketed between two clock reads the
    // same way EXPIRE is.
    #[test]
    fn try_from_frame_ok_set_relative_options() {
        for (option, amount, offset) in [
            (b"ex".as_slice(), "60", Milliseconds::new(60_000)),
            (b"EX".as_slice(), "60", Milliseconds::new(60_000)),
            (b"px".as_slice(), "1500", Milliseconds::new(1_500)),
            (b"PX".as_slice(), "1500", Milliseconds::new(1_500)),
        ] {
            let before = Milliseconds::now().unwrap();
            let cmd = Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"value".to_vec()),
                Frame::BulkString(option.to_vec()),
                Frame::BulkString(amount.as_bytes().to_vec()),
            ]))
            .unwrap();
            let after = Milliseconds::now().unwrap();

            let Command::Cache(CacheCommand::Write(WriteCommand::Set {
                expires_at: Some(expires_at),
                ..
            })) = cmd
            else {
                panic!("expected a SET carrying a deadline, got {cmd:?}");
            };
            let lo = before.saturating_add(offset);
            let hi = after.saturating_add(offset);
            assert!(
                (lo..=hi).contains(&expires_at),
                "{expires_at:?} not in [{lo:?}, {hi:?}]"
            );
        }
    }

    #[test]
    fn try_from_frame_err_set_option_without_amount() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"value".to_vec()),
                Frame::BulkString(b"ex".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_set_non_numeric_amount() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"value".to_vec()),
                Frame::BulkString(b"ex".to_vec()),
                Frame::BulkString(b"soon".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::ParseInt(
                _
            )))
        ));
    }

    #[test]
    fn try_from_frame_err_set_too_many_parts_after_option() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"value".to_vec()),
                Frame::BulkString(b"ex".to_vec()),
                Frame::BulkString(b"60".to_vec()),
                Frame::BulkString(b"extra".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    // PEXPIREAT is the verb the AOF speaks: already in the storage unit, so it must pass
    // through untouched.
    #[test]
    fn try_from_frame_ok_pexpireat_is_taken_verbatim() {
        for verb in [b"pexpireat".as_slice(), b"PEXPIREAT".as_slice()] {
            assert_eq!(
                Command::try_from(Frame::Array(vec![
                    Frame::BulkString(verb.to_vec()),
                    Frame::BulkString(b"foo".to_vec()),
                    Frame::BulkString(b"1700000000123".to_vec()),
                ]))
                .unwrap(),
                Command::expire_at("foo", Milliseconds::new(1_700_000_000_123))
            );
        }
    }

    #[test]
    fn try_from_frame_err_pexpireat_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"pexpireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"1700000000123".to_vec()),
                Frame::BulkString(b"extra".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_expireat_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"1700000000".to_vec()),
                Frame::BulkString(b"extra".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_ok_del() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"del".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::delete(b"foo")
        );
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"DEL".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::delete(b"foo")
        );
    }

    #[test]
    fn try_from_frame_ok_ping_without_message() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"ping".to_vec())])).unwrap(),
            Command::ping(None)
        );
        assert_eq!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"PING".to_vec())])).unwrap(),
            Command::ping(None)
        );
    }

    #[test]
    fn try_from_frame_ok_ping_with_message() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"ping".to_vec()),
                Frame::BulkString(b"hello".to_vec()),
            ]))
            .unwrap(),
            Command::ping(Some(b"hello".to_vec()))
        );
    }

    #[test]
    fn try_from_frame_ok_exists() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"exists".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::exists(b"foo")
        );
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"EXISTS".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::exists(b"foo")
        );
    }

    #[test]
    fn try_from_frame_ok_expireat() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"1700000000".to_vec()),
            ]))
            .unwrap(),
            Command::expire_at("foo", Seconds::new(1_700_000_000).into()),
        );
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"EXPIREAT".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"1700000000".to_vec()),
            ]))
            .unwrap(),
            Command::expire_at("foo", Seconds::new(1_700_000_000).into()),
        );
    }

    /// The deadline for a relative TTL comes off the wall clock, so pin it between two
    /// clock reads and assert it lands in the window.
    fn assert_deadline_about(
        cmd: Command,
        expected_key: &[u8],
        before: Milliseconds,
        after: Milliseconds,
        offset: Milliseconds,
    ) {
        match cmd {
            Command::Cache(CacheCommand::Write(WriteCommand::ExpireAt { key, expires_at })) => {
                assert_eq!(key, expected_key);
                let lo = before.saturating_add(offset);
                let hi = after.saturating_add(offset);
                assert!(
                    (lo..=hi).contains(&expires_at),
                    "{expires_at:?} not in [{lo:?}, {hi:?}]"
                );
            }
            other => panic!("expected ExpireAt, got {other:?}"),
        }
    }

    // EXPIRE is normalized to an absolute deadline at parse time, so the log only ever
    // stores the absolute form and replay stays time-invariant.
    #[test]
    fn try_from_frame_ok_expire_normalizes_to_absolute() {
        for verb in [b"expire".as_slice(), b"EXPIRE".as_slice()] {
            let before = Milliseconds::now().unwrap();
            let cmd = Command::try_from(Frame::Array(vec![
                Frame::BulkString(verb.to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"60".to_vec()),
            ]))
            .unwrap();
            let after = Milliseconds::now().unwrap();
            assert_deadline_about(cmd, b"foo", before, after, Seconds::new(60).into());
        }
    }

    #[test]
    fn try_from_frame_ok_ttl() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"ttl".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::ttl("foo"),
        );
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"TTL".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::ttl("foo"),
        );
    }

    #[test]
    fn try_from_frame_ok_persist() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"persist".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::persist("foo"),
        );
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"PERSIST".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::persist("foo"),
        );
    }

    // ---------- top-level shape errors ----------

    #[test]
    fn try_from_frame_err_empty_array() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_command_value_is_array() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::Array(vec![Frame::BulkString(
                b"get".to_vec()
            )])])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    #[test]
    fn try_from_frame_err_unrecognized_command() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"foo".to_vec())])),
            Err(CommandFromFrameError::CommandError(
                CommandError::UnrecognizedCommand
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_unexpected_value() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"get".to_vec()),
                Frame::Array(vec![Frame::BulkString(b"foo".to_vec())]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
        assert!(matches!(
            Command::try_from(Frame::BulkString(b"get".to_vec())),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- GET errors ----------

    #[test]
    fn try_from_frame_err_get_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"get".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_get_not_enough_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"get".to_vec()),])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_get_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"get".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- SET errors ----------

    // A fourth part is now read as an option keyword, so an unrecognized one is a syntax
    // error rather than an excess argument.
    #[test]
    fn try_from_frame_err_set_unknown_option() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"value".to_vec()),
                Frame::BulkString(b"value".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::Syntax))
        ));
    }

    #[test]
    fn try_from_frame_err_set_not_enough_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"set".to_vec()),])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_set_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::Array(vec![]),
                Frame::BulkString(b"value".to_vec()),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    #[test]
    fn try_from_frame_err_set_unexpected_frame_value() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"set".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- DEL errors ----------

    #[test]
    fn try_from_frame_err_del_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"del".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_del_not_enough_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"del".to_vec()),])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_del_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"del".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- EXISTS errors ----------

    #[test]
    fn try_from_frame_err_exists_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"exists".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_exists_not_enough_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"exists".to_vec())])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_exists_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"exists".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- PING errors ----------

    #[test]
    fn try_from_frame_err_ping_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"ping".to_vec()),
                Frame::BulkString(b"hello".to_vec()),
                Frame::BulkString(b"world".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_ping_unexpected_frame_message() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"ping".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- EXPIRE errors ----------

    #[test]
    fn try_from_frame_err_expire_not_enough_parts_zero_args() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"expire".to_vec())])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_expire_not_enough_parts_one_arg() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expire".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_expire_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expire".to_vec()),
                Frame::Array(vec![]),
                Frame::BulkString(b"123".to_vec()),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    #[test]
    fn try_from_frame_err_expire_unexpected_frame_ttl() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expire".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    #[test]
    fn try_from_frame_err_expire_ttl_invalid_utf8() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expire".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(vec![0xff, 0xfe]),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::Utf8(_)))
        ));
    }

    #[test]
    fn try_from_frame_err_expire_ttl_not_a_number() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expire".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"abc".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::ParseInt(
                _
            )))
        ));
    }

    #[test]
    fn try_from_frame_err_expire_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expire".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"60".to_vec()),
                Frame::BulkString(b"extra".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_expire_ttl_negative() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expire".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"-1".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::ParseInt(
                _
            )))
        ));
    }

    // ---------- EXPIREAT errors ----------

    #[test]
    fn try_from_frame_err_expireat_not_enough_parts_zero_args() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"expireat".to_vec())])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_expireat_not_enough_parts_one_arg() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_expireat_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::Array(vec![]),
                Frame::BulkString(b"1700000000".to_vec()),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    #[test]
    fn try_from_frame_err_expireat_unexpected_frame_ttl() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    #[test]
    fn try_from_frame_err_expireat_ttl_invalid_utf8() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(vec![0xff, 0xfe]),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::Utf8(_)))
        ));
    }

    #[test]
    fn try_from_frame_err_expireat_ttl_not_a_number() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"abc".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::ParseInt(
                _
            )))
        ));
    }

    #[test]
    fn try_from_frame_err_expireat_ttl_negative() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"expireat".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"-1".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(CommandError::ParseInt(
                _
            )))
        ));
    }

    // ---------- TTL errors ----------

    #[test]
    fn try_from_frame_err_ttl_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"ttl".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_ttl_not_enough_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"ttl".to_vec())])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_ttl_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"ttl".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- PERSIST errors ----------

    #[test]
    fn try_from_frame_err_persist_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"persist".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_persist_not_enough_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"persist".to_vec())])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_persist_unexpected_frame_key() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"persist".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- SUBSCRIBE ----------

    #[test]
    fn try_from_frame_ok_subscribe_single() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"subscribe".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::subscribe(vec![b"foo".to_vec()])
        );
    }

    #[test]
    fn try_from_frame_ok_subscribe_multiple() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"SUBSCRIBE".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
            ]))
            .unwrap(),
            Command::subscribe(vec![b"foo".to_vec(), b"bar".to_vec()])
        );
    }

    #[test]
    fn try_from_frame_err_subscribe_no_channels() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(b"subscribe".to_vec())])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_subscribe_non_bulk_channel() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"subscribe".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- UNSUBSCRIBE ----------

    #[test]
    fn try_from_frame_ok_unsubscribe_single() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"unsubscribe".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ]))
            .unwrap(),
            Command::unsubscribe(vec![b"foo".to_vec()])
        );
    }

    // No-arg UNSUBSCRIBE is legal. It means "every channel this session is in".
    #[test]
    fn try_from_frame_ok_unsubscribe_no_channels() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![Frame::BulkString(
                b"unsubscribe".to_vec()
            )]))
            .unwrap(),
            Command::unsubscribe(Vec::<Vec<u8>>::new())
        );
    }

    #[test]
    fn try_from_frame_err_unsubscribe_non_bulk_channel() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"unsubscribe".to_vec()),
                Frame::Array(vec![]),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }

    // ---------- PUBLISH ----------

    #[test]
    fn try_from_frame_ok_publish() {
        assert_eq!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"publish".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
            ]))
            .unwrap(),
            Command::publish(b"foo", b"bar")
        );
    }

    #[test]
    fn try_from_frame_err_publish_not_enough_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"publish".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::NotEnoughParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_publish_too_many_parts() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"publish".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
                Frame::BulkString(b"baz".to_vec()),
            ])),
            Err(CommandFromFrameError::CommandError(
                CommandError::TooManyParts
            ))
        ));
    }

    #[test]
    fn try_from_frame_err_publish_non_bulk_arg() {
        assert!(matches!(
            Command::try_from(Frame::Array(vec![
                Frame::BulkString(b"publish".to_vec()),
                Frame::Array(vec![]),
                Frame::BulkString(b"bar".to_vec()),
            ])),
            Err(CommandFromFrameError::UnexpectedFrame)
        ));
    }
}
