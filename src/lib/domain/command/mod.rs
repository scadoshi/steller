/// The commands the cache executes, split into reads and logged writes.
pub mod cache;
pub mod channel;
/// What a cache command produces, before the inbound layer renders it as a RESP reply.
pub mod outcome;

use std::num::ParseIntError;
use std::str::Utf8Error;

use thiserror::Error;

use crate::domain::command::cache::CacheCommand;
use crate::domain::command::cache::read::ReadCommand;
use crate::domain::command::cache::write::WriteCommand;
use crate::domain::command::channel::ChannelCommand;
use crate::domain::time::Milliseconds;

/// Why a well-formed frame did not make a valid command. The session answers each of these
/// with a `-ERR` reply and keeps reading.
#[derive(Error, Debug)]
pub enum CommandError {
    /// The verb is not one this server implements.
    #[error("unrecognized command")]
    UnrecognizedCommand,
    /// The array ended before every required argument, or was empty.
    #[error("not enough parts")]
    NotEnoughParts,
    /// Arguments remained after the last one the command takes.
    #[error("too many parts")]
    TooManyParts,
    /// An argument was where one belonged but isn't one this command knows, such as a
    /// `SET` option that isn't EX, PX, EXAT, or PXAT. Redis calls this a syntax error.
    #[error("syntax error")]
    Syntax,
    /// A numeric argument, such as a TTL or a deadline, was not UTF-8.
    #[error(transparent)]
    Utf8(#[from] Utf8Error),
    /// A numeric argument was UTF-8 but not a `u64`.
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
}

/// A parsed client command, sorted by who handles it.
///
/// The RESP layer builds it from a frame, and AOF replay builds it from the same bytes.
/// Cache commands go to the executor, channel commands to the session's pub/sub router, and
/// `PING` is answered by the session directly.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// A key-value command for the executor.
    Cache(CacheCommand),
    /// A pub/sub command for the session.
    Channel(ChannelCommand),
    /// `PING`, with the optional message to echo instead of `PONG`.
    Ping {
        /// The bytes to echo, or `None` for a bare `PING`.
        message: Option<Vec<u8>>,
    },
}

impl From<CacheCommand> for Command {
    fn from(value: CacheCommand) -> Self {
        Self::Cache(value)
    }
}

impl From<ChannelCommand> for Command {
    fn from(value: ChannelCommand) -> Self {
        Self::Channel(value)
    }
}

impl From<ReadCommand> for Command {
    fn from(value: ReadCommand) -> Self {
        Self::Cache(value.into())
    }
}

impl From<WriteCommand> for Command {
    fn from(value: WriteCommand) -> Self {
        Self::Cache(value.into())
    }
}

impl Command {
    /// A `GET` of `key`.
    pub fn get(key: impl Into<Vec<u8>>) -> Self {
        ReadCommand::get(key).into()
    }

    /// A `SET` of `key` to `value`, with an optional absolute deadline.
    pub fn set(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        expires_at: Option<Milliseconds>,
    ) -> Self {
        WriteCommand::set(key, value, expires_at).into()
    }

    /// A `DEL` of `key`.
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        WriteCommand::delete(key).into()
    }

    /// An `EXISTS` of `key`.
    pub fn exists(key: impl Into<Vec<u8>>) -> Self {
        ReadCommand::exists(key).into()
    }

    /// A deadline of `expires_at`, in absolute UNIX milliseconds, on `key`.
    pub fn expire_at(key: impl Into<Vec<u8>>, expires_at: Milliseconds) -> Self {
        WriteCommand::expire_at(key, expires_at).into()
    }

    /// A `TTL` of `key`.
    pub fn ttl(key: impl Into<Vec<u8>>) -> Self {
        ReadCommand::ttl(key).into()
    }

    /// A `PERSIST` of `key`.
    pub fn persist(key: impl Into<Vec<u8>>) -> Self {
        WriteCommand::persist(key).into()
    }

    /// A `PING`, echoing `message` if there is one.
    pub fn ping(message: Option<Vec<u8>>) -> Self {
        Self::Ping { message }
    }

    /// A `SUBSCRIBE` to `channel_ids`.
    pub fn subscribe(channel_ids: impl Into<Vec<Vec<u8>>>) -> Self {
        ChannelCommand::subscribe(channel_ids).into()
    }

    /// An `UNSUBSCRIBE` from `channel_ids`, where an empty list means every channel.
    pub fn unsubscribe(channel_ids: impl Into<Vec<Vec<u8>>>) -> Self {
        ChannelCommand::unsubscribe(channel_ids).into()
    }

    /// A `PUBLISH` of `message` to `channel_id`.
    pub fn publish(channel_id: impl Into<Vec<u8>>, message: impl Into<Vec<u8>>) -> Self {
        ChannelCommand::publish(channel_id, message).into()
    }
}
