pub mod cache;
pub mod channel;
pub mod outcome;

use crate::domain::{
    command::{
        cache::{CacheCommand, read::ReadCommand, write::WriteCommand},
        channel::ChannelCommand,
    },
    time::Milliseconds,
};
use std::{num::ParseIntError, str::Utf8Error};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("unrecognized command")]
    UnrecognizedCommand,
    #[error("not enough parts")]
    NotEnoughParts,
    #[error("too many parts")]
    TooManyParts,
    /// An argument was where one belonged but isn't one this command knows, such as a
    /// `SET` option that isn't EX, PX, EXAT, or PXAT. Redis calls this a syntax error.
    #[error("syntax error")]
    Syntax,
    #[error(transparent)]
    Utf8(#[from] Utf8Error),
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Cache(CacheCommand),
    Channel(ChannelCommand),
    Ping { message: Option<Vec<u8>> },
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
    pub fn get(key: impl Into<Vec<u8>>) -> Self {
        ReadCommand::get(key).into()
    }

    pub fn set(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        expires_at: Option<Milliseconds>,
    ) -> Self {
        WriteCommand::set(key, value, expires_at).into()
    }

    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        WriteCommand::delete(key).into()
    }

    pub fn exists(key: impl Into<Vec<u8>>) -> Self {
        ReadCommand::exists(key).into()
    }

    pub fn expire_at(key: impl Into<Vec<u8>>, expires_at: Milliseconds) -> Self {
        WriteCommand::expire_at(key, expires_at).into()
    }

    pub fn ttl(key: impl Into<Vec<u8>>) -> Self {
        ReadCommand::ttl(key).into()
    }

    pub fn persist(key: impl Into<Vec<u8>>) -> Self {
        WriteCommand::persist(key).into()
    }

    pub fn ping(message: Option<Vec<u8>>) -> Self {
        Self::Ping { message }
    }

    pub fn subscribe(channel_ids: impl Into<Vec<Vec<u8>>>) -> Self {
        ChannelCommand::subscribe(channel_ids).into()
    }

    pub fn unsubscribe(channel_ids: impl Into<Vec<Vec<u8>>>) -> Self {
        ChannelCommand::unsubscribe(channel_ids).into()
    }

    pub fn publish(channel_id: impl Into<Vec<u8>>, message: impl Into<Vec<u8>>) -> Self {
        ChannelCommand::publish(channel_id, message).into()
    }
}
