/// The commands that only read state, and so are never logged.
pub mod read;
pub mod write;

use read::ReadCommand;
use write::WriteCommand;

/// A command the cache executes, split by whether it changes state.
///
/// The split is what decides logging: the executor appends [`CacheCommand::Write`] to the
/// AOF and never appends [`CacheCommand::Read`].
#[derive(Debug, Clone, PartialEq)]
pub enum CacheCommand {
    /// A query. It never changes a value, though it can drop a key it finds expired.
    Read(ReadCommand),
    /// A mutation, applied to the cache and then appended to the log.
    Write(WriteCommand),
}

impl From<ReadCommand> for CacheCommand {
    fn from(value: ReadCommand) -> Self {
        Self::Read(value)
    }
}

impl From<WriteCommand> for CacheCommand {
    fn from(value: WriteCommand) -> Self {
        Self::Write(value)
    }
}
