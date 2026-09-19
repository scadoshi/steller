//! Append-only command log: the write-ahead log half of the persister.
//!
//! The AOF *is* the RESP wire format: each logged command is encoded with the same
//! `From<WriteCommand> for Frame` + `Frame::write_to` used on the network path, so the
//! file is byte-for-byte what a client would have sent. That symmetry means replay needs
//! no decoder of its own. It reuses `Frame::parse_one` and `Command::try_from`, the exact
//! inbound parsing path. The log is, in effect, a transcript of every mutation; replay is
//! "re-send that transcript."

use crate::{
    domain::{
        cache::{Cache, CacheError},
        command::{Command, cache::write::WriteCommand},
        ports::RepositoryError,
    },
    outbound::persister::persister_inner::PersisterInner,
    resp::{
        command::CommandFromFrameError,
        frame::{Frame, FrameError},
    },
};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read},
    ops::Deref,
};
use thiserror::Error;

type IoError = std::io::Error;

/// Failure on the AOF write or replay path. Boxed into [`RepositoryError`] at the boundary.
#[derive(Debug, Error)]
pub enum AofError {
    /// File I/O failed.
    #[error(transparent)]
    Io(#[from] IoError),
    /// A writer panicked while holding the log mutex.
    #[error("mutex poisoned")]
    MutexPoisoned,
    /// A replayed command failed to apply to the cache.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// A replayed frame parsed but didn't lift into a known command, meaning corruption or
    /// version skew. Fatal: this log is our own, so an uninterpretable entry means the
    /// rebuild can't be trusted.
    #[error(transparent)]
    Command(#[from] CommandFromFrameError),
    /// A frame failed to parse mid-stream (hard corruption, distinct from a torn tail).
    #[error(transparent)]
    Frame(#[from] FrameError),
}

impl From<AofError> for RepositoryError {
    fn from(value: AofError) -> Self {
        RepositoryError::Generic(Box::new(value))
    }
}

/// The append-only log, over the persister's shared writer and path.
#[derive(Debug, Clone)]
pub struct Aof(PersisterInner);

impl Deref for Aof {
    type Target = PersisterInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PersisterInner> for Aof {
    fn from(value: PersisterInner) -> Self {
        Self(value)
    }
}

impl Aof {
    /// Encode one mutation as a RESP frame and append it to the log. Holds the writer lock
    /// across the write so a frame is never interleaved with another thread's append.
    pub fn append(&self, command: WriteCommand) -> Result<(), AofError> {
        let mut guard = self.writer.lock().map_err(|_| AofError::MutexPoisoned)?;
        Frame::from(command).write_to(&mut *guard)?;
        Ok(())
    }

    /// Rebuild cache state by replaying the log: parse each frame, lift it to a command,
    /// apply it *in-memory only* (no re-logging).
    ///
    /// Reads the whole file, then walks it frame-by-frame. Stops on a trailing
    /// [`Incomplete`](FrameError::Incomplete) frame, which is the expected torn tail from a
    /// crash mid-append, and keeps everything parsed so far. Any *other* parse failure or an
    /// unknown command is fatal, because in our own log those mean real corruption rather
    /// than a normal partial write. Runs at startup before clients connect, so
    /// per-command locking inside `execute` is fine. There is no concurrency to coordinate.
    pub fn replay(&self, cache: &Cache) -> Result<(), AofError> {
        let aof = {
            let mut aof = Vec::<u8>::new();
            File::open(&*self.path)?.read_to_end(&mut aof)?;
            aof
        };
        let mut bytes = aof.as_slice();
        loop {
            match Frame::parse_one(bytes) {
                Ok((frame, remainder)) => {
                    bytes = remainder;
                    match Command::try_from(frame) {
                        Ok(Command::Cache(cc)) => {
                            cache.execute(&cc)?;
                        }
                        Ok(Command::Channel(_) | Command::Ping { .. }) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(FrameError::Incomplete) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    pub fn clear(&self) -> Result<(), AofError> {
        let mut guard = self.writer.lock().map_err(|_| AofError::MutexPoisoned)?;
        let cleared = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&*self.path)?;
        *guard = BufWriter::new(cleared);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{cache::Entry, time::Milliseconds},
        test_support::TempPath,
    };
    use std::{fs, io::Write};

    fn fresh() -> (Aof, TempPath) {
        let temp = TempPath::new("aof");
        let inner = PersisterInner::try_from(temp.path.clone()).unwrap();
        (Aof::from(inner), temp)
    }

    /// Flush the `BufWriter` so bytes actually land on disk for the next read.
    fn flush(aof: &Aof) {
        aof.writer.lock().unwrap().flush().unwrap();
    }

    // ---------- append ----------

    #[test]
    fn append_writes_resp_frame_to_disk() {
        let (aof, t) = fresh();
        aof.append(WriteCommand::set("foo", "bar", None)).unwrap();
        flush(&aof);
        let bytes = fs::read(&t.path).unwrap();
        assert_eq!(bytes, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    // A SET carrying a deadline logs it as PXAT, the millisecond verb, because that is
    // the form the parser reads back without converting.
    #[test]
    fn append_writes_set_deadline_as_pxat() {
        let (aof, t) = fresh();
        aof.append(WriteCommand::set(
            "foo",
            "bar",
            Some(Milliseconds::new(1_700_000_000_000)),
        ))
        .unwrap();
        flush(&aof);
        let bytes = fs::read(&t.path).unwrap();
        assert_eq!(
            bytes,
            b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$4\r\nPXAT\r\n$13\r\n1700000000000\r\n"
        );
    }

    #[test]
    fn append_multiple_commands_concatenates() {
        let (aof, t) = fresh();
        aof.append(WriteCommand::set("a", "1", None)).unwrap();
        aof.append(WriteCommand::delete("a")).unwrap();
        flush(&aof);
        let bytes = fs::read(&t.path).unwrap();
        let expected = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\nDEL\r\n$1\r\na\r\n";
        assert_eq!(bytes, expected);
    }

    // ---------- replay ----------

    #[test]
    fn replay_empty_file_is_noop() {
        let (aof, _t) = fresh();
        let cache = Cache::default();
        aof.replay(&cache).unwrap();
        assert_eq!(cache.lock().unwrap().len(), 0);
    }

    #[test]
    fn replay_applies_set_to_cache() {
        let (aof, _t) = fresh();
        aof.append(WriteCommand::set("foo", "bar", None)).unwrap();
        flush(&aof);
        let cache = Cache::default();
        aof.replay(&cache).unwrap();
        assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("bar", None)));
    }

    #[test]
    fn replay_applies_commands_in_order() {
        let (aof, _t) = fresh();
        aof.append(WriteCommand::set("foo", "old", None)).unwrap();
        aof.append(WriteCommand::set("foo", "new", None)).unwrap();
        aof.append(WriteCommand::delete("gone")).unwrap();
        flush(&aof);
        let cache = Cache::default();
        aof.replay(&cache).unwrap();
        assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("new", None)));
        assert_eq!(cache.get("gone").unwrap(), None);
    }

    // The deadline has to come back the same size it went in. This is the test that fails
    // if encoding ever picks a second-granular verb the parser then multiplies.
    #[test]
    fn replay_preserves_a_set_deadline_exactly() {
        let (aof, _t) = fresh();
        let deadline = Milliseconds::new(u64::MAX - 1);
        aof.append(WriteCommand::set("foo", "bar", Some(deadline)))
            .unwrap();
        flush(&aof);
        let cache = Cache::default();
        aof.replay(&cache).unwrap();
        assert_eq!(cache.get_expires_at("foo").unwrap(), Some(Some(deadline)));
    }

    // Proves a deadline in the log survives the whole round-trip: encode, disk, parse,
    // execute.
    #[test]
    fn replay_applies_expire_at() {
        let (aof, _t) = fresh();
        aof.append(WriteCommand::set("foo", "bar", None)).unwrap();
        aof.append(WriteCommand::expire_at("foo", Milliseconds::new(u64::MAX)))
            .unwrap();
        flush(&aof);

        let cache = Cache::default();
        aof.replay(&cache).unwrap();
        assert_eq!(
            cache.get_expires_at("foo").unwrap(),
            Some(Some(Milliseconds::new(u64::MAX)))
        );
    }

    // A torn tail is what a crash mid-append leaves behind, so replay keeps everything
    // before it rather than treating the file as corrupt.
    #[test]
    fn replay_trailing_partial_frame_is_tolerated() {
        let (aof, t) = fresh();
        aof.append(WriteCommand::set("foo", "bar", None)).unwrap();
        flush(&aof);
        // Write past the BufWriter, straight at the file.
        let mut handle = OpenOptions::new().append(true).open(&t.path).unwrap();
        handle.write_all(b"*3\r\n$3\r\nSET\r\n").unwrap();
        drop(handle);

        let cache = Cache::default();
        aof.replay(&cache).unwrap();
        assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("bar", None)));
    }

    #[test]
    fn replay_malformed_frame_errors() {
        let (aof, t) = fresh();
        let mut handle = OpenOptions::new().append(true).open(&t.path).unwrap();
        handle.write_all(b"?bogus\r\n").unwrap();
        drop(handle);
        let cache = Cache::default();
        assert!(matches!(aof.replay(&cache), Err(AofError::Frame(_))));
    }

    // ---------- clear ----------

    #[test]
    fn clear_truncates_file() {
        let (aof, t) = fresh();
        aof.append(WriteCommand::set("foo", "bar", None)).unwrap();
        flush(&aof);
        aof.clear().unwrap();
        assert_eq!(fs::read(&t.path).unwrap().len(), 0);
    }

    #[test]
    fn clear_allows_subsequent_appends() {
        let (aof, _t) = fresh();
        aof.append(WriteCommand::set("old", "v", None)).unwrap();
        flush(&aof);
        aof.clear().unwrap();
        aof.append(WriteCommand::set("new", "v", None)).unwrap();
        flush(&aof);
        let cache = Cache::default();
        aof.replay(&cache).unwrap();
        assert_eq!(cache.get("old").unwrap(), None);
        assert_eq!(cache.get("new").unwrap(), Some(Entry::new("v", None)));
    }
}
