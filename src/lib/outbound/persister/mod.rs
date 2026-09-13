//! The concrete [`CacheRepository`] backing the domain.
//!
//! [`Persister`] composes two pieces, each owning its own file. [`Aof`] is the append-only
//! log of mutations, which covers durability between snapshots. [`Snapshot`] is a
//! `wincode` dump of the whole map, which is the recovery baseline.
//!
//! Recovery uses both: load the snapshot for base state, then replay the AOF on top for
//! everything since. Taking a snapshot truncates the AOF, so the log only holds mutations
//! after the last snapshot. That truncation is the whole compaction story.
//!
//! Each concrete error type converts into the domain's [`RepositoryError`] through a local
//! `From` impl, so the translation happens at this boundary and the domain never sees an
//! outbound error type.

pub mod aof;
mod persister_inner;
pub mod snapshot;

use crate::{
    domain::{
        cache::Cache,
        command::cache::write::WriteCommand,
        ports::{CacheRepository, RepositoryError},
    },
    outbound::persister::{aof::Aof, persister_inner::PersisterInner, snapshot::Snapshot},
};
use std::path::PathBuf;
use thiserror::Error;

type IoError = std::io::Error;

/// On-disk path of the append-only command log.
static AOF_PATH: &str = "cache/aof";
/// On-disk path of the snapshot dump.
const SNAPSHOT_PATH: &str = "cache/snapshot";

/// Failure setting up or coordinating the persister itself, as opposed to the AOF- and
/// snapshot-specific errors. Boxed into [`RepositoryError`] at the boundary.
#[derive(Debug, Error)]
pub enum PersisterError {
    /// I/O failure opening or creating a persistence file.
    #[error(transparent)]
    Io(#[from] IoError),
    /// A writer panicked while holding the cache mutex.
    #[error("cache mutex poisoned")]
    MutexPoisoned,
}

impl From<PersisterError> for RepositoryError {
    fn from(value: PersisterError) -> Self {
        RepositoryError::Generic(Box::new(value))
    }
}

/// The persistence adapter, holding the log and the snapshot. Each half is a shared
/// handle, so cloning it into every session is cheap.
#[derive(Debug, Clone)]
pub struct Persister {
    pub aof: Aof,
    pub snapshot: Snapshot,
}

impl Persister {
    /// Open both files, creating them if absent, and assemble the persister. Runs once at
    /// startup, before recovery.
    pub fn initialize() -> Result<Self, PersisterError> {
        let aof_path: PathBuf = AOF_PATH.into();
        let aof = Aof::from(PersisterInner::try_from(aof_path)?);
        let snapshot_path: PathBuf = SNAPSHOT_PATH.into();
        let snapshot = Snapshot::from(PersisterInner::try_from(snapshot_path)?);
        Ok(Self { aof, snapshot })
    }
}

impl CacheRepository for Persister {
    fn append(&self, command: WriteCommand) -> Result<(), RepositoryError> {
        self.aof.append(command)?;
        Ok(())
    }

    /// Snapshot then compact, in one critical section.
    ///
    /// The cache lock is held across both the snapshot write and the AOF clear, so no
    /// mutation can land in the gap. Without that, a command written to the log after the
    /// snapshot read but before the clear would be wiped having never been captured.
    ///
    /// Order matters too: snapshot first for the durable baseline, then clear the log that
    /// just became redundant. A crash between the two only costs you a replay of commands
    /// already in the snapshot.
    fn snapshot(&self, cache: &Cache) -> Result<(), RepositoryError> {
        let guard = cache.lock().map_err(|_| PersisterError::MutexPoisoned)?;
        self.snapshot.store(&guard)?;
        self.aof.clear()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{domain::cache::Entry, test_support::TempPath};
    use std::{fs, io::Write};

    struct Built {
        persister: Persister,
        aof_path: TempPath,
        snapshot_path: TempPath,
    }

    impl Built {
        fn flush_aof(&self) {
            self.persister.aof.writer.lock().unwrap().flush().unwrap();
        }
    }

    fn fresh() -> Built {
        let aof_path = TempPath::new("persister_aof");
        let snapshot_path = TempPath::new("persister_snapshot");
        let aof = Aof::from(PersisterInner::try_from(aof_path.path.clone()).unwrap());
        let snapshot =
            Snapshot::from(PersisterInner::try_from(snapshot_path.path.clone()).unwrap());
        Built {
            persister: Persister { aof, snapshot },
            aof_path,
            snapshot_path,
        }
    }

    #[test]
    fn append_writes_to_aof_only() {
        let t = fresh();
        t.persister
            .append(WriteCommand::set("foo", "bar", None))
            .unwrap();
        t.flush_aof();
        assert!(fs::metadata(&t.aof_path.path).unwrap().len() > 0);
        // PersisterInner creates the snapshot file, but nothing has written to it.
        assert_eq!(fs::metadata(&t.snapshot_path.path).unwrap().len(), 0);
    }

    #[test]
    fn snapshot_persists_cache_contents() {
        let t = fresh();
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        t.persister.snapshot(&cache).unwrap();
        let loaded = t.persister.snapshot.load().unwrap();
        assert_eq!(
            loaded.get(b"foo".as_slice()),
            Some(&Entry::new("bar", None))
        );
    }

    // Snapshotting truncates the log. That truncation is the compaction.
    #[test]
    fn snapshot_clears_the_aof() {
        let t = fresh();
        t.persister
            .append(WriteCommand::set("foo", "bar", None))
            .unwrap();
        t.flush_aof();
        assert!(fs::metadata(&t.aof_path.path).unwrap().len() > 0);

        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        t.persister.snapshot(&cache).unwrap();

        assert_eq!(fs::metadata(&t.aof_path.path).unwrap().len(), 0);
    }

    #[test]
    fn append_after_snapshot_resumes_logging() {
        let t = fresh();
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        t.persister.snapshot(&cache).unwrap();

        t.persister
            .append(WriteCommand::Delete {
                key: b"foo".to_vec(),
            })
            .unwrap();
        t.flush_aof();

        // Replay the post-snapshot AOF on a cache rebuilt from the snapshot.
        let recovered = Cache::new(t.persister.snapshot.load().unwrap());
        t.persister.aof.replay(&recovered).unwrap();
        assert_eq!(recovered.get("foo").unwrap(), None);
    }
}
