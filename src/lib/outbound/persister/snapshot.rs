//! Point-in-time snapshot: the recovery baseline half of the persister.
//!
//! A `wincode`-serialized dump of the entire map. On recovery it's loaded first, then the
//! AOF is replayed on top. Writes go through a **temp-file-then-rename**: serialize to
//! `<path>.tmp`, then atomically `rename` it over the live snapshot. Rename is atomic on
//! the same filesystem, so a crash mid-write can never leave a half-written snapshot. You
//! always have either the complete old one or the complete new one.

use crate::{
    domain::{cache::Entry, ports::RepositoryError},
    outbound::persister::persister_inner::PersisterInner,
};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    ops::Deref,
};
use thiserror::Error;
use wincode::{ReadError, WriteError};

type IoError = std::io::Error;

/// Failure loading or storing a snapshot. Boxed into [`RepositoryError`] at the boundary.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// File I/O failed.
    #[error(transparent)]
    Io(#[from] IoError),
    /// `wincode` couldn't deserialize the file: corrupt, or an incompatible format.
    #[error(transparent)]
    Read(#[from] ReadError),
    /// `wincode` couldn't serialize the map.
    #[error(transparent)]
    Write(#[from] WriteError),
}

impl From<SnapshotError> for RepositoryError {
    fn from(value: SnapshotError) -> Self {
        RepositoryError::Generic(Box::new(value))
    }
}

/// The snapshot file. Newtype over `PersisterInner`; only the `path` is used (writes go
/// through a fresh temp handle, not the shared append writer).
#[derive(Debug, Clone)]
pub struct Snapshot(PersisterInner);

impl Deref for Snapshot {
    type Target = PersisterInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PersisterInner> for Snapshot {
    fn from(value: PersisterInner) -> Self {
        Self(value)
    }
}

impl Snapshot {
    /// Load the snapshot into a map. An empty file (no snapshot taken yet) yields an empty
    /// map rather than an error.
    pub fn load(&self) -> Result<HashMap<Vec<u8>, Entry>, SnapshotError> {
        let mut bytes = Vec::<u8>::new();
        File::open(&*self.path)?.read_to_end(&mut bytes)?;
        let map = if bytes.is_empty() {
            HashMap::new()
        } else {
            wincode::deserialize(&bytes)?
        };
        Ok(map)
    }

    /// Atomically write the map as the new snapshot: serialize to a `.tmp` sibling, then
    /// `rename` it over the live file. The caller passes the map behind the held cache
    /// lock so the dump is a consistent point-in-time view.
    pub fn store(&self, guard: &HashMap<Vec<u8>, Entry>) -> Result<(), SnapshotError> {
        let temp_path = self.path.with_extension("tmp");
        let mut temp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;
        let bytes = wincode::serialize(guard)?;
        temp.write_all(&bytes)?;
        fs::rename(temp_path, &*self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{domain::time::Milliseconds, test_support::TempPath};

    fn fresh() -> (Snapshot, TempPath) {
        let temp = TempPath::new("snapshot");
        let inner = PersisterInner::try_from(temp.path.clone()).unwrap();
        (Snapshot::from(inner), temp)
    }

    #[test]
    fn load_empty_file_returns_empty_map() {
        let (snap, _t) = fresh();
        assert!(snap.load().unwrap().is_empty());
    }

    #[test]
    fn store_then_load_round_trips_single_entry() {
        let (snap, _t) = fresh();
        let mut map = HashMap::new();
        map.insert(b"foo".to_vec(), Entry::new("bar", None));
        snap.store(&map).unwrap();
        assert_eq!(snap.load().unwrap(), map);
    }

    #[test]
    fn store_then_load_round_trips_multiple_entries() {
        let (snap, _t) = fresh();
        let mut map = HashMap::new();
        map.insert(b"a".to_vec(), Entry::new("1", None));
        map.insert(
            b"b".to_vec(),
            Entry::new("2", Some(Milliseconds::new(1_700_000_000))),
        );
        map.insert(b"c".to_vec(), Entry::new(vec![0xff, 0x00, 0x7f], None));
        snap.store(&map).unwrap();
        assert_eq!(snap.load().unwrap(), map);
    }

    #[test]
    fn store_overwrites_previous_snapshot() {
        let (snap, _t) = fresh();
        let mut first = HashMap::new();
        first.insert(b"foo".to_vec(), Entry::new("old", None));
        snap.store(&first).unwrap();

        let mut second = HashMap::new();
        second.insert(b"foo".to_vec(), Entry::new("new", None));
        snap.store(&second).unwrap();

        assert_eq!(snap.load().unwrap(), second);
    }

    #[test]
    fn store_leaves_no_lingering_tmp_file() {
        let (snap, t) = fresh();
        let mut map = HashMap::new();
        map.insert(b"foo".to_vec(), Entry::new("bar", None));
        snap.store(&map).unwrap();
        assert!(!t.path.with_extension("tmp").exists());
    }

    #[test]
    fn load_missing_file_errors() {
        let (snap, t) = fresh();
        // Delete the file PersisterInner created so load() hits NotFound.
        fs::remove_file(&t.path).unwrap();
        assert!(matches!(snap.load(), Err(SnapshotError::Io(_))));
    }
}
