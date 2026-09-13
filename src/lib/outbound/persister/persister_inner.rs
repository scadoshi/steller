//! Shared file-handle plumbing for the persistence pieces.
//!
//! [`Aof`](super::aof::Aof) and [`Snapshot`](super::snapshot::Snapshot) both wrap a
//! `PersisterInner` behind a newtype and `Deref`, reusing the open-a-file-and-guard-a-
//! writer machinery without sharing any behavior. The writer sits in an `Arc<Mutex>` so
//! one file handle is shared across threads and writes serialize. The `path` sticks around
//! for the recovery paths, which open their own transient handles.

use std::{
    fs::{File, OpenOptions, create_dir_all},
    io::BufWriter,
    path::PathBuf,
    sync::{Arc, Mutex},
};

type IoError = std::io::Error;

/// A guarded, append-mode writer plus the path it points at. The fields are `pub(super)`
/// so the newtypes in sibling modules can reach them directly.
#[derive(Debug, Clone)]
pub struct PersisterInner {
    pub(super) writer: Arc<Mutex<BufWriter<File>>>,
    pub(super) path: PathBuf,
}

impl TryFrom<PathBuf> for PersisterInner {
    type Error = IoError;
    /// Open the file in append mode, creating it and any missing parent dirs.
    ///
    /// Append mode is what makes the AOF correct on restart. The write cursor starts at
    /// end-of-file, so reopening an existing log extends it instead of overwriting from
    /// offset 0.
    fn try_from(value: PathBuf) -> Result<Self, Self::Error> {
        if let Some(dir) = value.parent()
            && !dir.as_os_str().is_empty()
        {
            create_dir_all(dir)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .append(true)
            .open(value.clone())?;
        let writer = Arc::new(Mutex::new(BufWriter::new(file)));
        Ok(Self {
            writer,
            path: value,
        })
    }
}
