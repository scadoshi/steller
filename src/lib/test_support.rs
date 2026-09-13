//! Shared test helpers: fakes, RAII temp files, and writer adapters used by the unit
//! tests across modules. Compiled only under `#[cfg(test)]`.

use crate::domain::{
    cache::Cache,
    command::cache::write::WriteCommand,
    ports::{CacheRepository, RepositoryError},
};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

/// [`CacheRepository`] fake that records every appended command. Snapshot calls are no-ops.
#[derive(Clone, Default)]
pub struct RecordingRepo {
    pub appended: Arc<Mutex<Vec<WriteCommand>>>,
}

impl CacheRepository for RecordingRepo {
    fn append(&self, command: WriteCommand) -> Result<(), RepositoryError> {
        self.appended.lock().unwrap().push(command.clone());
        Ok(())
    }
    fn snapshot(&self, _cache: &Cache) -> Result<(), RepositoryError> {
        Ok(())
    }
}

/// `Write` adapter backed by a shared `Vec<u8>` so tests can inspect the bytes a session
/// wrote *after* the session has flushed or dropped.
#[derive(Clone, Default)]
pub struct SharedWriter(pub Arc<Mutex<Vec<u8>>>);

impl SharedWriter {
    pub fn bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }
}

impl Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// RAII temp path. Removes the file (and its `.tmp` sibling, for the snapshot pattern)
/// on drop so tests don't litter `/tmp`.
pub struct TempPath {
    pub path: PathBuf,
}

impl TempPath {
    /// Build a fresh, unique temp path with the given prefix. Does not create the file;
    /// caller decides whether to open it via [`PersisterInner`] or directly.
    pub fn new(prefix: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "steller_{}_{}_{}",
            prefix,
            std::process::id(),
            id
        ));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("tmp"));
        Self { path }
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.path.with_extension("tmp"));
    }
}
