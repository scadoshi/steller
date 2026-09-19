//! In-memory key-value store with TTLs, expired lazily and swept actively.
//!
//! [`Cache`] wraps `Arc<Mutex<HashMap<Vec<u8>, Entry>>>`, so handles clone cheaply across
//! threads and every clone shares the map. Read paths drop expired keys as they encounter
//! them. The sweeper thread in `inbound::server` calls [`Cache::remove_expired`] on a tick
//! to reclaim keys nobody has touched.
//!
//! ## Expiry
//!
//! Deadlines are absolute UNIX milliseconds. Relative TTLs are made absolute at parse
//! time, so storage only ever sees one form and it serializes cleanly. See
//! [`crate::domain::time`] for the clock and the conversion.
//!
//! ## Persistence
//!
//! The cache has no persistence methods. Durability lives in `outbound::persister`: a
//! `wincode` snapshot of the map plus an append-only command log replayed on startup.

use crate::domain::{
    command::{
        cache::{CacheCommand, read::ReadCommand, write::WriteCommand},
        outcome::{CommandOutcome, TtlOutcome},
    },
    time::Milliseconds,
};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex},
    time::SystemTimeError,
};
use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

/// Failure inside a [`Cache`] operation.
#[derive(Debug, Error)]
pub enum CacheError {
    /// A thread panicked while holding the shared mutex. The in-memory state may be
    /// inconsistent, so the cache can't be used safely after this.
    #[error("cache mutex poisoned")]
    MutexPoisoned,
    /// The clock read landed before the UNIX epoch, which takes a badly wrong system
    /// clock.
    #[error(transparent)]
    SystemTime(#[from] SystemTimeError),
}

/// Value bytes plus an optional deadline.
///
/// `None` means no expiry: the entry lives until something removes it. A past timestamp
/// is accepted, and the next read on that key treats it as expired and drops it.
#[derive(Clone, Debug, Default, SchemaWrite, SchemaRead, PartialEq, Hash)]
pub struct Entry {
    /// Opaque payload. UTF-8 is never enforced, so a value can be a jpeg, a RESP
    /// fragment, anything.
    pub value: Vec<u8>,
    /// Absolute UNIX millisecond deadline, or `None` for no TTL.
    pub expires_at: Option<Milliseconds>,
}

impl Entry {
    /// Value bytes plus an optional deadline.
    pub fn new(value: impl Into<Vec<u8>>, expires_at: Option<Milliseconds>) -> Self {
        Self {
            value: value.into(),
            expires_at,
        }
    }
}

/// Thread-safe shared KV store.
///
/// Cloning clones the `Arc`, so every clone refers to the same backing map. That is how
/// the server hands the cache to its connection threads, the sweeper, and the persistence
/// loop.
///
/// `Default` gives you an empty cache, which is what tests want. To rebuild from disk,
/// pass a snapshot-loaded map to [`Cache::new`] and replay the log on top.
#[derive(Clone, Debug, Default)]
pub struct Cache {
    inner: Arc<Mutex<HashMap<Vec<u8>, Entry>>>,
}

impl Deref for Cache {
    type Target = Arc<Mutex<HashMap<Vec<u8>, Entry>>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Cache {
    pub fn new(data: HashMap<Vec<u8>, Entry>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(data)),
        }
    }

    /// Fetch a clone of the [`Entry`] for `key`, or `None` if it is missing or expired.
    ///
    /// An expired entry returns `None` and is removed on the way out. The lock is held
    /// across the check and the removal so a concurrent insert can't race in between.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Entry>, CacheError> {
        let now = Milliseconds::now()?;
        let mut guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        let entry = guard.get(key.as_ref()).cloned();
        if entry
            .as_ref()
            .is_some_and(|e| e.expires_at.is_some_and(|t| now >= t))
        {
            guard.remove(key.as_ref());
            Ok(None)
        } else {
            Ok(entry)
        }
    }

    /// Insert or replace `key`, returning whatever was there before.
    ///
    /// An already-past deadline is accepted as-is and the next read drops it. Callers do
    /// not need to check the clock first.
    pub fn insert(
        &self,
        key: impl Into<Vec<u8>>,
        entry: Entry,
    ) -> Result<Option<Entry>, CacheError> {
        let mut guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        Ok(guard.insert(key.into(), entry))
    }

    /// Remove `key`, returning whatever was there. Unconditional: TTL is not consulted.
    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Entry>, CacheError> {
        let mut guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        Ok(guard.remove(key.as_ref()))
    }

    /// Test for `key`. An expired key returns `false` and is dropped on the way out, so
    /// this agrees with what [`Cache::get`] would say.
    pub fn contains(&self, key: impl AsRef<[u8]>) -> Result<bool, CacheError> {
        let mut guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        let entry = guard.get(key.as_ref());
        match entry {
            Some(Entry {
                expires_at: Some(t),
                ..
            }) if Milliseconds::now()? >= *t => {
                guard.remove(key.as_ref());
                Ok(false)
            }
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// Put a deadline on an existing key. `true` if it was applied, `false` if `key` was
    /// missing.
    ///
    /// A past timestamp deletes the key immediately rather than storing a
    /// guaranteed-stale entry, and still reports `true`. That matches real Redis
    /// EXPIREAT, and it spares the lazy path a case it would otherwise have to handle.
    pub fn set_expires_at(
        &self,
        key: impl AsRef<[u8]>,
        expires_at: Milliseconds,
    ) -> Result<bool, CacheError> {
        if Milliseconds::now()? >= expires_at {
            return Ok(self.remove(key)?.is_some());
        }
        let mut guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        let Some(entry) = guard.get_mut(key.as_ref()) else {
            return Ok(false);
        };
        entry.expires_at = Some(expires_at);
        Ok(true)
    }

    /// Query the deadline on `key`. The nested `Option` carries three states:
    ///
    /// - `Ok(None)`, `key` is missing, or was expired and just got swept here.
    /// - `Ok(Some(None))`, `key` exists with no TTL.
    /// - `Ok(Some(Some(t)))`, `key` expires at UNIX millisecond `t`.
    ///
    /// A passed deadline removes the entry and reports `Ok(None)`.
    pub fn get_expires_at(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Option<Milliseconds>>, CacheError> {
        let guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        let Some(Entry { expires_at, .. }) = guard.get(key.as_ref()) else {
            return Ok(None);
        };
        let expires_at = expires_at.to_owned();
        drop(guard);
        let now = Milliseconds::now()?;
        if expires_at.is_some_and(|t| now >= t) {
            self.remove(key)?;
            return Ok(None);
        }
        Ok(Some(expires_at))
    }

    /// Time remaining on `key`, in milliseconds. Same three states as
    /// [`Cache::get_expires_at`]:
    ///
    /// - `Ok(None)`, `key` missing or just expired.
    /// - `Ok(Some(None))`, `key` exists with no TTL.
    /// - `Ok(Some(Some(n)))`, `n` millis left. Saturating, so it can't underflow.
    ///
    /// Feeds the `TTL` reply (`:-2`, `:-1`, `:n`), which reports seconds and so has to
    /// divide.
    pub fn time_to_live(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Option<Milliseconds>>, CacheError> {
        let expires_at = match self.get_expires_at(key.as_ref())? {
            None => return Ok(None),
            Some(None) => return Ok(Some(None)),
            Some(Some(t)) => t,
        };
        Ok(Some(Some(expires_at.saturating_sub(Milliseconds::now()?))))
    }

    /// Drop the TTL on `key` but keep the value. `false` if `key` is missing or had no
    /// TTL to begin with. Feeds `PERSIST`.
    pub fn remove_ttl(&self, key: impl AsRef<[u8]>) -> Result<bool, CacheError> {
        let mut guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        let Some(entry) = guard.get_mut(key.as_ref()) else {
            return Ok(false);
        };
        if entry.expires_at.is_some() {
            entry.expires_at = None;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Sweep every expired entry, returning how many went. The sweeper thread in
    /// `inbound::server` calls this on a tick.
    ///
    /// The lock is held for the whole scan-and-delete pass. At this scale that's
    /// microseconds. If it ever starts hurting tail latency, snapshot the expired keys
    /// first and re-check each TTL during eviction.
    pub fn remove_expired(&self) -> Result<usize, CacheError> {
        let now = Milliseconds::now()?;
        let mut guard = self.inner.lock().map_err(|_| CacheError::MutexPoisoned)?;
        let expired: Vec<Vec<u8>> = guard
            .iter()
            .filter(|(_, Entry { expires_at, .. })| expires_at.is_some_and(|t| now >= t))
            .map(|(key, _)| key.to_owned())
            .collect();
        for key in &expired {
            guard.remove(key);
        }
        Ok(expired.len())
    }

    pub fn execute(&self, command: &CacheCommand) -> Result<CommandOutcome, CacheError> {
        Ok(match command {
            CacheCommand::Read(ReadCommand::Get { key }) => match self.get(key)? {
                Some(Entry { value, .. }) => CommandOutcome::Value(Some(value)),
                None => CommandOutcome::Value(None),
            },
            CacheCommand::Read(ReadCommand::Exists { key }) => {
                CommandOutcome::Integer(i64::from(self.contains(key)?))
            }
            // TTL reports whole seconds, rounded up so a just-set 60s TTL reads back as
            // 60 rather than 59. A PTTL arm would hand back the Milliseconds untouched.
            CacheCommand::Read(ReadCommand::Ttl { key }) => {
                CommandOutcome::Ttl(match self.time_to_live(key)? {
                    None => TtlOutcome::KeyNotFound,
                    Some(None) => TtlOutcome::TtlNotFound,
                    Some(Some(ttl)) => TtlOutcome::Some(ttl.to_seconds_rounded_up()),
                })
            }
            // A fresh Entry is what clears any TTL the key already had, so plain SET gets
            // Redis's default behavior for free.
            CacheCommand::Write(WriteCommand::Set {
                key,
                value,
                expires_at,
            }) => {
                self.insert(key.as_slice(), Entry::new(value.as_slice(), *expires_at))?;
                CommandOutcome::Ok
            }
            CacheCommand::Write(WriteCommand::Delete { key }) => {
                CommandOutcome::Bool(self.remove(key)?.is_some())
            }
            CacheCommand::Write(WriteCommand::ExpireAt { key, expires_at }) => {
                CommandOutcome::Bool(self.set_expires_at(key, *expires_at)?)
            }
            CacheCommand::Write(WriteCommand::Persist { key }) => {
                CommandOutcome::Bool(self.remove_ttl(key)?)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::time::Seconds;

    // ---------- helpers ----------

    fn now() -> Milliseconds {
        Milliseconds::now().unwrap()
    }

    /// A deadline `secs` from now.
    fn in_secs(secs: u64) -> Milliseconds {
        now().saturating_add(Seconds::new(secs).into())
    }

    // ---------- Entry ----------

    #[test]
    fn entry_constructor_without_ttl() {
        assert_eq!(
            Entry::new("foo", None),
            Entry {
                value: b"foo".to_vec(),
                expires_at: None
            }
        );
    }

    #[test]
    fn entry_constructor_with_ttl() {
        assert_eq!(
            Entry::new("foo", Some(Milliseconds::new(123))),
            Entry {
                value: b"foo".to_vec(),
                expires_at: Some(Milliseconds::new(123))
            }
        );
    }

    // ---------- get ----------

    #[test]
    fn get_miss_returns_none() {
        let cache = Cache::default();
        assert_eq!(cache.get("missing").unwrap(), None);
    }

    #[test]
    fn get_hit_no_ttl_returns_entry() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("bar", None)));
    }

    #[test]
    fn get_hit_future_ttl_returns_entry() {
        let cache = Cache::default();
        let future = in_secs(3600);
        cache
            .insert("foo", Entry::new("bar", Some(future)))
            .unwrap();
        assert_eq!(
            cache.get("foo").unwrap(),
            Some(Entry::new("bar", Some(future)))
        );
    }

    #[test]
    fn get_hit_expired_returns_none_and_removes() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(Milliseconds::new(1))))
            .unwrap();
        assert_eq!(cache.get("foo").unwrap(), None);
        // Verify the lazy expiry actually removed it.
        assert!(!cache.contains("foo").unwrap());
    }

    #[test]
    fn get_hit_at_exact_expiry_returns_none() {
        let cache = Cache::default();
        let t = now();
        cache.insert("foo", Entry::new("bar", Some(t))).unwrap();
        // now >= t triggers the expired branch.
        assert_eq!(cache.get("foo").unwrap(), None);
    }

    // ---------- insert ----------

    #[test]
    fn insert_new_returns_none() {
        let cache = Cache::default();
        assert_eq!(cache.insert("foo", Entry::new("bar", None)).unwrap(), None);
    }

    #[test]
    fn insert_existing_returns_old_entry() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("old", None)).unwrap();
        assert_eq!(
            cache.insert("foo", Entry::new("new", None)).unwrap(),
            Some(Entry::new("old", None))
        );
    }

    #[test]
    fn insert_replaces_value_and_ttl() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("old", Some(in_secs(100))))
            .unwrap();
        cache.insert("foo", Entry::new("new", None)).unwrap();
        assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("new", None)));
    }

    // ---------- remove ----------

    #[test]
    fn remove_hit_returns_entry() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert_eq!(cache.remove("foo").unwrap(), Some(Entry::new("bar", None)));
    }

    #[test]
    fn remove_miss_returns_none() {
        let cache = Cache::default();
        assert_eq!(cache.remove("missing").unwrap(), None);
    }

    #[test]
    fn remove_actually_removes() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        cache.remove("foo").unwrap();
        assert_eq!(cache.get("foo").unwrap(), None);
    }

    // ---------- contains ----------

    #[test]
    fn contains_miss_returns_false() {
        let cache = Cache::default();
        assert!(!cache.contains("missing").unwrap());
    }

    #[test]
    fn contains_hit_no_ttl_returns_true() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(cache.contains("foo").unwrap());
    }

    #[test]
    fn contains_hit_future_ttl_returns_true() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(in_secs(3600))))
            .unwrap();
        assert!(cache.contains("foo").unwrap());
    }

    #[test]
    fn contains_hit_expired_returns_false_and_removes() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(Milliseconds::new(1))))
            .unwrap();
        assert!(!cache.contains("foo").unwrap());
        assert_eq!(cache.get("foo").unwrap(), None);
    }

    // ---------- set_expires_at ----------

    #[test]
    fn set_expires_at_existing_key_future_returns_true_and_sets() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        let future = in_secs(3600);
        assert!(cache.set_expires_at("foo", future).unwrap());
        assert_eq!(
            cache.get("foo").unwrap(),
            Some(Entry::new("bar", Some(future)))
        );
    }

    #[test]
    fn set_expires_at_missing_key_future_returns_false() {
        let cache = Cache::default();
        assert!(!cache.set_expires_at("missing", in_secs(3600)).unwrap());
    }

    #[test]
    fn set_expires_at_past_existing_key_removes_and_returns_true() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(cache.set_expires_at("foo", Milliseconds::new(0)).unwrap());
        assert_eq!(cache.get("foo").unwrap(), None);
    }

    #[test]
    fn set_expires_at_past_missing_key_returns_false() {
        let cache = Cache::default();
        assert!(
            !cache
                .set_expires_at("missing", Milliseconds::new(0))
                .unwrap()
        );
    }

    #[test]
    fn set_expires_at_overwrites_existing_ttl() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(in_secs(100))))
            .unwrap();
        let new_ttl = in_secs(7200);
        assert!(cache.set_expires_at("foo", new_ttl).unwrap());
        assert_eq!(cache.get_expires_at("foo").unwrap(), Some(Some(new_ttl)));
    }

    // ---------- get_expires_at ----------

    #[test]
    fn get_expires_at_missing_key_returns_none() {
        let cache = Cache::default();
        assert_eq!(cache.get_expires_at("missing").unwrap(), None);
    }

    #[test]
    fn get_expires_at_no_ttl_returns_some_none() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert_eq!(cache.get_expires_at("foo").unwrap(), Some(None));
    }

    #[test]
    fn get_expires_at_future_returns_some_some_timestamp() {
        let cache = Cache::default();
        let future = in_secs(3600);
        cache
            .insert("foo", Entry::new("bar", Some(future)))
            .unwrap();
        assert_eq!(cache.get_expires_at("foo").unwrap(), Some(Some(future)));
    }

    #[test]
    fn get_expires_at_expired_removes_and_returns_none() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(Milliseconds::new(1))))
            .unwrap();
        assert_eq!(cache.get_expires_at("foo").unwrap(), None);
        assert!(!cache.contains("foo").unwrap());
    }

    // ---------- time_to_live ----------

    #[test]
    fn time_to_live_missing_key_returns_none() {
        let cache = Cache::default();
        assert_eq!(cache.time_to_live("missing").unwrap(), None);
    }

    #[test]
    fn time_to_live_no_ttl_returns_some_none() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert_eq!(cache.time_to_live("foo").unwrap(), Some(None));
    }

    #[test]
    fn time_to_live_future_returns_remaining_millis() {
        let cache = Cache::default();
        let future = in_secs(3600);
        cache
            .insert("foo", Entry::new("bar", Some(future)))
            .unwrap();
        let remaining = match cache.time_to_live("foo").unwrap() {
            Some(Some(t)) => t,
            other => panic!("expected Some(Some(_)), got {other:?}"),
        };
        // ~3600s of millis, with slack for the clock ticking during the call.
        assert!((3_590_000..=3_600_000).contains(&remaining.get()));
    }

    #[test]
    fn time_to_live_expired_returns_none() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(Milliseconds::new(1))))
            .unwrap();
        assert_eq!(cache.time_to_live("foo").unwrap(), None);
    }

    // ---------- remove_ttl ----------

    #[test]
    fn remove_ttl_had_ttl_returns_true_and_clears() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(in_secs(100))))
            .unwrap();
        assert!(cache.remove_ttl("foo").unwrap());
        assert_eq!(cache.get_expires_at("foo").unwrap(), Some(None));
    }

    #[test]
    fn remove_ttl_no_ttl_returns_false() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(!cache.remove_ttl("foo").unwrap());
    }

    #[test]
    fn remove_ttl_missing_key_returns_false() {
        let cache = Cache::default();
        assert!(!cache.remove_ttl("missing").unwrap());
    }

    #[test]
    fn remove_ttl_keeps_value_intact() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(in_secs(100))))
            .unwrap();
        cache.remove_ttl("foo").unwrap();
        assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("bar", None)));
    }

    // ---------- remove_expired ----------

    #[test]
    fn remove_expired_empty_returns_zero() {
        let cache = Cache::default();
        assert_eq!(cache.remove_expired().unwrap(), 0);
    }

    #[test]
    fn remove_expired_drops_expired_keys() {
        let cache = Cache::default();
        cache
            .insert("a", Entry::new("v", Some(Milliseconds::new(1))))
            .unwrap();
        cache
            .insert("b", Entry::new("v", Some(Milliseconds::new(1))))
            .unwrap();
        assert_eq!(cache.remove_expired().unwrap(), 2);
        assert!(!cache.contains("a").unwrap());
        assert!(!cache.contains("b").unwrap());
    }

    #[test]
    fn remove_expired_keeps_future_keys() {
        let cache = Cache::default();
        cache
            .insert("a", Entry::new("v", Some(in_secs(3600))))
            .unwrap();
        assert_eq!(cache.remove_expired().unwrap(), 0);
        assert!(cache.contains("a").unwrap());
    }

    #[test]
    fn remove_expired_keeps_no_ttl_keys() {
        let cache = Cache::default();
        cache.insert("a", Entry::new("v", None)).unwrap();
        assert_eq!(cache.remove_expired().unwrap(), 0);
        assert!(cache.contains("a").unwrap());
    }

    // ---------- execute ----------

    fn get(key: impl Into<Vec<u8>>) -> CacheCommand {
        ReadCommand::get(key).into()
    }
    // fn set(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> CacheCommand {
    //     WriteCommand::set(key, value).into()
    // }
    fn delete(key: impl Into<Vec<u8>>) -> CacheCommand {
        WriteCommand::delete(key).into()
    }
    fn exists(key: impl Into<Vec<u8>>) -> CacheCommand {
        ReadCommand::exists(key).into()
    }
    fn expire_at(key: impl Into<Vec<u8>>, expires_at: Milliseconds) -> CacheCommand {
        WriteCommand::expire_at(key, expires_at).into()
    }
    fn ttl(key: impl Into<Vec<u8>>) -> CacheCommand {
        ReadCommand::ttl(key).into()
    }
    fn persist(key: impl Into<Vec<u8>>) -> CacheCommand {
        WriteCommand::persist(key).into()
    }

    #[test]
    fn execute_get_miss_returns_value_none() {
        let cache = Cache::default();
        assert!(matches!(
            cache.execute(&get("missing")).unwrap(),
            CommandOutcome::Value(None)
        ));
    }

    #[test]
    fn execute_get_hit_returns_value_some() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        match cache.execute(&get("foo")).unwrap() {
            CommandOutcome::Value(Some(v)) => assert_eq!(v, b"bar".to_vec()),
            other => panic!("expected Value(Some), got {other:?}"),
        }
    }

    #[test]
    fn execute_get_expired_returns_value_none() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(Milliseconds::new(1))))
            .unwrap();
        assert!(matches!(
            cache.execute(&get("foo")).unwrap(),
            CommandOutcome::Value(None)
        ));
    }

    // #[test]
    // fn execute_set_returns_ok_and_inserts() {
    //     let cache = Cache::default();
    //     assert!(matches!(
    //         cache.execute(&set("foo", "bar")).unwrap(),
    //         CommandOutcome::Ok
    //     ));
    //     assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("bar", None)));
    // }

    // #[test]
    // fn execute_set_overwrites() {
    //     let cache = Cache::default();
    //     cache.insert("foo", Entry::new("old", None)).unwrap();
    //     cache.execute(&set("foo", "new")).unwrap();
    //     assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("new", None)));
    // }

    #[test]
    fn execute_delete_hit_returns_bool_true() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(matches!(
            cache.execute(&delete("foo")).unwrap(),
            CommandOutcome::Bool(true)
        ));
        assert!(!cache.contains("foo").unwrap());
    }

    #[test]
    fn execute_delete_miss_returns_bool_false() {
        let cache = Cache::default();
        assert!(matches!(
            cache.execute(&delete("missing")).unwrap(),
            CommandOutcome::Bool(false)
        ));
    }

    #[test]
    fn execute_exists_hit_returns_integer_one() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(matches!(
            cache.execute(&exists("foo")).unwrap(),
            CommandOutcome::Integer(1)
        ));
    }

    #[test]
    fn execute_exists_miss_returns_integer_zero() {
        let cache = Cache::default();
        assert!(matches!(
            cache.execute(&exists("missing")).unwrap(),
            CommandOutcome::Integer(0)
        ));
    }

    #[test]
    fn execute_expire_at_existing_returns_bool_true() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(matches!(
            cache.execute(&expire_at("foo", in_secs(3600))).unwrap(),
            CommandOutcome::Bool(true)
        ));
    }

    #[test]
    fn execute_expire_at_missing_returns_bool_false() {
        let cache = Cache::default();
        assert!(matches!(
            cache.execute(&expire_at("missing", in_secs(3600))).unwrap(),
            CommandOutcome::Bool(false)
        ));
    }

    #[test]
    fn execute_ttl_missing_returns_key_not_found() {
        let cache = Cache::default();
        assert!(matches!(
            cache.execute(&ttl("missing")).unwrap(),
            CommandOutcome::Ttl(TtlOutcome::KeyNotFound)
        ));
    }

    #[test]
    fn execute_ttl_no_ttl_returns_ttl_not_found() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(matches!(
            cache.execute(&ttl("foo")).unwrap(),
            CommandOutcome::Ttl(TtlOutcome::TtlNotFound)
        ));
    }

    #[test]
    fn execute_ttl_with_ttl_returns_ttl_some() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(in_secs(3600))))
            .unwrap();
        match cache.execute(&ttl("foo")).unwrap() {
            CommandOutcome::Ttl(TtlOutcome::Some(t)) => {
                assert!((3590..=3600).contains(&t.get()));
            }
            other => panic!("expected Ttl(Some), got {other:?}"),
        }
    }

    #[test]
    fn execute_persist_had_ttl_returns_bool_true() {
        let cache = Cache::default();
        cache
            .insert("foo", Entry::new("bar", Some(in_secs(3600))))
            .unwrap();
        assert!(matches!(
            cache.execute(&persist("foo")).unwrap(),
            CommandOutcome::Bool(true)
        ));
    }

    #[test]
    fn execute_persist_no_ttl_returns_bool_false() {
        let cache = Cache::default();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        assert!(matches!(
            cache.execute(&persist("foo")).unwrap(),
            CommandOutcome::Bool(false)
        ));
    }

    #[test]
    fn execute_persist_missing_returns_bool_false() {
        let cache = Cache::default();
        assert!(matches!(
            cache.execute(&persist("missing")).unwrap(),
            CommandOutcome::Bool(false)
        ));
    }

    #[test]
    fn remove_expired_mixed() {
        let cache = Cache::default();
        cache
            .insert("expired_a", Entry::new("v", Some(Milliseconds::new(1))))
            .unwrap();
        cache
            .insert("expired_b", Entry::new("v", Some(Milliseconds::new(1))))
            .unwrap();
        cache
            .insert("future", Entry::new("v", Some(in_secs(3600))))
            .unwrap();
        cache.insert("no_ttl", Entry::new("v", None)).unwrap();
        assert_eq!(cache.remove_expired().unwrap(), 2);
        assert!(!cache.contains("expired_a").unwrap());
        assert!(!cache.contains("expired_b").unwrap());
        assert!(cache.contains("future").unwrap());
        assert!(cache.contains("no_ttl").unwrap());
    }
}
