//! The orchestrator behind the [`CacheService`] port.
//!
//! [`Service`] is the one place that knows how a command becomes both an in-memory effect
//! and a durable one: apply to the cache, then append to the log if it mutated anything.
//!
//! Generic over `CR: CacheRepository`, so the concrete persister is a plug-in and tests
//! can substitute a recording fake.

use crate::domain::{
    cache::Cache,
    command::{cache::CacheCommand, outcome::CommandOutcome},
    ports::{CacheRepository, CacheService, ServiceError},
};

/// Command orchestrator. Both fields are shared handles, so cloning is cheap and every
/// session thread can hold a copy pointing at the same cache and log.
#[derive(Clone)]
pub struct Service<CR: CacheRepository> {
    cache: Cache,
    cache_repo: CR,
}

impl<CR: CacheRepository> Service<CR> {
    /// A service over a shared cache and a repository.
    pub fn new(cache: Cache, cache_repo: CR) -> Self {
        Self { cache, cache_repo }
    }
}

impl<CR: CacheRepository> CacheService for Service<CR> {
    /// Run a command against the cache only. No persistence, so rebuilding from the log
    /// doesn't re-write the log.
    fn execute(&self, command: &CacheCommand) -> Result<CommandOutcome, ServiceError> {
        let outcome = self.cache.execute(command)?;
        Ok(outcome)
    }

    /// Run a command and, if it is a [`CacheCommand::Write`], append it to the log.
    ///
    /// The [`CacheCommand`] variant already tells reads from writes, so there is no verb
    /// matching here. Ordering matters: the cache effect and the log append have to land in
    /// the same order across concurrent writers or replay diverges from the live cache. The
    /// two locks are taken independently, so that window isn't fully closed. A single lock
    /// spanning both, or a single-writer model, is what would close it.
    ///
    /// [`CacheCommand::Write`]: crate::domain::command::cache::CacheCommand::Write
    fn execute_logged(&self, command: CacheCommand) -> Result<CommandOutcome, ServiceError> {
        let outcome = self.execute(&command)?;
        if let CacheCommand::Write(wc) = command {
            self.cache_repo.append(wc)?;
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::time::Milliseconds;
    use crate::{
        domain::{
            cache::Entry,
            command::cache::{read::ReadCommand, write::WriteCommand},
        },
        test_support::RecordingRepo,
    };

    fn build() -> (Service<RecordingRepo>, Cache, RecordingRepo) {
        let cache = Cache::default();
        let repo = RecordingRepo::default();
        let service = Service::new(cache.clone(), repo.clone());
        (service, cache, repo)
    }

    // ---------- execute ----------

    #[test]
    fn execute_get_miss_returns_value_none() {
        let (svc, _, _) = build();
        assert!(matches!(
            svc.execute(&ReadCommand::get("missing").into()).unwrap(),
            CommandOutcome::Value(None)
        ));
    }

    #[test]
    fn execute_set_writes_to_cache() {
        let (svc, cache, _) = build();
        svc.execute(&WriteCommand::set("foo", "bar", None).into())
            .unwrap();
        assert_eq!(cache.get("foo").unwrap(), Some(Entry::new("bar", None)));
    }

    #[test]
    fn execute_set_with_deadline_writes_it_through() {
        let (svc, cache, _) = build();
        let deadline = Milliseconds::now()
            .unwrap()
            .saturating_add(Milliseconds::new(60_000));
        svc.execute(&WriteCommand::set("foo", "bar", Some(deadline)).into())
            .unwrap();
        assert_eq!(cache.get_expires_at("foo").unwrap(), Some(Some(deadline)));
    }

    // A plain SET replaces the whole Entry, which is how it clears an existing TTL.
    #[test]
    fn execute_plain_set_clears_an_existing_ttl() {
        let (svc, cache, _) = build();
        let deadline = Milliseconds::now()
            .unwrap()
            .saturating_add(Milliseconds::new(60_000));
        cache
            .insert("foo", Entry::new("bar", Some(deadline)))
            .unwrap();
        svc.execute(&WriteCommand::set("foo", "baz", None).into())
            .unwrap();
        assert_eq!(cache.get_expires_at("foo").unwrap(), Some(None));
    }

    #[test]
    fn execute_does_not_append_to_repo() {
        let (svc, _, repo) = build();
        svc.execute(&WriteCommand::set("foo", "bar", None).into())
            .unwrap();
        svc.execute(&WriteCommand::delete("foo").into()).unwrap();
        svc.execute(&WriteCommand::expire_at("foo", Milliseconds::new(u64::MAX)).into())
            .unwrap();
        assert!(repo.appended.lock().unwrap().is_empty());
    }

    // ---------- execute_logged ----------

    #[test]
    fn execute_logged_set_appends() {
        let (svc, _, repo) = build();
        svc.execute_logged(WriteCommand::set("foo", "bar", None).into())
            .unwrap();
        let log = repo.appended.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert!(matches!(
            &log[0],
            WriteCommand::Set { key, value, expires_at: None }
                if key == b"foo" && value == b"bar"
        ));
    }

    #[test]
    fn execute_logged_delete_appends() {
        let (svc, _, repo) = build();
        svc.execute_logged(WriteCommand::delete("foo").into())
            .unwrap();
        let log = repo.appended.lock().unwrap();
        assert!(matches!(&log[0], WriteCommand::Delete { key } if key == b"foo"));
    }

    #[test]
    fn execute_logged_expire_at_appends() {
        let (svc, cache, repo) = build();
        cache.insert("foo", Entry::new("bar", None)).unwrap();
        svc.execute_logged(WriteCommand::expire_at("foo", Milliseconds::new(u64::MAX)).into())
            .unwrap();
        let log = repo.appended.lock().unwrap();
        assert!(matches!(
            &log[0],
            WriteCommand::ExpireAt { key, expires_at }
                if key == b"foo" && expires_at.get() == u64::MAX
        ));
    }

    #[test]
    fn execute_logged_persist_appends() {
        let (svc, _, repo) = build();
        svc.execute_logged(WriteCommand::persist("foo").into())
            .unwrap();
        let log = repo.appended.lock().unwrap();
        assert!(matches!(&log[0], WriteCommand::Persist { key } if key == b"foo"));
    }

    #[test]
    fn execute_logged_get_does_not_append() {
        let (svc, _, repo) = build();
        svc.execute_logged(ReadCommand::get("foo").into()).unwrap();
        assert!(repo.appended.lock().unwrap().is_empty());
    }

    #[test]
    fn execute_logged_exists_does_not_append() {
        let (svc, _, repo) = build();
        svc.execute_logged(ReadCommand::exists("foo").into())
            .unwrap();
        assert!(repo.appended.lock().unwrap().is_empty());
    }

    #[test]
    fn execute_logged_ttl_does_not_append() {
        let (svc, _, repo) = build();
        svc.execute_logged(ReadCommand::ttl("foo").into()).unwrap();
        assert!(repo.appended.lock().unwrap().is_empty());
    }

    #[test]
    fn execute_logged_set_returns_ok_outcome() {
        let (svc, _, _) = build();
        assert!(matches!(
            svc.execute_logged(WriteCommand::set("foo", "bar", None).into())
                .unwrap(),
            CommandOutcome::Ok
        ));
    }
}
