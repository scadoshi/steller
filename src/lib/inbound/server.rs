//! TCP server and lifecycle. Owns the accept loop, the shared [`Cache`], and the
//! background threads.
//!
//! ## Threads
//!
//! The main thread runs the accept loop and spawns one [`Session`] thread per connection.
//! A persistence thread snapshots every 10s and once more on the way out. A sweeper
//! thread calls `remove_expired` on the same tick. A shutdown thread blocks on stdin and
//! flips the shared flag on EOF, `quit`, or `exit`.
//!
//! Every long-running thread holds the same `Arc<AtomicBool>` and checks it every 100ms,
//! which bounds how long shutdown takes. `JoinHandle`s are pruned with `is_finished()`
//! while the server runs and joined on exit, so nothing gets dropped silently.
//!
//! The listener is non-blocking so the accept loop can poll the flag instead of parking
//! inside `accept()`. The 50ms sleep on `WouldBlock` is what keeps that from spinning a
//! core when nobody is connecting.

use crate::{
    domain::{
        cache::Cache, channels::Channels, ports::CacheRepository, service::Service as CacheService,
    },
    inbound::session::Session,
    outbound::persister::Persister,
};
use std::{
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{JoinHandle, sleep, spawn},
    time::Duration,
};

/// Address the server binds for client connections.
const BIND_ADDRESS: &str = "127.0.0.1:3000";

/// Zero-sized handle. The server is really just a function; this exists so callers have
/// a stable `Server::run` to call.
pub struct Server;
impl Server {
    /// Start the server. Blocks until shutdown is signaled, then joins every spawned
    /// thread before returning.
    ///
    /// `Err` only on fatal setup: a failed bind or a snapshot that won't load.
    /// Per-connection errors are logged and stay put.
    #[expect(clippy::too_many_lines)]
    pub fn run() -> anyhow::Result<()> {
        let persister = Persister::initialize()?;
        let cache = Cache::new(persister.snapshot.load()?);
        persister.aof.replay(&cache)?;
        cache.remove_expired()?;

        let cache_service = CacheService::new(cache.clone(), persister.clone());
        let channels = Channels::new();

        let shutdown = Arc::new(AtomicBool::new(false));
        let mut id = 0u32;

        let mut handles = Vec::<JoinHandle<()>>::new();

        // shutdown
        let shutdown_trigger = shutdown.clone();
        handles.push(spawn(move || {
            let mut s = String::new();
            loop {
                s.clear();
                match std::io::stdin().read_line(&mut s) {
                    Ok(0) => {
                        shutdown_trigger.store(true, Ordering::Relaxed);
                        break;
                    }
                    Ok(_) if matches!(s.trim().to_lowercase().as_str(), "quit" | "exit") => {
                        shutdown_trigger.store(true, Ordering::Relaxed);
                        break;
                    }
                    _ => (),
                }
            }
        }));

        // persistence
        let persistence_cache = cache.clone();
        let persist = move || match persister.snapshot(&persistence_cache) {
            Ok(()) => println!("cache persisted"),
            Err(e) => eprintln!("failed to persist cache: {e}"),
        };

        let persistence_shutdown = shutdown.clone();
        handles.push(spawn(move || {
            loop {
                for _ in 0..100 {
                    sleep(Duration::from_millis(100));
                    if persistence_shutdown.load(Ordering::Relaxed) {
                        persist();
                        return;
                    }
                }
                persist();
            }
        }));

        // ttl
        let sweeper_cache = cache.clone();
        let remove_expired = move || match sweeper_cache.remove_expired() {
            Ok(expired) => println!("{expired} expired keys removed"),
            Err(e) => eprintln!("failed to remove expired keys: {e}"),
        };
        let sweeper_shutdown = shutdown.clone();
        handles.push(spawn(move || {
            loop {
                for _ in 0..100 {
                    sleep(Duration::from_millis(100));
                    if sweeper_shutdown.load(Ordering::Relaxed) {
                        remove_expired();
                        return;
                    }
                }
                remove_expired();
            }
        }));

        // main
        let listener = TcpListener::bind(BIND_ADDRESS)?;
        listener.set_nonblocking(true)?;
        println!("listening on {BIND_ADDRESS}");
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            handles.retain(|t| !t.is_finished());
            match listener.accept() {
                Ok((writer_stream, _)) => {
                    let shutdown_clone = shutdown.clone();
                    id = id.wrapping_add(1);
                    println!("client {id} connected");
                    let cache_service_clone = cache_service.clone();
                    let channels_clone = channels.clone();
                    handles.push(spawn(move || {
                        let reader_stream = match writer_stream.try_clone() {
                            Ok(stream) => stream,
                            Err(e) => {
                                eprintln!("failed to clone stream: {e}");
                                return;
                            }
                        };
                        if let Err(e) =
                            reader_stream.set_read_timeout(Some(Duration::from_millis(500)))
                        {
                            eprintln!("failed to set stream read timeout: {e}");
                        }
                        let session = Session::new(
                            id,
                            reader_stream,
                            writer_stream,
                            cache_service_clone,
                            channels_clone,
                        );
                        match session.repl(&shutdown_clone) {
                            Ok(()) => (),
                            Err(e) => eprintln!("failed to repl: {e}"),
                        }
                    }));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => eprintln!("failed to accept connection: {e}"),
            }
        }
        for h in handles {
            let _ = h.join();
        }
        Ok(())
    }
}
