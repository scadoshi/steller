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
//! The listener blocks in `accept()`, so an idle server costs nothing and a new connection
//! is picked up the instant it arrives. To get the accept loop out of that block on
//! shutdown, the stdin thread flips the flag and then opens one throwaway connection to
//! the listener's own address: `accept()` returns it, the loop sees the flag, drops the
//! stream, and stops. (The listener used to be non-blocking with a 50ms sleep between
//! polls, which put up to 50ms on every new connection's first request.)

use crate::{
    domain::{
        cache::Cache, channels::Channels, ports::CacheRepository, service::Service as CacheService,
    },
    inbound::session::Session,
    outbound::persister::Persister,
};
use std::{
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{JoinHandle, sleep, spawn},
    time::Duration,
};

/// Address the server listens on.
const BIND_ADDRESS: &str = "127.0.0.1:3000";

/// Zero-sized handle; the server is [`Server::run`].
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

        // Bind before spawning anything: a failed bind is then a clean error with no
        // threads to unwind, and the shutdown thread knows where to knock.
        let listener = TcpListener::bind(BIND_ADDRESS)?;
        let wake_address = listener.local_addr()?;

        // shutdown
        let shutdown_trigger = shutdown.clone();
        handles.push(spawn(move || {
            // Flip the flag, then knock on the listener so `accept()` returns and the loop
            // can see it. `Release` pairs with the `Acquire` load in the accept loop: it
            // must never mistake the knock for a client, because spawning a session for it
            // and blocking in `accept()` again would hang shutdown with nobody left to
            // connect.
            let trigger = || {
                shutdown_trigger.store(true, Ordering::Release);
                if let Err(e) = TcpStream::connect(wake_address) {
                    eprintln!("failed to wake the accept loop: {e}");
                }
            };
            let mut s = String::new();
            loop {
                s.clear();
                match std::io::stdin().read_line(&mut s) {
                    Ok(0) => {
                        trigger();
                        break;
                    }
                    Ok(_) if matches!(s.trim().to_lowercase().as_str(), "quit" | "exit") => {
                        trigger();
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
        println!("listening on {BIND_ADDRESS}");
        loop {
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            handles.retain(|t| !t.is_finished());
            match listener.accept() {
                Ok((writer_stream, _)) => {
                    if shutdown.load(Ordering::Acquire) {
                        // The shutdown thread's knock, or a client that raced it. Either
                        // way it gets no session.
                        drop(writer_stream);
                        break;
                    }
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
                Err(e) => eprintln!("failed to accept connection: {e}"),
            }
        }
        for h in handles {
            let _ = h.join();
        }
        Ok(())
    }
}
