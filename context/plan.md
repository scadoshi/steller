# steller progress

Where the project actually is: milestone status, decisions made, gotchas surfaced, what's next.

## Where I am

**M1 through M6 complete.** SET options and the millisecond migration landed most recently, written up in full in `context/set_options.md`.

GET / SET / DEL / EXISTS / EXPIRE / EXPIREAT / TTL / PERSIST / PING (with optional message echo) all working end-to-end over real RESP. Persistence shipped via hexagonal ports: snapshot baseline (`wincode` + temp+rename) plus an AOF (RESP-encoded mutations) replayed on startup. `redis-cli -p 3000` is the verified client. Session is generic over `R: Read` / `W: Write` / `CS: CacheService`; `SessionReader` owns the frame-accumulation buf and handles drain/Incomplete/hard-err paths correctly. Bad commands return `-ERR ...\r\n` SimpleError and the session continues.

**Graceful shutdown complete.** `Arc<AtomicBool>` shutdown flag, blocking `accept()` woken on shutdown by a self-connect from the stdin thread (was a non-blocking listener with a 50ms `WouldBlock` throttle, which put up to 50ms on every new connection's first request; changed 2026-09-19, see the benchmarking entry below), stdin EOF / "quit" / "exit" trigger shutdown, all spawned threads (persistence + sweeper + per-client) are collected via `JoinHandle` and joined cleanly before `Server::run` returns. Persistence and sweeper threads cooperate with the flag via short-tick (100ms × 100) sleep loops so shutdown latency is bounded.

**Testing posture.** 238 unit tests passing. Frame parser, Command-from-Frame, Reply serializer, Crlf, and SessionReader all have unit tests. `Cache` has a comprehensive unit suite (every public method, lazy-expiry, past-TTL semantics, bulk remove-expired) plus per-variant coverage of `Cache::execute`. The new persistence layer is fully covered: `Snapshot` round-trip (incl. atomic temp+rename, missing-file error), `Aof` (append → exact RESP bytes, replay, torn-tail tolerance, malformed-frame error, clear), and `Persister` (append + snapshot-then-clear checkpoint, post-snapshot logging resumes). `Service` tests verify `execute` never logs and `execute_logged` appends iff mutating. `Session::execute` has per-variant wire-byte assertions and `get_command` has happy-path + bad-frame + EOF coverage. The `CommandOutcome → Reply` mapping is exhaustively tested. Every error path on every command has explicit coverage in `resp/command.rs`. Tests grouped under `// ---------- name ----------` section headers for navigability. Shared fakes (`RecordingRepo`, `SharedWriter`, `TempPath` RAII temp-file helper) live in `src/lib/test_support.rs` behind `#[cfg(test)]`, with no `tempfile` crate.

**Strategic phase shift.** From here forward, work is allocated by *what's novel vs. what's rehearsed*, not by milestone order. The user has already shipped LSM-style persistence (WAL + memtable + SSTable + compaction + bloom filters) in `chickadee`. That makes append-only-log mechanics rehearsed muscle, so AI-assisted is fine. The unrehearsed pieces (pub/sub fan-out, MULTI/EXEC, async migration) get hand-written.

## Testing next up

Coverage is comprehensive through M4. Outstanding tests are deferred to the features that drive them:

- **End-to-end `repl`.** Happy multi-command flow ending in clean disconnect, and bad-command-mid-stream proving the session continues after a SimpleError. Lower-priority since `Session::execute` + `get_command` are both individually tested.
- **Concurrent persister stress.** Multi-threaded append + concurrent snapshot, verifying the checkpoint serialization holds. Owed once we have a real load story.
- **Subscribed-mode session tests.** Deferred with the feature itself: subscribed-mode command restriction was never implemented in M5, so there is nothing to test yet.
- **EXEC atomicity tests.** Once MULTI/EXEC lands, the lock-once-across-the-queue property needs a test where a second session is blocked mid-EXEC and observes a consistent before and after state.

## Status by milestone

### M0 TCP echo server ✅
- [x] `TcpListener` bind + accept loop (`inbound/server.rs`)
- [x] Thread-per-connection via `std::thread::spawn`
- [x] Per-connection `Session` struct owns reader/writer halves

### M1 Protocol and dispatch ✅
- [x] **Byte-slice utilities.** `Crlf` trait in `src/lib/inbound/resp/crlf.rs` with `is_crlf` and `split_crlf`. Contract A: `split_crlf` returns `None` when no CRLF is found. That `None` is the load-bearing Incomplete signal for the parser layer above.
- [x] **RESP parser layer.** `Frame::parse_one(&[u8]) -> Result<(Frame, &[u8]), FrameError>` in `src/lib/inbound/resp/frame.rs`. Iterative array parsing (no stack-recursion risk for large MGETs). Error variants: `Incomplete`, `Malformed`, `UnknownSigil`, `InvalidLength`, `MissingTerminator`. Full unit test coverage.
- [x] **`Frame → Command` mapping.** `impl TryFrom<Frame> for Command` in `src/lib/inbound/resp/command.rs`. Peel array → ASCII-lowercase verb → match on `b"get" | b"set" | b"del" | b"ping"` → arity check. Full unit test coverage.
- [x] **Serializer (`Reply`).** `src/lib/outbound/resp/reply.rs`. Five variants. `SimpleInner` newtype validates no-CR/LF; `ok()`/`pong()`/`sanitized(...)` constructors for trusted/untrusted payloads. `write_to(&mut impl Write)` streams bytes via `write_all`. Full unit test coverage.
- [x] **Session wiring.** `Session<R: Read, W: Write>` generic. `SessionReader<R>` owns the frame-accumulation buf and handles drain (success), preserve (Incomplete), and clear (hard err). `execute` returns a `Reply` per Command variant. SessionReader has unit tests for read count/EOF and parse_frame drain behavior.
- [x] **Bad-command resilience.** Malformed frames and unknown commands return `-ERR ...\r\n` via `SimpleInner::sanitized` (strips CR/LF from arbitrary error message bytes) without killing the session. Disconnect (0-byte read) cleanly returns the session.
- [x] **Smoke verified.** `redis-cli -p 3000 ping/set/get/del` all working over real RESP.
- [x] **PING `TooManyParts` check.** Fixed as part of M3; PING also now accepts an optional message bulk string and echoes it back (matches real Redis).
- [ ] Pre-existing nit: in `parse_one`, length parse runs before sigil check, so `+OK\r\n` returns `InvalidLength` instead of `UnknownSigil`. Only matters if you ever support `+`/`-`/`:` inbound (you won't, per scope).

### M2 GET / SET / DEL / EXISTS ✅
- [x] `Cache` API: `get`, `set`, `delete`, `exists`
- [x] `Arc<Mutex<HashMap>>` behind a real method boundary
- [x] Generic `impl AsRef<[u8]>` / `impl Into<Vec<u8>>` ergonomics
- [x] EXISTS, full pipeline (RESP arm + Command variant + Cache::exists via `contains_key` + Reply::Integer)
- More commands (INCR, DECR, MGET, APPEND, STRLEN, etc.) deliberately deferred. Proven extensible, and the learning per new arm is marginal

### M3 EXPIRE / EXPIREAT / TTL / PERSIST ✅
- [x] Track per-key expiry timestamps. Unified `Entry { value: Vec<u8>, expires_at: Option<Milliseconds> }`. Originally absolute UNIX seconds; migrated to milliseconds in M6 so `PX` and `PXAT` mean something. Iterated through two shapes: first inline `(Value, Option<Instant>)`, then sidecar `HashMap<Vec<u8>, u64>`, finally collapsed into a single `Entry` struct because perf gains from sidecar-for-low-TTL-usage didn't matter at this scale and the unified shape is simpler. `SystemTime`-based absolute seconds on disk over `Instant` because `Instant` is process-local and unserializable by design.
- [x] Single mutex over the `HashMap<Vec<u8>, Entry>`. No deadlock-via-ordering risk, and the sweep is small.
- [x] Relative + absolute TTL APIs (`set_relative_ttl`, `set_absolute_ttl`, `get_relative_ttl`, `get_absolute_ttl`, `remove_ttl`). EXPIRE feeds into relative; EXPIREAT feeds into absolute directly. Semantics mirror real Redis.
- [x] Lazy expiration on read. `get`, `contains`, and `get_expires_at` all drop expired keys on access so clients never see them between sweeps.
- [x] Active background sweep: `Cache::remove_expired()` plus a sweeper thread in `server.rs`. Holds the lock for the whole pass rather than snapshotting, which is defensible while the sweep is microseconds. Would switch to snapshot-then-re-check if profiling showed tail latency hurting.
- [x] A past absolute TTL removes the key immediately and returns `1` if it existed. Matches real Redis EXPIREAT, so clients never have to wrap calls in clock checks.
- [x] Insert of an "already expired" Entry is accepted (no clock check at the boundary). The next read removes it lazily, which is consistent with the rest of the expiry model. Real Redis does the same.
- [x] TTL command returns `:-2` for missing, `:-1` for no-TTL, `:n` for seconds remaining, rounded up so a freshly-set 60s TTL reads back as 60.

### M4 Persistence ✅ (snapshot baseline plus AOF, hybrid recovery)
- [x] **Architecture: hexagonal ports.** `CacheRepository` (outbound port) + `CacheService` (inbound port) defined in `domain/ports.rs`; the domain `Service<CR>` orchestrates cache execution + AOF append; the outbound `Persister` implements `CacheRepository`, composing an `Aof` and a `Snapshot` (each a newtype over a shared `PersisterInner` writer+path). Adapter errors map into a domain-owned `RepositoryError` at the boundary.
- [x] **Snapshot.** `wincode` serialize/deserialize of the whole `HashMap<Vec<u8>, Entry>`. `load` on startup (empty file → empty map). `store` writes to a `.tmp` sibling then atomically `rename`s over the live file. Crash-safe: never a half-written snapshot.
- [x] **AOF write path.** `Service::execute_logged` classifies the command (the `CacheCommand::Write` variant, so reads are excluded by type), executes against the cache, then appends the mutation to the log. The AOF entry *is* RESP: `From<WriteCommand> for Frame` plus `Frame::write_to`, byte-for-byte what a client sends. Relative TTLs are normalized to absolute at *parse* time (not encode), and `WriteCommand` has no relative variant, so a relative deadline can never reach the log. Append mode (`OpenOptions::append`) so restarts extend rather than clobber.
- [x] **AOF replay on startup.** Load snapshot for the baseline, then replay the log on top via the *same* parse path (`Frame::parse_one` → `Command::try_from` → cache-only `execute`, no re-logging). A trailing `Incomplete` frame is the torn tail from a crash mid-append, so replay stops cleanly and keeps what parsed. A mid-stream parse error or unknown command is fatal, because this is our own log and an uninterpretable entry means the rebuild can't be trusted. Runs before clients connect, so per-command locking is fine.
- [x] **Compaction (snapshot-then-truncate).** Taking a snapshot truncates the AOF, so the log only ever holds mutations *since* the last snapshot. Held under the cache lock across both the snapshot write and the AOF clear, so no mutation can be wiped in between. Order is snapshot first for the durable baseline, then clear. A crash between the two only re-applies already-snapshotted commands, which is harmless. Persist task loops on a 10s interval in `server.rs`.
- [x] Crash safety: atomic temp-and-rename on snapshot, torn-tail-tolerant replay on the AOF.
- [ ] **Full AOF rewrite without blocking writers.** The current compaction holds the cache lock for the snapshot duration (clone-under-lock semantics). The interesting version, a consistent snapshot *without* stalling writers, is still future work. Options are fork-and-COW, copy-on-write structures, or a rewrite buffer for concurrent writes. Fine at current scale; revisit under load.
- [ ] **fsync durability tier.** Appends buffer through `BufWriter` with no per-write
  `fsync`, and nothing flushes them until the 10s snapshot tick or a clean shutdown. A
  `SIGKILL` in between loses every write since the last tick; verified, not theoretical.
  `drop` doesn't save you either, since a killed process never runs it. A configurable
  `everysec`/`always` policy is the fix.

### M5 Pub/Sub ✅ (merged, PR #1)

Shipped: `SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH`, a shared `Channels` registry keyed by
session id, and fan-out through each session's existing writer thread.

The session split is what made it work. `ReadHalf` queues reply bytes on an mpsc;
`WriteHalf` solely owns the socket's write end and drains that queue. A `PUBLISH` from any
session drops bytes into a subscriber's queue and that subscriber's writer delivers them
while its reader is still blocked on its own client. No per-subscriber thread needed.

Registry details that mattered: the inner collection is keyed by session id, so unsubscribe
and disconnect cleanup are O(1) and a session can't double-register. A channel is dropped
once its last subscriber leaves. `publish` prunes dead senders as it fans out, and every
exit from the repl loop runs `unsubscribe_from_all`.

Still deferred, deliberately: pattern subscriptions (`PSUBSCRIBE`), subscribed-mode command
restriction, and any slow-subscriber policy beyond an unbounded queue.

### M6 SET options ✅

`SET key value [EX | PX | EXAT | PXAT]`, plus a storage migration from second-granular
deadlines to milliseconds and `Seconds` / `Milliseconds` newtypes. Full write-up in
`context/set_options.md`. The load-bearing detail: the AOF logs `PXAT` and `PEXPIREAT`
rather than the second-granular verbs, because those are the forms the parser reads back
without converting.

**Still open from the original M5 plan:**

- [ ] **Pattern subscriptions** (`PSUBSCRIBE` / `PUNSUBSCRIBE`). Needs a parallel pattern
  registry and the `["pmessage", pattern, channel, payload]` push shape.
- [ ] **Subscribed-mode command restriction.** Once a session subscribes, real Redis only
  accepts `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PING`, and `QUIT`.
  Deferred on purpose: a per-session flag is easy, but nothing depends on it yet.
- [ ] **Slow-subscriber policy.** The queue is unbounded today, so a subscriber that never
  drains grows memory without limit. Real Redis disconnects past a buffer limit. Worth
  picking a policy and writing down why before any load story.
- [ ] **Session tests.** Still commented out from the split; the old inline `tests` mod
  assumed the single-threaded `execute`/`writer` shape. Re-home them against `ReadHalf`
  and `WriteHalf`.
- [ ] **`to_bytes` / `write_to` duplication.** `Reply::to_bytes` repeats `write_to`'s
  per-variant logic. Route one through the other.

### M7 MULTI / EXEC ⬜ (next)
- [ ] **Session state machine.** Sessions get a `Mode { Normal, Queueing }` toggle. `MULTI` flips to `Queueing`; subsequent commands return `+QUEUED\r\n` instead of executing; `EXEC` flushes; `DISCARD` aborts.
- [ ] **Per-session command queue.** `Vec<Command>` on the `Session`. Commands are parsed (so syntax errors still reject immediately with `-ERR` and abort the transaction per Redis semantics, via a `tx_dirty` flag), just not executed.
- [ ] **Atomic EXEC.** Take the cache lock *once* across the whole queue; execute every command; collect every `CommandOutcome` into an array reply. While the lock is held, no other session interleaves, and that is the atomicity guarantee. This is the load-bearing part: it's why EXEC isn't just "loop and call execute_logged."
- [ ] **AOF interaction.** Every queued mutation must hit the log. Options: (a) buffer per-command AOF frames and write them all under the EXEC lock; (b) wrap the whole block in a `MULTI`/`EXEC` envelope frame on disk and have replay re-execute under the same atomic semantics. (b) preserves atomicity on replay, (a) is simpler. Decide before coding.
- [ ] **Error semantics.** A *parse* error during `Queueing` poisons the transaction: `EXEC` rejects with `-EXECABORT`. A *runtime* error during EXEC (e.g. `INCR` on a non-numeric value, once that exists) does *not* abort. That command's reply is the error and the rest still run, which matches real Redis and holds up once data types broaden.
- [ ] **WATCH/UNWATCH (optimistic locking).** Deferred to a follow-up. Real Redis uses WATCH to abort EXEC if a watched key changed between WATCH and EXEC. Interesting but needs a per-key version counter or change-notification hook; punt past the base MULTI/EXEC landing.
- [ ] **Reply shape.** EXEC reply is a RESP array of N replies (one per queued command). Wants the same array reply shape pub/sub already builds, so reuse it rather than adding a second.

### M8 Stretch ⬜
- [ ] RDB-style snapshot format, Streams (XADD/XREAD), RESP3, INCR/DECR + Lists/Hashes/Sets, WATCH, KEYS/SCAN cursor pattern

## Cross-cutting work owed

- **Graceful shutdown.** ✅ done. `Arc<AtomicBool>` flag, nonblocking listener with 50ms throttle, stdin-EOF/quit/exit as the trigger (no `ctrlc` crate), all spawned threads join cleanly. Persistence and sweeper threads check the flag on a 100ms-tick budget so shutdown latency is bounded.
- **Async migration.** Still `std::thread` per connection. M5 shipped sync anyway, which was the right call for the muscle, so the tokio rewrite is owed but no longer blocking anything.
- **Connection lifecycle on errors.** ✅ read-timeout wired. Accepted streams get `TcpStream::set_read_timeout(500ms)`; the timeout bubbles up to `repl`, which checks the shutdown flag between waits (`TimedOut`/`WouldBlock` → continue). So an idle client no longer wedges the repl against shutdown. `get_command` still writes errors back and continues on malformed frames (session stays alive).
- **Service / Session error types.** `SessionError` (renamed from `ReplError`) wraps `io::Error` + `ServiceError`; `ServiceError` is the domain union of `CacheError` + `RepositoryError`. The split has held up through the AOF work, with `execute_logged` failures lifting cleanly through `?`.
- **Logging migration.** `tracing` / `tracing-subscriber` are in `Cargo.toml` as prep. Plan: replace the scattered `println!` / `eprintln!` calls in `server.rs` and `session.rs` with structured `tracing` events (`info!` for connection lifecycle, `warn!` for recoverable errors like bad commands, `error!` for session-fatal) and add a `tracing_subscriber::fmt()` init in `main.rs`. This was meant to land before AOF and didn't, so the persist path is still debugged through `println!`.

## Hand-coding vs AI-assist allocation

This is the strategic split going forward. The user has already shipped LSM persistence in chickadee; remaining milestones get sorted by whether the *concept* is rehearsed or novel.

### Worth hand-writing (novel muscle)

- **TTL / EXPIRE / active sweep (M3).** ✅ done. Unified `Entry { value, expires_at }`, lazy expiry on every read path plus an active sweep. The sweep holds the lock for the whole pass, which is defensible at this scale; would switch to snapshot-then-re-check under load.
- **Graceful shutdown.** ✅ done. `Arc<AtomicBool>` + nonblocking listener + stdin-EOF/quit trigger + JoinHandle collection and join on exit.
- **AOF rewrite/compaction design.** The *consistent-snapshot-without-blocking-writers* problem is still novel. Sketch the algorithm by hand (fork+COW vs. clone-the-HashMap vs. copy-on-write structures, how to buffer concurrent writes during rewrite, atomic swap at the end) before writing code. The snapshot strategy is the interesting bit; file mechanics are mechanical.
- **Async/tokio migration.** Paradigm shift, not a feature. Hand-write to feel the model and to have the lived sync→async rewrite experience.
- **Pub/Sub fan-out (M5).** Different concurrency shape than request/response. mpsc-per-subscriber vs. broadcast tradeoffs, slow-subscriber handling, subscription registry under contention. Easier after async lands (tokio broadcast > std::sync::mpsc fan-out).
- **MULTI/EXEC (M6).** First per-session state machine the codebase has. Lock-once-across-the-queue is the atomicity story; the AOF-envelope decision is the interesting secondary call (preserve atomicity on replay vs. flatten and lose it). Decoupled from async; can land before or after.

### AI-jet (rehearsed in chickadee or mechanical extension)

- **AOF base path.** Append every state-mutating command, fsync, replay on startup. Same shape as chickadee's WAL-to-memtable replay with `Command` swapped for `Entry`. Mechanical.
- **File atomicity.** Tempfile + rename. Known.
- **Background task scaffolding.** Periodic-loop spawn pattern is already in `server.rs`.
- **More commands.** INCR, DECR, MGET, APPEND, STRLEN. Proven extensible in <14 minutes per command.
- **Logging migration.** `tracing` macros replacing `println!`/`eprintln!`. Pure mechanical.
- **Test gap closure.** Cache unit tests, end-to-end repl tests with scripted RESP bytes. Rote.
- **README updates.** Writing, not engineering.

### Strategic order

1. ~~**EXPIRE/TTL by hand** (M3)~~ done
2. ~~**Graceful shutdown by hand**~~ done
3. ~~**AOF + snapshot by hand** (M4)~~ done. Hexagonal ports landed (`CacheRepository`, `CacheService`, `Service<CR>` orchestrator), snapshot via temp+rename, AOF replay is RESP through the same parse path.
4. **Async/tokio migration by hand.** Paradigm shift, rewrite experience. Owed before M5; Pub/Sub fan-out is the forcing function.
5. ~~**Pub/Sub by hand** (M5)~~ done, sync rather than async. Fan-out landed; slow-subscriber policy and subscribed-mode restriction did not.
6. **MULTI/EXEC by hand.** The first per-session state machine with cross-command atomicity. Lock-once-across-the-queue is the load-bearing lesson; AOF envelope versus flat is the secondary decision.
7. AI sweeps in between or after: more commands (INCR/DECR/MGET/Lists/Hashes), tracing migration, README updates.

## Discipline note

Each milestone gets its own commit (or small series). Don't merge milestones. Easier to compare against the Go sibling later.

## 2026-09-19: first benchmark, the AOF hole, blocking accept

Ran `redis-benchmark` against steller for the first time, with a real Redis on the next port
for a baseline. Two bugs and one real number came out of it.

**The AOF hole (durability bug, fixed).** After any run past 8 KiB of writes, the server
could not restart: `failed to parse length: invalid digit found in string`. Cause was in
`Aof::clear()`. It opened a second handle with `write + truncate` and swapped it in as the
`BufWriter`, which did two wrong things at once: the replacement was not append-mode, so it
carried a real cursor, and the old `BufWriter` was dropped *after* the truncate, so its
pending bytes (up to 8 KiB) were flushed at the stale cursor into an empty file. The OS
fills the gap with zeros. Result on disk after a graceful shutdown: 8,992,628 NULs then
7,372 bytes of real frames (those two sum to exactly 200k SETs x 45 bytes). Replay reads
byte 0, sees `\0` where `*` belongs, and refuses, which is the right call for mid-file
garbage. It only ever fires past 8 KiB between snapshots, because below that the
`BufWriter` never flushed and the cursor never left zero. No manual session could hit it.

Fix: `clear()` now flushes the writer first, then `set_len(0)` through the same handle.
No second handle ever exists, so the writer stays the original `O_APPEND` one. Regression
test `clear_after_a_flushed_writer_leaves_no_hole` (two rounds past 8 KiB, asserts 0 bytes
after each clear, no NUL in the file, clean replay). Verified it fails on the old body.
Proven end to end: 200k SETs, graceful shutdown with six periodic snapshots during the run,
AOF is 0 bytes after, restart answers PONG and the key reads back.

**Accept latency (fixed).** First request on a fresh connection took ~49ms: the accept
loop slept 50ms on `WouldBlock`. Replaced with a blocking listener. The stdin thread now
stores the flag with `Release` and then connects once to the listener's own address; the
accept loop loads with `Acquire` after every accept and drops the stream and exits when the
flag is set. The ordering is load-bearing: if the loop ever took the knock for a client it
would block in `accept()` again with nobody left to connect. First request now 0.2ms.
EOF shutdown 70ms, `quit` 38ms, with a live client 502ms (the session's 500ms read timeout).

**Numbers** (Apple Silicon laptop, client and server local, median of 3 runs, n=50k,
Redis 8.10.1 with persistence off):

| conns | steller SET | Redis SET | steller GET | Redis GET |
|---|---|---|---|---|
| 1 | 55,991 | 14,767 | 56,243 | 14,741 |
| 4 | 124,378 | 36,630 | 121,951 | 36,390 |
| 16 | 185,185 | 116,009 | 185,185 | 117,647 |
| 50 | 186,567 | 200,000 | 188,679 | 203,252 |
| 100 | 185,874 | 222,222 | 182,482 | 224,215 |

Pipelined (`-P 16`, c=1): steller ~510k/s, Redis ~216k SET / ~245k GET.

How to read it honestly:
- Steller wins below ~16 connections and plateaus at ~185k above. Redis keeps climbing.
  Thread-per-connection around one mutex vs a single event loop, as expected.
- The c=1 lead is a wake-up latency artifact, not processing speed. Pipelining shows Redis
  does 216k/s when it is not waiting on a round trip. A thread blocked in `read()` wakes
  faster than a kqueue event-loop iteration, especially on macOS. It would shrink on Linux.
- **Not a controlled before/after.** Last night's run (different machine state) had steller
  at 15,972 for c=50 and Redis at 82,034; tonight Redis alone is 2.4x higher with no change
  on its side. So do not attribute steller's jump to the accept fix without an A/B against
  the old binary in the same session.
- Above ~c=16, `redis-benchmark` (single-threaded) is probably the ceiling, not the servers.
  Rerun with `--threads` before claiming anything about the top end.

**Next**
- A/B the old vs new binary back to back before writing up the accept change's effect.
- `--threads` run to find the real ceiling; then a README benchmark section under the gif.
- `server.rs` has no test module. Shutdown and accept behaviour are smoke-tested only
  (fresh-connection latency, EOF / quit / live-client shutdown). A unit test needs an
  ephemeral port and a driven stdin; small design job, deferred on purpose.

**Provenance note.** The `aof.rs` and `server.rs` changes above were AI-written at Scotty's
explicit request and reviewed by him, contrary to the default in `rules.md`. Both are small.
If the point is the muscle, re-deriving either by hand is a fair exercise; the mechanism is
written out here so that is possible without the diff.

### Later that night: the dense sweep, the high end, and the ceiling

Full write-up now lives in `BENCHMARKS.md` (with `demo/bench.svg`); the README carries a
six-row summary under the gif. Headlines beyond the earlier table: crossover sits between
32 and 64 clients; at 1,000 clients steller runs 2,004 threads / 57 MB against Redis's
4 / 22 MB; at 4,000 it dies in `thread::spawn` (macOS caps a process at 6,144 threads,
two per connection puts the wall near 3,000) and recovers cleanly on restart. The
`--threads` client run is still the open item before any claim about the top end.

