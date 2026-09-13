# steller

A minimal Redis-compatible in-memory key-value server in Rust. Hand-written from the wire protocol up.

Speaks RESP over TCP, so real `redis-cli` clients work: ping, get, set, delete, check existence, set relative or absolute TTLs, query them, persist keys, and subscribe to channels. Data survives restarts on a snapshot baseline plus an append-only command log. Shutdown is stdin EOF or `quit`/`exit`.

## Why "steller"

Steller's jay is the loud blue corvid all over the Pacific Northwest. It is a good mimic, and its party trick is imitating a red-tailed hawk well enough to clear a feeder.

That is what this does. It speaks Redis's protocol convincingly enough that `redis-cli` never notices it is talking to 6,000 lines of my Rust instead.

The sibling storage engine is `chickadee`, another bird from the same forest. Chickadees cache thousands of seeds and grow extra hippocampus every autumn to remember where they put them, which is roughly a storage engine's job description.

## Status

| Milestone | State |
| --- | --- |
| M0 TCP echo server | done |
| M1 RESP protocol and dispatch | done (PING with optional echo, framing, error replies) |
| M2 GET / SET / DEL / EXISTS | done |
| M3 EXPIRE / EXPIREAT / PEXPIREAT / TTL / PERSIST | done (lazy and active expiry) |
| M4 AOF persistence | done (snapshot baseline, AOF replay, snapshot-then-truncate compaction) |
| M5 Pub/Sub | done (SUBSCRIBE / UNSUBSCRIBE / PUBLISH, per-session fan-out) |
| M6 SET options | done (EX / PX / EXAT / PXAT) |
| Graceful shutdown | done |

Try it:

```
cargo run
# in another terminal
redis-cli -p 3000 ping
redis-cli -p 3000 ping "hello"
redis-cli -p 3000 set foo bar
redis-cli -p 3000 set foo bar ex 60
redis-cli -p 3000 set foo bar pxat 1893456000000
redis-cli -p 3000 get foo
redis-cli -p 3000 exists foo
redis-cli -p 3000 expire foo 30
redis-cli -p 3000 expireat foo 1893456000
redis-cli -p 3000 ttl foo
redis-cli -p 3000 persist foo
redis-cli -p 3000 del foo
```

To shut down cleanly, send EOF (Ctrl-D) or type `quit` / `exit` on the server's stdin. The accept loop stops, persistence flushes once more, and every spawned thread joins before `main` returns.

## What's interesting in here

- **Hand-written RESP parser** with real streaming semantics. The parser is also the framer: `Incomplete` when bytes are short, `Malformed` when they're wrong, `Ok((frame, leftover))` otherwise. That leftover slice borrows from the input, so there's no allocation for the rest of the buffer.
- **Binary safe end to end.** Keys and values are `Vec<u8>`, not `String`. A bulk-string payload can be a jpeg or hold an interior `\r\n`. UTF-8 is never enforced where the protocol doesn't require it.
- **Newtypes carry the invariants.** `SimpleInner` makes a `\r` or `\n` in a simple-string payload unrepresentable, with trusted constructors (`ok()`, `pong()`) and a sanitizing one for arbitrary error bytes. `Seconds` and `Milliseconds` do the same for TTL units, so a seconds value can't be compared against a millisecond deadline.
- **Ports and adapters.** The domain defines two trait ports: `CacheRepository`, implemented by the persister, and `CacheService`, implemented by the domain `Service` and called by the session. The dependency arrow points inward. Adapter errors map into a domain-owned `RepositoryError` at the boundary, so the domain never names an outbound type.
- **The AOF is the wire protocol.** The log stores each mutation as the exact RESP bytes a client would have sent, using the same `From<WriteCommand> for Frame` and `Frame::write_to` as the network path. Replay therefore needs no decoder of its own; it reuses `Frame::parse_one` and `Command::try_from`, the inbound path.

  Two rules keep replay honest. Relative TTLs are made absolute at parse time and have no representation in `WriteCommand`, so a relative deadline can never reach the log. And deadlines are logged in the millisecond verbs (`PXAT`, `PEXPIREAT`) rather than the second ones, because those are the forms the parser reads back without converting. Logging `EXPIREAT` would hand replay a millisecond value that the seconds arm multiplies a second time, pushing every deadline 1000x further out on each restart.

  Recovery loads the snapshot, then replays the log on top. A trailing torn frame from a crash mid-append stops replay cleanly; mid-stream corruption or an unknown command is fatal, because in our own log those mean something is actually wrong. Snapshots are written atomically (temp file, then `rename`) and truncate the log, which is the whole compaction story.
- **Split session, which is what makes pub/sub work.** A session owns both socket halves and splits into a `ReadHalf` that queues reply bytes on an mpsc and a `WriteHalf` that solely owns the write end and drains that mpsc. A `PUBLISH` from any session drops bytes into a subscriber's queue, and that subscriber's writer delivers them while its reader is still blocked on its own client.

  It's also generic: `Session<R: Read, W: Write, CS: CacheService>` tests against a `Cursor<Vec<u8>>` and a fake service instead of a socket and a real cache.
- **Iterative array parsing.** A recursive `parse_array` would blow the stack on `MGET key1..key100000`. The iterative loop costs one extra concept and removes the risk.
- **One TTL representation.** `Entry { value: Vec<u8>, expires_at: Option<Milliseconds> }`, a single absolute UNIX millisecond deadline. Simpler than a sidecar map, and the sidecar's performance edge isn't worth the complexity at this scale. `SystemTime` rather than `Instant`, because `Instant` is process-local and unserializable by design.

  Milliseconds rather than seconds so `PX` and `PXAT` mean something: anyone reaching for them wants sub-second precision, and rounding at the edge would throw away the only reason to use them. `EX`, `PX`, `EXAT`, `PXAT`, and `EXPIRE` all normalize onto that one deadline at parse time, so storage never learns which verb produced it. `TTL` divides back to seconds on the way out, rounding up so a freshly-set 60s TTL reads back as 60 rather than 59.

  A past timestamp deletes immediately and returns `1`, matching real Redis. Lazy expiry on every read path drops expired keys on access, and a sweeper thread evicts on a 10s tick.
- **Graceful shutdown.** One `Arc<AtomicBool>` that every thread watches. The listener is non-blocking with a 50ms throttle on `WouldBlock`, so the accept loop polls the flag instead of parking inside `accept()` and without spinning a core. Stdin EOF, `quit`, or `exit` flips it, which avoids a `ctrlc` dependency. Every thread is collected as a `JoinHandle` and joined before `main` returns, and the background threads check the flag every 100ms so shutdown latency stays bounded.
- **Error-path test coverage.** Every parser arm has explicit coverage for `TooManyParts`, `NotEnoughParts`, `UnexpectedFrame`, and where applicable `Syntax`, `Utf8`, and `ParseInt` on numeric args. 237 tests at last count.

## Layout

```
src/
  main.rs                  # binary entry
  lib/
    lib.rs                 # module roots
    domain/                # wire- and storage-agnostic core
      cache.rs             # Arc<Mutex<HashMap<Vec<u8>, Entry>>>, lazy expiry, sweep
      channels.rs          # pub/sub registry: channel -> subscribers, fan-out
      time.rs              # Seconds / Milliseconds newtypes, the clock
      ports.rs             # CacheRepository + CacheService traits, domain errors
      service.rs           # Service<CR>, orchestrates cache execution and AOF append
      command/
        mod.rs             # Command enum + CommandError
        cache/             # ReadCommand (not logged) / WriteCommand (logged)
        channel.rs         # ChannelCommand + its outcome
        outcome.rs         # CommandOutcome / TtlOutcome
    inbound/               # driving adapter
      mod.rs               # outcome -> Reply translation
      server.rs            # accept loop, thread per connection, sweeper/persist/shutdown
      session.rs           # ReadHalf + WriteHalf, the per-connection repl
    outbound/              # driven adapter
      persister/
        mod.rs             # Persister, the CacheRepository impl over aof + snapshot
        aof.rs             # append-only command log: append / replay / clear
        snapshot.rs        # wincode snapshot: load / store (atomic temp + rename)
        persister_inner.rs # shared writer-handle and path plumbing
    resp/                  # shared RESP codec, used by inbound AND outbound
      crlf.rs              # Crlf trait on [u8]: is_crlf / split_crlf
      frame.rs             # parse_one (decode), write_to (encode), From<WriteCommand>
      command.rs           # TryFrom<Frame> for Command
      reply.rs             # Reply enum + write_to + SimpleInner newtype
```

## Sibling

`chickadee` is the other half: an LSM storage engine (WAL, memtable, SSTable, bloom filters, compaction). Between them they cover both sides of how a production KV system gets built. This one is in-memory first, where persistence buys durability. That one is on-disk first, where persistence is the whole point.

## Development context

`context/` holds the design notes: plan and milestone status, RESP working notes, commit guidelines, discipline rules. Useful if you're poking at the architecture or picking up where I left off.
