# steller project context

## Name

Steller's jay, the loud blue corvid all over the Pacific Northwest. It mimics red-tailed hawks well enough to clear a feeder, which is the same trick this plays on `redis-cli`.

Named alongside `chickadee`, the sibling storage engine. Both birds from the same forest.

## What this is

A minimal Redis-compatible KV server in Rust. Speaks RESP over TCP. In-memory store with TTL, AOF persistence, and pub/sub.

Built by hand to rebuild Rust async, concurrency, and protocol muscle. The point is the muscle, not shipping a product, so shortcuts that skip the muscle defeat the project.

## Trajectory

Phased milestones (full detail in `context/plan.md`):

- **M0** ✅ TCP echo server (sync threads first; the async migration is still owed)
- **M1** ✅ RESP parser and PING/PONG
- **M2** ✅ GET / SET / DEL / EXISTS
- **M3** ✅ EXPIRE / EXPIREAT / TTL / PERSIST, lazy plus active sweep
- **M4** ✅ AOF persistence, append-only log replayed on boot
- **M5** ✅ Pub/Sub, sync fan-out through each session's writer thread
- **M6** ✅ SET options (EX / PX / EXAT / PXAT) and the millisecond migration
- **M7** MULTI / EXEC
- **M8 (stretch)** RDB snapshots, Streams, RESP3

## Sibling repo

`chickadee` is a separate LSM storage engine in Rust (WAL, memtable, SSTable, bloom filters, compaction). Steller is deliberately *not* LSM. Redis is in-memory first, and persistence here buys durability rather than being the storage itself. Different design center, different project.

A Go port was planned once and is not happening. Any reference to `wombat` in older notes is dead.

## Discipline rules

See `context/rules.md`. The **AI collaboration mode** section at the top is mandatory reading for any AI assistant.

The short version:

- **Write by hand:** RESP parser, command dispatch, core data structures, connection loop.
- **AI lane:** boilerplate (Cargo.toml deps, test scaffolding), explanations, debugging help *after* you've read the compiler error yourself.
- **AI mode:** guide educationally. No straight answers, no code in `.rs` files. Lead with questions. Affirm correctness when it's right rather than pushing toward churn.
- **Read compiler errors first.** Always.
- **Commit small.** One feature, one commit.
