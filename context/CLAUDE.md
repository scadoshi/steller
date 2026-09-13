# diprotodon project context

## Name

The diprotodon was a giant extinct marsupial, basically a hippo-sized wombat, that roamed Australia until around 40,000 years ago. The Rust version of this project gets the dignified ancient-giant name. The Go sibling is `wombat`, diprotodon's goofy modern cousin. Same suborder, Vombatiformes.

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

`~/Projects/wombat`, the same feature ladder ported to Go. Don't copy code between them; the *translation* is the point.

Related: `~/Projects/nighthawk` is a separate LSM-style storage engine in Rust (WAL, memtable, SSTable, bloom filters, compaction). Diprotodon is deliberately *not* LSM. Redis is in-memory first, and persistence there is durability rather than storage. Different design center, different project.

## Discipline rules

See `context/rules.md`. The **AI collaboration mode** section at the top is mandatory reading for any AI assistant.

The short version:

- **Write by hand:** RESP parser, command dispatch, core data structures, connection loop.
- **AI lane:** boilerplate (Cargo.toml deps, test scaffolding), explanations, debugging help *after* you've read the compiler error yourself.
- **AI mode:** guide educationally. No straight answers, no code in `.rs` files. Lead with questions. Affirm correctness when it's right rather than pushing toward churn.
- **Read compiler errors first.** Always.
- **Commit small.** One feature, one commit.
