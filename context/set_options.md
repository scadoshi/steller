# SET options and the millisecond migration

**Status: done.** `SET key value [EX s | PX ms | EXAT unix-s | PXAT unix-ms]` parses,
executes, logs, and replays. 237 tests green, clippy clean.

This started as SET options and turned into a unit migration, because you can't honor `PX`
without sub-second storage.

## What shipped

**Storage is milliseconds.** `Entry { value, expires_at: Option<Milliseconds> }`. Every
clock read and comparison in `cache.rs` goes through `Milliseconds::now()`.

**Units are newtypes.** `domain/time.rs` holds `Seconds` and `Milliseconds`. `Seconds`
exists only between the parser reading a wire token and converting it; nothing else takes
one except `TtlOutcome`, on the way back out. The compiler now rejects the mismatch that a
bare `u64` let through.

**All four options normalize at parse.** `EX`, `PX`, `EXAT`, and `PXAT` land on the same
absolute `Milliseconds`, so `SetExpiry` collapsed entirely: `Set` carries
`expires_at: Option<Milliseconds>` and nothing downstream can tell which verb was used.

**The log speaks millisecond verbs.** `From<WriteCommand> for Frame` writes `PXAT` and
`PEXPIREAT`, and a matching `b"pexpireat"` parse arm reads them verbatim. See below.

**`TTL` rounds up.** `Milliseconds::to_seconds_rounded_up`. A `SET k v EX 60` read back a
microsecond later has 59.999s left; Redis answers 60, and truncating would answer 59.

## Why the log can't use EXPIREAT

This was the bug that nearly shipped. Once `ExpireAt` held millis, encoding it as
`EXPIREAT <n>` meant replay hit the seconds arm, which multiplies by 1000 a second time.
Every restart would push every deadline 1000x further out, silently, and only after a
restart.

The fix is symmetry: encode the verb whose parse arm needs no conversion. `PEXPIREAT` and
`PXAT` are already in the storage unit, so a logged command round-trips unchanged.
`aof::replay_preserves_a_set_deadline_exactly` is the test that fails if anyone changes
this back.

## The invariant worth keeping

The replay path has to be time-invariant. `Command::try_from` is shared by the live network
path and by AOF replay, so a clock read inside the parser looks dangerous. It isn't, for a
specific reason: `WriteCommand` has no relative variant. A relative TTL is unrepresentable
as a command, so it can never be written to the log, so replay never reaches the arms that
read the clock. Removing the relative variant is what makes clock-in-parser safe.

If a future command ever adds a relative form to `WriteCommand`, that guarantee is gone.

## Deliberate omissions

`KEEPTTL`, `NX`, `XX`, and `GET` are not implemented. When they land, the `Set` field grows
back into a struct:

```rust
enum SetExpiry { At(Milliseconds), KeepTtl }
enum Existence { Nx, Xx }
struct SetOptions { expiry: Option<SetExpiry>, existence: Option<Existence>, get: bool }
```

`PTTL` and `PEXPIRE` aren't implemented either. `PTTL` is nearly free now: the same
`time_to_live` call, skipping the `to_seconds_rounded_up`.

## Gotcha for anyone with an old snapshot

Deadlines changed unit. A snapshot written before this work holds second-granular values
that will deserialize as millis and date to 1970. Delete `cache/` before the first run.
