# RESP, worked out

Loose notes from the first pass. These capture the mental model, not the spec; the [real spec](https://redis.io/docs/latest/develop/reference/protocol-spec/) is the source of truth.

Some names here are from the first draft and have since changed: `Value` became `Frame`, `ValueError` became `FrameError`, `utils.rs` became `resp/crlf.rs`. Left as written, since the reasoning is the point.

## What RESP actually is

A wire protocol. It's the language clients and the server use to talk over TCP. Nothing about storage. The server can hold values however it wants internally; RESP only governs the bytes on the socket.

Two clean boundaries:

- `bytes -> Command` (parser, inbound)
- `Reply -> bytes` (serializer, outbound)

Everything between those two is the server's house, its rules. Keys and values get stored as byte strings, not as `RespValue`. Don't tangle the wire enum into the cache.

## Frame shape

Sigil on the first byte tells you the type. Every framed piece ends in `\r\n`.

- `+OK\r\n` simple string
- `-ERR something\r\n` simple error
- `:123\r\n` integer. ASCII digits, not 8 binary bytes, and bounded so it needs no length prefix
- `$5\r\npedro\r\n` bulk string: header `$<len>\r\n`, then raw bytes with no sigil, then `\r\n`
- `$-1\r\n` null bulk string, the RESP2 legacy null
- `*2\r\n...\r\n...\r\n` array of N RESP values, recursive

The whole protocol is human-readable ASCII *except* bulk-string payloads, which are arbitrary bytes. That's why you can `nc` into a server and type commands by hand.

## Scope for this project

A Redis server speaking GET / SET / DEL only ever needs to handle a narrow slice:

**Inbound (parser must handle):** array of bulk strings. That's it. Every command from every real client looks like `*<n>\r\n$<n>\r\n<cmd>\r\n$<n>\r\n<arg>\r\n...`. Command args are never integers or simple strings on the wire, always bulk strings.

**Outbound (serializer must handle):** five types.
- Simple string, `+OK\r\n` for SET
- Bulk string, `$3\r\nbar\r\n` for GET hits
- Null bulk, `$-1\r\n` for a GET miss
- Integer, `:1\r\n` for DEL count and later EXISTS
- Simple error, `-ERR ...\r\n`

RESP3 types (Map, Set, Push, Attribute, BigNumber, VerbatimString, Boolean, Double, BulkError) are out of scope. RESP2 bulk-string arrays are what real clients send, and untested RESP3 surface would be a footgun.

Inline commands (telnet-style `PING\r\n` without RESP framing) are also out of scope. Reject anything that doesn't start with `*` as `-ERR`.

## Example frames

GET:

```
*2\r\n$3\r\nGET\r\n$5\r\npedro\r\n
```

SET:

```
*3\r\n$3\r\nSET\r\n$5\r\npedro\r\n$4\r\ngood\r\n
```

## The framing problem

Naive instinct: "read until `\r\n`, then parse." This breaks because bulk-string payloads can contain `\r\n`. `$6\r\nhe\r\nlo\r\n` is a valid 6-byte payload of `he\r\nlo`. You can't find the end of a frame by scanning; you have to parse the `$<len>` header and *count bytes*.

Slightly less naive: "wait until the buffer ends in `\r\n`, then parse." Also breaks. TCP delivers bytes in arbitrary chunks, and a partial read can land right after an interior `\r\n` and look complete. The first chunk might be `*2\r\n$3\r\nGET\r\n`, which ends in `\r\n`, while the key is still on its way.

**The only thing that knows whether a frame is complete is the parser**, because completeness depends on the `$<len>` counts and the `*<n>` array size. So the parser returns three states, not two:

- `Ok((value, rest))`, parsed one frame, here's what was left over
- `Err(malformed)`, the bytes were invalid RESP
- `Incomplete`, ran out of bytes mid-parse, need more

The reader loop: try to parse; on `Incomplete`, read more bytes into the buffer and retry from the start. On `Ok`, dispatch the value and keep `rest` for the next round, because TCP can deliver several commands in one read.

In other words, **the parser is the framer**. There's no separate "is this complete" pass.

## Parser shape

Free function over `&[u8]`, returning slices that borrow from the input:

```
fn next_value(buf: &[u8]) -> ParseResult<(RespValue, &[u8])>
```

Both outputs share the input's lifetime, elided, no annotations needed. Slices are pointer plus length, no allocation. Cloning into a `Vec<u8>` at every step is the wrong instinct; one-input-many-outputs is the gentlest lifetime case Rust has.

Sigil-on-first-byte dispatch:

- `+` simple string
- `-` simple error
- `:` integer
- `$` bulk string: read the length, *count bytes*, then consume the trailing `\r\n`
- `*` array: read the length, then recurse N times

Arrays are recursive. An array element is itself any RESP type, parsed by the same function.

## Layering

Two-step pipeline:

```
bytes -> RespValue -> Command
```

- `RespValue` knows nothing about GET/SET. Just RESP types.
- `Command` knows nothing about `\r\n`. Just domain.
- The serializer is the inverse, `Reply -> bytes`. `RespValue` is reusable there, since you have to *write* RESP as well as read it.

Each layer tests in isolation, and the parser never has to learn a new command. Adding EXISTS doesn't touch `Frame`, because `Frame` knows nothing about Redis semantics.

## Implementation decisions (running log)

Captured as the parser came together. What was chosen and why, so future-me doesn't re-litigate.

### `Value` enum shape

Only two variants: `Array(Vec<Value>)` and `BulkString(Vec<u8>)`. That's the entire inbound surface, since commands are always arrays of bulk strings. Reply-side variants wait for the serializer; no point modeling what isn't parsed or emitted yet.

(They landed in a separate `Reply` enum rather than on `Frame`, which turned out better: the inbound and outbound vocabularies really are different.)

### `BulkString(Vec<u8>)`, not `String`

First pass was `String`. Switched to `Vec<u8>` to stay binary-safe, because RESP bulk payloads are arbitrary bytes: a jpeg, or something with an interior `\r\n`. `String` would force UTF-8 validation at parse time and reject valid frames. The cost is that command dispatch compares byte slices (`b"get"`, `b"set"`, `b"del"`) instead of strings, which is free.

### Parser signature

```rust
fn parse_one(bytes: &[u8]) -> Result<(Value, &[u8]), ValueError>
```

- An associated method rather than a `TryFrom` impl, because `TryFrom` can't return the leftover slice. ~~`TryFrom<&[u8]> for Value` stays as the outer entry point that parses exactly one whole frame.~~ **Update:** `TryFrom` got dropped entirely. The leftover-bytes contract is structural to streaming, and no caller wants the "one whole frame" shape without it.
- Borrows in, borrows out. The rest of the buffer is a sub-slice of the input and shares its lifetime.
- `Result<_, ValueError>` rather than a three-state `Ok/Err/Incomplete` enum, since incompleteness is just one variant. **Update:** `Incomplete` did become its own variant, and it earns its keep. The session loop branches on it specifically: preserve the buffer and read more, versus clear it and reply `-ERR`.

### Helper: `Crlf` trait on `[u8]`

`utils.rs` exposes two extension methods on `[u8]`:

- `is_crlf(&self) -> bool`, true iff the slice starts with `\r\n`.
- `split_crlf(&self) -> Option<(&[u8], &[u8])>`, finds the first `\r\n` and returns (before, after). `None` when there isn't one, which means an incomplete header.

Both return borrows. The trait shape is just for the `bytes.split_crlf()` ergonomics; nothing else implements it.

### Length parsing without allocation

```rust
std::str::from_utf8(&sigil_len_str[1..])?.parse::<usize>()?
```

`from_utf8` returns a `&str`, a view over the existing bytes with no allocation, and `.parse::<usize>()` consumes it. The two error types collapse into one `ParseLengthError` with `#[from]` conversions so `?` works.

A digit-by-digit accumulator (`n = n*10 + (b - b'0') as usize`) would skip the validation entirely, show what `parse` does underneath, and avoid a UTF-8 check on bytes we already know are ASCII digits. Still on the table, still not done.

### `parse_bulk_string(bytes, len)`

`parse_one` handles the header, consuming `$<n>\r\n` through `split_crlf`. `parse_bulk_string` takes the bytes *after* that plus the parsed length, and returns the value with the leftover from *after* the trailing `\r\n`.

Three things it has to do (all three landed; this list was written before they did):

1. Bounds-check: `bytes.len() >= len + 2` (payload + terminator), else incomplete.
2. Validate `&bytes[len..len+2] == b"\r\n"`. If the length lied, the frame is malformed.
3. Return `&bytes[len+2..]` as the leftover, not `&bytes[len..]`.

### `parse_array(bytes, len)`

Stubbed at the time. The plan, which is what shipped: loop `len` times, calling `parse_one` on the current leftover, pushing each `Value` into a `Vec` and threading the new leftover forward. Return `(Value::Array(vec), leftover)`. State lives on the call stack, so there's no need for `Parser<Mode>` machinery.

Note the loop is iterative rather than recursive, which is what keeps `MGET key1..key100000` from blowing the stack.

### Considered and rejected: split-all-on-`\r\n`

Tempting one-liner: `bytes.split(|&b| ...)` to chop the whole frame into tokens. Forbidden, because bulk-string payloads can contain `\r\n`. `$6\r\nhe\r\nlo\r\n` is a valid 6-byte payload and split would shred it. The only correct framer respects length prefixes and counts bytes.

### Considered and rejected: `Parser<Mode>` type-state machine

Overkill. The state needed to parse one frame fits on the call stack. A type-state machine is for streaming megabyte values chunk by chunk without materializing them, or for enforcing "you can't call `read_body` before `read_header`" at compile time. Neither is in scope.
