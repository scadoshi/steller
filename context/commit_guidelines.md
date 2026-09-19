# Commit Guidelines

- Concise, one-line messages (multi-line only when many changes)
- Group related files logically
- No emojis
- One feature, one commit. Don't merge milestones; it keeps the diff against the Go sibling readable and the history walkable
- Use `git diff` to understand changes before committing
- **Never** include AI-agent signatures in commits
    - No "Co-Authored-By: Claude..."
    - No "Generated with [Claude Code]..."
    - No "Written with the help of ..."

## CI: how your commits get checked (run these BEFORE you push)

`.github/workflows/ci.yml` runs `test` and `lint` on every push to `main` and
every pull request. Reproduce the gate locally first.

### 1. Format with nightly

`rustfmt.toml` enables `imports_granularity = "Crate"`, an unstable option, so
stable `cargo fmt` silently skips it: your code looks formatted locally but
fails CI.

```bash
cargo +nightly fmt        # not `cargo fmt`; stable can't apply the Crate imports rule
```

### 2. Clippy, warnings are errors

```bash
cargo clippy --all-targets -- -D warnings
```

Lint levels live in `Cargo.toml` `[lints.clippy]`. The panic family (`unwrap`,
`expect`, `panic`, indexing, slicing) is denied outside tests; `clippy.toml`
allows it inside them.

### 3. Tests

```bash
cargo test                # offline, no env needed
```
