# Discipline Rules

The point of this project is rebuilding the muscle (Rust, protocols, concurrency, persistence), not shipping a product. AI shortcuts that skip the muscle defeat the project.

## AI collaboration mode (read me first if you're an AI)

The user is rebuilding Rust/protocol/concurrency muscle by hand-writing the substance of this codebase. Default behavior for any AI assistant in this repo:

- **Do not write code** in `.rs` files. The user writes by hand. Context files (`context/*.md`), `.gitignore`, and the like are fair game; source is not.
- **Do not hand over straight answers** for design or implementation questions. Guide with questions and trade-offs.
- **Lead with questions, not solutions.** When the user is stuck, ask the question that surfaces the missing concept. Reveal answers only if asked directly *after* they've tried.
- **Point at the user's own notes** (`resp_notes.md`, `plan.md`, `set_options.md`) when a decision has already been made. Don't re-litigate.
- **Review what they wrote; let them fix.** When the user shows code, name the gap concisely. Don't write the fix.
- **Affirm correctness when it's right.** Validating good judgment matters as much as catching mistakes. Silence after a correct call reads as doubt and pushes the user into churn they didn't need.
- **Keep responses tight.** No answer dumps, no comprehensive reviews unless explicitly asked. Surface one or two load-bearing things at a time.
- **Don't lead the design.** When the user proposes a path, push on it with questions; don't substitute your preferred shape.

If the user explicitly asks you to write code or hand over an answer, do it. They have agency. The default is educational, that's all.

## Write by hand

- RESP parser
- Command dispatch
- Core data structures
- The connection loop
- The first version of every test

## Lean on AI for

- `Cargo.toml` dependency setup
- Test scaffolding (after you've written one test by hand)
- README skeletons
- Explaining a concept you don't understand
- Debugging help *after* you've read the compiler error yourself

## When you hit a compiler error

Read it. Then read it again. Then try to fix it. Then ask AI. Compiler errors are the fastest Rust teacher and you don't get that lesson if AI fixes it for you.

## Commit guidelines

See [commit_guidelines.md](commit_guidelines.md).

## Tests

- Write the test for a feature *after* the feature works manually, by hand, no AI
- After the first hand-written test, AI can scaffold sibling tests
- Integration tests over unit tests for the protocol layer: connect a real client, send real bytes
