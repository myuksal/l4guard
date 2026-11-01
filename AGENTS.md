# l4guard

L4 TCP guard proxy for blockchain nodes. Design: `plan.md`.

## Stack
Rust 2024 · 1.85 · single crate · tokio, serde, toml, ipnet, tracing

## Layout
`src/main.rs` entry · `config` · `policy` · `relay` · `sessions` · `observe` · `rpc`
Tests: inline `#[cfg(test)]`, integration in `tests/`

## Commands
`make build|release|run|run-release|clean`
`make run RUN_ARGS="--config guard.toml"`
Check: `cargo fmt --check` · `cargo clippy --all-targets --all-features -- -D warnings`

## Rules
- Style: `@rust-style-guide` (Google Rust Style Guide)
- No `unsafe` without `SAFETY:` + justification
- Errors: `Option` = obvious · `Result` = recoverable · `panic!` = bug only
- Prefer borrows over `.clone()`; no `Rc`/`Arc` without reason; no `unwrap` (use `expect`)
- Docs: `///` items, `//!` modules, document panics
- Application code: simple and direct; no premature abstraction
- Imports: no globs, one module per `use`, types unqualified, free functions `module::fn()`

## Invariants
- Policy reload/blacklist terminates existing sessions, not just new ones
- Half-close preserved; per-direction copy buffer 8 KiB
- Every external path (inbound, outbound, RPC, IPv6, UDP) is explicitly allowed or blocked
- Guard failure must not stop the backend process or its data
