---
name: Google Rust Style Guide
description: >-
  Use when writing, reviewing, or refactoring Rust code in this project. Encodes Google's
  Rust Style Guide (google.github.io/styleguide/rustguide.html): code organization, imports,
  formatting, ownership and borrowing, function inputs/outputs, error reporting, types,
  traits, naming, doc comments, unsafe code, #[non_exhaustive], #[must_use], and
  implementation concerns. Keywords: rust style, rust coding guide, rust review, rust lint,
  rustfmt, clippy, import rules, API design, doc comment, naming convention, unsafe, 러스트
  코드 스타일, 러스트 코드 리뷰, 러스트 가이드, 코딩 컨벤션, 네이밍, 문서화.
---

# Google Rust Style Guide

Source: <https://google.github.io/styleguide/rustguide.html>

This skill turns Google's Rust style guide into an actionable workflow. The guide is a
*style* guide in the broad sense: it covers API and implementation design, not just
formatting.

## How to use this skill

1. **Writing new code** — read *Ground rules* and *Top rules at a glance* below, then open
   the reference file for the area you are touching.
2. **Reviewing code** — work through `references/review-checklist.md`.
3. **Formatting** — run `cargo fmt`, then
   `cargo clippy --all-targets --all-features -- -D warnings`.

## Ground rules

- Follow the official public Rust rules where this guide does not deliberately deviate:
  the [Rust Style Guide](https://doc.rust-lang.org/nightly/style-guide/) (mostly enforced
  by rustfmt) and the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/).
- **Library code vs. application code.** API-design rules are *binding* for library code
  and a *strong recommendation* for application code. Application code should favor
  simplicity and directness over broad reusability; avoid premature generalization.
- **Readability and maintainability beat writeability.** Expect the code to live a long
  time.
- **Automation friendliness matters.** Consistent, refactor-friendly patterns make
  large-scale changes cheap.
- **Scope tags.** Rules marked `[chrome]` or `[fuchsia]` apply only to those codebases.
  They do not automatically apply to this project; treat them as context, not mandates.

## Project defaults

Unless the repository says otherwise, Rust code in this project uses:

```toml
[package]
edition = "2024"
rust-version = "1.85"

[lints.rust]
unsafe_code = "warn"

[lints.clippy]
all = "warn"
pedantic = "warn"
```

## Top rules at a glance

| Area | Rule |
| --- | --- |
| Imports | No glob imports (except `use super::*` in inline modules and `pub use` in non-leaf modules) |
| Imports | One module per `use` statement; no `::` inside `{..}` (`rustfmt: imports_granularity = Module`) |
| Imports | Import types/traits and use them unqualified; qualify top-level functions as `module::function()` |
| Imports | Prefer importing traits inside the function body that uses them |
| Formatting | Use rustfmt; opt out only for manual alignment or generated code |
| Ownership | Avoid `Rc`/`Arc`; prefer unique ownership and borrowing |
| Ownership | Prefer borrows over `.clone()`; clone only for a genuine deep copy |
| API | `&T` over `&Box<T>`, `&[T]` over `&Vec<T>`, `&str` over `&String`, `Option<&T>` over `&Option<T>` |
| API | Prefer `impl Iterator` returns and `impl IntoIterator` parameters in reusable library code |
| API | `enum` instead of `bool` when the parameter's meaning is not obvious |
| Errors | `Option` for obvious failures, `Result` for recoverable errors needing context, `panic!` only for bugs/invariant violations |
| Types | Newtype over type alias when semantics differ; named fields over tuples |
| Traits | Implement standard traits (`FromStr`, `From`, `Display`) instead of ad-hoc methods; `#[derive]` where possible |
| Traits | Generic (`impl Trait`/`T: Trait`) over `dyn Trait` in library parameters |
| Traits | Define a trait only for generics, `impl Trait`/`dyn Trait`, or extension traits (`~Ext` suffix) |
| Naming | `new()` / `new_with_foo()` / `try_new()`; `as_`/`to_`/`into_` conversions; descriptive lifetimes |
| Naming | Avoid abbreviations that outsiders would not know |
| Docs | `///` for items, `//!` for modules/crates; document panics and `unsafe` invariants |
| Unsafe | Avoid it; when needed, isolate it, add a `SAFETY:` comment and a technical justification |
| API surface | `#[non_exhaustive]` on public enums and public-field structs used across team boundaries |
| API surface | `#[must_use]` where ignoring the return value is always a bug |
| Implementation | Prefer pattern matching over check-then-`unwrap`; prefer `expect` over `unwrap` |
| Implementation | Avoid long iterator combinator chains and chains in `for` headers; prefer a plain loop when clearer |
| Implementation | Don't call `panic::catch_unwind`; don't run code before `main()` |

## References

Read only what the task needs. Paths are relative to this file.

| File | Contents |
| --- | --- |
| `references/code-organization.md` | Crates, files/modules, leaf modules, import rules, qualification |
| `references/formatting.md` | rustfmt usage, rustfmt options, `#[rustfmt::skip]` policy |
| `references/ownership-and-api.md` | Borrowing, `.clone()`, function inputs/outputs, error reporting, types, traits |
| `references/naming-and-docs.md` | Method/lifetime/generic/extension-trait naming, initialization functions, doc comment rules |
| `references/language-features.md` | `unsafe`, unstable features, getters/setters, `const fn`, `#[non_exhaustive]`, `#[must_use]` |
| `references/concurrency-and-macros.md` | Poisoned mutexes, scope-tagged concurrency rules, macro preference order |
| `references/implementation.md` | Type inference, pattern matching, assertions, iterator style, `format!`/`write!` |
| `references/review-checklist.md` | Ordered checklist for reviewing a Rust change |
