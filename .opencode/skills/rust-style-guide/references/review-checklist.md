# Rust Style Review Checklist

Use in order. Mark each item pass, fail, or not-applicable, and cite the file and line for
failures.

## 0. Automated gates

- [ ] `cargo fmt --check` passes.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes.
- [ ] No new `unsafe` without a `SAFETY:` comment and justification.
- [ ] No unstable features (`#![feature(...)]`) unless the user explicitly approved them.

## 1. Code organization

- [ ] Each crate has a single clear purpose; oversized crates are split.
- [ ] Non-leaf modules contain only `mod` declarations and re-exports.
- [ ] Leaf modules and files are named after their primary item.
- [ ] No glob imports (allowed: `use super::*` in inline modules, `pub use` in non-leaf
      modules, enum-local `use Enum::*`).
- [ ] Each `use` imports from one module; no `::` inside `{..}`.
- [ ] Traits are imported where used (function scope) unless used throughout the file.
- [ ] Top-level functions are called as `module::function()`.
- [ ] Types and traits are imported and used unqualified.

## 2. Ownership and borrowing

- [ ] No `Rc`/`Arc` without a stated ownership-complexity reason.
- [ ] `.clone()` is a real deep copy, not a borrow-checker workaround.
- [ ] Borrows are preferred where the lifetime is expressible.

## 3. Function signatures and errors

- [ ] Parameters take `&T` / `&[T]` / `&str` / `Option<&T>`, not `&Box<T>` / `&Vec<T>` /
      `&String` / `&Option<T>`.
- [ ] Reusable library returns prefer `impl Iterator` over `Vec<T>` where sensible.
- [ ] Reusable library parameters prefer `impl IntoIterator` where sensible.
- [ ] `impl Into<T>` is used only where call-site ergonomics justify it.
- [ ] `bool` parameters are replaced by enums when the meaning is unclear.
- [ ] Failure mechanism matches the contract: `Option` for obvious, `Result` for
      recoverable/contextual, `panic!` only for invariant violations.
- [ ] Panics on malformed input are documented.

## 4. Types and traits

- [ ] Newtypes are used where semantics differ or type safety matters.
- [ ] Named fields are preferred over tuples and non-newtype tuple structs.
- [ ] Standard traits are implemented instead of ad-hoc methods (`FromStr`, `From`, `Display`,
      `TryFrom`).
- [ ] `#[derive]` is used where possible.
- [ ] Generics are preferred over `dyn Trait` for library parameters.
- [ ] Traits exist only for generics, `impl Trait`/`dyn Trait`, or extension traits.
- [ ] Extension traits use the `~Ext` suffix.
- [ ] Expected trait sets are implemented: `Copy` for error types, `Clone`/`Default`,
      `PartialEq`, `Hash`/`Eq` for keys, `len()` paired with `is_empty()`.
- [ ] Important `From` impls are paired with `new()` / `from_foo()`.
- [ ] Options structs implement `Default`.

## 5. Naming

- [ ] Constructors: `new()`, `new_with_foo()`, `try_new()`.
- [ ] Conversions follow `as_` / `to_` / `into_` with the correct `self` convention.
- [ ] Getters/setters follow `foo()`, `foo_mut()`, `take_foo()`, `set_foo()`, `with_foo()`,
      `replace_foo()`.
- [ ] Lifetimes are descriptive where they are type lifetimes.
- [ ] Generic parameters are short but indicative.
- [ ] Abbreviations are avoided unless widely understood.

## 6. Documentation

- [ ] Public items have doc comments (`///`) or the crate opts into `missing_docs`.
- [ ] Module/crate docs use `//!` at the top of the file.
- [ ] Panics and `unsafe` invariants are documented.
- [ ] Docs use capitalization, punctuation, and Markdown.
- [ ] Comments add information; trivial restatements are removed.
- [ ] Implementation notes and TODOs use `//`, not `///`.

## 7. API surface and language features

- [ ] Public cross-team enums are `#[non_exhaustive]`.
- [ ] Public cross-team structs with public fields are `#[non_exhaustive]` (unless FRU is
      required).
- [ ] `#[must_use]` is applied where ignoring the result is always a bug.
- [ ] `const fn` is used only where constness is expected to last.
- [ ] No operator overloads used as syntax sugar.
- [ ] No `#[doc(hidden)]` items from dependencies.
- [ ] No code runs before `main()`.
- [ ] `unsafe` is isolated behind a safe abstraction with a `SAFETY:` comment and a
      justification covering "why not safe" and, for performance, benchmarks.

## 8. Implementation details

- [ ] Types are explicit where inference would be non-obvious (`.into()`, `.parse()`,
      iterator chains).
- [ ] Pattern matching replaces check-then-`unwrap`.
- [ ] `expect("reason")` is used instead of `unwrap()`.
- [ ] `#[non_exhaustive]` enums list all known variants explicitly.
- [ ] Assertions include context strings that explain why.
- [ ] Iterator chains are short, or replaced by a readable loop.
- [ ] No combinator chain in a `for` header.
- [ ] No `panic::catch_unwind`.
- [ ] Strings are built with `format!`/`write!`, not `+` chains or hand-rolled templating.

## 9. Concurrency and macros

- [ ] Poisoned mutexes are never unwrapped into their contents.
- [ ] Scope-tagged rules (`[chrome]`, `[fuchsia]`) are not applied outside their codebases.
- [ ] Macros are used only where ordinary Rust facilities cannot express the API, and the
      simplest adequate macro form is chosen.
