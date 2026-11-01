# Naming and Documentation

Source: <https://google.github.io/styleguide/rustguide.html#naming>

## Naming methods

Follow standard library style.

- `as_...()` — cheap borrowed conversion.
- `to_...()` — expensive conversion that may allocate.
- `into_...()` — consuming conversion.
- Choose the matching `self` convention; see the Rust API Guidelines and Clippy
  `wrong_self_convention`.

## Naming lifetimes

Use descriptive names, especially for type lifetimes.

Exceptions:

- Extremely generic abstractions such as `Cow`.
- Anonymous `'_` where it works.
- Single-character lifetimes that only express the relationship between a method's input
  and output, or a local higher-ranked bound (`for<'a>`). Up to two are fine; beyond that,
  use descriptive names.

## Naming generic parameters

Use short names related to what the parameter represents. A single letter is fine when it
suggests the most important bound. `T`/`U` are fine for mostly unconstrained APIs such as
`Vec<T>`.

## Naming extension traits

Use the `~Ext` suffix, e.g. `ForeignTypeExt`.

## Naming initialization functions

| Purpose | Name |
| --- | --- |
| Constructor | `new()`, or `new_with_foo()` when there is more than one |
| Fallible constructor | `try_new()` |
| Value-to-value conversion | `from_foo()` plus the corresponding `From` impl |

## Avoid abbreviations

Don't abbreviate by deleting letters inside a word, and avoid acronyms/initialisms that
someone outside the project would not know.

Exceptions:

- Abbreviations common enough to appear on Wikipedia.
- Abbreviations unlikely to confuse outsiders, such as `IpAddr`. Even then, prefer not to
  abbreviate.

## Documentation

Add doc comments to public APIs; consider enabling the `missing_docs` lint.

### What to document

- Examples: not every function needs one, but the crate should demonstrate its main use.
- Panic cases for functions that may panic on malformed input.
- Invariants for `unsafe` functions and traits.
- Use capitalization and punctuation.
- Don't explain basics of the language or restate the item name.

```rust
// Bad: explains Rust, not the code.
/// A mock implementation of the FileSystem trait, intended to be used in unit tests.
impl FileSystem for MockFileSystem { /* ... */ }
```

```rust
// Bad: repeats the signature.
/// Removes an element.
pub fn remove_element() -> Element { /* ... */ }
```

If there is nothing insightful to say, omit the comment (it is fine to `allow` the lint in
that case). Documentation on individual enum variants is often unnecessary when the name is
self-explanatory.

### Format

- `///` documents the following item (function, struct, enum, trait, type alias).
- `//!` documents the enclosing module or crate; place it at the top of the file.
- Follow the official Rust Style Guide rules for doc comments.
- Use Markdown.
- Use plain `//` for implementation comments, TODOs not meant for users, and commented-out
  code.
- Prefer documenting with `///` and `//!` directly rather than generating docs from macros,
  so the docs are readable in the source.

It is encouraged to mix doc comments and ordinary comments:

```rust
/// Finds the first `FooItem` with the given `name`, or `None` if not present.
// TODO: crbug.com/12345 - `name` should be `&str`, not `String`.
pub fn get_named(items: &[FooItem], name: String) -> Option<&FooItem> { /* ... */ }
```

Rationale: doc comments are part of the AST. Tooling (rustdoc, IDEs, Kythe, macros) can
consume them, they follow standard Markdown formatting, and misplacement is linted.
