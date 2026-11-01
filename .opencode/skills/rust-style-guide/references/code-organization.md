# Code Organization and Imports

Source: <https://google.github.io/styleguide/rustguide.html#code-organization>

## Terminology

- A **crate** is a unit of compilation. It always contains a nameless Rust module, the
  *root module*.
- A **module** is a unit of code organization within a crate. Modules control visibility,
  can be nested, and can split code across files and directories.

## Prefer small crates `[chrome]`

- Prefer small, focused crates; ideally one file per abstraction.
- The root module may `pub use` APIs from leaf submodules so submodules stay an
  implementation detail.
- Larger crates are acceptable when trait coherence forces types and traits together, or
  for large foundational libraries.

Rationale: fine-grained targets keep the build graph and parallel compilation healthy.
In a plain Cargo project this is a direction, not a hard rule.

## Flat file layout for library sources `[chrome]`

- In Chromium, all `.rs` files of one crate live in a single directory; no subdirectories.
- If subdirectories feel necessary, the crate is probably too large and should be split.
- Exception: `//third_party` code built with Cargo follows the conventional Cargo layout.

## Name source files and leaf modules after the primary item

- A leaf module whose main item is `Foo` should be `mod foo` / `foo.rs`.
- Leaf modules that are not about one abstraction can be named as appropriate.

## Don't implement APIs in non-leaf modules

- Non-leaf modules contain only `mod` declarations and re-exports.
- Allowed submodules of a leaf module: `mod test`, cross-language binding modules such as
  `#[cxx::bridge]`.
- Exception: a small amount of "ties things together" implementation code is acceptable.

## Don't use glob imports

Glob imports make it harder to find where a name comes from and complicate large-scale
refactors.

Allowed at your discretion:

- `use super::*` inside inline modules.
- `use enum::*` in method bodies of that enum and functions primarily about it.
- `pub use submodule::*` in non-leaf modules.
- C++ interop namespace merging (must only merge C++ namespaces, and the merged module
  must not be public).
- Third-party crates that ship a prelude: reviewer's discretion.

Do not define your own preludes.

## One module per `use` statement

Corresponds to `rustfmt` `imports_granularity = Module`. Avoid `::` inside the `{..}`
group.

```rust
// Good
use foo::Foo;
use foo::bar::Bar;
use foo::bar::Baz;

// Bad
use foo::bar::{Bar, Baz};
use foo::{Foo, bar::Bar};
```

## Prefer locally-scoped trait imports

Import a trait inside the function body unless its methods are used extensively across the
file.

```rust
fn parse(s: &str) -> Result<u8, std::num::ParseIntError> {
    use std::str::FromStr;

    u8::from_str(s)
}
```

## Qualify calls to top-level functions

Import the module, then call `module::function()`.

```rust
// Good
use std::mem;
let x = mem::transmute(y);

// Bad
use std::mem::transmute;
let x = transmute(y);
```

Exceptions:

- Functions with sufficiently disambiguating names (typical for C++ free functions).
- Functions from small, single-purpose first-party crates `[chrome]`.
- Module names prone to collisions (for example a C++ namespace name shared by multiple
  crates).

## Import type names and use them unqualified

Prefer `Regex` with `use regex::Regex;` over `regex::Regex`.

Exceptions:

- Ambiguous common names such as `io::Error`, or renaming (`use std::io::Error as IoError`).
- A large group of types from one module (for example rustc's `hir`).
- Macros and generated code.
- Code with many dependencies where a huge import block would not pay off.

## Use the appropriate import mechanism `[chrome]`

Chromium uses `chromium::import!()`. Not applicable outside Chromium.
