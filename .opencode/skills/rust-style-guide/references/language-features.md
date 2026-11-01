# Language Features and API Surface

Source: <https://google.github.io/styleguide/rustguide.html#other-rust-features>

## Unsafe code

Don't write `unsafe` unless there is no safe way to implement the same functionality.

When `unsafe` is unavoidable:

1. **Encapsulate** it in a safe abstraction, isolated from business logic. Ideally put it in
   a separate crate with few dependencies beyond `std`.
2. **Add a `SAFETY:` comment** explaining why the code is correct.
3. **Add a technical justification** for the unsafe code. It can cover a block, a file, or
   a build target, and must answer:
   - Why can't this be written safely in an equivalent way?
   - If the reason is performance, where are the benchmarks?

Project lint: `unsafe_code = "warn"`, so new `unsafe` should be deliberate and justified.

## No unstable features

Don't use unstable features.

Exceptions (other codebases): Android embedded, a small Chrome-arbiter-approved list with a
path to stabilization, and Fuchsia-specific guidance. Verify with the user before allowing
any `#![feature(...)]`.

## Getters and setters

| Form | Meaning |
| --- | --- |
| `foo()` | Getter returning `&T`, or `T` by value when `T: Copy` |
| `foo_mut()` | Getter returning `&mut T` |
| `take_foo()` | Getter that clears the value (sets `None`/default) and returns the old value |
| `set_foo()` | Setter taking `T` by value |
| `with_foo()` / `with_mut_foo()` | Accessor that operates through a closure |
| `replace_foo()` | Setter that returns the old value |

```rust
impl Stuff {
    fn foo(&self) -> &Foo { /* ... */ }
    fn foo_mut(&mut self) -> &mut Foo { /* ... */ }

    /// Sets "baz" to `None` and returns the old value.
    fn take_baz(&mut self) -> Option<Baz> { /* ... */ }

    fn set_foo(&mut self, foo: Foo) { /* ... */ }

    /// Calls `f` with a borrow of the "bar" property and returns its result.
    fn with_bar<R>(&self, f: impl FnOnce(&Bar) -> R) -> R { /* ... */ }

    /// Calls `f` with a mutable borrow of the "bar" property and returns its result.
    fn with_mut_bar<R>(&self, f: impl FnOnce(&mut Bar) -> R) -> R { /* ... */ }

    /// Sets "foo" and returns the old value.
    fn replace_foo(&mut self, foo: Foo) -> Foo { /* ... */ }
}
```

### Closure-accepting accessors

`object.with_foo(|foo| frob(foo))` is a good choice when:

- The property lives behind interior mutability (`Mutex`, `RefCell`). The closure scopes the
  access and avoids leaking the guard type as an implementation detail.
- Follow-up work or validation must happen after a change, so a plain `set_foo()` is not
  enough (or the type is expensive/impossible to clone).

Often the closure's return value is forwarded by `with_foo()`. Provide `with_foo()` and
`with_mut_foo()` when both read-only and mutable closure access are needed.

## Implement expected API sets together

If you provide `len()`, also provide `is_empty()`. See Clippy `len_without_is_empty`.

## Don't use `#[doc(hidden)]` items you don't own

Treat them as private unless you own the code.

## Use `const fn` responsibly

Mark a public function `const` only if you expect it to remain `const`, or commit to helping
clients migrate. Demoting `const` to a regular function is a breaking change.

## Avoid overloading for non-mathematical purposes

Use operator overloads for their mathematical/logical meaning, not as syntax sugar.

## `#[non_exhaustive]` on public enums

Mark enums in public APIs used across teams `#[non_exhaustive]`.

Exceptions:

- The enum will never change (ontologies such as `Color { Red, Green, Blue }`, or two-state
  enums like `Visibility`).
- The enum may change, but breakages will be fixed by a large-scale change.

Rationale: forces callers to handle future variants instead of breaking on upgrade.

## `#[non_exhaustive]` on public structs with public fields

Mark structs with public fields `#[non_exhaustive]` when they are part of an API used
across teams.

Exceptions:

- Fields are unlikely to change.
- Breakages will be fixed by a large-scale change.
- You want to enable functional record update (`Person { name: "foo", ..Default::default() }`).

Rationale: adding even a private field breaks callers that name every field in a struct
literal. `#[non_exhaustive]` permits adding private fields without breakage.

## `#[must_use]` on functions

Use `#[must_use]` when:

- Ignoring the return value means the caller cannot be using the function correctly —
  including pure functions such as `HashMap::get()`.
- The return value represents an error or other critical information. If the return type is
  already `#[must_use]` (like `Result`), the function does not need the attribute.

## `#[must_use]` on types

Use `#[must_use]` on types returned by functions that would otherwise need the function
attribute:

- Error types (similar to `Result` or `StatusOr`).
- Lazily evaluated computations whose value must be consumed for the effect to happen.

## Do not run code before `main()`

Don't run code before `main()`, for example via the `ctor` crate.
