# Ownership, Functions, Types, and Traits

Source: <https://google.github.io/styleguide/rustguide.html#ownership-and-borrowing>

Application-code note: the API-design rules below are binding for library code and a
strong recommendation for application code. In application code, prefer simplicity over
flexibility that no caller needs.

## Ownership and borrowing

### Avoid `Rc` and `Arc`

Prefer unique ownership and borrowing. Use `Rc`/`Arc` only when the alternative ownership
structure would be unreasonably complex to write, understand, or maintain — a judgment call
for review. These pointers add runtime overhead and their own complexity.

### Prefer borrows over deep copies

- `.clone()` is for a genuine deep copy, not a substitute for borrowing.
- Design around the borrow checker first; when borrow relationships become excessively
  complex, `.clone()` is a pragmatic escape hatch.
- Weigh performance needs when choosing.
- Cloning `Rc`/`Arc` is not a deep copy and is a normal use of those types.

## Function inputs and outputs

### Accept flexible references for immutable data

| Use | Instead of | Why |
| --- | --- | --- |
| `&T` | `&Box<T>` | Borrow the data inside the box |
| `&[T]` | `&Vec<T>` | Accept any slice |
| `&str` | `&String` | Accept a string slice |
| `Option<&T>` | `&Option<T>` | Borrow the data inside the `Option` |

Exception: non-linear character data such as Rope/Cord. See Clippy `borrowed_box` and
`ptr_arg`.

### Iterators vs. owned collections in return types

Prefer returning an iterator (concrete or `impl Iterator`) over `Vec<T>` in reusable
library code.

Exceptions where `Vec<T>` is fine:

- Most callers need an owned vector.
- The implementation naturally builds a vector.
- Callers need random indexing.
- A `for` loop reads better and the allocation is not important.
- The function's purpose is creating an owned collection.
- The function returns a pre-existing vector.

Application code may return `Vec<T>` when it simplifies things and the flexibility or
performance win is not clearly needed.

### Iterators vs. slices in function parameters

Reusable library code should accept `impl IntoIterator` (or an iterator) when it only
iterates over the input. Application code may accept `&[T]` when simpler.

### Accepting `Into`

Consider `impl Into<T>` over `T` only when call-site ergonomics are paramount, balancing
against code bloat and inference difficulty. Application code should almost never use
`impl Into<T>` parameters.

### Prefer `enum` over `bool` in APIs

Use an `enum` when the purpose of a boolean parameter is not obvious. If you keep a
`bool`, add an inline comment documenting its meaning at the call site or in the signature.

### Reporting errors

| Mechanism | Use for |
| --- | --- |
| `Option<T>` | Simple, obvious failures where extra detail would not help (missing value, parse mismatch, overflow) |
| `Result<T, E>` | Recoverable failures where the caller needs context to distinguish and recover (I/O, network) |
| `panic!` | Bugs and violated invariants — conditions avoidable by fixing the code |

Notes:

- Calls that panic due to malformed input should document that behavior.
- Some codebases forbid panics entirely and require `Result` even for broken invariants.
- Developer-facing binaries may panic for non-fatal errors; tests may panic for assertions.

## Types

### Newtypes versus type aliases

Use a newtype instead of a type alias when:

- The type has notable semantic differences.
- The type warrants its own API.
- You want to change the underlying type later.
- Type safety matters because values could get mixed up (e.g. `Day`, `Month`, `Year`).

### Prefer named fields over tuples

Prefer named fields over tuples and non-newtype tuple structs; `.0`, `.1` quickly becomes
confusing.

Exceptions: true newtype tuple structs, and very local contexts where no confusion is
possible.

## Traits

### Add trait implementations when they become necessary

Low-level library code should anticipate users' needs; application code should add impls
as needed.

### Implement known standard library traits

If a method matches a standard trait, implement the trait: `impl FromStr for Foo` rather
than `fn from_str(&str) -> Self`. Having both is acceptable for discoverability. See Clippy
`should_implement_trait`.

### Use `#[derive()]` if possible

Avoid manual impls that a derive can express.

### Generics versus `dyn Trait`

In libraries, prefer generic parameters (`T: Trait`, `impl Trait`) over `dyn Trait` in
function parameters, except when code size matters.

Reasons: `dyn Trait` adds a fat pointer and vtable dispatch, blocks inlining and other
optimizations, and some traits are not `dyn`-compatible (generic methods, associated
functions without `self`, methods returning `Self`).

### When to define a trait versus an inherent impl

Define a trait only to:

- Enable generic programming (`T: Trait`).
- Accept or return `impl Trait` or `dyn Trait`.
- Define an extension trait.

In all other cases use an inherent `impl`. Don't use traits merely to organize code.

### Extension traits

An extension trait exists solely to add methods to a type from another crate; it is not for
generics or dynamic dispatch. Name it with an `~Ext` suffix (e.g. `ForeignTypeExt`). The
team owning the original type should generally own the extension trait.

Exception: extension methods expected to become broadly known in foundational libraries.

```rust
use foreign_crate::ForeignType;

trait ForeignTypeExt {
    fn additional_method(&self);
}

impl ForeignTypeExt for ForeignType {
    fn additional_method(&self) { /* ... */ }
}
```

### Specific guidance for important traits

| Trait | Guidance |
| --- | --- |
| Public-field types | Consider implementing traits that all subfields implement |
| Error types | Implement `Copy` whenever possible |
| `Copy` | Implement when the type will reasonably remain `Copy` |
| `Clone` | Most types should implement `Clone` unless they represent an external resource |
| `Default` | Implement when the type has `new()` or another default state (Clippy `new_without_default`) |
| `PartialEq` / `Eq` | Prefer `PartialEq`; `Eq` additionally requires reflexivity |
| `PartialOrd` / `Ord` | Prefer `PartialOrd`; `Ord` additionally requires total ordering |
| Map keys | Implement `Hash` + `Eq` if the type may be a key or cached; add `Ord` when a clear total order exists |

### Pair important `From` impls with constructors

If an important `From` impl is a primary construction path, also provide an inherent
`new()`, `new_from_foo()`, or `from_foo()`.

### Options structs should implement `Default`

Structs with all-public fields used as a bag of options should implement `Default`,
enabling functional record update (`Struct { a: 0, ..Default::default() }`).
