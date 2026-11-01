# Implementation Concerns

Source: <https://google.github.io/styleguide/rustguide.html#implementation-concerns>

## Don't overuse type inference

Add enough explicit types that someone familiar with the codebase can tell what the inferred
types are.

- Prefer annotating variable declarations over turbofish.
- If the compiler forces a turbofish, adding a `_` inference marker is generally fine.
- Especially annotate:
  - Generic-return call sites such as `.into()` or `.parse()`.
  - Results of complex iterator chains.

## Prefer pattern matching over check-then-unwrap

Avoid potentially panicking operations when the input already guarantees success. Prove it
to the compiler with a pattern.

```rust
// Good: the `if let` arm proves `option` is `Some`; a panic is impossible.
if let Some(value) = option {
    use_value(value);
}
```

```rust
// Bad: correctness relies on a runtime check plus `unwrap`.
if option.is_some() {
    use_value(option.unwrap());
}
```

## Prefer `expect` over `unwrap`

When a panic is necessary because an invariant cannot be expressed in the type system, use
`expect("reason")` to document why instead of `unwrap()`.

## Don't omit patterns on `non_exhaustive` enums

List all variants in `match` statements over `#[non_exhaustive]` enums.

- Variants you don't care about go in the wildcard arm.
- If you care about only specific variants, use `if let` or `let ... else {}`.

```rust
#[non_exhaustive]
pub enum Shape {
    Circle,
    Square,
    Triangle,
}

fn process_shape(shape: Shape) {
    match shape {
        Shape::Circle => println!("It's a circle"),
        Shape::Square => println!("It's a square"),
        // `Triangle` and any future variants need no action.
        Shape::Triangle | _ => {}
    }
}
```

```rust
fn process_shape(shape: Shape) {
    if let Shape::Circle = shape {
        println!("It's a circle");
    }
}
```

Exception: you only care about specific variants *and* `match` reads better than `if let`.

## Use context strings to clarify assertions

Use the format-string argument of `assert!` to explain why the assertion should hold, what
violated it, or whether the failure means API misuse versus an implementation bug.

```rust
assert!(
    values.is_sorted(),
    "`values` is populated from a `PriorityQueue`, so we expect it to be sorted"
);

assert!(refund > 0, "Refund must be positive, got {refund:?}");

// API misuse is reported clearly.
assert!(self.stream.is_some(), "Call connect() before executing queries");
```

Do not use the context string merely to repeat the condition:

```rust
// Bad: adds nothing.
assert!(values.is_sorted(), "values should always be sorted");
```

Consider a context string especially when:

- You are tempted to write a comment next to the assertion — the format string shows up in
  logs.
- Users will see the assertion failure.
- Logging a value explains the failure.
- It is unclear whether the client misused the API or the implementation has a bug.

## Don't use iterator combinator chains when direct iteration is clearer

- Avoid long or complex combinator chains.
- Don't build a chain in the header of a `for` loop; assign it to a named variable first.
- Avoid `Iterator::for_each` unless you have a good reason; a `for` loop is usually easier to
  read.
- Combinators are preferable for short chains and when the combinator carries a lot of
  logic that would otherwise be written out.

```rust
// Hard to read: conditionals plus mutation of external state.
let mut error_count = 0;
let _processed_items: Vec<_> = items
    .into_iter()
    .map(transformation_that_may_fail)
    .map(|result| {
        if result.is_err() {
            error_count += 1;
        }
        result
    })
    .filter_map(Result::ok)
    .collect();

// Better.
let mut error_count = 0;
let mut processed_items = Vec::with_capacity(items.len());

for item in items {
    match transformation_that_may_fail(item) {
        Ok(val) => processed_items.push(val),
        Err(_) => error_count += 1,
    }
}
```

```rust
// Bad: chain in the `for` header.
for bug in bug_descriptions
    .iter()
    .filter_map(|description| parse_bug_description(description).ok())
    .filter(|bug| bug.priority <= Priority::P1)
{
    assign_bug(bug);
}

// Good: name the chain, then consume it.
let urgent_bugs = bug_descriptions
    .iter()
    .filter_map(|description| parse_bug_description(description).ok())
    .filter(|bug| bug.priority <= Priority::P1);

for bug in urgent_bugs {
    assign_bug(bug);
}
```

```rust
// Good: the combinator holds the logic; the loop version would reinvent `any()`.
let threshold_exceeded = values.iter().any(|&v| v > threshold);
```

## Don't call `panic::catch_unwind`

Prohibited in first-party Rust code.

Exception: defense in depth that is never relied upon for any property, in a codebase
already doing most of what it can to avoid panics, and only when built with `panic=unwind`.
The author must convince the reviewer. This exception does not apply to Chrome, Android, or
Fuchsia, which require `panic=abort`.

## Prefer `format!` / `write!` for concatenation and substitution

Use `format!()` and `write!()` when:

- Concatenating more than two strings: `format!("{a}{b}{c}")`, not `a + b + c`.
- Substituting values into a literal string, instead of `str::replace`.

Rationale: fewer allocations than `+` concatenation, and standard format strings beat custom
templating syntax.

## Tests

Use the test framework appropriate to the codebase. Chromium uses `rust_gtest_interop`;
Fuchsia uses `#[fuchsia::test]`. A plain Cargo project uses `#[test]` and integration tests
under `tests/`.
