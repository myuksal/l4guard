# Concurrency and Macros

Source: <https://google.github.io/styleguide/rustguide.html#concurrency>

## Handling poison

Never reach inside a poisoned mutex to get at its contents, no matter how tempting.
(`Mutex::lock()` returning `Err(PoisonError)` means another thread panicked while holding
the lock; the protected data may be inconsistent. Propagate or abort instead of
`into_inner()`-ing through the poison.)

## Concurrency rules scoped to other codebases

These rules carry scope tags and must not be applied automatically to this project:

| Rule | Scope | Note |
| --- | --- | --- |
| Don't use `async` / `.await` | `[chrome]` | Chromium prefers sequences. In a Cargo project, async is a normal design choice. |
| Don't start threads through the Rust standard library | `[chrome]` | Chromium prefers sequences over `std::thread`. |
| Use `fuchsia_sync::Mutex` | `[fuchsia]` | Fuchsia-specific mutex type instead of `std`/`parking_lot`. |

If the user asks about these, explain the scope before recommending anything.

## Macros

Preference order when expressing an API:

1. Regular Rust functions, types, traits, objects, and modules — avoid macros, code
   generation, and metaprogramming where possible.
2. `macro_rules!`-style macros.
3. Other code-generation strategies (for example a build rule).
4. `proc_macro` macros.

Weigh benefits against costs before using a macro or code generation:

- IDEs, code search, and other tooling work less well on macro-heavy code.
- Macros create language dialects that make cross-codebase work harder.
- Large-scale changes become more difficult and expensive.

Guidance:

- **Prefer non-macro facilities.** Readers can assume less about a macro invocation than
  about ordinary code, and macros are harder to write correctly.
- **Use `macro_rules!` for simple code generation.** Rust macros operate on the AST, which
  is less fragile than textual generation.
