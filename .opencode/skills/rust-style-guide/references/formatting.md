# Formatting and Aesthetics

Source: <https://google.github.io/styleguide/rustguide.html#formatting>

## Use the appropriate formatting tool

- Let the codebase's formatter do the work (`cargo fmt` in a Cargo project, `git cl format`
  in Chromium). Do not hand-format.
- Use the official Rust Style Guide where this guide does not deviate.

Recommended rustfmt configuration:

| Option | Value |
| --- | --- |
| `max_width` | `100` |
| `hex_literal_case` | `Lower` (`0xab123cdef`, not `0xAB123CDEF`) |
| `format_macro_matchers` | `true` |
| Invisible characters in literals | Use escapes (e.g. `\u{200D}`) |

A codebase may skip options that are unavailable on its stable Rust toolchain.

## Opting out of automatic formatting

`#[rustfmt::skip]` is allowed only for:

- **Manual alignment** — situation-specific alignment that aids readability (similar lines
  kept aligned, matrix constructors).
- **Generated code** — prefer a generation rule instead.

Never skip formatting for personal style preferences.
