//! `l4guard` — an L4 TCP guard proxy for blockchain nodes.
//!
//! See `plan.md` for the design. This entry point is a skeleton: the modules
//! described in the plan (`config`, `policy`, `relay`, `sessions`, `observe`,
//! `rpc`) are added as they are implemented.

fn main() {
    println!("l4guard {}", env!("CARGO_PKG_VERSION"));
}
