# syntax=docker/dockerfile:1

# Build the release binary in a pinned Rust image. The dummy build caches the
# dependency graph before the real sources are copied, so code-only changes do
# not recompile every crate.
FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && printf 'fn main() {}\n' > src/main.rs
RUN cargo build --release --locked
RUN rm -rf src
COPY src ./src
# The dummy build above leaves a fingerprint for this package. Remove only the
# package artifacts so dependencies stay cached but the real sources compile.
RUN cargo clean -p l4guard --release
RUN cargo build --release --locked

# Minimal runtime. The guard needs glibc, DNS resolution, and /bin/sh for the
# process probes used by the Kubernetes manifests. It does not write files.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 l4guard \
    && useradd --system --uid 65532 --gid 65532 --no-create-home \
        --shell /usr/sbin/nologin l4guard

COPY --from=builder /src/target/release/l4guard /usr/local/bin/l4guard

LABEL org.opencontainers.image.title="l4guard" \
      org.opencontainers.image.description="L4 TCP guard proxy for blockchain nodes"

USER 65532:65532
EXPOSE 26656 26657 31001 31002 9100
ENTRYPOINT ["/usr/local/bin/l4guard"]
CMD ["--config", "/etc/l4guard/guard.toml"]
