# Chain profiles

Backend-side configuration examples for the fixed-egress design. Guard itself
is chain-agnostic; these profiles describe what the node must change so that
its only paths to peers and state-sync RPC go through Guard.

- [`cometbft/`](cometbft/): P2P fixed peer, `allow_duplicate_ip`,
  `addr_book_strict`, PEX/seed settings, and state-sync RPC host rewrite.
- [`geth/`](geth/): discovery disabled, static peers, `NetRestrict`, enode/ENR
  advertisement.

The operational checklist and acceptance procedure are in
[`docs/e8.md`](../../docs/e8.md).

## Shared rules

1. The backend's peer address points at a Guard private Service, not at the
   real peer's public address.
2. The backend's advertised address points at the Guard public endpoint, not
   at the backend Pod IP.
3. Direct discovery paths (PEX, seeds, UDP discovery) are disabled or
   explicitly reviewed.
4. state-sync RPC hostname overrides apply only to the backend Pod. Guard must
   resolve the real provider hostname.
5. Guard never terminates TLS and never appends a PROXY header to the backend
   stream.
