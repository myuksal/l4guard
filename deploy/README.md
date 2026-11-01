# l4guard deployment assets

Reference Kubernetes assets for the Guard-only worker design. The full
operating procedure, fail-closed checklist, and E7 task mapping are in
[`docs/deploy.md`](../docs/deploy.md).

## Quick start

```sh
# 1. Build and push the image, then update `image:` in deploy/base.
docker build -t ghcr.io/example/l4guard:0.1.0 .
docker push ghcr.io/example/l4guard:0.1.0

# 2. Label and taint dedicated Guard workers.
kubectl label node guard-a l4guard.io/role=guard
kubectl label node guard-b l4guard.io/role=guard
kubectl taint node guard-a l4guard.io/guard=true:NoSchedule
kubectl taint node guard-b l4guard.io/guard=true:NoSchedule

# 3. Apply the backend access policy, then Guard.
kubectl apply -k deploy/backend
kubectl apply -k deploy/base

# 4. Verify.
kubectl -n l4guard rollout status deploy/l4guard-p2p
kubectl -n l4guard rollout status deploy/l4guard-rpc
kubectl -n l4guard rollout status deploy/l4guard-egress
kubectl -n l4guard get svc,pod,networkpolicy,pdb
```

## Non-negotiable rules

- Backend Service stays `ClusterIP`. Never use `NodePort`, `LoadBalancer`, or
  `hostPort` for the backend.
- The public LoadBalancer targets Guard workers only, with
  `externalTrafficPolicy: Local`.
- One public P2P endpoint maps to one backend Node ID.
- Guard readiness (`/readyz`) never waits for the backend.
- Replace the placeholder image, peer addresses, and namespace labels before
  applying to a real cluster.

## Chain profiles and E8 checks

- `deploy/chains/cometbft/`: fixed peer, `allow_duplicate_ip`,
  `addr_book_strict`, PEX/seed settings, state-sync RPC host rewrite.
- `deploy/chains/geth/`: discovery disabled, static peers, `NetRestrict`,
  enode/ENR advertisement.
- `scripts/e8-cometbft-sync.sh`: real CometBFT block sync through Guard.
- `scripts/e8-tls-passthrough.py`: state-sync RPC TLS/SNI preservation.

```sh
make e8-check
```

The operating procedure and acceptance criteria are in
[`docs/e8.md`](../docs/e8.md).

## Load, failure, and bypass checks

- `scripts/e9-verify.py`: connection flood, emergency block, manifest/IPv6/UDP
  bypass, Guard replacement, RPC policy bypass, and replica policy versions.
- `scripts/e9-cometbft-recovery.sh`: real CometBFT peer/head-lag/RPC recovery
  after a Guard replacement.

```sh
make e9-check
make e9-recovery
```

The operating procedure, measured results, and canary order are in
[`docs/e9.md`](../docs/e9.md).
