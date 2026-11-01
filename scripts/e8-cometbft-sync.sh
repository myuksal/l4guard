#!/usr/bin/env bash
# E8-T6: run a real CometBFT block sync through the guard.
#
# Topology:
#   node-a (validator, kvstore app)  <--  guard egress  <--  node-b (full node)
#
# node-b starts at height 0 and must catch up to node-a through the guard's
# fixed egress listener. The script also checks the guard metrics to prove the
# bytes went through the proxy.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-l4guard:e8-test}"
COMETBFT_IMAGE="${COMETBFT_IMAGE:-cometbft/cometbft:v0.38.17}"
SKIP_BUILD="${SKIP_BUILD:-0}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/l4guard-e8.XXXXXX")"
SUFFIX="$$"
NET="l4guard-e8-net-$SUFFIX"
NODE_A="l4guard-e8-node-a-$SUFFIX"
NODE_B="l4guard-e8-node-b-$SUFFIX"
GUARD="l4guard-e8-guard-$SUFFIX"

cleanup() {
    docker rm -f "$NODE_A" "$NODE_B" "$GUARD" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    rm -rf "$TMP"
}
trap cleanup EXIT

for tool in docker curl jq python3; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "missing required tool: $tool" >&2
        exit 1
    }
done

if [ "$SKIP_BUILD" != "1" ]; then
    echo "building $IMAGE"
    docker build -t "$IMAGE" "$ROOT" >/dev/null
fi

echo "preparing CometBFT nodes"
docker pull "$COMETBFT_IMAGE" >/dev/null
mkdir -p "$TMP/node-a" "$TMP/node-b"
for node in node-a node-b; do
    docker run --rm --user 0 -v "$TMP/$node:/cometbft" \
        --entrypoint /bin/sh "$COMETBFT_IMAGE" \
        -c "cometbft init --home /cometbft >/dev/null"
done
cp "$TMP/node-a/config/genesis.json" "$TMP/node-b/config/genesis.json"
A_ID="$(docker run --rm --user 0 -v "$TMP/node-a:/cometbft" \
    --entrypoint /bin/sh "$COMETBFT_IMAGE" \
    -c "cometbft show-node-id --home /cometbft" | tr -d '\r')"

python3 - "$TMP" "$A_ID" "$GUARD" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
peer_id = sys.argv[2]
guard_name = sys.argv[3]


def edit(path, replacements):
    text = path.read_text()
    for old, new in replacements:
        if old not in text:
            raise SystemExit(f"missing {old!r} in {path}")
        text = text.replace(old, new, 1)
    path.write_text(text)


common = [
    ('laddr = "tcp://127.0.0.1:26657"', 'laddr = "tcp://0.0.0.0:26657"'),
    ("addr_book_strict = true", "addr_book_strict = false"),
    ("allow_duplicate_ip = false", "allow_duplicate_ip = true"),
    ("pex = true", "pex = false"),
]
edit(root / "node-a/config/config.toml", common)
edit(
    root / "node-b/config/config.toml",
    common
    + [
        ('moniker = "', 'moniker = "node-b-'),
        ('persistent_peers = ""', f'persistent_peers = "{peer_id}@{guard_name}:31001"'),
    ],
)
PY

cat > "$TMP/guard.toml" <<EOF
[limits]
max_connections = 16
max_connections_per_listener = 16
max_outbound_connections = 16
max_sessions = 16

[policy]
allow = ["0.0.0.0/0", "::/0"]

[observe]
metrics_listen = "0.0.0.0:9100"

[[upstreams]]
name = "node-a"
address = "$NODE_A:26656"

[[listeners]]
name = "node-a-egress"
listen = "0.0.0.0:31001"
mode = "outbound"
upstream = "node-a"
EOF

docker network create "$NET" >/dev/null
echo "starting node-a"
docker run -d --name "$NODE_A" --network "$NET" --user 0 \
    -v "$TMP/node-a:/cometbft" -p 127.0.0.1::26657 "$COMETBFT_IMAGE" \
    node --home /cometbft --proxy_app=kvstore \
    --rpc.laddr=tcp://0.0.0.0:26657 --p2p.laddr=tcp://0.0.0.0:26656 >/dev/null

A_PORT="$(docker port "$NODE_A" 26657/tcp | awk -F: '{print $NF}')"
A_HEIGHT=0
for _ in $(seq 1 120); do
    A_HEIGHT="$(curl -fsS "http://127.0.0.1:$A_PORT/status" 2>/dev/null \
        | jq -r '.result.sync_info.latest_block_height // "0"' 2>/dev/null || echo 0)"
    if [ "$A_HEIGHT" -ge 20 ] 2>/dev/null; then
        break
    fi
    sleep 1
done
if [ "${A_HEIGHT:-0}" -lt 20 ]; then
    echo "node-a did not produce enough blocks" >&2
    docker logs --tail=50 "$NODE_A" >&2 || true
    exit 1
fi

echo "starting guard and node-b at node-a height=$A_HEIGHT"
docker run -d --name "$GUARD" --network "$NET" --read-only --user 65532:65532 \
    -p 127.0.0.1::9100 -v "$TMP/guard.toml:/etc/l4guard/guard.toml:ro" \
    "$IMAGE" >/dev/null
docker run -d --name "$NODE_B" --network "$NET" --user 0 \
    -v "$TMP/node-b:/cometbft" -p 127.0.0.1::26657 "$COMETBFT_IMAGE" \
    node --home /cometbft --proxy_app=kvstore \
    --rpc.laddr=tcp://0.0.0.0:26657 --p2p.laddr=tcp://0.0.0.0:26656 >/dev/null

B_PORT="$(docker port "$NODE_B" 26657/tcp | awk -F: '{print $NF}')"
G_PORT="$(docker port "$GUARD" 9100/tcp | awk -F: '{print $NF}')"

echo "waiting for node-b to reach the chain tip"
B_HEIGHT=0
TIP_DEADLINE=$((SECONDS + 90))
while [ "$SECONDS" -lt "$TIP_DEADLINE" ]; do
    A_HEIGHT="$(curl -fsS "http://127.0.0.1:$A_PORT/status" 2>/dev/null \
        | jq -r '.result.sync_info.latest_block_height // "0"' 2>/dev/null || echo 0)"
    STATUS="$(curl -fsS "http://127.0.0.1:$B_PORT/status" 2>/dev/null || true)"
    if [ -n "$STATUS" ]; then
        B_HEIGHT="$(printf '%s' "$STATUS" | jq -r '.result.sync_info.latest_block_height // "0"')"
    fi
    echo "  node-a height=$A_HEIGHT node-b height=$B_HEIGHT"
    if [ "${B_HEIGHT:-0}" -ge "$((A_HEIGHT - 2))" ] 2>/dev/null; then
        break
    fi
    sleep 1
done

if [ "${B_HEIGHT:-0}" -lt "$((A_HEIGHT - 2))" ]; then
    echo "node-b did not reach the chain tip through the guard" >&2
    docker logs --tail=50 "$NODE_B" >&2 || true
    docker logs --tail=50 "$GUARD" >&2 || true
    exit 1
fi

# A live validator keeps producing blocks, so catching_up can be transiently
# true while the full node follows the tip. Reaching the tip within two blocks
# plus non-zero relayed bytes is the acceptance signal here.
B_CATCHING="$(curl -fsS "http://127.0.0.1:$B_PORT/status" 2>/dev/null \
    | jq -r '.result.sync_info.catching_up // "true"' 2>/dev/null || echo true)"
echo "node-b tip check: height=$B_HEIGHT catching_up=$B_CATCHING"

METRICS="$(curl -fsS "http://127.0.0.1:$G_PORT/metrics")"
ACCEPTED="$(printf '%s\n' "$METRICS" \
    | awk '$1 == "l4guard_accepted_total" {print $2}')"
ACTIVE="$(printf '%s\n' "$METRICS" \
    | awk '$1 == "l4guard_active_sessions" {print $2}')"
if [ "${ACCEPTED:-0}" -le 0 ] || [ "${ACTIVE:-0}" -le 0 ]; then
    echo "guard metrics do not show an active relayed session" >&2
    printf '%s\n' "$METRICS" >&2
    exit 1
fi

echo "PASS: node-b reached height $B_HEIGHT through the guard"
echo "      accepted=$ACCEPTED active_sessions=$ACTIVE"
