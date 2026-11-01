#!/usr/bin/env bash
# E9-T4/T5: measure recovery after a guard replacement.
#
# Topology:
#   node-a (validator, kvstore app)  <--  guard egress  <--  node-b (full node)
#
# The script first proves that node-b syncs through the guard, then removes the
# guard, starts a replacement with the same fixed egress configuration, and
# measures:
#
#   * guard downtime,
#   * time until node-b sees its peer again,
#   * time until node-b is back within two blocks of node-a,
#   * RPC success rate observed while the guard was down and recovering.
#
# node-a and node-b keep their processes and data throughout. Only the guard is
# replaced.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-l4guard:e9-test}"
COMETBFT_IMAGE="${COMETBFT_IMAGE:-cometbft/cometbft:v0.38.17}"
SKIP_BUILD="${SKIP_BUILD:-0}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/l4guard-e9-recovery.XXXXXX")"
SUFFIX="$$"
NET="l4guard-e9-net-$SUFFIX"
NODE_A="l4guard-e9-node-a-$SUFFIX"
NODE_B="l4guard-e9-node-b-$SUFFIX"
GUARD="l4guard-e9-guard-$SUFFIX"

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
max_connections = 32
max_connections_per_listener = 16
max_outbound_connections = 16
max_sessions = 32

[policy]
allow = ["0.0.0.0/0", "::/0"]

[observe]
metrics_listen = "0.0.0.0:9100"

[[upstreams]]
name = "node-a"
address = "$NODE_A:26656"

[[upstreams]]
name = "node-b-rpc"
address = "$NODE_B:26657"

[[listeners]]
name = "node-a-egress"
listen = "0.0.0.0:31001"
mode = "outbound"
upstream = "node-a"

# RPC path through the guard, used only to measure RPC success during the
# replacement. It is not the chain's public RPC endpoint.
[[listeners]]
name = "node-b-rpc"
listen = "0.0.0.0:26657"
upstream = "node-b-rpc"

[listeners.l7]
allow_methods = ["status"]
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

start_guard() {
    docker rm -f "$GUARD" >/dev/null 2>&1 || true
    docker run -d --name "$GUARD" --network "$NET" --read-only --user 65532:65532 \
        -p 127.0.0.1::9100 -p 127.0.0.1::26657 \
        -v "$TMP/guard.toml:/etc/l4guard/guard.toml:ro" \
        "$IMAGE" >/dev/null
    G_PORT="$(docker port "$GUARD" 9100/tcp | awk -F: '{print $NF}')"
    G_RPC_PORT="$(docker port "$GUARD" 26657/tcp | awk -F: '{print $NF}')"
    for _ in $(seq 1 100); do
        if curl -fsS "http://127.0.0.1:$G_PORT/healthz" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    echo "guard did not become ready" >&2
    docker logs --tail=50 "$GUARD" >&2 || true
    exit 1
}

echo "starting guard and node-b at node-a height=$A_HEIGHT"
start_guard
docker run -d --name "$NODE_B" --network "$NET" --user 0 \
    -v "$TMP/node-b:/cometbft" -p 127.0.0.1::26657 "$COMETBFT_IMAGE" \
    node --home /cometbft --proxy_app=kvstore \
    --rpc.laddr=tcp://0.0.0.0:26657 --p2p.laddr=tcp://0.0.0.0:26656 >/dev/null
B_PORT="$(docker port "$NODE_B" 26657/tcp | awk -F: '{print $NF}')"

node_a_height() {
    curl -fsS "http://127.0.0.1:$A_PORT/status" 2>/dev/null \
        | jq -r '.result.sync_info.latest_block_height // "0"' 2>/dev/null || echo 0
}

node_b_height() {
    curl -fsS "http://127.0.0.1:$B_PORT/status" 2>/dev/null \
        | jq -r '.result.sync_info.latest_block_height // "0"' 2>/dev/null || echo 0
}

node_b_peers() {
    curl -fsS "http://127.0.0.1:$B_PORT/net_info" 2>/dev/null \
        | jq -r '.result.peers | length' 2>/dev/null || echo 0
}

echo "waiting for node-b to reach the chain tip"
B_HEIGHT=0
TIP_DEADLINE=$((SECONDS + 90))
while [ "$SECONDS" -lt "$TIP_DEADLINE" ]; do
    A_HEIGHT="$(node_a_height)"
    B_HEIGHT="$(node_b_height)"
    echo "  node-a height=$A_HEIGHT node-b height=$B_HEIGHT peers=$(node_b_peers)"
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

BASELINE_PEERS="$(node_b_peers)"
BASELINE_HEIGHT="$(node_b_height)"
echo "baseline: node-b height=$BASELINE_HEIGHT peers=$BASELINE_PEERS"

# Measure RPC success while the guard is gone and recovering. A failure is a
# refused connection or a non-zero curl exit, not an HTTP error from node-b.
RPC_TOTAL=0
RPC_OK=0
rpc_probe() {
    RPC_TOTAL=$((RPC_TOTAL + 1))
    if curl -fsS --max-time 1 "http://127.0.0.1:$G_RPC_PORT/status" >/dev/null 2>&1; then
        RPC_OK=$((RPC_OK + 1))
    fi
}

echo "removing the guard"
DOWN_START="$(python3 -c 'import time; print(time.time())')"
docker rm -f "$GUARD" >/dev/null
PEER_LOSS_DEADLINE=$((SECONDS + 30))
while [ "$(node_b_peers)" != "0" ]; do
    rpc_probe
    if [ "$SECONDS" -ge "$PEER_LOSS_DEADLINE" ]; then
        echo "node-b did not notice the guard outage" >&2
        exit 1
    fi
    sleep 0.2
done
echo "node-b lost its peer after $(python3 -c "import time; print(f'{time.time() - $DOWN_START:.3f}')")s"

# Hold the guard down long enough to observe RPC failures through it. Without
# this window a local replacement can complete before the first probe.
OUTAGE_HOLD="${OUTAGE_HOLD:-3}"
echo "holding the guard down for ${OUTAGE_HOLD}s"
HOLD_END=$((SECONDS + OUTAGE_HOLD))
while [ "$SECONDS" -lt "$HOLD_END" ]; do
    rpc_probe
    sleep 0.2
done

echo "starting the replacement guard"
REPLACEMENT_START="$(python3 -c 'import time; print(time.time())')"
start_guard

RECONNECT=""
RECONNECT_DEADLINE=$((SECONDS + 60))
while [ -z "$RECONNECT" ]; do
    rpc_probe
    if [ "$(node_b_peers)" -gt 0 ]; then
        RECONNECT="$(python3 -c "import time; print(f'{time.time() - $REPLACEMENT_START:.3f}')")"
        break
    fi
    if [ "$SECONDS" -ge "$RECONNECT_DEADLINE" ]; then
        echo "node-b did not reconnect to the replacement guard" >&2
        docker logs --tail=50 "$NODE_B" >&2 || true
        docker logs --tail=50 "$GUARD" >&2 || true
        exit 1
    fi
    sleep 0.2
done
echo "node-b reconnected to the replacement guard after ${RECONNECT}s"

CATCHUP=""
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    rpc_probe
    A_HEIGHT="$(node_a_height)"
    B_HEIGHT="$(node_b_height)"
    echo "  node-a height=$A_HEIGHT node-b height=$B_HEIGHT peers=$(node_b_peers)"
    if [ "${B_HEIGHT:-0}" -ge "$((A_HEIGHT - 2))" ] 2>/dev/null; then
        CATCHUP="$(python3 -c "import time; print(f'{time.time() - $REPLACEMENT_START:.3f}')")"
        break
    fi
    sleep 0.2
done

if [ -z "$CATCHUP" ]; then
    echo "node-b did not recover to the chain tip" >&2
    docker logs --tail=50 "$NODE_B" >&2 || true
    docker logs --tail=50 "$GUARD" >&2 || true
    exit 1
fi

METRICS="$(curl -fsS "http://127.0.0.1:$G_PORT/metrics")"
ACCEPTED="$(printf '%s\n' "$METRICS" \
    | awk '$1 == "l4guard_accepted_total" {print $2}')"
ACTIVE="$(printf '%s\n' "$METRICS" \
    | awk '$1 == "l4guard_active_sessions" {print $2}')"
if [ "${ACCEPTED:-0}" -le 0 ] || [ "${ACTIVE:-0}" -le 0 ]; then
    echo "replacement guard metrics do not show an active relayed session" >&2
    printf '%s\n' "$METRICS" >&2
    exit 1
fi

if [ "$RPC_TOTAL" -gt 0 ]; then
    RPC_RATE="$(python3 -c "print(f'{100 * $RPC_OK / $RPC_TOTAL:.1f}')")"
else
    RPC_RATE="n/a"
fi

echo "PASS recovery:"
echo "  guard outage before replacement: $(python3 -c "import time; print(f'{$REPLACEMENT_START - $DOWN_START:.3f}')")s"
echo "  peer reconnect: ${RECONNECT}s"
echo "  head lag recovery (within 2 blocks): ${CATCHUP}s"
echo "  total outage to head lag recovery: $(python3 -c "import time; print(f'{time.time() - $DOWN_START:.3f}')")s"
echo "  RPC success during outage/recovery: ${RPC_OK}/${RPC_TOTAL} (${RPC_RATE}%)"
echo "  replacement guard accepted=$ACCEPTED active_sessions=$ACTIVE"
echo "  node-b height=$B_HEIGHT peers=$(node_b_peers)"
