#!/usr/bin/env bash
# Bare-metal e2e for sandboxd v0: warm pool, claim tiers, reap, reconcile.
# Run on a node with cocoon and a silkd-baked template image (see os-image/).
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7777}
TOKEN=${TOKEN:-e2e}
WARM=${WARM:-2}
REPO=$(cd "$(dirname "$0")/.." && pwd)

DATA=$(mktemp -d /tmp/sandboxd-e2e.XXXXXX)
DAEMON_PID=""

cleanup() {
  status=$?
  if [[ $status -ne 0 && -f "$DATA/daemon.log" ]]; then
    echo "== daemon log tail"
    tail -20 "$DATA/daemon.log"
  fi
  [[ -n $DAEMON_PID ]] && kill "$DAEMON_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  cocoon vm list --format json 2>/dev/null |
    jq -r '.[] | select(.config.name | startswith("sbx-")) | .config.name' |
    while read -r vm; do
      # Force-stop first: rm's stop-before-delete waits a 30s graceful
      # window these guests never answer.
      cocoon vm stop --force "$vm" >/dev/null 2>&1 || true
      cocoon vm rm --force "$vm" >/dev/null 2>&1 || true
    done || true
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

api() { curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/$1"; }

start_daemon() {
  "$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
  DAEMON_PID=$!
  for _ in $(seq 1 20); do
    curl -sf "http://$ADDR/healthz" >/dev/null && return 0
    sleep 0.5
  done
  echo "daemon never came up"
  return 1
}

echo "== build"
# Prebuilt binaries let the script run on nodes without a Go toolchain.
if [[ -n ${SANDBOXD_BIN:-} && -n ${DEMO_BIN:-} && -n ${SMOKE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$DEMO_BIN" "$DATA/demo" && cp "$SMOKE_BIN" "$DATA/smoke"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/demo" ./cmd/demo)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/smoke" ./cmd/smoke)
fi

# BRIDGE (optional) attaches the node to an egress bridge, adds a small
# egress pool, and turns on the smoke's egress-lane check; lane detection
# needs a NIC, not connectivity, so a plain `ip link add ... type bridge`
# with no uplink is enough.
BRIDGE_LINE=""
EGRESS_POOL=""
if [[ -n ${BRIDGE:-} ]]; then
  BRIDGE_LINE="\"bridges\": [\"$BRIDGE\"],"
  EGRESS_POOL=", {\"template\": \"$TEMPLATE\", \"net\": \"egress\", \"size\": \"small\", \"warm\": 1}"
fi

# S3_ENDPOINT (optional, with S3_BUCKET and AWS creds in the env) switches
# the checkpoint store to the s3 backend — the checkpoint/promote steps
# then exercise object storage end to end.
STORE_LINE=""
if [[ -n ${S3_ENDPOINT:-} ]]; then
  STORE_LINE="\"checkpoint_store\": {\"kind\": \"s3\", \"s3\": {\"bucket\": \"${S3_BUCKET:-sbx-checkpoints}\", \"prefix\": \"e2e/\", \"endpoint\": \"$S3_ENDPOINT\", \"region\": \"us-east-1\", \"force_path_style\": true}},"
fi

echo "== start (pool: $TEMPLATE none/small warm=$WARM${BRIDGE:+, egress via $BRIDGE}${S3_ENDPOINT:+, s3 store at $S3_ENDPOINT})"
cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  $BRIDGE_LINE
  $STORE_LINE
  "pools": [{"template": "$TEMPLATE", "net": "none", "size": "small", "warm": $WARM}$EGRESS_POOL]
}
EOF
start_daemon

echo "== wait for goldens + warm pools (cold boot + snapshot export + refill)"
for i in $(seq 1 120); do
  if api info | jq -e \
    '(.pools | length) > 0 and all(.pools[]; .golden and .warm >= .target)' >/dev/null 2>&1; then
    break
  fi
  [[ $i == 120 ]] && { echo "pools never became ready"; api info | jq . || true; exit 1; }
  sleep 1
done
api info | jq .

echo "== claim/exec/release x3 (expect: warm-hit ms-scale, then clone-tier)"
"$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 3

echo "== v2 smoke: files/session/find/replace/watch/git/pty through the relay"
# LSP_TEMPLATE (optional, a python-flavor ref) adds the LSP broker step.
"$DATA/smoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" ${BRIDGE:+-egress} ${LSP_TEMPLATE:+-lsp-template "$LSP_TEMPLATE"}

echo "== reap: leaked 5s-ttl claim is destroyed by the owner"
"$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 1 -ttl 5 -leak
sleep 12
claimed=$(api info | jq .claimed)
[[ $claimed == 0 ]] || { echo "leaked claim survived reap: claimed=$claimed"; exit 1; }

echo "== restart: live claim re-adopts, warm VMs of the old life are replaced"
"$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 1 -ttl 300 -leak
kill "$DAEMON_PID" && wait "$DAEMON_PID" 2>/dev/null || true
start_daemon
claimed=$(api info | jq .claimed)
[[ $claimed == 1 ]] || { echo "claim not re-adopted: claimed=$claimed"; exit 1; }

echo "PASS"
