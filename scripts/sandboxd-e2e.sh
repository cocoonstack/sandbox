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

echo "== start (pool: $TEMPLATE none/small warm=$WARM)"
cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "pools": [{"template": "$TEMPLATE", "net": "none", "size": "small", "warm": $WARM}]
}
EOF
start_daemon

echo "== wait for golden + warm pool (cold boot + snapshot export + refill)"
for i in $(seq 1 120); do
  if api info | jq -e --argjson w "$WARM" \
    '.pools[0].golden and .pools[0].warm >= $w' >/dev/null 2>&1; then
    break
  fi
  [[ $i == 120 ]] && { echo "pool never became ready"; api info | jq . || true; exit 1; }
  sleep 1
done
api info | jq .

echo "== claim/exec/release x3 (expect: warm-hit ms-scale, then clone-tier)"
"$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 3

echo "== v2 smoke: files/session/find/replace/watch/git/pty through the relay"
"$DATA/smoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE"

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
