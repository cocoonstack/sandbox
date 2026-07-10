#!/usr/bin/env bash
# Bare-metal acceptance for lifecycle auto-archive: a claim idles into
# hibernation, then to a store checkpoint (its local VM dropped), and the next
# access wakes it from the store with the same id and guest state intact.
# Runs a dedicated sandboxd on port 7778 so it never collides with
# sandboxd-e2e.sh (7777).
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7778}
TOKEN=${TOKEN:-e2e}
REPO=$(cd "$(dirname "$0")/.." && pwd)

DATA=$(mktemp -d /tmp/archive-e2e.XXXXXX)
DAEMON_PID=""

cleanup() {
  status=$?
  if [[ $status -ne 0 && -f "$DATA/daemon.log" ]]; then
    echo "== daemon log tail"
    tail -30 "$DATA/daemon.log"
  fi
  [[ -n $DAEMON_PID ]] && kill "$DAEMON_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  cocoon vm list --format json 2>/dev/null |
    jq -r '.[] | select(.config.name | startswith("sbx-")) | .config.name' |
    while read -r vm; do
      cocoon vm stop --force "$vm" >/dev/null 2>&1 || true
      cocoon vm rm --force "$vm" >/dev/null 2>&1 || true
    done || true
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

api() { curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/$1"; }

echo "== build"
if [[ -n ${SANDBOXD_BIN:-} && -n ${LIFECYCLE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$LIFECYCLE_BIN" "$DATA/lifecycle"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/lifecycle" ./cmd/lifecycle)
fi

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "pools": [{"template": "$TEMPLATE", "net": "none", "size": "small", "warm": 1,
             "idle_hibernate_seconds": 1, "archive_after_seconds": 2, "archive_delete_after_seconds": 3600}]
}
EOF

echo "== start sandboxd (idle=1s archive=2s delete=3600s)"
"$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 20); do curl -sf "http://$ADDR/healthz" >/dev/null && break; sleep 0.5; done

echo "== wait for golden + warm pool"
for i in $(seq 1 120); do
  if api info | jq -e '(.pools|length)>0 and all(.pools[]; .golden and .warm>=.target)' >/dev/null 2>&1; then break; fi
  [[ $i == 120 ]] && { echo "pools never ready"; api info | jq . || true; exit 1; }
  sleep 1
done
api info | jq '{pools:(.pools|length),claimed,hibernated,archived}'

echo "== archive lifecycle: claim -> idle-hibernate -> archive -> wake"
"$DATA/lifecycle" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -net none -wait 90s

echo "== post-run node state"
api info | jq '{claimed,hibernated,archived}'
echo "PASS"
