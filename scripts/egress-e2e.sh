#!/usr/bin/env bash
# Bare-metal acceptance for none-lane guarded egress: a NIC-less sandbox
# reaches an allowed origin through the host proxy with a host-injected
# credential it never holds, and a disallowed host is denied — all over vsock.
# Runs a dedicated sandboxd on port 7779 (7777=e2e, 7778=archive).
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7779}
TOKEN=${TOKEN:-e2e}
REPO=$(cd "$(dirname "$0")/.." && pwd)
export EGRESS_PROBE_TOKEN=${EGRESS_PROBE_TOKEN:-tok-$(date +%s)}

DATA=$(mktemp -d /tmp/egress-e2e.XXXXXX)
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
  echo "== audit tail (egress events)"
  grep -o '"op":"egress"[^}]*}' "$DATA/state/audit.jsonl" 2>/dev/null | tail -4 || true
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

echo "== build"
if [[ -n ${SANDBOXD_BIN:-} && -n ${EGRESSSMOKE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$EGRESSSMOKE_BIN" "$DATA/egresssmoke"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/egresssmoke" ./cmd/egresssmoke)
fi

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "audit_log": true,
  "secrets": [
    {"name": "probe", "header": "X-Egress-Token", "value_env": "EGRESS_PROBE_TOKEN"}
  ],
  "pools": [
    {"template": "$TEMPLATE", "net": "none", "size": "small", "warm": 1,
     "egress": {"allow": [{"host": "127.0.0.1", "secret": "probe"}]}}
  ]
}
EOF

echo "== start sandboxd (none-lane pool + egress policy)"
"$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 40); do curl -sf "http://$ADDR/healthz" >/dev/null && break; sleep 0.5; done
# Wait for the warm pool so the claim is a warm hit (proxy armed at claim).
for _ in $(seq 1 120); do
  curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" 2>/dev/null |
    jq -e '.pools[0].warm >= 1' >/dev/null 2>&1 && break
  sleep 1
done

echo "== egresssmoke"
"$DATA/egresssmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -secret "$EGRESS_PROBE_TOKEN"
echo "PASS"
