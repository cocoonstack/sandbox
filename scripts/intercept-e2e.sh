#!/usr/bin/env bash
# Bare-metal acceptance for HTTPS interception: an intercept-pool golden bakes
# the cluster root into the guest, and a none-lane sandbox reaches an HTTPS echo
# through the proxy where the leaf — signed by this node's intermediate, chaining
# to the cluster root — is trusted and the credential is injected into the HTTPS
# request; direct egress is blocked. Runs a dedicated sandboxd on port 7779.
# Needs outbound to $ECHO and a $TEMPLATE whose silkd binds the egress relay.
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7779}
TOKEN=${TOKEN:-e2e}
ECHO=${ECHO:-postman-echo.com}
REPO=$(cd "$(dirname "$0")/.." && pwd)
PROBE=${EGRESS_PROBE_TOKEN:-probe-$(date +%s)}
export EGRESS_PROBE_TOKEN=$PROBE

DATA=$(mktemp -d /tmp/intercept-e2e.XXXXXX)
DAEMON_PID=""

cleanup() {
  status=$?
  if [[ $status -ne 0 && -f "$DATA/daemon.log" ]]; then
    echo "== daemon log tail"
    tail -30 "$DATA/daemon.log"
  fi
  if [[ -n $DAEMON_PID ]]; then
    kill "$DAEMON_PID" 2>/dev/null || true
  fi
  wait 2>/dev/null || true
  cocoon vm list --format json 2>/dev/null |
    jq -r '.[] | select(.config.name | startswith("sbx-")) | .config.name' |
    while read -r vm; do
      cocoon vm stop --force "$vm" >/dev/null 2>&1 || true
      cocoon vm rm --force "$vm" >/dev/null 2>&1 || true
    done || true
  echo "== egress audit tail"
  grep -o '"op":"egress"[^}]*}' "$DATA/state/audit.jsonl" 2>/dev/null | tail -4 || true
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

echo "== build"
if [[ -n ${SANDBOXD_BIN:-} && -n ${INTERCEPTSMOKE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$INTERCEPTSMOKE_BIN" "$DATA/interceptsmoke"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/interceptsmoke" ./cmd/interceptsmoke)
fi

echo "== provision cluster root + this node's intermediate"
"$DATA/sandboxd" ca init -out "$DATA/ca" -cn "e2e cluster root" >/dev/null
"$DATA/sandboxd" ca issue-intermediate -root-cert "$DATA/ca/root.crt" \
  -root-key "$DATA/ca/root.key" -node e2e-node -out "$DATA/node" >/dev/null

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "audit_log": true,
  "secrets": [{"name": "probe", "header": "X-Egress-Token", "value_env": "EGRESS_PROBE_TOKEN"}],
  "egress_ca": {"root_cert": "$DATA/ca/root.crt", "intermediate_cert": "$DATA/node/e2e-node.crt", "intermediate_key": "$DATA/node/e2e-node.key"},
  "pools": [
    {"template": "$TEMPLATE", "net": "none", "size": "small", "warm": 1,
     "egress": {"allow": [{"host": "$ECHO", "secret": "probe", "intercept": true}]}}
  ]
}
EOF

echo "== start sandboxd (intercept pool; golden build bakes the cluster root via silkd)"
"$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 40); do curl -sf "http://$ADDR/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
for _ in $(seq 1 300); do
  curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" 2>/dev/null |
    jq -e '.pools[0].warm >= 1' >/dev/null 2>&1 && break
  sleep 1
done

echo "== interceptsmoke"
"$DATA/interceptsmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -echo "$ECHO" -secret "$PROBE"
echo "PASS"
