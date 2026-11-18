#!/usr/bin/env bash
# Bare-metal acceptance for guarded egress: a sandbox reaches an allowed origin
# through the host proxy with a host-injected credential it never holds, direct
# egress is blocked (no NIC on the none lane, an nft lock on the egress lane),
# and a disallowed host is denied. NET=none|egress selects the lane.
# Runs a dedicated sandboxd on port 7779 (7777=e2e, 7778=archive); cleanup removes every sbx-* VM on the cocoon root.
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7779}
TOKEN=${TOKEN:-e2e}
NET=${NET:-none}
BRIDGE=${BRIDGE:-sbxbr0}
ECHO=${ECHO:-postman-echo.com}
REPO=$(cd "$(dirname "$0")/.." && pwd)
export EGRESS_PROBE_TOKEN=${EGRESS_PROBE_TOKEN:-tok-$(date +%s)}

REACH=127.0.0.1
BRIDGE_CFG=""
NICADDR=""
if [[ $NET == egress ]]; then
  REACH=$(ip -4 addr show "$BRIDGE" 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1 | head -1 || true)
  [[ -z $REACH ]] && { echo "bridge $BRIDGE has no IPv4 — egress lane needs a gateway"; exit 1; }
  BRIDGE_CFG="\"bridges\": [\"$BRIDGE\"],"
  NICADDR="${REACH%.*}.222/24" # static guest NIC (image does not DHCP), so a blocked direct egress is the nft lock
fi

DATA=$(mktemp -d /tmp/egress-e2e.XXXXXX)
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

cleanup_extra() {
  echo "== audit tail (egress events)"
  grep -o '"op":"egress"[^}]*}' "$DATA/state/audit.jsonl" 2>/dev/null | tail -4 || true
}

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
  $BRIDGE_CFG
  "secrets": [
    {"name": "probe", "header": "X-Egress-Token", "value_env": "EGRESS_PROBE_TOKEN"}
  ],
  "pools": [
    {"template": "$TEMPLATE", "net": "$NET", "size": "small", "warm": 1,
     "egress": {"allow": [{"host": "$ECHO", "secret": "probe"}]}}
  ]
}
EOF

echo "== start sandboxd ($NET-lane pool + egress policy, reach=$REACH)"
start_daemon
# Wait for the warm pool so the claim is a warm hit (proxy armed at claim).
wait_pools '.pools[0].warm >= 1'

echo "== egresssmoke"
"$DATA/egresssmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -secret "$EGRESS_PROBE_TOKEN" -net "$NET" -reach "$REACH" -nicaddr "$NICADDR" -echo "$ECHO"
echo "PASS"
