#!/usr/bin/env bash
# Bare-metal acceptance for SOCKS5 egress: a none-lane sandbox tunnels to an
# allowed host over 127.0.0.1:1080, a GET-only host, an unlisted host and an
# unlisted port are refused there, IMAPS rides the tunnel on its listed port,
# the HTTP door refuses unlisted ports on CONNECT and forward requests, and a
# pool without a policy or without socks5 leaves the host door unwired. Runs a
# dedicated sandboxd on port 7780 (7779=egress); cleanup removes every sbx-* VM on the cocoon root.
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7780}
TOKEN=${TOKEN:-e2e}
ECHO=${ECHO:-postman-echo.com}
GET_ONLY=${GET_ONLY:-example.com}
IMAP=${IMAP:-imap.163.com}
REPO=$(cd "$(dirname "$0")/.." && pwd)

DATA=$(mktemp -d /tmp/socks-e2e.XXXXXX)
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

cleanup_extra() {
  echo "== audit tail (SOCKS5 events)"
  grep -o '"op":"egress"[^}]*"method":"SOCKS5"[^}]*}' "$DATA/state/audit.jsonl" 2>/dev/null | tail -6 || true
}

echo "== build"
if [[ -n ${SANDBOXD_BIN:-} && -n ${SOCKSSMOKE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$SOCKSSMOKE_BIN" "$DATA/sockssmoke"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/sockssmoke" ./cmd/sockssmoke)
fi

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "audit_log": true,
  "pools": [
    {"template": "$TEMPLATE", "net": "none", "size": "small", "warm": 1,
     "egress": {"socks5": true,
                "allow": [{"host": "$ECHO", "ports": [80]}, {"host": "$GET_ONLY", "methods": ["GET"]}, {"host": "$IMAP", "ports": [993]}]}},
    {"template": "$TEMPLATE", "net": "none", "size": "medium", "warm": 1},
    {"template": "$TEMPLATE", "net": "none", "size": "large", "warm": 1,
     "egress": {"allow": [{"host": "$ECHO"}]}}
  ]
}
EOF

echo "== start sandboxd (small pool opted into SOCKS5, medium pool without a policy, large pool with a policy that did not opt in)"
start_daemon
wait_pools '(.pools | length) == 3 and all(.pools[]; .warm >= 1)'

echo "== sockssmoke"
"$DATA/sockssmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -echo "$ECHO" -get-only "$GET_ONLY" -imap "$IMAP"
echo "PASS"
