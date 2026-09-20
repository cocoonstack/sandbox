#!/usr/bin/env bash
# Bare-metal acceptance for SOCKS5 egress: a none-lane sandbox tunnels to an
# allowed host over 127.0.0.1:1080, a GET-only host, an unlisted host and an
# unlisted port are refused there, IMAPS rides the tunnel on its listed port,
# the HTTP door refuses unlisted ports on CONNECT and forward requests, and a
# pool without a policy or without socks5 leaves the host door unwired. Runs a
# dedicated sandboxd on port 7780 (7779=egress).
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7780}
TOKEN=${TOKEN:-e2e}
ECHO=${ECHO:-postman-echo.com}
GET_ONLY=${GET_ONLY:-example.com}
IMAP=${IMAP:-imap.163.com}
REPO=$(cd "$(dirname "$0")/.." && pwd)

DATA=$(mktemp -d /tmp/socks-e2e.XXXXXX)
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
  echo "== audit tail (SOCKS5 events)"
  grep -o '"op":"egress"[^}]*"method":"SOCKS5"[^}]*}' "$DATA/state/audit.jsonl" 2>/dev/null | tail -6 || true
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

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
"$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 40); do curl -sf "http://$ADDR/healthz" >/dev/null && break; sleep 0.5; done
for i in $(seq 1 180); do
  curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" 2>/dev/null |
    jq -e '(.pools | length) == 3 and all(.pools[]; .warm >= 1)' >/dev/null 2>&1 && break
  [[ $i == 180 ]] && { echo "pools never became warm"; exit 1; }
  sleep 1
done

echo "== sockssmoke"
"$DATA/sockssmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -echo "$ECHO" -get-only "$GET_ONLY" -imap "$IMAP"
echo "PASS"
