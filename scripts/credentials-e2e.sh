#!/usr/bin/env bash
# Bare-metal acceptance for claim credentials: claims on one intercept pool each
# send their own env-injected header to an HTTPS echo, the guest never holds the
# value, an open event stream reaches the guest event by event, and a credential
# survives a sandboxd restart; a pool whose only rule is intercept "inject"
# intercepts only the claims that hold a credential for a host and fills query
# and body placeholders. Runs a dedicated sandboxd on $ADDR.
# Needs outbound to $ECHO and a $TEMPLATE whose silkd binds the egress relay.
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7882}
TOKEN=${TOKEN:-e2e}
ECHO=${ECHO:-postman-echo.com}
OTHER=${OTHER:-example.com}
SSE_PORT=${SSE_PORT:-18998}
REPO=$(cd "$(dirname "$0")/.." && pwd)
PROBE=${EGRESS_PROBE_TOKEN:-probe-$(date +%s)}
export EGRESS_PROBE_TOKEN=$PROBE

DATA=$(mktemp -d /tmp/credentials-e2e.XXXXXX)
DAEMON_PID=""
SSE_PID=""

cleanup() {
  status=$?
  if [[ $status -ne 0 && -f "$DATA/daemon.log" ]]; then
    echo "== daemon log tail"
    tail -30 "$DATA/daemon.log"
  fi
  for pid in $DAEMON_PID $SSE_PID; do
    kill "$pid" 2>/dev/null || true
  done
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

echo "== build"
if [[ -n ${SANDBOXD_BIN:-} && -n ${CREDSMOKE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$CREDSMOKE_BIN" "$DATA/credsmoke"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/credsmoke" ./cmd/credsmoke)
fi

echo "== provision cluster root + this node's intermediate"
"$DATA/sandboxd" ca init -out "$DATA/ca" -cn "e2e cluster root" >/dev/null
"$DATA/sandboxd" ca issue-intermediate -root-cert "$DATA/ca/root.crt" \
  -root-key "$DATA/ca/root.key" -node e2e-node -out "$DATA/node" >/dev/null

echo "== event stream origin on 127.0.0.1:$SSE_PORT (one event, then holds)"
python3 -c '
import http.server, sys, time
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: one\n\n")
        self.wfile.flush()
        time.sleep(30)
    def log_message(self, *a):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
' "$SSE_PORT" &
SSE_PID=$!

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "audit_log": true,
  "secrets": [{"name": "probe", "header": "X-Egress-Token", "value_env": "EGRESS_PROBE_TOKEN"}],
  "egress_ca": {"root_cert": "$DATA/ca/root.crt", "intermediate_cert": "$DATA/node/e2e-node.crt", "intermediate_key": "$DATA/node/e2e-node.key"},
  "egress_internal_allow": ["127.0.0.1/32:$SSE_PORT"],
  "pools": [
    {"template": "$TEMPLATE", "net": "none", "size": "small", "warm": 2,
     "egress": {"allow": [{"host": "$ECHO", "secret": "probe", "intercept": true}, {"host": "127.0.0.1"}]}},
    {"template": "$TEMPLATE", "net": "none", "size": "medium", "warm": 2,
     "egress": {"allow": [{"host": "*", "intercept": "inject"}]}}
  ]
}
EOF

start() {
  "$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
  DAEMON_PID=$!
  for _ in $(seq 1 40); do curl -sf "http://$ADDR/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
}

echo "== start sandboxd (intercept pool)"
start
for i in $(seq 1 300); do
  curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" 2>/dev/null |
    jq -e 'all(.pools[]; .warm >= 1)' >/dev/null 2>&1 && break
  [[ $i == 300 ]] && { echo "pool never became warm"; exit 1; }
  sleep 1
done

echo "== credsmoke"
"$DATA/credsmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -echo "$ECHO" -secret "$PROBE" \
  -sse "http://127.0.0.1:$SSE_PORT/" | tee "$DATA/smoke.log"
reattach=$(awk '/^REATTACH / {print $2}' "$DATA/smoke.log")
value=${reattach##*:}

echo "== audit: claim credentials named, values absent"
grep -q '"secret":"probe,claim:API_KEY"' "$DATA/state/audit.jsonl" || { echo "audit names no claim:API_KEY"; exit 1; }
if grep -q "$value" "$DATA/state/audit.jsonl" "$DATA/state/usage.jsonl" 2>/dev/null; then
  echo "a credential value reached the audit or usage journal"
  exit 1
fi
grep -q "$value" "$DATA/state/claims.json" || { echo "claims.json lost the credential"; exit 1; }

echo "== inject pool: conditional interception, query and body placeholders"
"$DATA/credsmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -echo "$ECHO" -other "$OTHER" \
  -inject-size medium | tee "$DATA/inject.log"
grep -q '"ev":"egress".*"secret":"claim:SERP"' "$DATA/state/usage.jsonl" || { echo "usage names no claim:SERP"; exit 1; }
vault=$(awk '/^VAULT / {sub(/^VAULT /, ""); print}' "$DATA/inject.log")
for v in "${vault%%|*}" "${vault##*|}"; do
  if grep '"op":"egress"' "$DATA/state/audit.jsonl" | grep -qF "$v" || grep -qF "$v" "$DATA/state/usage.jsonl"; then
    echo "an inject value reached an egress audit record or the usage journal"
    exit 1
  fi
done

echo "== restart sandboxd, recheck the surviving claim"
kill "$DAEMON_PID"
wait "$DAEMON_PID" 2>/dev/null || true
start
"$DATA/credsmoke" -addr "$ADDR" -token "$TOKEN" -echo "$ECHO" -reattach "$reattach"
echo "PASS"
