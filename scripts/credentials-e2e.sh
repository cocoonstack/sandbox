#!/usr/bin/env bash
# Bare-metal acceptance for HTTPS interception and claim credentials: the golden
# bakes the cluster root, this node's intermediate signs the leaf, direct egress
# is blocked; claims on one intercept pool each send their own env-injected header
# to an HTTPS echo, the guest never holds the value, an open event stream reaches
# the guest event by event, a credential survives a sandboxd restart; an "inject"
# pool intercepts only claims that hold a host's credential and fills query and body placeholders. Runs a dedicated sandboxd on $ADDR.
# Needs outbound to $ECHO and a $TEMPLATE whose silkd binds the egress relay; cleanup removes every sbx-* VM on the cocoon root.
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
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

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

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "audit_log": true,
  "egress_usage_events": true,
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

echo "== start sandboxd (intercept pool)"
start_daemon
wait_pools 'all(.pools[]; .warm >= 1)' 300

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
while IFS= read -r v; do
  if grep '"op":"egress"' "$DATA/state/audit.jsonl" | grep -F "$v" >/dev/null || grep -qF "$v" "$DATA/state/usage.jsonl"; then
    echo "an inject value reached an egress audit record or the usage journal"
    exit 1
  fi
done < <(sed -n 's/^VAULT //p' "$DATA/inject.log")

echo "== restart sandboxd, recheck the surviving claim"
stop_daemon
start_daemon
"$DATA/credsmoke" -addr "$ADDR" -token "$TOKEN" -echo "$ECHO" -reattach "$reattach"
echo "PASS"
