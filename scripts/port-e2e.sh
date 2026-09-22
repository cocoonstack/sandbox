#!/usr/bin/env bash
# Bare-metal proof for sandboxd's guest-port relay, the path an edge proxy takes
# into a sandbox with no NIC: GET /v1/sandboxes/{id}/ports/{port}.
# Run as root on a node with cocoon; K names a kit holding bin/sandboxd,
# bin/portsmoke, bin/guestserver, bin/jq and a cocoon on PATH.
set -uo pipefail
K=${K:?kit dir}
ADDR=${ADDR:-127.0.0.1:7990}
TOKEN=${TOKEN:-portrelay}
TEMPLATE=${TEMPLATE:-rt:24.04}
WARM=${WARM:-2}
export PATH=$K/bin:$PATH
DATA=$(mktemp -d /tmp/port-e2e.XXXXXX)
DAEMON_PID=""

cleanup() {
  status=$?
  echo "== daemon log tail"
  tail -30 "$DATA/daemon.log" 2>/dev/null
  [[ -n $DAEMON_PID ]] && kill "$DAEMON_PID" 2>/dev/null
  wait 2>/dev/null
  cocoon vm list --format json 2>/dev/null |
    jq -r '.[] | select(.config.name | startswith("sbx-")) | .config.name' |
    while read -r vm; do
      cocoon vm stop --force "$vm" >/dev/null 2>&1
      cocoon vm rm --force "$vm" >/dev/null 2>&1
    done
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "pools": [{"template": "$TEMPLATE", "net": "none", "size": "small", "warm": $WARM}]
}
EOF

echo "== start sandboxd $("$K/bin/sandboxd" -version 2>/dev/null)"
"$K/bin/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 40); do curl -sf "http://$ADDR/healthz" >/dev/null && break; sleep 0.5; done
curl -sf "http://$ADDR/healthz" >/dev/null || { echo "daemon never came up"; exit 1; }

echo "== wait for golden + warm"
for i in $(seq 1 180); do
  curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" |
    jq -e '(.pools|length)>0 and all(.pools[]; .golden and .warm >= .target)' >/dev/null 2>&1 && break
  [[ $i == 180 ]] && { echo "pools never became ready"; curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" | jq .; exit 1; }
  sleep 1
done
curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" | jq -c '.pools'

echo "== portsmoke"
"$K/bin/portsmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -listener "$K/bin/guestserver"
