#!/usr/bin/env bash
# Bare-metal proof for the e2b image flavor: envd inside a cocoon microVM,
# reached only through sandboxd's guest-port relay. The pool's warmup gates a
# warm clone on envd answering, so a claim never hands out a sandbox whose data
# plane is still starting.
# Run as root on a node with cocoon; K names a kit holding bin/sandboxd,
# bin/envdsmoke, bin/jq and a cocoon on PATH.
set -uo pipefail
K=${K:?kit dir}
ADDR=${ADDR:-127.0.0.1:7996}
TOKEN=${TOKEN:-e2benvd}
TEMPLATE=${TEMPLATE:-e2b-rt:24.04}
ENVD_VERSION=${ENVD_VERSION:-0.8.0}
export PATH=$K/bin:$PATH
DATA=$(mktemp -d /tmp/envd-e2e.XXXXXX); DAEMON_PID=""
cleanup() {
  status=$?
  echo "== daemon log tail"; tail -25 "$DATA/daemon.log" 2>/dev/null
  [[ -n $DAEMON_PID ]] && kill "$DAEMON_PID" 2>/dev/null
  wait 2>/dev/null
  cocoon vm list --format json 2>/dev/null |
    jq -r '.[] | select(.config.name | startswith("sbx-")) | .config.name' |
    while read -r vm; do
      cocoon vm stop --force "$vm" >/dev/null 2>&1
      cocoon vm rm --force "$vm" >/dev/null 2>&1
    done
  rm -rf "$DATA"; exit "$status"
}
trap cleanup EXIT
# warmup gates a warm clone on envd answering: a clone that hands out a sandbox
# whose data plane is not up yet is worse than a slower clone.
cat >"$DATA/config.json" <<EOF
{"listen":"$ADDR","data_dir":"$DATA/state","api_token":"$TOKEN",
 "pools":[{"template":"$TEMPLATE","net":"none","size":"small","warm":2,
   "warmup":["sh","-c","for i in \$(seq 1 200); do curl -sf -o /dev/null http://127.0.0.1:49983/health && exit 0; sleep 0.05; done; exit 1"]}]}
EOF
echo "== start sandboxd $("$K/bin/sandboxd" -version 2>/dev/null)"
"$K/bin/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 40); do curl -sf "http://$ADDR/healthz" >/dev/null && break; sleep 0.5; done
curl -sf "http://$ADDR/healthz" >/dev/null || { echo "daemon never came up"; exit 1; }
echo "== wait for golden + warm (warmup gates on envd /health)"
for i in $(seq 1 300); do
  curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" |
    jq -e '(.pools|length)>0 and all(.pools[]; .golden and .warm >= .target)' >/dev/null 2>&1 && break
  [[ $i == 300 ]] && { echo "pools never became ready"; curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" | jq .; exit 1; }
  sleep 1
done
curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/info" | jq -c '.pools'
echo "== envdsmoke"
"$K/bin/envdsmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -envd-version "$ENVD_VERSION" ${HOLD:+-hold "$HOLD"}
