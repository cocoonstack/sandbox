#!/usr/bin/env bash
# Bare-metal proof for sandboxd's guest-port relay, the path an edge proxy takes
# into a sandbox with no NIC: GET /v1/sandboxes/{id}/ports/{port}.
# Run as root on a node with cocoon; K names a kit holding bin/sandboxd,
# bin/portsmoke, bin/guestserver, bin/jq and a cocoon on PATH; cleanup removes every sbx-* VM on the cocoon root.
set -uo pipefail
K=${K:?kit dir}
ADDR=${ADDR:-127.0.0.1:7990}
TOKEN=${TOKEN:-portrelay}
TEMPLATE=${TEMPLATE:-rt:24.04}
WARM=${WARM:-2}
export PATH=$K/bin:$PATH
DATA=$(mktemp -d /tmp/port-e2e.XXXXXX)
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "pools": [{"template": "$TEMPLATE", "net": "none", "size": "small", "warm": $WARM}]
}
EOF

echo "== start sandboxd $("$K/bin/sandboxd" -version 2>/dev/null)"
start_daemon "$K/bin/sandboxd" || exit 1

echo "== wait for golden + warm"
wait_pools '(.pools|length)>0 and all(.pools[]; .golden and .warm >= .target)' || exit 1
api info | jq -c '.pools'

echo "== portsmoke"
"$K/bin/portsmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -listener "$K/bin/guestserver"
