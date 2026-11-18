#!/usr/bin/env bash
# Bare-metal acceptance for lifecycle auto-archive: a claim idles into
# hibernation, then to a store checkpoint (its local VM dropped), and the next
# access wakes it from the store with the same id and guest state intact.
# Runs a dedicated sandboxd on port 7778 (7777=e2e), but cleanup removes every
# sbx-* VM on the cocoon root, so it cannot run beside another e2e or sandboxd.
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7778}
TOKEN=${TOKEN:-e2e}
REPO=$(cd "$(dirname "$0")/.." && pwd)

DATA=$(mktemp -d /tmp/archive-e2e.XXXXXX)
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

echo "== build"
if [[ -n ${SANDBOXD_BIN:-} && -n ${LIFECYCLE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$LIFECYCLE_BIN" "$DATA/lifecycle"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/lifecycle" ./cmd/lifecycle)
fi

cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "pools": [{"template": "$TEMPLATE", "net": "none", "size": "small", "warm": 1,
             "idle_hibernate_seconds": 1, "archive_after_seconds": 2, "archive_delete_after_seconds": 3600}]
}
EOF

echo "== start sandboxd (idle=1s archive=2s delete=3600s)"
start_daemon

echo "== wait for golden + warm pool"
wait_pools '(.pools|length)>0 and all(.pools[]; .golden and .warm>=.target)'
api info | jq '{pools:(.pools|length),claimed,hibernated,archived}'

echo "== archive lifecycle: claim -> idle-hibernate -> archive -> wake"
"$DATA/lifecycle" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -net none -wait 90s

echo "== post-run node state"
api info | jq '{claimed,hibernated,archived}'
echo "PASS"
