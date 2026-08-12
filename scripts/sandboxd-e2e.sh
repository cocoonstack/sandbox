#!/usr/bin/env bash
# Bare-metal e2e for sandboxd v0: warm pool, claim tiers, reap, reconcile.
# Run on a node with cocoon and a silkd-baked template image (see os-image/).
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
ADDR=${ADDR:-127.0.0.1:7777}
TOKEN=${TOKEN:-e2e}
WARM=${WARM:-2}
REPO=$(cd "$(dirname "$0")/.." && pwd)
# VOLUME_IMAGE opts into the CH volume proof; its filesystem must contain the non-empty VOLUME_PROBE file.
VOLUME_IMAGE=${VOLUME_IMAGE:-}
VOLUME_NAME=${VOLUME_NAME:-e2e-data}
VOLUME_PROBE=${VOLUME_PROBE:-volume-e2e.txt}
VOLUME_DIRECTIO=${VOLUME_DIRECTIO:-off}
# VOLUME_RW_IMAGE adds the writable leg; its own image is mutated by design, so it must not be VOLUME_IMAGE.
VOLUME_RW_IMAGE=${VOLUME_RW_IMAGE:-}
VOLUME_RW_NAME=${VOLUME_RW_NAME:-e2e-scratch}

VOLUME_CHECKSUM=""
if [[ -n $VOLUME_IMAGE ]]; then
  [[ $VOLUME_IMAGE == /* ]] || { echo "VOLUME_IMAGE must be absolute"; exit 1; }
  [[ -f $VOLUME_IMAGE && -r $VOLUME_IMAGE ]] || { echo "VOLUME_IMAGE must be a readable file"; exit 1; }
  [[ $VOLUME_NAME =~ ^[a-z][a-z0-9_-]{0,19}$ && $VOLUME_NAME != cocoon-* ]] || {
    echo "VOLUME_NAME must match ^[a-z][a-z0-9_-]{0,19}$ and not start with cocoon-"
    exit 1
  }
  [[ $VOLUME_DIRECTIO == on || $VOLUME_DIRECTIO == off || $VOLUME_DIRECTIO == auto ]] || {
    echo "VOLUME_DIRECTIO must be on, off, or auto"
    exit 1
  }
  (( WARM >= 2 )) || { echo "volume e2e needs WARM>=2 for the concurrent warm-volume assertion"; exit 1; }
  VOLUME_CHECKSUM=$(sha256sum -- "$VOLUME_IMAGE" | awk '{print $1}')
fi

VOLUME_RW_CHECKSUM=""
if [[ -n $VOLUME_RW_IMAGE ]]; then
  [[ -n $VOLUME_IMAGE ]] || { echo "VOLUME_RW_IMAGE needs VOLUME_IMAGE: the writable leg extends the volume proof"; exit 1; }
  [[ $VOLUME_RW_IMAGE == /* ]] || { echo "VOLUME_RW_IMAGE must be absolute"; exit 1; }
  [[ -f $VOLUME_RW_IMAGE && -w $VOLUME_RW_IMAGE ]] || { echo "VOLUME_RW_IMAGE must be a writable file"; exit 1; }
  [[ $VOLUME_RW_IMAGE != "$VOLUME_IMAGE" ]] || { echo "VOLUME_RW_IMAGE must differ from VOLUME_IMAGE: the writable leg mutates its image"; exit 1; }
  [[ $VOLUME_RW_NAME =~ ^[a-z][a-z0-9_-]{0,19}$ && $VOLUME_RW_NAME != cocoon-* ]] || {
    echo "VOLUME_RW_NAME must match ^[a-z][a-z0-9_-]{0,19}$ and not start with cocoon-"
    exit 1
  }
  [[ $VOLUME_RW_NAME != "$VOLUME_NAME" ]] || { echo "VOLUME_RW_NAME must differ from VOLUME_NAME"; exit 1; }
  VOLUME_RW_CHECKSUM=$(sha256sum -- "$VOLUME_RW_IMAGE" | awk '{print $1}')
fi

DATA=$(mktemp -d /tmp/sandboxd-e2e.XXXXXX)
DAEMON_PID=""

cleanup() {
  status=$?
  if [[ $status -ne 0 && -f "$DATA/daemon.log" ]]; then
    echo "== daemon log tail"
    tail -20 "$DATA/daemon.log"
  fi
  [[ -n $DAEMON_PID ]] && kill "$DAEMON_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  cocoon vm list --format json 2>/dev/null |
    jq -r '.[] | select(.config.name | startswith("sbx-")) | .config.name' |
    while read -r vm; do
      # Force-stop first: rm's stop-before-delete waits a 30s graceful
      # window these guests never answer.
      cocoon vm stop --force "$vm" >/dev/null 2>&1 || true
      cocoon vm rm --force "$vm" >/dev/null 2>&1 || true
    done || true
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

api() { curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/$1"; }
metrics() { curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/metrics"; }
warm_claims() {
  metrics | awk '$1 == "sandboxd_claims_total{tier=\"warm\"}" {print $2}'
}

start_daemon() {
  "$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
  DAEMON_PID=$!
  for _ in $(seq 1 20); do
    curl -sf "http://$ADDR/healthz" >/dev/null && return 0
    sleep 0.5
  done
  echo "daemon never came up"
  return 1
}

echo "== build"
# Prebuilt binaries let the script run on nodes without a Go toolchain.
if [[ -n ${SANDBOXD_BIN:-} && -n ${DEMO_BIN:-} && -n ${SMOKE_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$DEMO_BIN" "$DATA/demo" && cp "$SMOKE_BIN" "$DATA/smoke"
  if [[ -n $VOLUME_IMAGE ]]; then
    [[ -n ${VOLUME_SMOKE_BIN:-} ]] || { echo "VOLUME_SMOKE_BIN is required with prebuilt binaries"; exit 1; }
    cp "$VOLUME_SMOKE_BIN" "$DATA/volumesmoke"
  fi
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/demo" ./cmd/demo)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/smoke" ./cmd/smoke)
  if [[ -n $VOLUME_IMAGE ]]; then
    (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/volumesmoke" ./cmd/volumesmoke)
  fi
fi

# BRIDGE (optional) attaches the node to an egress bridge, adds a small
# egress pool, and turns on the smoke's egress-lane check; lane detection
# needs a NIC, not connectivity, so a plain `ip link add ... type bridge`
# with no uplink is enough.
BRIDGE_LINE=""
EGRESS_POOL=""
if [[ -n ${BRIDGE:-} ]]; then
  BRIDGE_LINE="\"bridges\": [\"$BRIDGE\"],"
  EGRESS_POOL=", {\"template\": \"$TEMPLATE\", \"net\": \"egress\", \"size\": \"small\", \"warm\": 1}"
fi

# S3_ENDPOINT (optional, with S3_BUCKET and AWS creds in the env) switches
# the checkpoint store to the s3 backend — the checkpoint/promote steps
# then exercise object storage end to end.
STORE_LINE=""
if [[ -n ${S3_ENDPOINT:-} ]]; then
  STORE_LINE="\"checkpoint_store\": {\"kind\": \"s3\", \"s3\": {\"bucket\": \"${S3_BUCKET:-sbx-checkpoints}\", \"prefix\": \"e2e/\", \"endpoint\": \"$S3_ENDPOINT\", \"region\": \"us-east-1\", \"force_path_style\": true}},"
fi

VOLUME_LINE=""
if [[ -n $VOLUME_IMAGE ]]; then
  VOLUME_CATALOG=$(jq -cn --arg name "$VOLUME_NAME" --arg path "$VOLUME_IMAGE" --arg directio "$VOLUME_DIRECTIO" \
    '[{name: $name, path: $path, directio: $directio}]')
  if [[ -n $VOLUME_RW_IMAGE ]]; then
    VOLUME_CATALOG=$(jq -cn --argjson catalog "$VOLUME_CATALOG" --arg name "$VOLUME_RW_NAME" --arg path "$VOLUME_RW_IMAGE" \
      '$catalog + [{name: $name, path: $path, writable: true}]')
  fi
  VOLUME_LINE="\"volumes\": $VOLUME_CATALOG,"
fi

echo "== start (pool: $TEMPLATE none/small warm=$WARM${BRIDGE:+, egress via $BRIDGE}${S3_ENDPOINT:+, s3 store at $S3_ENDPOINT}${VOLUME_IMAGE:+, volume $VOLUME_NAME}${VOLUME_RW_IMAGE:+, writable volume $VOLUME_RW_NAME})"
cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  $BRIDGE_LINE
  $STORE_LINE
  $VOLUME_LINE
  "pools": [{"template": "$TEMPLATE", "net": "none", "size": "small", "warm": $WARM}$EGRESS_POOL]
}
EOF
start_daemon

echo "== wait for goldens + warm pools (cold boot + snapshot export + refill)"
for i in $(seq 1 120); do
  if api info | jq -e \
    '(.pools | length) > 0 and all(.pools[]; .golden and .warm >= .target)' >/dev/null 2>&1; then
    break
  fi
  [[ $i == 120 ]] && { echo "pools never became ready"; api info | jq . || true; exit 1; }
  sleep 1
done
api info | jq .

echo "== claim/exec/release x3 (expect: warm-hit ms-scale, then clone-tier)"
if [[ -n $VOLUME_IMAGE ]]; then
  warm_before=$(warm_claims)
  "$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 1
  warm_after=$(warm_claims)
  [[ $warm_after -eq $((warm_before + 1)) ]] || {
    echo "ordinary claim missed the warm tier: before=$warm_before after=$warm_after"
    exit 1
  }
  echo "ordinary warm claim counter: $warm_before -> $warm_after"
  "$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 2
else
  "$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 3
fi

echo "== v2 smoke: files/session/find/replace/watch/git/pty through the relay"
# LSP_TEMPLATE (optional, a python-flavor ref) adds the LSP broker step.
"$DATA/smoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" ${BRIDGE:+-egress} ${LSP_TEMPLATE:+-lsp-template "$LSP_TEMPLATE"}

if [[ -n $VOLUME_IMAGE ]]; then
  echo "== volumes: two warm read-only claims, shared bytes, and EROFS${VOLUME_RW_IMAGE:+, then the writable publish round}"
  for i in $(seq 1 30); do
    if api info | jq -e 'all(.pools[]; .warm >= .target)' >/dev/null 2>&1; then
      break
    fi
    [[ $i == 30 ]] && { echo "pool did not refill before volume smoke"; api info | jq . || true; exit 1; }
    sleep 1
  done
  warm_before=$(warm_claims)
  "$DATA/volumesmoke" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" \
    -volume "$VOLUME_NAME" -probe "$VOLUME_PROBE" ${VOLUME_RW_IMAGE:+-rw-volume "$VOLUME_RW_NAME"}
  warm_after=$(warm_claims)
  [[ $warm_after -ge $((warm_before + 2)) ]] || {
    echo "volume claims did not consume the two ready warm VMs: before=$warm_before after=$warm_after"
    exit 1
  }
  echo "volume warm claim counter: $warm_before -> $warm_after"
  after_checksum=$(sha256sum -- "$VOLUME_IMAGE" | awk '{print $1}')
  [[ $after_checksum == "$VOLUME_CHECKSUM" ]] || {
    echo "volume backing image changed: before=$VOLUME_CHECKSUM after=$after_checksum"
    exit 1
  }
  echo "volume backing checksum unchanged: $after_checksum"
  # The writable image is mutated on purpose: an unchanged digest means the
  # writer's bytes never reached the host file.
  if [[ -n $VOLUME_RW_IMAGE ]]; then
    rw_checksum=$(sha256sum -- "$VOLUME_RW_IMAGE" | awk '{print $1}')
    [[ $rw_checksum != "$VOLUME_RW_CHECKSUM" ]] || {
      echo "writable backing image unchanged after the writable leg: $rw_checksum"
      exit 1
    }
    echo "writable backing checksum advanced: $VOLUME_RW_CHECKSUM -> $rw_checksum"
  fi
fi

echo "== reap: leaked 5s-ttl claim is destroyed by the owner"
"$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 1 -ttl 5 -leak
sleep 12
claimed=$(api info | jq .claimed)
[[ $claimed == 0 ]] || { echo "leaked claim survived reap: claimed=$claimed"; exit 1; }

echo "== restart: live claim re-adopts, warm VMs of the old life are replaced"
"$DATA/demo" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n 1 -ttl 300 -leak
kill "$DAEMON_PID" && wait "$DAEMON_PID" 2>/dev/null || true
start_daemon
claimed=$(api info | jq .claimed)
[[ $claimed == 1 ]] || { echo "claim not re-adopted: claimed=$claimed"; exit 1; }

echo "PASS"
