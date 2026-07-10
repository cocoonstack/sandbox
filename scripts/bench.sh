#!/usr/bin/env bash
# Claim-tier + data-plane benchmarks, emitting a dated markdown table for
# docs/benchmarks.md. Run on a node with cocoon and silkd-baked template
# images (see docs/benchmarks.md for the tier definitions and knobs).
set -euo pipefail

TEMPLATE=${TEMPLATE:-rt:24.04}
COLD_TEMPLATE=${COLD_TEMPLATE:-python:3.12}
ADDR=${ADDR:-127.0.0.1:7877}
TOKEN=${TOKEN:-bench}
# The warm burst must not outrun the pool: refill clones (~hundreds of ms)
# lose the race against back-to-back claims, so iterations beyond the warm
# depth would measure the clone tier instead.
WARM=${WARM:-6}
WARM_N=${WARM_N:-6}
CLONE_N=${CLONE_N:-10}
COLD_N=${COLD_N:-3}
RPC_N=${RPC_N:-200}
PULL_MB=${PULL_MB:-128}
PULL_N=${PULL_N:-3}
REPO=$(cd "$(dirname "$0")/.." && pwd)

DATA=$(mktemp -d /tmp/sandboxd-bench.XXXXXX)
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
      cocoon vm stop --force "$vm" >/dev/null 2>&1 || true
      cocoon vm rm --force "$vm" >/dev/null 2>&1 || true
    done || true
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT

api() { curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/$1"; }

# claim_ms <demo args...> — runs the demo and prints one claim latency (ms)
# per line.
claim_ms() {
  "$DATA/demo" -addr "$ADDR" -token "$TOKEN" "$@" |
    sed -n 's/.*claim=\([0-9.]*\)ms.*/\1/p'
}

# stats — reads one value per line, prints "p50 | p90 | max | n" cells.
stats() {
  sort -n | awk '{v[NR]=$1} END {
    if (NR == 0) { printf "- | - | - | 0"; exit }
    printf "%.1f ms | %.1f ms | %.1f ms | %d",
      v[int(0.5*(NR-1))+1], v[int(0.9*(NR-1))+1], v[NR], NR
  }'
}

echo "== build"
if [[ -n ${SANDBOXD_BIN:-} && -n ${DEMO_BIN:-} ]]; then
  cp "$SANDBOXD_BIN" "$DATA/sandboxd" && cp "$DEMO_BIN" "$DATA/demo"
  cp "$RPCBENCH_BIN" "$DATA/rpcbench" && cp "$PULLBENCH_BIN" "$DATA/pullbench"
else
  (cd "$REPO/sandboxd" && GOWORK=off go build -o "$DATA/sandboxd" .)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/demo" ./cmd/demo)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/rpcbench" ./cmd/rpcbench)
  (cd "$REPO/e2e" && GOWORK=off go build -o "$DATA/pullbench" ./cmd/pullbench)
fi

echo "== start (warm pool small×$WARM + golden-only medium, template $TEMPLATE)"
cat >"$DATA/config.json" <<EOF
{
  "listen": "$ADDR",
  "data_dir": "$DATA/state",
  "api_token": "$TOKEN",
  "pools": [
    {"template": "$TEMPLATE", "net": "none", "size": "small", "warm": $WARM},
    {"template": "$TEMPLATE", "net": "none", "size": "medium", "warm": 0}
  ]
}
EOF
"$DATA/sandboxd" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 20); do
  curl -sf "http://$ADDR/healthz" >/dev/null && break
  sleep 0.5
done

echo "== wait for goldens + warm pool"
for i in $(seq 1 180); do
  if api info | jq -e \
    '(.pools | length) == 2 and all(.pools[]; .golden and .warm >= .target)' >/dev/null 2>&1; then
    break
  fi
  [[ $i == 180 ]] && { echo "pools never became ready"; api info | jq . || true; exit 1; }
  sleep 1
done

echo "== warm tier ($WARM_N claims, burst within the warm depth)"
warm_row=$(claim_ms -template "$TEMPLATE" -size small -n "$WARM_N" | stats)

echo "== clone tier ($CLONE_N claims from the golden-only pool)"
clone_row=$(claim_ms -template "$TEMPLATE" -size medium -n "$CLONE_N" | stats)

cold_row="- | - | - | 0"
if [[ $COLD_TEMPLATE == "$TEMPLATE" ]]; then
  # A pooled COLD_TEMPLATE would measure warm/clone claims mislabeled as cold,
  # and its pool drain + refill churn would contaminate the RTT window below.
  echo "== cold tier SKIPPED: COLD_TEMPLATE == TEMPLATE (pooled, not a cold boot)"
  COLD_TEMPLATE=""
fi
if [[ -n $COLD_TEMPLATE ]]; then
  echo "== cold tier ($COLD_N claims of unpooled $COLD_TEMPLATE)"
  cold_row=$(claim_ms -template "$COLD_TEMPLATE" -n "$COLD_N" | stats)
fi

echo "== data plane: exec RTT (rpcbench n=$RPC_N) + fs_pull throughput (${PULL_MB}MiB ×$PULL_N)"
rpc_line=$("$DATA/rpcbench" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -n "$RPC_N" |
  sed -n 's/.*dial-per-RPC[^n]*\(n=.*\)/\1/p')
pull_best=$("$DATA/pullbench" -addr "$ADDR" -token "$TOKEN" -template "$TEMPLATE" -size "$PULL_MB" -n "$PULL_N" |
  sed -n 's/.* \([0-9.]*\) MiB\/s/\1/p' | sort -n | tail -1)

virt=$(systemd-detect-virt 2>/dev/null || true); virt=${virt:-unknown}
[[ $virt == none ]] && envlabel="bare metal" || envlabel="nested ($virt)"
cpu=$(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)
digest=$(cocoon image list 2>/dev/null | awk -v t="$TEMPLATE" '$2 ~ t {print $3; exit}')

echo
echo "== results (paste into docs/benchmarks.md)"
cat <<EOF

### $(date -u +%F) — $envlabel

| environment | |
|---|---|
| host | $envlabel, $cpu, $(nproc) cores, $(awk '/MemTotal/{printf "%.0f GiB", $2/1048576}' /proc/meminfo) |
| kernel | $(uname -r) |
| cpufreq | $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)/$(cat /sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference 2>/dev/null || echo -) |
| cocoon | $(cocoon version 2>/dev/null | awk '/Version/{print $2; exit}') |
| template | $TEMPLATE @ ${digest:-unknown} |

| claim tier | p50 | p90 | max | n |
|---|---|---|---|---|
| warm pool hit | $warm_row |
| clone from golden | $clone_row |
| cold boot (unpooled ${COLD_TEMPLATE:-—}) | $cold_row |

| data plane | measured |
|---|---|
| exec RTT (dial per RPC) | $rpc_line |
| fs_pull throughput (${PULL_MB} MiB) | ${pull_best:-?} MiB/s best of $PULL_N |
EOF
echo
echo "BENCH DONE"
