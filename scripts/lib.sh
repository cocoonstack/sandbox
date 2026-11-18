#!/usr/bin/env bash
# Sourced by the e2e and bench scripts after they set DATA, ADDR and TOKEN; the EXIT cleanup removes every sbx-* VM on the cocoon root.
DAEMON_PID=""

api() { curl -sf -H "Authorization: Bearer $TOKEN" "http://$ADDR/v1/$1"; }

start_daemon() {
  "${1:-$DATA/sandboxd}" -config "$DATA/config.json" >>"$DATA/daemon.log" 2>&1 &
  DAEMON_PID=$!
  for _ in $(seq 1 40); do
    curl -sf "http://$ADDR/healthz" >/dev/null && return 0
    sleep 0.5
  done
  echo "daemon never came up"
  return 1
}

stop_daemon() {
  kill "$DAEMON_PID" 2>/dev/null || true
  wait "$DAEMON_PID" 2>/dev/null || true
}

wait_pools() {
  for _ in $(seq 1 "${2:-180}"); do
    api info | jq -e "$1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "pools never became ready"
  api info | jq . || true
  return 1
}

cleanup() {
  status=$?
  if [[ $status -ne 0 && -f "$DATA/daemon.log" ]]; then
    echo "== daemon log tail"
    tail -30 "$DATA/daemon.log"
  fi
  # shellcheck disable=SC2046
  kill $(jobs -p) 2>/dev/null || true
  wait 2>/dev/null || true
  cocoon vm list --format json 2>/dev/null |
    jq -r '.[] | select(.config.name | startswith("sbx-")) | .config.name' |
    while read -r vm; do
      # rm's stop-before-delete waits a 30s graceful window these guests never answer.
      cocoon vm stop --force "$vm" >/dev/null 2>&1 || true
      cocoon vm rm --force "$vm" >/dev/null 2>&1 || true
    done || true
  if declare -F cleanup_extra >/dev/null; then
    cleanup_extra
  fi
  rm -rf "$DATA"
  exit "$status"
}
trap cleanup EXIT
