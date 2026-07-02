#!/usr/bin/env bash
# Boot-phase timing harness for the sandbox boot chain. Runs cloud-hypervisor
# directly — use a Linux host with KVM (e.g. a cocoon node), not macOS.
#
#   make boot extract
#   KERNEL=dist/boot/vmlinuz-sandbox INITRD=dist/boot/initrd.img-sandbox \
#     LAYERS=/path/to/ubuntu.erofs ./scripts/boot-bench.sh
#
# Env knobs:
#   LAYERS         comma-separated EROFS layer files (required)
#   KERNEL INITRD  boot artifacts (required)
#   CH_BIN         cloud-hypervisor binary [cloud-hypervisor]
#   CPUS=2 MEM=512M COW_SIZE=1G RUNS=3
#   AGENT_PORT     also probe cocoon-agent readiness via the CH hybrid vsock
#                  socket (cocoon-agent default port: 1024)
#   EXTRA_CMDLINE  appended verbatim (e.g. "sandbox.debug=1")
#
# Reported phases (from sandbox-init's self-reported uptime markers, which
# stay visible at the production loglevel=3 the harness boots with):
#   kernel->init     uptime at "sandbox-init: start"
#   init->handoff    "rootfs ready" uptime minus start uptime
#   spawn->agent     wallclock from CH spawn to first successful vsock CONNECT
set -euo pipefail

: "${KERNEL:?set KERNEL=path/to/vmlinuz-sandbox}"
: "${INITRD:?set INITRD=path/to/initrd.img-sandbox}"
: "${LAYERS:?set LAYERS=layer0.erofs[,layer1.erofs,...]}"
CH_BIN=${CH_BIN:-cloud-hypervisor}
CPUS=${CPUS:-2}
MEM=${MEM:-512M}
COW_SIZE=${COW_SIZE:-1G}
RUNS=${RUNS:-3}
AGENT_PORT=${AGENT_PORT:-}
EXTRA_CMDLINE=${EXTRA_CMDLINE:-}

ts() { date +%s.%N; }

# Blocks until the pattern appears in the serial log, echoes the full line.
# Deadline is integer epoch seconds — float epoch math through awk loses
# precision (default OFMT collapses to 6 significant digits).
wait_line() { # $1=log $2=pattern $3=deadline (integer epoch seconds)
  local line
  while :; do
    line=$(grep -m1 -E "$2" "$1" 2>/dev/null || true)
    if [ -n "$line" ]; then
      echo "$line"
      return 0
    fi
    if [ "$(date +%s)" -gt "$3" ]; then
      return 1
    fi
    sleep 0.005
  done
}

probe_agent() { # $1=vsock unix socket $2=port -> echoes wallclock ts on success
  python3 - "$1" "$2" <<'PY'
import socket, sys, time
path, port = sys.argv[1], sys.argv[2]
deadline = time.time() + 30
while time.time() < deadline:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        s.connect(path)
        s.sendall(f"CONNECT {port}\n".encode())
        if s.recv(32).startswith(b"OK"):
            print(f"{time.time():.9f}")
            sys.exit(0)
    except OSError:
        time.sleep(0.005)
    finally:
        s.close()
sys.exit(1)
PY
}

run_once() {
  local n="$1" work
  work=$(mktemp -d)
  local cow="$work/cow.ext4" log="$work/serial.log" vsock="$work/vsock.sock"
  local ch= # CH pid, visible to the trap before the first spawn
  # shellcheck disable=SC2064
  trap "kill \$ch 2>/dev/null || true; rm -rf '$work'" RETURN

  truncate -s "$COW_SIZE" "$cow"
  mkfs.ext4 -F -m 0 -q -E lazy_itable_init=1,lazy_journal_init=1,discard "$cow"

  # image_type=raw is mandatory: the cocoonstack CH fork blocks sector-0
  # writes on disks whose image type was autodetected rather than declared.
  local disk_args=() ids=() i=0 f layer_files
  IFS=, read -ra layer_files <<<"$LAYERS"
  for f in "${layer_files[@]}"; do
    disk_args+=(--disk "path=$f,readonly=on,image_type=raw,serial=l$i")
    ids+=("l$i")
    i=$((i + 1))
  done
  local layers_csv
  layers_csv=$(IFS=,; echo "${ids[*]}")
  local cmdline="console=ttyS0 loglevel=3 reboot=k clocksource=kvm-clock rw cocoon.layers=$layers_csv cocoon.cow=cow $EXTRA_CMDLINE"

  local vsock_args=()
  [ -n "$AGENT_PORT" ] && vsock_args=(--vsock "cid=88,socket=$vsock")

  local t0
  t0=$(ts)
  "$CH_BIN" --cpus "boot=$CPUS" --memory "size=$MEM" \
    --kernel "$KERNEL" --initramfs "$INITRD" --cmdline "$cmdline" \
    "${disk_args[@]}" --disk "path=$cow,image_type=raw,serial=cow" \
    --serial "file=$log" --console off "${vsock_args[@]}" >"$work/ch.log" 2>&1 &
  ch=$!

  local deadline line k_init k_handoff
  deadline=$(($(date +%s) + 30))
  line=$(wait_line "$log" 'sandbox-init: start at' "$deadline") \
    || { echo "run $n: sandbox-init start marker never appeared (logs kept in $work)"; trap - RETURN; kill "$ch" 2>/dev/null || true; return 1; }
  k_init=$(echo "$line" | grep -oE 'at [0-9.]+s' | grep -oE '[0-9.]+')
  line=$(wait_line "$log" 'sandbox-init: rootfs ready' "$deadline") \
    || { echo "run $n: sandbox-init never handed off (logs kept in $work)"; trap - RETURN; kill "$ch" 2>/dev/null || true; return 1; }
  k_handoff=$(echo "$line" | grep -oE 'at [0-9.]+s' | grep -oE '[0-9.]+')

  local agent_ms="-"
  if [ -n "$AGENT_PORT" ]; then
    local t_agent
    if t_agent=$(probe_agent "$vsock" "$AGENT_PORT"); then
      agent_ms=$(awk -v a="$t0" -v b="$t_agent" 'BEGIN{printf "%.1f", (b-a)*1000}')
    fi
  fi

  awk -v n="$n" -v ki="$k_init" -v kh="$k_handoff" -v am="$agent_ms" 'BEGIN{
    printf "run %s: kernel->init %7.1f ms | init->handoff %6.1f ms | spawn->agent %s ms\n",
      n, ki*1000, (kh-ki)*1000, am
  }'
}

echo "kernel=$KERNEL initrd=$INITRD layers=$LAYERS runs=$RUNS"
# `|| echo` keeps set -e from aborting the remaining runs on one failure.
for n in $(seq 1 "$RUNS"); do
  run_once "$n" || echo "run $n: FAILED"
done
