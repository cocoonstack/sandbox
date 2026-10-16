#!/usr/bin/env python3
"""Gate real attach completion, cancel a caller, or restart an isolated daemon."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

root = Path(os.environ["VOLUME_SMOKE_DIR"])
mode = sys.argv[1]
stage = os.environ["VOLUME_HOOK_STAGE"]
sandbox_id = os.environ["SANDBOX_ID"]
config = json.loads((root / "config.json").read_text())
address = "http://" + config["listen"]


def view():
    request = urllib.request.Request(
        f"{address}/v1/sandboxes/{sandbox_id}/volumes",
        headers={"Authorization": "Bearer " + config["api_token"]},
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


def restart():
    old = int((root / "daemon.pid").read_text())
    os.kill(old, signal.SIGKILL)
    (root / "gate-attach").unlink(missing_ok=True)
    with (root / "candidate-daemon.log").open("a") as log:
        process = subprocess.Popen(
            [str(root / "sandboxd"), "-config", str(root / "config.json")],
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
    (root / "daemon.pid").write_text(str(process.pid))
    deadline = time.monotonic() + 30
    while True:
        try:
            with urllib.request.urlopen(address + "/healthz", timeout=1):
                break
        except OSError:
            if time.monotonic() > deadline:
                raise RuntimeError("isolated daemon did not restart") from None
            time.sleep(0.05)
    print(f"HOOK daemon_restart old={old} new={process.pid}", flush=True)


def wait_for(marker):
    deadline = time.monotonic() + 10
    while not marker.exists():
        if time.monotonic() > deadline:
            raise RuntimeError(f"runtime never reached {marker.name}")
        time.sleep(0.01)
    print("HOOK runtime_event=" + marker.read_text(), flush=True)


def runtime_view():
    data = subprocess.check_output(
        [os.environ["COCOON_REAL_BIN"], "vm", "list", "--format", "json"]
    )
    running = []
    for vm in json.loads(data):
        if not vm["config"]["name"].startswith("sbx-"):
            continue
        pid = vm.get("pid", 0)
        running.append(
            {
                "id": vm["id"],
                "name": vm["config"]["name"],
                "pid": pid,
                "pid_exists": pid > 0 and Path(f"/proc/{pid}").exists(),
                "disks": vm.get("storage_configs"),
            }
        )
    print("HOOK real_vm=" + json.dumps(running), flush=True)


if stage == "arm":
    marker = "remove-failed" if mode == "retire" else "gate-entered"
    (root / marker).unlink(missing_ok=True)
    (root / ("fail-remove" if mode == "retire" else "gate-attach")).touch()
elif stage == "wait":
    wait_for(root / ("remove-failed" if mode == "retire" else "gate-entered"))
    runtime_view()
    if mode == "retire":
        restart()
        runtime_view()
    else:
        print("HOOK gated_view=" + json.dumps(view()), flush=True)
elif stage == "interrupt":
    if mode == "restart":
        restart()
    elif mode == "cancel":
        time.sleep(0.5)
        print("HOOK canceled_view=" + json.dumps(view()), flush=True)
        (root / "gate-attach").unlink(missing_ok=True)
    elif mode == "retire":
        (root / "fail-remove").unlink()
    else:
        raise RuntimeError("unknown hook mode " + mode)
else:
    raise RuntimeError("unknown hook stage " + stage)
