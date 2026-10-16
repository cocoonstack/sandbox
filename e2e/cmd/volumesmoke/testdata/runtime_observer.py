#!/usr/bin/env python3
"""Forward Cocoon commands and expose explicit hardware fault injection gates."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

root = Path(os.environ["VOLUME_SMOKE_DIR"])
args = sys.argv[1:]
event = {"time": time.time(), "pid": os.getpid(), "args": args}
with (root / "commands.jsonl").open("a") as log:
    log.write(json.dumps(event) + "\n")
if args[:2] == ["vm", "rm"] and (root / "fail-remove").exists():
    (root / "remove-failed").write_text(json.dumps(event))
    print("volumesmoke injected VM removal failure", file=sys.stderr)
    sys.exit(73)
result = subprocess.run([os.environ["COCOON_REAL_BIN"], *args], check=False)
if args[:3] == ["vm", "disk", "attach"] and result.returncode == 0:
    if (root / "gate-attach").exists():
        (root / "gate-entered").write_text(json.dumps(event))
        while (root / "gate-attach").exists():
            time.sleep(0.02)
sys.exit(result.returncode)
