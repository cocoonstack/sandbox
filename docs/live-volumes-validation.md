# Local live volume validation

This report records local validation for PR #339 on 2026-10-16. The baseline
is `645e49756492e7d8e9337377a958185e615804fc`. The initial full software
run used `ceda20dd4ad944e354e6f300b72b610b3d2422d6`. Independent
review then found that invalid requests overwrote convergence diagnostics.
Regressions reproduced that defect before its fix in
`cc444095b3ba000de9ac11d7ef0674e386153804`. The complete real hardware smoke
was rerun at that revision, followed by a real invalid-request probe. The final
application candidate is `da56205bb31b489c15af3ea670e1cbe87905bf01`, whose only
changes after that hardware run rename two test-local error variables. The
affected sandboxd and e2e race, Linux and Darwin lint, and format checks passed
there. Later documentation does not change the tested application code.

The [hardware program](../e2e/cmd/volumesmoke/README.md) extends the existing
`volumesmoke` command. Its caller sequence matches the
[replacement tutorial](live-volumes.md), using the Go SDK. The literal Python
tutorial was not run against the VMM. Python SDK behavior is covered by its
tests and the Caddy transport integration.

## Software checks

All 29 distinct checks passed after the naming correction. The
[check manifest](test-results/pr339/checks.json) and
[complete output](test-results/pr339/checks.log) retain the failed lint attempt
and the successful reruns, with exact source revisions. The later regressions
also needed test-local shadow-name corrections. Failed suite attempts remain
in the evidence. The [red/green output](test-results/pr339/diagnostic-regression.log)
records the diagnostic defect before and after its fix.

| Check | Result |
| --- | --- |
| `go test -race -count=1 ./...` in all five Go modules | PASS |
| Pinned golangci-lint v2.13.2 on Linux and Darwin in all five modules | PASS |
| Formatter diff in all five modules | PASS |
| Ruff format and lint | PASS |
| Strict Python SDK mypy | PASS |
| Python SDK, OpenAI adapter, and LangChain adapter | 247, 10, and 15 tests passed |
| PostgreSQL tenant and pool-set stores | PASS, no skipped PostgreSQL tests |
| PostgreSQL multi-node pool integration | PASS, no skipped PostgreSQL tests |
| Caddy v2.11.4 TLS integration through Go and Python | PASS |

The toolchain was Go 1.27.1, as pinned by the repository. Permission tests ran
with `CAP_DAC_OVERRIDE` and `CAP_DAC_READ_SEARCH` removed because the local user
is root. Each lint run used a new cache. The stock pinned analyzer panicked
with a shared cache and passed on the same source with a fresh cache. No
analyzer was disabled and no lint suppression was added.

The controlled tests include tenant API-token authorization and sandbox-token
denial, held-volume repair after tenant or ACL removal, no device calls for
settled entries, release before reservation, cancellation and late completion,
re-add after a failed detach, uncertain journal writes, startup adoption,
retirement deletion failure, unknown-orphan admission fencing, and capture
rejection before trim. These use fake engines or controlled filesystem faults.
They are separate from the VMM results below.

## Real Cloud Hypervisor results

The node used the official Cocoon v0.7.2 release, Git
`b3b40f3bdedc384c482eb81dcdc60a774dd8d560`, with Cloud Hypervisor v54.0.0.
The Cocoon archive matched its published checksum. The guest used Linux
6.18.37 and the current statically built sandbox-init.

The daemon, data directory, images, runtime directories, network scope, and
cgroup parent were isolated. No existing local service was removed or
reconfigured. The test used two disposable 32 MiB ext4 images. Both catalog
entries permit writes for the final reuse probe. Ordinary replacement mounts
request read-only mode.

| Caller-visible scenario | Result |
| --- | --- |
| Two sandboxes read the same dataset, and writes fail with EROFS | PASS |
| Replace v1 with v2 at the same guest path, repeat the target, then clear it | PASS |
| Preserve sandbox, session, boot ID, PID, process start time, and session environment | PASS |
| Busy cwd, bind mount, and foreign mount return 409 with a guest path | PASS |
| Close the reference and retry the same target | PASS |
| Repeat a settled target while it has a bind mount | PASS, zero attach or detach calls |
| Run exec, file read/write, and HTTP port requests while a real attach result is blocked | PASS |
| Kill sandboxd after real attach succeeds but before its result returns | PASS, restart adopts the original VMM and retry converges |
| Cancel the caller at that same boundary | PASS, desired intent survives and retry converges |
| Fail VM removal, restart sandboxd, and attempt a conflicting writer | PASS, writer receives 409 before and after restart |
| Permit real VM removal, then mount and write through a new writer | PASS |
| Reject duplicate volume names with 400 and preserve the settled GET view | PASS, no device calls or new convergence error |
| Remove all resources owned by the test | PASS, zero claims, retirements, VMs, sockets, and owned TAPs |

The [raw smoke output](test-results/pr339/hardware.log) includes identities,
failure views, and recovery results. The
[runtime command log](test-results/pr339/hardware-commands.jsonl) contains the
actual command attempts. The observer forwards ordinary commands to the
official Cocoon binary. It blocks only after a successful real attach, or
injects the declared exit 73 before VM removal. It does not emulate version
support, attach idempotence, or successful deletion.

The original VMM PID `1082008` survived the daemon restart. Retiring VMM PID
`1084550` survived the failed removal and restart until real cleanup succeeded.
The [cleanup snapshot](test-results/pr339/hardware-cleanup.json) confirms both
PIDs and the separate invalid-request probe's VM are gone. The
[invalid-request probe](test-results/pr339/hardware-invalid-put.json) records
HTTP 400 with the duplicate-name reason, unchanged GET and file contents, and
zero disk calls. The [final journal](test-results/pr339/hardware-journal.json)
has empty live and retirement maps. The
[retained-target window](test-results/pr339/retained-disk-calls.json) contains
zero device calls. The read-only v2 backing checksum did not change. The v1
image received the intentional writable reuse probe.

The [baseline hardware run](test-results/pr339/baseline.log) passed the shared
reads and EROFS checks, then returned HTTP 500 for the busy cwd case where the
new candidate returns HTTP 409. This establishes a behavioral difference on
the actual device path.

## Reproduce the checks

The [CI workflows](../.github/workflows/sandboxd.yml) provide the Go,
PostgreSQL, and Caddy commands. The
[Python workflow](../.github/workflows/python.yml) pins its tools and SDK test
commands. Local Go modules are `protocol/wire`, `sandboxd`, `sdk/go`, `e2e`,
and `mcp`. `GOWORK=off` keeps each module independent. The hardware command and
fault-hook setup are in the [hardware instructions](../e2e/cmd/volumesmoke/README.md).
The [local runtime preparation](test-results/pr339/hardware-prep.md) records
the guest image compatibility fixes and exact input checksums.

## Coverage limits and operational requirements

Missing guest eject ACK was not established. An external experiment paused
Cloud Hypervisor, but official Cocoon resumed the orphaned pause before
detach. The guest could therefore acknowledge removal. The
[experiment result](test-results/pr339/pending-eject.json) is inconclusive for
timeout and retry with a missing ACK.

Separate hardware crashes after guest umount, after host detach, and at every
journal or fsync boundary were not performed. The hardware daemon used
`sync_claims: false`. The demonstrated recovery is a daemon-process restart,
not physical host power loss. Controlled unit tests cover additional journal
cuts without claiming hardware proof.

Catalog names must remain bound to immutable images while held. The journal
cannot detect an operator changing a backing path after restart. Journal
format v2 includes durable retirement ownership. An older daemon rejects that
format, so downgrade requires draining claims and retirement work first. A
failed unknown-orphan cleanup fences new admissions and held repair until
the VM is confirmed gone. A sandbox whose VMM dies while holding volumes is
release-only. These constraints are documented in the deployment and API
guides.

The [decision trail](test-results/pr339/decisions.tsv) records implementation
choices and failed attempts. Its relative evidence paths refer to the retained
local `.audit/pr339-implementation` directory. The artifacts linked in this
report provide the portable test evidence. Local paths in raw logs identify
the machine used for the run and are not installation requirements.
The build metadata has trailing whitespace removed. The recorded test-only
diff omits context lines. Test observations retain their original contents.

The [independent follow-up review](test-results/pr339/review.md) verifies the
diagnostic fix, source revisions, test evidence, and decision trail. It found
no remaining actionable issue. The hardware coverage limits above remain.
