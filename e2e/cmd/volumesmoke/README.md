# Live volume hardware validation

Run this command against a real Cloud Hypervisor node with Cocoon v0.7.2 or later. The guest needs silkd, mount, umount, awk, and ordinary coreutils. Fault hooks need Python 3 on the host. Register two catalog datasets with distinct nonempty contents at the same relative probe path. Keep their catalog backing paths unchanged throughout the run.

Build from the repository's `e2e` directory.

```sh
GOTOOLCHAIN=go1.27.1 GOWORK=off go build -o "$VOLUME_SMOKE_DIR/volumesmoke" ./cmd/volumesmoke
"$VOLUME_SMOKE_DIR/volumesmoke" -addr "$SANDBOX_ADDR" -token "$SANDBOX_API_TOKEN" \
  -template "$SANDBOX_TEMPLATE" -volume dataset-v1 -next-volume dataset-v2 -probe version
```

The command checks shared read-only mounts, EROFS, repeated updates, and removal. `-next-volume` adds a same-path replacement while preserving the sandbox ID, session ID, guest boot ID, PID, process start time, and session environment. It tests cwd, bind, and foreign-mount conflicts with HTTP 409, then retries after releasing the reference. `RETAINED begin/end` timestamps identify the repeated-target window. The runtime command log must contain zero disk attach or detach commands in that window.

The optional fault hooks operate only on an isolated daemon. Use disposable backing files for `-release-hook`, which writes `.retirement-reuse` through a later writable mount. That catalog entry must allow writable attachment. The helper never prints the API token.

1. Set `VOLUME_SMOKE_DIR` to an absolute scratch directory. Place the candidate `sandboxd` binary and its `config.json` there. The config must use a dedicated listen address and data directory. Its `cocoon_bin` must point to the absolute path of `testdata/runtime_observer.py`.
2. Set `COCOON_REAL_BIN` to the absolute path of the official Cocoon binary. Configure isolated `COCOON_ROOT_DIR`, `COCOON_RUN_DIR`, and `COCOON_LOG_DIR`, and a distinct network scope and cgroup parent. Preserve these environment variables for the daemon and smoke command.
3. Start the daemon as a child process and write its actual PID to `$VOLUME_SMOKE_DIR/daemon.pid`. The restart hook kills only this PID. The real VMM must keep running.
4. Build the existing guest listener with `CGO_ENABLED=0 GOTOOLCHAIN=go1.27.1 GOWORK=off go build -o "$VOLUME_SMOKE_DIR/guestserver" ./cmd/guestserver` from the `e2e` directory. Run the extended command with the three hooks. Set `VOLUME_HOOK` to the absolute path of `testdata/fault_hook.py`.

```sh
"$VOLUME_SMOKE_DIR/volumesmoke" -addr "$SANDBOX_ADDR" -token "$SANDBOX_API_TOKEN" \
  -template "$SANDBOX_TEMPLATE" -volume dataset-v1 -next-volume dataset-v2 -probe version \
  -listener "$VOLUME_SMOKE_DIR/guestserver" \
  -change-hook "$VOLUME_HOOK" \
  -cancel-hook "$VOLUME_HOOK" \
  -release-hook "$VOLUME_HOOK"
```

The observer executes every ordinary command through the official Cocoon binary, including `version`. An attach gate pauses only after a successful real disk attach and before returning its result to sandboxd. The restart hook kills sandboxd at that gate. The cancellation hook cancels the caller at the same gate. Both save the API view and real VM PID and disks. Independent exec, file, and HTTP port operations must complete while the gate is active. Retrying the same desired target must converge.

The retirement hook injects exit 73 before `vm rm` while its failure marker exists. It checks that a new writer is refused before and after a daemon restart. Removing the marker permits the ordinary cleanup worker to execute real Cocoon removal. Only successful admission, mounting, and writing by a later writer counts as backing reuse. Save `commands.jsonl`, `candidate-daemon.log`, smoke output, runtime versions, and the backing checksums outside the repository.

The command runs the supplied absolute script path through `python3` with a fixed `restart`, `cancel`, or `retire` mode argument. It does not interpret a shell command. The hooks use `arm`, `wait`, and `interrupt` stages supplied in `VOLUME_HOOK_STAGE`, plus the sandbox ID in `SANDBOX_ID`. A failed run can leave fault markers behind. Stop the isolated daemon before removing its `gate-attach` or `fail-remove` marker and cleaning up its VMs. Do not apply these hooks to a shared production daemon.
