# Deploying sandboxd

sandboxd is a single static binary per node. It drives VM lifecycle through
the cocoon CLI and needs a template image with silkd baked in.

## Prerequisites

- Linux with KVM (`/dev/kvm`)
- [cocoon](https://github.com/cocoonstack/cocoon) **v0.6.9 or newer** installed
  and working (`cocoon vm run` boots a Cloud Hypervisor VM). v0.5.2 brought
  the disk hot-attach used by read-only volumes and the parallel-clone and
  snapshot/store performance work the
  [performance](performance.md) numbers assume; v0.6.9 prints `[]` for an
  empty `snapshot list --format json`, which sandboxd's snapshot reconcile
  reads. sandboxd logs a warning at startup when the detected cocoon is below
  v0.6.9 (a dev/`master-<sha>` build is assumed current); changing volumes on
  a running claim needs v0.7.2, and a pool's `reader` needs v0.7.3
- The sandbox boot artifact installed where cocoon finds it
  (`/boot/vmlinuz-sandbox`, `/boot/initrd.img-sandbox` — from
  `ghcr.io/cocoonstack/sandbox/boot:<kernel-ver>`)
- A silkd-baked template image, e.g. `ghcr.io/cocoonstack/sandbox/base:24.04`
  (pull via cocoon, or `cocoon image import` a tar)

Prebuilt static linux/amd64 and linux/arm64 binaries (`sandboxd`,
`sandboxd.dbg`, `sandbox-mcp`, `silkd`, with `checksums.txt`) ship with every
[GitHub release](https://github.com/cocoonstack/sandbox/releases); the boot
artifact and the `base`/`rt`/`python`/`python-rt`/`node`/`node-rt` images are
multi-arch manifests (`browser`, `android`, `e2b-rt` and `e2b-ci` remain
amd64-only). Build from source with `make sandboxd` (produces
`dist/sandboxd`); either way `sandboxd -version` reports what you are running.

## Upgrading

This release does not convert VM or snapshot state from older releases. Drain old
claims and use fresh `data_dir` and `checkpoint_dir` locations when upgrading;
older checkpoints and promoted templates must not be reused.

The S3 `checkpoint_store` changed its object layout (chunk manifests instead of
whole export files). Records written by an earlier release are not readable and
there is no fallback reader: before starting the new binary, wake or release
every archived claim and delete the checkpoints, then re-promote templates, or
point `prefix` at an empty location. Objects of the old layout under a reused
prefix stay in the bucket until you delete them.

The scalar egress-attachment keys are retired: rename `"bridge": "br0"` to
`"bridges": ["br0"]` and `"network": "cni"` to `"networks": ["cni"]` before
starting the new binary — config loading rejects the old spellings loudly
rather than silently dropping the egress lane.

Dataset volumes require a lockstep rollout. Upgrade every sandboxd
node and cocoon to the required version before enabling the catalog or shipping
an SDK that requests volumes. Mixed-version serving is unsupported. Once a
volume claim has finalized, do not roll a node back to an older sandboxd until
all volume claims are gone; the older daemon cannot preserve the volume
claims' admission holds and marker bookkeeping recorded in `claims.json`.

The guest images and sandboxd move together on the egress lane: sandboxd tells
a NIC-bearing guest how its traffic leaves through `/etc/silkd-lane`, and a
guest image from another release reads a different file or none, which on a
locked bridge lane leaves its execs routing into the lock. Roll the images
with the daemon. A config that uses the `socks5` or `ports` policy keys does
not load on an older sandboxd (unknown keys are rejected), so a rollback takes
the config with it. The SOCKS5 door is opt-in and the pool policy owns it: a
pool that relied on the door binding for any bare host rule adds
`"socks5": true` to its policy or loses the door silently, since its config
still loads; a tenant policy's flag counts only on a claim outside any
configured pool, where it is the whole policy.

## Configuration

sandboxd reads one JSON file (`-config`, default
`/etc/sandboxd/config.json`):

```json
{
  "listen": ":7777",
  "data_dir": "/var/lib/sandboxd",
  "cocoon_bin": "cocoon",
  "restore_mode": "mmap",
  "no_direct_io": true,
  "advertise_addr": "10.0.0.5:7777",
  "bridges": ["br0"],
  "volumes": [
    {"name": "imagenet", "path": "/srv/datasets/imagenet.img"},
    {"name": "weights-llama", "path": "/srv/datasets/llama.img", "directio": "on"},
    {"name": "acme-corpus", "path": "/srv/datasets/acme.img", "tenants": ["acme"]}
  ],
  "api_token": "…",
  "mesh": {
    "node_id": "node-a",
    "bind": "10.0.0.5:7946",
    "join": ["10.0.0.6:7946"],
    "cluster_key": "base64…"
  },
  "pools": [
    {"template": "base:24.04", "net": "none",   "size": "small", "warm": 4},
    {"template": "base:24.04", "net": "egress", "size": "small", "warm": 2}
  ]
}
```

As written the `egress` pool's guests reach nothing: on a bridge lane the NIC
is locked default-deny and the proxy door is bound only for a claim that has a
policy. Give the pool an [`egress`](egress.md) block before expecting traffic;
a tenant's claim of it (`acme` above) needs the tenant's own block as well and
gets the intersection of the two.

| field | default | meaning |
|---|---|---|
| `listen` | `:7777` | control- and data-plane HTTP listener |
| `data_dir` | `/var/lib/sandboxd` | golden snapshot exports, the claims journal, the usage/audit journals (`usage.jsonl`, `audit.jsonl` + `.1` backups), and `checkpoints/` by default |
| `cocoon_bin` | `cocoon` | cocoon CLI binary |
| `restore_mode` | unset | clone and wake-restore memory mode: `copy`, `ondemand`, or `mmap`; use `mmap` for dense pools |
| `no_direct_io` | false | use buffered writable disks for Cloud Hypervisor cold boots and clones; recommended for dense ephemeral pools to avoid direct-I/O CoW journal contention |
| `sync_claims` | false | fsync `claims.json` and its directory on every commit (claim, release, renew, hibernate, wake, fork, archive, reap), about 0.3 ms each on NVMe; without it a host power loss can leave an empty journal, which fails the next start's reconcile until the file is removed, after which hibernate snapshots are reaped as orphans and archive checkpoints stay stranded. Off by default; turn it on when running claims must come back from a power loss (see `vmm_restart`) |
| `cocoond_socket` | unset | `cocoon daemon`'s read-only API socket (by default `<cocoon run_dir>/cocoond.sock`); set, the node detects VMM exits from its event stream and polls only as a 60 s backstop. See [dead VMMs](#dead-vmms-and-host-restarts) |
| `vmm_restart` | `cold` | what the node does with a running claim whose VMM exited outside a transition (a VMM crash, or a host restart that took it): `cold` cold-boots the claim's own VM from its disk under the same sandbox id and token (guest memory, processes, and the instance-metadata document are lost), `none` keeps the claim listed as `failed` for the caller to release or wake. Egress-lane and volume claims, a VM record gone from cocoon, and a VMM that exits more than 3 times in 10 minutes are marked `failed` under either value. See [dead VMMs](#dead-vmms-and-host-restarts) |
| `no_balloon` | false | boot pool and template VMs without the virtio-balloon (cocoon otherwise returns 25% of guest memory to the host); clones inherit it from the golden. A guest that thrashes before deflate-on-OOM fires — a 16G build tier running a large typecheck — needs its whole memory |
| `advertise_addr` | = `listen` | internal HTTP host:port for peer traffic and preview forwarding; also used by clients when `client_advertise` is unset. Must be routable when `listen` is a wildcard; a node with `mesh` set refuses to load while it names an unspecified host |
| `client_advertise` | unset | client-facing HTTP(S) origin for this node, e.g. `https://node-a.sandbox.example.com`; published in owner, redirect, and peer responses. No path, query, fragment, or userinfo |
| `bridges` / `networks` | unset | egress-lane attachment: a list of host bridge devices, or a list of CNI conflist names. Mutually exclusive; with neither set the node serves only the no-network lane. A Linux bridge holds at most 1024 ports (kernel `BR_MAX_PORTS`), so an N-entry list raises the node's egress ceiling to N×1024 — VMs spread over the list by a stable hash of the VM name, so size it with headroom (the spread is statistical, not exact). `bridges` keeps the raw TAP-on-bridge attachment (taps in the root netns, no per-VM network namespace or CNI plugin execution); `networks` runs the CNI chain per VM. [Guarded egress](egress.md) on the egress lane (an egress-lane pool policy or any tenant policy) needs `bridges` and rejects a CNI network at load; none-lane pool policies ride the proxy on either |
| `volumes` | unset | node-local catalog of operator-managed dataset images: `[ {"name":"imagenet","path":"/srv/datasets/imagenet.img","directio":"off","tenants":["acme"]}, {"name":"scratch-db","path":"/srv/datasets/scratch.img","writable":true} ]`. Names match `^[a-z][a-z0-9_-]{0,19}$` and cannot start with `cocoon-`; paths are absolute; `directio` is `on`, `off`, or `auto` and defaults to `off` for both read-only and writable entries. `tenants` is an optional access list: empty means every authenticated scope, and a listed name the tenant set does not hold grants nothing and logs a warning at boot (the [tenant API](sandboxd-api.md#tenants-v1tenants) may add it later); root-owned claims have access; live additions use the claim owner's access list. `writable` (default `false`) lets a claim request `mode: "rw"` on that entry — see [Dataset volumes](#dataset-volumes). The catalog is intentionally not part of the cluster digest |
| `secrets` | unset | credentials the egress proxy injects by name: `[{"name": "gh", "header": "Authorization", "value_env": "GH_TOKEN"}]`. A pool or egress-class rule references the name (on a claim where both apply, only the pool rule's secret is injected); the value comes from the claim's `guest: false` env, then the node's environment, never this file. Without `value_env` the secret's name is the env to read, and only claims supply it. See [egress](egress.md#claim-env-and-secrets) |
| `egress_internal_allow` | unset | CIDR prefixes re-admitted through the egress proxy's SSRF guard, node-wide (every pool and tenant); `prefix:port,port` scopes an entry to those ports, a bare prefix admits every port. Prefer the port form. Prefixes, not a permit-private switch — the guest bridges are themselves ULA/RFC1918. See [egress](egress.md) |
| `egress_upstream` | unset | per-claim upstream proxies: `{"claim_env": "EGRESS_UPSTREAM", "allow": ["res.example.com", "10.1.0.0/16"]}`. A claim's `guest: false` entry under `claim_env` (`http://` or `socks5://`, optional `user:pass@`, or `direct`) routes its egress through that upstream, which must be on `allow` (host names, IPs, CIDR prefixes). See [upstream proxies](egress.md#upstream-proxies) |
| `egress_conn_rate` | unset | caps the new upstream connections each claim's proxy opens: `{"per_second": 30, "burst": 600}`, a token bucket per claim; a reused keep-alive connection and a connection to an internal IP address (the SSRF guard decides those) cost nothing, while a host name costs a token even when it resolves to one `egress_internal_allow` re-admits. A connection past it gets 429 (SOCKS5: not allowed) and an audit `decision` of `rate_limited`. A claim's bucket starts full at its first connection, and again when a reload changes `per_second` or `burst`. Unset leaves the rate unlimited |
| `egress_usage_bytes` | `false` | append an `egress_bytes` usage event with the payload bytes of every allowed egress tunnel or request when it ends; one extra usage-journal write per connection |
| `egress_ca` | unset | [HTTPS-interception](egress.md#https-interception) PKI: `root_cert` (the cluster root baked into intercepted guests; may bundle old+new roots during rotation) plus this node's `intermediate_cert`/`intermediate_key` from `sandboxd ca issue-intermediate`. Required when any pool rule sets `intercept` |
| `api_token` | unset | the operator (root) credential: when set, guards the node-level endpoints (Bearer) with full access, including release-by-id cleanup. Per-sandbox tokens guard ordinary sandbox-scoped calls |
| `meta_store` | unset | `{"kind": "pg", "dsn_env": "SANDBOXD_PG_DSN"}`: keep the tenant set in one shared PostgreSQL instead of each node's `tenants.json` (see [shared tenant database](#shared-tenant-database)). `dsn_env` names the node env holding the connection string. An optional `cell` also keeps the API-applied pool set there, one per cell, instead of in each node's `pools.json` (see [shared pool set](#shared-pool-set)); without it pool targets stay per node. Needs `api_token`. Restart only |
| `max_fork_count` | 16 | children a single `fork` may create; each is a full-RAM VM, so this bounds one request's memory blast radius to the node's capacity |
| `refill_concurrency` | 0 (auto) | concurrent VM provisioning budget, shared by warm-pool refills, fork clones, and the reap/hibernate/reconcile engine batches; the batches never take the last quarter of it (at least one slot), so a mass expiry or idle sweep cannot stop refills. 0 sizes it from the node: `NumCPU*2/3` clamped to [4, 256] — a 384-core node gets 256; small nodes keep a floor of 4 |
| `release_delay_seconds` | 0 | seconds a released VM waits in the removal queue before `cocoon vm rm`. The claim is dropped and journaled and its egress listener closed at release; the VM removal, the volume hold release and the egress tap unlock run on the first reap tick (5 s) after the delay, on the `refill_concurrency` budget like every other batch teardown, so a burst of releases does not compete with the claims still running. Until then the VM holds its memory and its volume reservations without counting as a claim. 0 removes inline |
| `preview_listen` | (off) | address for a preview HTTP server that serves guest ports under signed URLs; needs `preview_secret` |
| `preview_secret` | — | cluster-shared HMAC secret signing preview tokens (all nodes share one) |
| `preview_advertise` | = `preview_listen` | the browser-facing preview base URL, minted into every preview URL, so it must name a routable host (a wildcard `preview_listen` needs it set explicitly); nodes behind one TLS proxy may share it, while signed tokens route internally through each owner's `advertise_addr` |
| `checkpoint_dir` | `<data_dir>/checkpoints` | where checkpoints and promoted templates live. Point it at a shared FUSE mount (JuiceFS over object storage, NFS) and every node sharing the mount can branch every checkpoint — records are generation-addressed with `meta.json` as the atomic commit pointer, so no cross-node locking is required of the filesystem. A re-publish retains its superseded export generation for at least ~1h and until a following hourly sweep so an in-flight clone that resolved the old metadata can finish; budget the current generation plus every generation retained across that grace-and-sweep window. An explicit delete can make a concurrent clone fail visibly. One contract on any shared root (mount or bucket): a template key has a single writer — promotes go to the sandbox's owner node, and operators must not race promotes of one name from different nodes (checkpoint ids are node-generated and never collide) |
| `checkpoint_store` | dir | checkpoint AND promoted-template backend (both live in one store root, id-namespaced ck_/tp_): `{"kind": "s3", "s3": {"bucket": "…", "prefix": "ck/", "endpoint": "…", "region": "…", "force_path_style": true}}` stores checkpoints in object storage (any node claims any checkpoint, no shared mount needed). Credentials come from the standard AWS chain (env/IAM role), never this file. An export file is stored as 16 MiB chunks keyed by SHA-256 under `<prefix>chunks/<lineage>/`, with one `<prefix>manifests/<id>/<generation>.json` per published generation naming each file's size and present chunks, and `<prefix><id>/meta.json` uploaded last as the commit marker (the record prefix exists only while the record is live). Holes and all-zero chunks are never uploaded, and a fetch recreates the file at its full size and writes only the present chunks, so a mostly-empty disk costs its data, not its size, on the wire and on the fetching node. A lineage is the dedup scope: it defaults to the record id, and every checkpoint and archive of one claim shares the claim's sandbox id, so consecutive captures upload only the chunks that changed (the first capture after a wake uploads everything again when no other live record of that claim remains, because the wake deletes the record it restored from); a publish into a shared lineage writes a zero-byte marker `<prefix>lineages/<lineage>/<id>` after its manifest and before `meta.json`, and reads only the records those markers list; a crash between manifest and marker only costs a missed dedup, and Delete removes the record's marker right away. Re-publish retains the prior generation's manifest so an in-flight fetch that selected old metadata can finish. sandboxd never deletes a chunk or a manifest from the bucket: Delete removes `meta.json`, the labels and the lineage markers and re-stamps the manifests, so a fetch that already selected the record still completes, and reclaiming storage is the operator's job, see [Reclaiming S3 storage](#reclaiming-s3-storage). A publish or fetch is aborted after 30 minutes. Do not put an expiry lifecycle rule on the prefix: it would delete chunks a live record still names. Budget storage for every generation and chunk until your reclaim runs. When caching a new generation, each node prunes other cached generations of that record unused for at least an hour. Absent = the dir backend at `checkpoint_dir` |
| `checkpoint_ttl_hours` | 0 (keep forever) | ages out checkpoints older than this; the sweep runs hourly and at startup. Explicit deletes never wait for it. Must be nonzero and match fleet-wide when `checkpoint_peer_heal` is on — it is the expiry eligibility point for a healed replica a delete broadcast missed, after which its next successful hourly sweep removes it; persistent sweep failure extends retention until one succeeds, so it is not a hard ceiling |
| `checkpoint_peer_heal` | false | on a cluster, lets a node pull a checkpoint it lacks from a peer — found via a live probe, not gossip — rather than failing the branch; see [placement lifecycle](cluster.md#checkpoints-on-a-cluster). Three requirements, all enforced at config load: a nonempty `api_token` (the blob transfer between peers authenticates with it; without one the raw record stream would be open), `mesh.cluster_key` set (the pull presents the fleet `api_token` to an address learned from the peer probe, so the gossip layer carrying that address must itself be authenticated), and `checkpoint_ttl_hours` nonzero (a replica a delete broadcast missed becomes eligible for expiry after it, and its next successful hourly sweep removes it — so it is the finite eligibility point, not an exact ceiling). A shared checkpoint store (`checkpoint_store` kind `s3`) ignores this setting — every node already resolves every checkpoint directly, so there is nothing to heal |
| `warm_max` (pool entry) | 0 (static) | turns on the demand-adaptive watermark for that pool: the warm target rises from `warm` toward `warm_max` while claims arrive faster than the measured provision lead covers, and decays back over ~a minute of silence. The pool follows the target down: warm VMs above it are destroyed once the count has stayed above it for a full decay period, so a burst does not leave the pool parked at its high-water mark |
| `warmup` (pool entry) | unset | argv run in the golden VM after readiness and before its snapshot, so the files it touches are page-cache-resident in every clone, and again in every clone before it joins the warm pool, so those pages are already faulted into the restored VM when the first command runs — e.g. `["node", "-e", "0"]` on a Node flavor. It runs under the engine's 2-minute command timeout in silkd's base environment (`PATH`, `TERM`, and the guest image's proxy variables wherever nothing routes directly — the none lane and the locked bridge egress lane — with the proxy not yet serving, since a door pre-bound at refill only starts serving at claim); a non-zero exit or a timeout fails the golden build, so the pool stays unfilled until the config is fixed. Config-owned like `egress`: `PUT /v1/pools` rejects it, and a golden built with a different warmup is rebuilt |
| `capture_trim` (pool entry) | `false` | Trim the guest's copy-on-write disk before a promote or checkpoint of this pool's sandboxes (and of the clones of a template promoted from it), so the record carries only live data: blocks a build deleted — a `COPY` step's archive, a `RUN` step's caches — are no longer captured. sandboxd mounts the disk init resolved from `cocoon.cow` a second time at `/run/sandboxd-trim` through silkd as root, runs `fstrim` and unmounts, under the capture's transition lock (about 15 ms on a warm clone). A failed trim is logged and the capture proceeds, larger. A hibernated or archived sandbox is captured untrimmed. Config-owned like `egress`: `PUT /v1/pools` rejects it |
| `reader` (pool entry) | `false` | Serve read-only file calls on this pool's hibernated or archived claims from a warm VM of the same pool instead of waking them; needs `warm` of at least 1 and a cocoon whose `vm disk attach` takes `--from-snapshot` and `--from-dir`. See [Reading sleeping claims](#reading-sleeping-claims-without-waking-them). Config-owned like `capture_trim`: `PUT /v1/pools` rejects it |
| `egress_classes` | unset | named tenant egress layers: `[{"name": "desk", "egress": {…}, "egress_upstream_env": "DESK_UPSTREAM"}]`. A tenant names one with `egress_class` through the [tenant API](sandboxd-api.md#tenants-v1tenants) and its claims take that policy intersected with the pool's. `egress` is required; `intercept` stays a pool-rule flag. Reloadable: a class's policy reaches live claims on their next request. A class removed while tenants still name it leaves them with no egress and is logged at load and at reload |
| `egress_upstream_env` (pool or egress class entry) | unset | names a node environment variable holding the default upstream proxy URL for the pool's or class's claims (class before pool, a claim's own entry before both); needs `egress_upstream`, and the URL must be on its `allow`. Read and checked at startup. Config-owned like `egress`: `PUT /v1/pools` rejects it |
| `replicate_seconds` (pool entry) | 0 (off) | snapshot every running claim of this pool to the checkpoint store this often and point the claim's registry row at the snapshot, so another node restores the claim, memory and processes included, when this node is lost (see [running claims across node loss](cluster.md#running-claims-across-node-loss)). Each round pauses the guest for the length of a memory and disk capture. A restore resumes from the last committed replica and loses everything after it: up to the longer of the interval and one replication pass, plus the 5-second tick, plus one more pass, since a claim can run first in one pass and last in the next. A pass covers all the node's due claims of the replicating pools with bounded concurrency, so it grows with claim count and guest memory; the first round of a claim after a wake or adoption uploads the whole claim, so that pass is the long pass. The claim's row carries no retention deadline while it runs. Requires `archive_failover` and a `none`-lane pool, both checked at config load; a claim with volumes is skipped. Config-owned like `egress`: `PUT /v1/pools` rejects it. Restart only: a claim is replicated by its pool's setting from the moment it is claimed, so a reload that changes it is refused (a pool restarted with 0 drops its claims' rows and replicas on the first pass) |
| `storage` (pool entry) | unset (cocoon's default, 10G) | size of the copy-on-write disk the pool's golden is cold-booted with, in cocoon's own `--storage` spelling (`"40G"`, `"40GiB"`, `"40Gi"`: Docker/Kubernetes units, all binary) and passed to it verbatim; every clone, fork, checkpoint branch and promoted template of the pool inherits it. At least cocoon's default of 10G. Equal sizes spelled differently build the same golden; a changed size rebuilds the golden on the next build, and claims already out keep their disk. Config-owned like `egress`: `PUT /v1/pools` rejects it |
| `max_claims` | 0 (unlimited) | node-wide cap on live claims; claim/fork/branch requests beyond it answer 429 with the pool state unharmed (on a cluster a non-volume claim tries a warm-peer redirect first; a volume claim answers the 429 with no redirect) |
| `audit_log` | false | append every relayed frame that opens an RPC — its op + addressing fields, never payloads — to `<data_dir>/audit.jsonl`; continuation `data`, `data_end`, `stdin`, and `stdin_close` frames pass through unrecorded. The file is size-rotated with one `.1` backup. Records are `{t, id, op}` plus whichever addressing fields the op carries (`argv`, `path`, `dest`, `from`, `to`, `url`, `session`, `port`), plus `method` (`GET`, `CONNECT`, `SOCKS5`, …), `decision` (`allow`, `deny`, `rate_limited`), `secret` (the ref name, never its value), `upstream` (the [upstream proxy](egress.md#upstream-proxies)'s `host:port`, when one carried it), `tenant`, and on an allowed connection `src_addr` and `dst_addr` (the node-side and destination `ip:port` of the upstream TCP connection, the tunnel target through an upstream proxy) on `egress` records, which a relayed frame cannot set; preview accesses record as op `preview`, one per request; a tenant change records as op `tenants` with the `added`, `changed` and `removed` names, never a token. An opening frame whose first line exceeds 4 KiB records as op `oversized` with no addressing fields |
| `disk_reserve` | unset | `{"free": "100G", "dirs": ["/srv/sbx/run"]}`: while the filesystem of any listed directory (default `data_dir`) has less than `free` available, checked every 2 s, the node refuses new claims, forks, checkpoint claims, wakes of hibernated or archived sandboxes, hibernates, checkpoint captures and promotes with 429, and parks refill; the idle sweep and a lease-end archive leave their sandboxes running and retry on the next tick; `/v1/info` reports it as at capacity and the mesh sees no warm VMs on it. Running sandboxes keep running. On an XFS project quota the free space is the project's. List cocoon's `run_dir` when it sits on another filesystem, since the copy-on-write disks grow there. Size `free` to at least two sandboxes' worst-case growth (copy-on-write disk plus memory file each) and keep the refusal above your disk alert's warning level, so the warning comes first: on a 640G project with 40G disks and 8G memory files, `100G` refuses at 84% used |
| `idle_hibernate_seconds` | 0 (off) | node-wide idle policy for unpooled claims (template/checkpoint claims): a none-lane claim is hibernated once it has had no open data-plane connection (relay, buffered exec, preview) and no egress request in flight for this long; the clock restarts when the last connection closes, so a long command is never cut short. The next call that reaches the guest wakes it transparently. Per-pool `idle_hibernate_seconds` does the same for that pool's claims; pooled keys ignore the node-wide value, and egress pools reject it because they cannot resume safely. Opt in deliberately: a wake costs latency and the snapshot, so callers with their own idle logic must not pay twice |
| `archive_after_seconds` | 0 (off) | tier below hibernation: a hibernated claim idle this long is checkpointed to the store and its local VM dropped, freeing the node entirely; the next call that reaches the guest restores it transparently (a checkpoint restore's latency) with a fresh lease of the length the claim was granted (the server default of 5m when it asked for none); a lease moves only when a client calls [`/renew`](sandboxd-api.md#post-v1sandboxesidrenew). Keep `idle_hibernate_seconds` under the leases your claims use, or a woken sandbox that idles again is destroyed by the reap before it can hibernate, its archive already consumed by the wake; one still in use when its lease ends is destroyed the same way, unless it claimed `on_expire: archive`, which archives at the lease end whatever this setting. Requires `idle_hibernate_seconds > 0` and must exceed it. Node-wide for unpooled keys; per-pool overrides for that pool |
| `archive_delete_after_seconds` | 0 (keep) | purge an archived claim's store checkpoint this long after it was archived; the claim is then gone for good. On S3, chunks and manifests remain until [offline reclamation](#reclaiming-s3-storage). On archive this retention window replaces the live claim deadline; 0 clears the deadline so the archive is kept forever. Same node-wide/per-pool split |
| `archive_failover` | false | let another node adopt this node's archived claims once its node lease expires, and the running claims of pools with `replicate_seconds`: each archive also writes a row (token SHA-256, owner, epoch, checkpoint id, retention deadline) to the shared PostgreSQL, and a `Lookup` that reaches any node takes the claim over there (see [archived claims across node loss](cluster.md#archived-claims-across-node-loss)). Requires `meta_store` with a `cell` (rows are scoped by it, so meshes sharing one database never adopt from each other), a `checkpoint_store` of kind `s3` and a `mesh`, all checked at config load. The node also renews a lease row every 5 s (see [node lease](cluster.md#node-lease)). With the meta store down, archives, wakes and releases of archived claims fail and retry, and replicated claims stop being served once the lease is overdue (see [running claims](cluster.md#running-claims-across-node-loss)); other running claims never touch it. Restart only |
| `mesh` | unset | join a cluster ([Clusters](cluster.md)); unset = single node |
| `pools[]` | — | warm pools, keyed by `(template, net, size)`. `warm` defaults to 4 only when both `warm` and `warm_max` are omitted; an explicit `warm: 0` is kept, so `warm: 0` under a `warm_max` starts the pool empty and grows it on demand, and `warm: 0` alone registers the key with no warm VMs (`PUT /v1/pools` never defaults `warm`); `net` is `none` or `egress`; `size` is a tier, below. Retune online without a restart via [`PUT /v1/pools`](sandboxd-api.md#put-v1pools) — omitted pools drain, and their goldens are deleted once they are idle (`warm: 0` keeps the golden). This is the **first-boot seed**: once a node takes a `PUT /v1/pools`, the applied set persists to `<data_dir>/pools.json` and overrides this section on every later boot (a startup log notes it); delete `pools.json` to return to config-owned pools. With a `meta_store` `cell`, the cell's set overrides it instead (see [shared pool set](#shared-pool-set)). Egress stays config-owned either way. A template re-pulled or re-imported under the same ref is picked up within a minute: the pool rebuilds its golden and replaces its warm VMs. See [state ownership](cluster.md#state-ownership) |

Size tiers (free-form CPU/memory is deliberately not accepted — it would
fragment the warm pools):

| size | CPU | memory |
|---|---|---|
| `small` | 1 | 512M |
| `medium` | 2 | 1G |
| `large` | 4 | 4G |
| `xlarge` | 4 | 8G |
| `2xlarge` | 8 | 16G |

### Reading sleeping claims without waking them

A client that only lists or reads files (a UI showing an idle agent's
workspace, a tool collecting results) would otherwise wake the claim and
hold its memory until the next idle hibernate, and for an archived claim
fetch the whole checkpoint first. With `reader` set on the claim's pool,
`fs_stat`, `fs_list`, `fs_read` and `fs_pull` on a hibernated or archived
claim are served from a reader instead; anything else on the connection wakes
the claim as before ([API](sandboxd-api.md#get-v1sandboxesidagent)).

```json
{
  "pools": [
    {"template": "rt:24.04", "warm": 4, "reader": true, "idle_hibernate_seconds": 600}
  ]
}
```

It needs Cloud Hypervisor and a cocoon whose `vm disk attach` takes
`--from-snapshot` and `--from-dir` (v0.7.3 or newer). Leave `reader` off
on a Firecracker node or with an older cocoon: there every read spends a warm VM
on an attach that fails, then wakes the claim. An archive exported by an older
cocoon, whose `snapshot.json` has no `disk_attach_files`, wakes before any VM is
taken. Unpooled claims (template and checkpoint claims) always wake.
`reader` is reloadable like `capture_trim`; turning it off leaves open reader
connections to finish, and the next connection wakes the claim.

A reader is a warm VM taken from the claim's own pool, so it boots the claim's
image with the claim's layers, and it has that pool's lane: no NIC on the none
lane, the bridge NIC the claim had on the bridge lane (egress-lane claims never
hibernate). Taking one removes a VM from the pool until the refill replaces it,
and an empty pool never cold-boots one: the call wakes the claim. A reader is never shared: it
serves one claim, and only the claim's own token reaches it. The guest's own
kernel does the parsing, as it does for the claim:

- a hibernated claim's copy-on-write disk is copied by cocoon (`vm disk attach
  --from-snapshot`) into the reader VM and hot-attached read-write; cocoon uses
  a reflink where the filesystem has them, copies only that disk, never the
  memory image, and removes the copy with the VM. The claim's transition lock is
  held for the copy so a wake cannot consume the snapshot meanwhile;
- an archived claim's record is fetched only for `snapshot.json` and the files
  its `disk_attach_files` lists, into the reader's own directory under
  `data_dir` (the dir backend copies them out of the record, the s3 backend
  downloads only the chunks those files name, holes stay holes), then attached with `--from-dir`. The fetched files
  are deleted once cocoon has its copy, and nothing lands in the store's shared
  fetch cache;
- the reader mounts the attached disk writable, so the guest replays the ext4
  journal onto the copy and never onto the original, and mounts read-only the
  layers named by its own `cocoon.layers=` kernel parameter. It assembles a
  read-only overlay at `/view` and starts the claim's own `silkd`
  (`/usr/local/bin/silkd`) chrooted into it on guest vsock port 2051. Paths and
  symlinks resolve inside the claim's filesystem, and a write fails at the
  read-only overlay.

The pool's image must offer `mount`, `chroot`, `env`, `mkdir` and `sync` and
boot a kernel that mounts erofs, ext4 and overlayfs, which the shipped images
do. The claim's image must ship `silkd` at `/usr/local/bin/silkd`, as the
shipped Linux images do; the android image does not, so its claims always wake.
A missing silkd is detected right after the view is mounted, so such a claim
pays one reader VM, the copy and the mounts before its call wakes it, never a
probe timeout. A reader is kept for 30 s after its last connection, then its VM
is destroyed. A wake waits for a reader only while cocoon copies the disk:
milliseconds with a reflink, the time to copy the whole disk without one. A
reader serves only the sleep it was built from: a later hibernate or archive
gets a fresh one. The claim's own code runs in its reader with root, as it did
in the claim, and the reader serves one claim and is destroyed afterwards, so
the claim's bytes never reach a shared VM or the host kernel.

Whenever a reader cannot be built (no warm VM in the claim's pool, the disk
reserve is low, a failed attach, an archive without
`disk_attach_files`, an image without `silkd` at that path), the call wakes the
claim as it does without the feature. With the feature on for the pool, every
hibernate of its claims runs a guest `sync` right before the pause, and a reader
shows the disk as of that `sync`: a write the guest makes in the instant between
the `sync` and the pause is visible only after a wake. A hibernate whose `sync`
fails or times out is marked not reader-safe, as is any sleep that began before
the pool had `reader`, and its reads wake the claim as without the feature.

### Dataset volumes

Volume `tenants` entries use the tenant API name syntax. Static configuration
warns about invalid names and skips them during access checks. A nonempty list containing
only invalid names grants no tenant access. Runtime registration rejects invalid
tenant names. Valid names for tenants that have not yet been created remain
accepted.

Running claims can change mounted read-only catalog entries through the
[GET, PUT, and PATCH volume API](sandboxd-api.md#get-v1sandboxesidvolumes)
and both SDKs. The image must already exist on the owner node. Live changes
require Cocoon v0.7.2 or newer for idempotent disk attach and detach. PUT and
PATCH reject an older release, or a runtime whose version cannot be queried,
with 503 before any intent, hold, or device change; a dev build is assumed
current. sandboxd reads the version once, so restart it after upgrading cocoon.
GET and release remain available.

The claim journal stores confirmed applied `Volumes`, plus desired
`VolumesWant` while it differs.
Admission holds cover their union across daemon restart and teardown. Retry
an interrupted operation with the same PUT target. Release cancels convergence
and transfers holds to teardown. Capture is refused while either set is
nonempty.

A sandbox whose VMM exits while holding volumes is release-only. Release it and
create a new sandbox. Daemon restart recovery assumes the original VMM remains
alive and does not recreate guest processes. Unfinished changes report an
interrupted diagnostic after restart and require a caller retry.

`e2e/cmd/volumesmoke -live` (`VOLUME_LIVE=1` for `scripts/sandboxd-e2e.sh`)
exercises GET, repeated full-set PUT, PATCH add and remove, and clearing a new
claim.

Each catalog path must name a disk image containing a mountable whole-device
filesystem. Keep catalog names bound to the same image while any claim holds
them. Use versioned names such as `dataset-v1` and
`dataset-v2`; sandboxd cannot detect a repointed catalog entry. A read-only
entry's image must stay immutable: do not replace,
truncate, or delete it while attached; publish a new catalog name or path
instead. A missing path produces a startup warning and fails only claims that
request it, allowing images to be distributed after sandboxd starts.

For example, build a whole-device ext4 image directly from a prepared tree,
then make the published file host-read-only:

```bash
truncate -s 200G /srv/datasets/imagenet.img
mkfs.ext4 -F -d /srv/datasets/imagenet-root /srv/datasets/imagenet.img
chmod 0444 /srv/datasets/imagenet.img
```

Use the same dataset identity and access list for a volume name on every node,
then distribute its immutable image to each node that should advertise it.
sandboxd gossips only catalog names; it never copies content or gossips paths or
access lists.

A claim requests up to eight unique names and may set an absolute, clean custom
mount for each; the default is `/volumes/<name>`. Mounts must stay outside the
guest OS directories and cannot duplicate or nest within one claim. Volume
mounts may shadow an existing populated guest directory for that claim's life.

A volume claim may consume an ordinary warm Cloud Hypervisor VM. sandboxd
attaches after the warm pop or provision, waits for the device settle — a
serial match under `/sys/block`, then the `/dev/<name>` node itself, since the
kernel publishes sysfs before devtmpfs creates it — up to 2 seconds, then
mounts the device before finalizing the claim: `mode: "ro"` (the default)
attaches and mounts read-only; `mode: "rw"` requires the catalog entry's
`writable: true` and attaches and mounts read-write. Setup failure destroys
the VM without quiescing — the claim was never handed out, so no workload
write happened — and a popped warm VM is refilled normally.

A multi-volume claim brings every volume up concurrently: each volume's own
marker→attach→mount order is preserved, but cocoon serializes the hypervisor
attach per VM, so what actually overlaps across volumes is the CLI spawns,
device settles, and guest mounts. A partial failure still fails the whole
claim and destroys the VM without quiescing, so any `rw` volume already
attached keeps its dirty marker — including one the caller never got a handle
to; recovery is the normal `rw` cycle (see
[Writable dataset volumes](#writable-dataset-volumes)).

Warm candidates retain their normal ranking, but a volume claim's candidate
set, promoted-template intersection, and redirect follow the fleet-wide rule
in [cluster](cluster.md#volumes-and-placement).

An empty catalog `tenants` list allows every authenticated scope; a nonempty
list limits the image to those claim owners. Root-owned claims can use any
entry. Live additions use the claim owner's ACL, including requests sent by root. A
listed tenant the set no longer holds grants nothing and logs a warning at
boot.
`GET /v1/volumes` reports the caller-visible fleet union and holder count, plus
the answering node's current local availability and whether the entry is
`writable`, without exposing host paths or node addresses. Applied names,
effective mounts, and (for `rw` entries) mode are persisted with the claim.
Such a claim cannot hibernate, fork, checkpoint, or promote; the idle
hibernate sweep leaves it running. Release removes the VM but never deletes
the operator-owned backing image — an `rw` release quiesces (unmounts) first,
below. With `directio=off`, readers share the host page cache; use
`directio=on` when cache interference matters.
A dataset mounted into an egress-lane sandbox can be uploaded wherever that
tenant's egress policy permits, so treat the ACL and egress policy as one access
decision. Image replication, detach, and refcounting remain out of scope.

#### Writable dataset volumes

A catalog entry with `writable: true` may be claimed with `mode: "rw"`
(omitted, or `"ro"`, always mounts read-only, even against a writable entry).
`directio` still defaults to `off` for a writable entry — the durability
nuance is that the host page cache sits between the guest's flush and the
device, so `directio=on` is the knob when that gap matters, not just cache
interference.

**Single holder, by operator contract.** A writable name must be attached
from exactly one node — the same weight as "replacing an attached image is
operator error." Nodes advertise only the catalog paths they actually hold,
so every claim for that name, `ro` or `rw`, already funnels to that one node;
running the same writable name from two nodes at once is dataset divergence
that nothing in the protocol detects for you.

**The `<path>.dirty` marker.** Before the first `rw` attach of an image,
sandboxd durably creates a `<path>.dirty` sidecar beside it; a clean `rw`
release removes it. It is a write-ahead record, not a lock: it survives a
killed sandboxd or a dead VMM, so anything short of a clean unmount leaves it
in place. It is operator-visible — `ls` next to the image shows whether it's
mid-write — and travels with the image on shared storage. A leftover marker
is not corruption (a journaling filesystem already makes a hard VM kill
crash-consistent); it means the image needs one `rw` claim, which replays the
journal as a side effect of mounting, followed by a clean release, before it
can serve `ro` again. A `ro` claim against a dirty image is refused rather
than auto-healed: replaying the journal takes the write lock, which conflicts
with concurrent readers.

**Concurrency, for one image name:**

- `rw` ∥ `rw` — refused (409).
- `rw` ∥ `ro`, either order — refused (409): a writer under live readers hands
  every reader torn metadata, since ext4/xfs are not cluster filesystems.
- `ro` ∥ `ro` — fine, same as v1.
- Sequential `rw` → release → `ro` is the supported publish/update workflow,
  gated by the dirty marker: a clean writer release clears it and readers
  proceed; a crashed writer leaves it, and `ro` claims are refused until one
  `rw` claim replays and releases cleanly.

**Attach-only claims opt out of the marker, not out of admission.** A claim
sent with `volumes_attach_only` gets the device attached and nothing else, so
sandboxd neither writes nor clears `<path>.dirty` for it — it cannot verify a
mount it did not perform. Everything in this section still applies to the
default, mounting claims, and the exclusion rules above apply to attach-only
claims exactly the same way, which is what keeps other tenants safe. The
operator-visible difference: an image whose attach-only writer released
without unmounting cleanly carries no marker, so the next `ro` claim is
admitted and fails at mount time (500) instead of being refused early (409).
Whoever hands out attach-only `rw` access owns that trade; see
[sandboxd-api](sandboxd-api.md#attach-only-volumes).

### Reclaiming S3 storage

sandboxd never garbage-collects the S3 `checkpoint_store`. It only adds
objects and, on a delete, removes the record's `meta.json`, labels and lineage
markers; chunks, manifests and everything a superseded record wrote stay in the
bucket until an operator job removes them. The `dir` backend is different: it
keeps its own local generation sweep.

**Key layout** (`<prefix>` is the configured prefix, ending in `/`):

| Key | Content |
|---|---|
| `<prefix><id>/meta.json` | The live record, uploaded last as the commit marker. Absent once the record is deleted. Its current generation is `export-` plus the first 16 hex characters of the SHA-256 of the object body. |
| `<prefix><id>/labels.json` | Optional labels, removed with the record. |
| `<prefix>manifests/<id>/<gen>.json` | One JSON manifest per published generation: `lineage` and, per file, `path`, `size` and the present `chunks` (`index`, `sha256`). `<gen>` is `export-<16 hex>`. |
| `<prefix>chunks/<lineage>/<sha256>` | A 16 MiB chunk. A chunk is shared by every record of its lineage that names the same digest, and only by those. |
| `<prefix>lineages/<lineage>/<id>` | Zero-byte marker: record `<id>` published into `<lineage>`. It only drives dedup, never chunk liveness. |

**What sandboxd never deletes:** chunks, manifests of deleted records,
superseded generations of a live record, and stale markers.

**Offline reclamation only.** Stop every sandboxd node using the prefix and
wait for all in-flight publishes, deletes and fetches to finish. Keep the
prefix offline throughout the inventory-and-delete pass. Online reclamation
is not supported: a publish can replace an old chunk after it was listed but
before it is deleted. Object age and the transfer timeout do not close that
window.

From that stable inventory:

1. Read every live `meta.json` and keep the manifest of the generation it
   selects, for both checkpoints and templates.
2. Read all kept manifests and collect their chunk keys, including the
   lineage. If the inventory or a required read fails, stop before deleting.
3. Delete manifests outside that set and chunks no kept manifest references.
   A lineage marker or `labels.json` may be deleted when its record has no
   `meta.json`.

Deletion is idempotent under this offline precondition; a later pass must
build a fresh inventory. Do not use a bucket lifecycle expiry instead: it
would remove chunks a live record still names.

**Cost of not running it.** Storage grows without bound. Every archive wake,
user checkpoint delete and, with replication, every replica round leaves the
superseded record's unique chunks behind. Successive captures of a running
claim share few memory chunks, because most of them change between captures, so
each superseded capture leaves most of its memory image behind; an archive that
took 10,752 MiB as whole files stored about 912 MiB as chunks.

### A fuller config

The block above is the minimum. A production node with guarded
egress + HTTPS interception, previews, an object-store checkpoint backend,
the idle→hibernate→archive tiers, and a mesh looks like this — every field
here validates on load:

```json
{
  "listen": ":7777",
  "data_dir": "/var/lib/sandboxd",
  "advertise_addr": "10.0.0.5:7777",
  "bridges": ["br0"],
  "restore_mode": "mmap",
  "no_direct_io": true,

  "api_token": "op-root-token",

  "secrets": [
    {"name": "gh", "header": "Authorization", "value_env": "GH_TOKEN"}
  ],
  "egress_ca": {
    "root_cert": "/etc/sandboxd/egress-ca/root.crt",
    "intermediate_cert": "/etc/sandboxd/egress-ca/node-a.crt",
    "intermediate_key": "/etc/sandboxd/egress-ca/node-a.key"
  },

  "preview_listen": ":8443",
  "preview_secret": "cluster-shared-hmac-secret",
  "preview_advertise": "https://preview.example.com",

  "checkpoint_store": {
    "kind": "s3",
    "s3": {"bucket": "sandbox-ckpt", "prefix": "ck/", "region": "us-east-1"}
  },
  "checkpoint_ttl_hours": 168,

  "max_claims": 200,
  "audit_log": true,
  "idle_hibernate_seconds": 120,
  "archive_after_seconds": 3600,
  "archive_delete_after_seconds": 604800,

  "mesh": {
    "node_id": "node-a",
    "bind": "10.0.0.5:7946",
    "join": ["10.0.0.6:7946"],
    "cluster_key": "MDEyMzQ1Njc4OWFiY2RlZg=="
  },

  "pools": [
    {"template": "rt:24.04", "net": "none", "size": "small", "warm": 4, "warm_max": 12},
    {"template": "rt:24.04", "net": "egress", "size": "medium", "warm": 2,
     "egress": {"socks5": true, "allow": [
       {"host": "api.github.com", "methods": ["GET", "POST"], "secret": "gh", "intercept": true},
       {"host": "*.googleapis.com"},
       {"host": "imap.example.com", "ports": [993]}
     ]}}
  ]
}
```

Secret values come from the environment named by `value_env` (here
`GH_TOKEN`), never the file. The `egress_ca` files are provisioned with
`sandboxd ca` — see [Guarded egress](egress.md#https-interception). On a
cluster, `api_token`, `preview_secret`, and `cluster_key` must match on
every node, and so must the tenant set.

### High-density no-network pools

Both lanes use Cloud Hypervisor; `net: "none"` launches it with no NIC. For
dense ephemeral pools, set `restore_mode` to `mmap`, enable `no_direct_io`,
place Cocoon's `run_dir` on a capacity-sized tmpfs, and set Cocoon's
`cni_conf_dir` empty on none-only nodes to avoid per-VM network namespaces.
The last setting also removes the VMM's network-namespace quarantine. On a
384-core host, start with `refill_concurrency: 64`; higher values need
measurement. Pause `cocoon daemon` or lengthen its reconcile interval during a
mass fill.

### Host tuning for dense egress-lane nodes

The no-network lane is CPU-bound and needs none of this. The egress lane
serializes on the kernel's global rtnl lock — tap create, bridge attach, and
the VMM's own tap open all take it — so at hundreds of VMs host configuration
dominates fill throughput:

- **Keep the kernel console quiet** (`loglevel=4` on the kernel cmdline, or
  drop `console=ttyS0`; dmesg and the journal keep everything either way).
  Bridge port transitions printk synchronously to every registered console
  *while holding rtnl*: measured on a serial+fbcon host, one bridge attach is
  44 ms noisy vs 3 ms quiet, and a 1000-VM fill roughly triples. Fix the
  console before tuning anything else — netlink-level optimizations are
  unmeasurable, or negative, on a noisy console.
- **Keep udev off the sandbox taps.** Every tap add/remove fires a udev
  event; rule evaluation plus hooks like `ifupdown-hotplug` (a fork+exec per
  interface) contend on the same rtnl lock — stopping the udev exec queue
  during a fill measured ~4x throughput, and a backed-up event queue can
  collapse a fill outright. Shadow the net-setup rules for these taps, or
  pause the exec queue around planned mass fills.
- **Set `refill_concurrency` explicitly (~64) on egress-heavy nodes.** The
  auto default scales with cores (a 384-core node gets 256), which suits the
  no-network lane but overshoots the bridge lane: rtnl does not plateau under
  pressure, it collapses — on a quiet host RC 256 halves the fill rate RC 64
  achieves.

### A caching resolver for egress

The egress proxy resolves the destination of every new upstream connection
(a CONNECT tunnel, a direct dial, and the proxy host on a routed `upstream`)
with Go's resolver, which reads `/etc/resolv.conf` and caches nothing. Against
a network resolver one lookup measured 3.2–3.6 ms, about a fifth of a new
HTTPS tunnel's setup. Run a caching resolver on the node — the
systemd-resolved stub, dnsmasq or unbound; node-local-dns on Kubernetes — and
point `/etc/resolv.conf` at it. It honors record TTLs, and the internal-address
guard still checks every address it returns.

### Reserving CPU for the control plane

A saturated node can leave clone/wake execution and sandboxd itself nothing
to run on — under full-node load CH clone p95 has measured in the tens of
seconds. cocoon newer than v0.5.8 runs every VMM in its own cgroup v2 CPU
scope (Guaranteed-at-N: a VM's CPU count is also its hard host-CPU cap) and
adds `cgroup_cpus`, a one-time cpuset fence keeping the whole VM population —
including the virtio and io-wq worker threads vCPU affinity cannot reach —
off reserved host cores. On dense nodes set the fence in cocoon's config
(e.g. `cgroup_cpus: "0-379"` on a 384-core host) so the reserved cores stay
free for sandboxd, its cocoon invocations, and the OS; cocoon's CPU-isolation
docs cover validation semantics and the per-VM knobs.

### Auth model

Three token kinds. The root `api_token` has full access — operators and
single-tenant deployments need nothing else. Tenant tokens, added through
the [tenant API](sandboxd-api.md#tenants-v1tenants) and kept in
`<data_dir>/tenants.json` (or the [shared tenant database](#shared-tenant-database)),
create and manage their own resources: claims, forks, checkpoints,
promoted templates, and preview URLs are stamped with the tenant name;
sandbox and checkpoint listings filter to the caller's tenant, and a tenant
can delete only its own checkpoints and templates (root sees everything).
Operator surfaces stay root-only — per-id sandbox reads and stats,
`GET /v1/checkpoints/{id}/blob`, `GET /v1/info`, `PUT /v1/pools`,
`/v1/tenants`, `/v1/config/reload`, `POST/DELETE /v1/drain`, `/metrics`; a
tenant token there is authenticated but not authorized, so it answers 403 (a
wrong token stays 401). Per-sandbox
tokens are unchanged: whoever holds a sandbox's token drives that sandbox.
Fork children inherit the parent's tenant and count against its
`max_claims` (0 = unlimited, a per-node cap, so a tenant's cluster limit is
`max_claims` × nodes); a tenant at its cap gets 429 exactly like a node at
`max_claims`, and the usage journal's claim events carry the tenant for
per-tenant billing. `config.json` has no `tenants` field: a node starts with
no tenants until the API adds them.

## Running

```bash
sandboxd -config /etc/sandboxd/config.json
```

On start the node reconciles: persisted claims whose VMs cocoon still holds
are re-adopted, everything else `sbx-`-prefixed is removed. A running one
whose removal fails keeps admission holds on the volumes it still has
attached until cocoon confirms it gone, so a conflicting claim gets 409
instead of an attach failure. A record still in cocoon's `creating` state is
reclaimed through `vm reconcile-stale-create` (cocoon ≥ v0.5.8), which
refuses while a clone is in flight instead of deleting the VM out from under
it; older cocoons fall back to forced removal. Any golden no pool uses is
deleted. Then the refill loop builds one golden snapshot per pool (a one-time
cold boot + snapshot export, tens of seconds) and keeps each pool topped up
with claim-ready clones.
`GET /v1/info` shows `"golden": true` and `warm` at target when the node is
ready to serve warm claims.

A minimal systemd unit (shipped as
[`packaging/sandboxd.service`](https://github.com/cocoonstack/sandbox/blob/main/packaging/sandboxd.service)):

```ini
[Unit]
Description=sandboxd
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/local/bin/sandboxd -config /etc/sandboxd/config.json
Restart=on-failure
KillMode=process
Environment=SANDBOXD_LOG_LEVEL=info

[Install]
WantedBy=multi-user.target
```

Each record carries its syslog level, so alerting keys on systemd priority:
`journalctl -u sandboxd -p err` lists the errors and `-p warning` the
degradations. The one line an unparsable `SANDBOXD_LOG_LEVEL` prints before
the logger is up carries no level, so read it with `journalctl -u sandboxd` or
`systemctl status sandboxd`. With stderr off a terminal, as under this unit, the record
itself is one JSON object; a terminal gets the console format. `journalctl`
answers an empty range with `-- No entries --`, so a check that counts lines
reads one problem where there are none — count records (`-o json | wc -l`) or
test the output for emptiness.

Stopping sandboxd leaves VMs alive; the next start reconciles them. The unit's
`KillMode=process` is what guarantees it: systemd signals sandboxd alone, so a
VMM that cocoon could not move into its own scope, and any `cocoon` call still
in flight, outlive the stop instead of dying with the service's cgroup. Claimed
sandboxes are reaped when their TTL expires (default 5m, capped at 24h).

### Dead VMMs and host restarts

With `cocoond_socket` set, the node follows `cocoon daemon`'s event stream
(`GET /v1/events` on that socket): the daemon watches every VMM through a
pidfd, so an exit reaches sandboxd within a daemon pass and a recycled PID
never reads as a live VMM. The node then lists cocoon's VMs only every 60 s as
a backstop for a dropped event, and every 10 s while the stream is down (it
reconnects with backoff) or when no socket is configured. Each list runs only
while the node holds a running claim; each suspect is then checked with a
single-VM `cocoon vm inspect` under its lock. A claim
whose VMM is gone — killed, crashed, or lost with the host — is handled per
`vmm_restart`. A VMM that exits because the guest powered off is treated the
same way. A claim whose lease has already lapsed is left to the reap, unless it
claimed `on_expire: archive`, which needs a running VM to archive:

| outcome | when | client sees |
|---|---|---|
| cold restart | `cold` (default), none-lane claim without volumes, within 3 restarts in 10 min | `restarts` +1 and `restarted_at` on the sandbox row, `vmm_exit` then `vmm_restart` usage events; a call before the next pass gets `502`, one during the boot waits for it, open relays see EOF and redial |
| failed | `none`, an egress-lane or volume claim, the VM record gone, the restart budget spent, or 3 failed boots | `failed: "<reason>"` on the row, `vmm_exit` then `vmm_failed` usage events, `409` from exec, relays, ports, hibernate, fork, checkpoint and promote |

A cold restart boots the claim's own copy-on-write disk: files the guest
synced survive, memory and processes do not. The claim's guest env entries
are written again and its egress proxy is re-armed; the instance-metadata
document is not kept, so re-send it when `restarts` moves. A failed claim keeps its VM and
disk until release or lease end (it is destroyed at expiry even with
`on_expire: archive`); `POST /v1/sandboxes/{id}/wake` cold-boots it and resets
the restart budget. After a host restart the same rule applies to every
running claim whose VM record survived, so hibernated, archived and running
claims all come back; with `sync_claims` off a power loss can still lose the
journal itself. Cocoon keeps each VM's copy-on-write disk under its `run_dir`,
so a `run_dir` on tmpfs ([high-density pools](#high-density-no-network-pools)) loses every local disk
with the host: running claims then end `failed`, and hibernated ones cannot
wake. Cold restart is verified on the direct-boot (OCI) templates
this project ships; a cloudimg (UEFI) clone boots again without its per-clone
cidata, and no shipped pool uses one. Without the daemon stream the poll reads
`cocoon vm list`, whose liveness check needs a cocoon that verifies the VMM's
PID (cocoonstack/cocoon#274) to never read a recycled PID as a live VMM.

### Maintenance

To empty a node for maintenance, cordon it first:
[`POST /v1/drain`](sandboxd-api.md#post-v1drain) (root) stops new claims and
drains the warm pools; poll `GET /v1/info` until `claimed` reaches zero (or
let the leases expire), then stop sandboxd. `DELETE /v1/drain` uncordons.
The drain is not persisted — a restarted node serves again.

### Shared tenant database

With `meta_store` set, every node reads tenants from one PostgreSQL table
instead of its own `tenants.json`. Tenants are added, changed and removed
through `/v1/tenants` on any node.

- **Schema:**
  - sandboxd creates the table at boot when it is missing:

    ```sql
    CREATE TABLE IF NOT EXISTS sandboxd_tenants (
        name         text        PRIMARY KEY,
        token_sha256 bytea       NOT NULL UNIQUE DEFERRABLE INITIALLY DEFERRED CHECK (length(token_sha256) = 32),
        max_claims   integer     NOT NULL DEFAULT 0 CHECK (max_claims >= 0),
        egress_class text        NOT NULL DEFAULT '',
        updated_at   timestamptz NOT NULL DEFAULT now()
    );
    ```

  - Create it ahead with the same DDL when the node's role has no `CREATE` right.
  - Rows hold token hashes only, never a token.
- **Connection:**
  - Each node opens a pool for queries and one long-lived connection that
    runs `LISTEN sandboxd_tenants`.
  - The pool defaults to `max(4, CPUs)` connections; set `pool_max_conns` in
    the DSN (4 suffices) so a fleet stays within the database's
    `max_connections`.
  - LISTEN needs a session-mode connection, so a connection pooler in front
    of the database must not run in transaction mode for that DSN.
  - A write runs in one transaction with a `pg_notify`.
- **Outage:**
  - running claims, exec and egress keep working;
  - cached tenants authenticate;
  - uncached ones answer 503, and tenant writes fail;
  - recovery needs no restart.
- **Quota:** `max_claims` stays a per-node cap, read from the shared row.

### Shared pool set

With `meta_store.cell` set, the pool set applied through `PUT /v1/pools`
belongs to the node's cell rather than to the node: a `PUT` on any node of the
cell replaces the warm targets of every node in it. Each pool's `warm`,
`warm_max` and other targets apply per node, as they do with `pools.json`, so
every node of a cell runs the same targets. Nodes that differ in capacity or
attachment go in different cells. A caller that gives nodes different targets
— `Client.SetPoolsCluster` with per-node sets, or a controller that spreads a
replica count across nodes — needs nodes without a `cell`: in one cell its
per-node writes overwrite each other. Without `cell`, `meta_store` shares only
the tenant set.

- **Schema:** sandboxd creates the table at boot when it is missing; create it
  ahead with the same DDL when the node's role has no `CREATE` right:

  ```sql
  CREATE TABLE IF NOT EXISTS sandboxd_pool_sets (
      cell        text        PRIMARY KEY,
      config_seed text        NOT NULL DEFAULT '',
      pools       jsonb       NOT NULL,
      version     bigint      NOT NULL CHECK (version > 0),
      updated_at  timestamptz NOT NULL DEFAULT now()
  );
  ```

- **Writes:** a `PUT` commits the cell's row with a `NOTIFY` on
  `sandboxd_pool_sets`, then applies it on the node that took it. Every other
  node of the cell reloads the row on the notify and applies it, and also
  reloads it each time its listener reconnects, so a missed notify is caught
  up; a read that fails is retried every 5 s. A node applies a version only
  once and never an older one. A node the set does not fit, such as one
  without an egress attachment for an `egress` pool, logs a warning and keeps
  its pools, at runtime and at boot.
- **Boot:** a node applies the cell's row over the `pools` section of
  `config.json`. Until the cell's first `PUT`, nodes run their config pools,
  and a `pools.json` left from before the node joined the cell is ignored with
  a warning; `PUT` its set once to share it. A node with `meta_store` needs the
  database at boot, since both shared sets open it there. Each node still
  writes the set it applied to its `pools.json`, which it serves when the row
  read fails after the connection succeeds, and which it keeps if it leaves
  the cell.
- **Connection:** the pool set has its own query pool, idle between writes,
  and its own `LISTEN sandboxd_pool_sets` session, so each node holds two
  LISTEN sessions; count them in the database's `max_connections`.
- **Outage:** the applied pools keep serving and refilling; a `PUT` that
  cannot reach the database answers 503. Retry it: a `PUT` is a declarative
  replace, so a retry after a timeout that did commit changes nothing more.
- **Reverting** to config-owned pools is a `PUT` of the config pools; changing
  a node's `cell` takes a restart.

### Reloading the config

`kill -HUP <sandboxd pid>` or a root `POST /v1/config/reload`
([API](sandboxd-api.md#post-v1configreload)) re-reads `config.json`,
validates it with the same code as boot, and applies the reloadable settings
all at once or not at all. Every field falls into one of three classes:

- **Reloaded**:
  - per pool: `egress`, `warmup`, `capture_trim`, `storage`, `reader`, `egress_upstream_env`, for existing keys and for keys new in the file;
  - `egress_classes`: each class's `egress` and `egress_upstream_env`;
  - node-wide: `egress_internal_allow`, `secrets`, `egress_upstream`, `egress_usage_bytes`, `egress_conn_rate` and `disk_reserve`.
- **Left to their API**: a pool's targets (`warm`, `warm_max`, idle and archive
  durations) belong to `PUT /v1/pools`. A reload never changes them and lists
  them as `ignored`. A pool key new in the file gets its settings at once and stays
  at no warm VMs until `PUT /v1/pools` names it.
- **Restart only**: every other field. A reload that changes one is refused
  with the field names, and nothing applies.

Effects on what already runs:

- **Egress changes** reach live claims on their next request or connection;
  open tunnels keep their path. A reload that removes the pool policy live
  claims still use, by dropping the pool's entry or its `egress` block, is
  refused with the pool and the claim count: keep the entry until those claims
  are released. A claim whose tenant class a reload removes is denied
  everything. A claim armed while it had no policy has no egress proxy, so a
  policy a reload adds reaches it only after a release or a wake. A restart
  cannot refuse, so it logs a warning for each pool whose live claims have no
  policy and reach nothing, on a node that still holds any egress policy.
- **Armed once per claim**: the SOCKS5 door (`socks5`) and
  `egress_usage_bytes` apply to claims armed after the reload.
- **Golden rebuilds**: a change to a key's `warmup` or `storage`, or turning
  its interception on or off, retires that key's golden and destroys its warm
  VMs, so the next clone carries the new settings. Live claims and their VMs
  are untouched. A golden that was building during the reload is discarded
  and rebuilt.
- **Interception**: turning `intercept` on is refused for a key this node has
  already served (its guests do not trust the CA) and on a node that loaded
  no `egress_ca` at boot. Templates and checkpoints of the key captured
  elsewhere do not trust the CA either, so turn interception on under a new
  pool key. Changing which hosts an already-intercepting pool intercepts
  reloads normally.
- **Environment values**: `value_env` and `egress_upstream_env` name variables
  in sandboxd's own environment, which only a restart changes. A reload can
  point at a different variable, but only one that was already set when
  sandboxd started.

A successful reload writes an `op:"config_reload"` audit record listing what
changed. Each node reloads its own file; on a cluster, distribute the file and
reload every node.

## Verifying a node

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:7777/v1/info | jq .
```

The repository's `scripts/sandboxd-e2e.sh` runs the full loop on a real node
(golden build → warm pool → claim tiers → the complete verb smoke → reap →
restart reconcile); set `BRIDGE=<dev>` to include the egress lane.

To include the read-only volume proof, put a nonempty `volume-e2e.txt` in the
filesystem image and run `VOLUME_IMAGE=/srv/datasets/imagenet.img
scripts/sandboxd-e2e.sh`. The script verifies two concurrent mounts, read-only
enforcement, warm-pool consumption, and an unchanged source checksum. On a node
using prebuilt binaries, also set `VOLUME_SMOKE_BIN` beside `SANDBOXD_BIN`,
`DEMO_BIN`, and `SMOKE_BIN`.

Add `VOLUME_RW_IMAGE=/srv/datasets/scratch.img` (a second, writable image) to
also run the writable leg: a durable write across release, second-writer
exclusion, and a clean read-only claim afterward. `VOLUME_LIVE=1` adds the live
volume leg on cocoon v0.7.2 or newer.

## TLS for SDK clients

sandboxd serves plain HTTP behind a TLS-terminating proxy. Configure a stable
client origin on **every node** reachable through that proxy:

```json
{
  "listen": ":7777",
  "advertise_addr": "node-a.internal:7777",
  "client_advertise": "https://node-a.sandbox.example.com"
}
```

The mesh gossips both addresses. Client-facing owner replies, claim/template/
checkpoint redirects, and peer discovery use `client_advertise`. Checkpoint
probing, healing, deletion broadcasts, and preview forwarding continue to use
`advertise_addr` over internal HTTP. When `client_advertise` is unset, direct
HTTP deployments keep their existing address behavior. Configure all cluster
members before using external clients; a member without a client origin still
advertises its internal address to clients. Internal SDK users must also be
able to reach the configured client origins.

One proxy can serve the whole cluster, but each owner origin must route to
one particular node. An entry load balancer may choose any node for the initial
claim; a shared random-balancing owner origin cannot route later agent and
release requests to the owning node. Unlike Preview URLs, the SDK API does not
forward arbitrary sandbox requests between nodes.

Caddy reference configuration (node B uses the corresponding hostname and
internal upstream):

```caddyfile
node-a.sandbox.example.com {
    reverse_proxy node-a.internal:7777 {
        transport http {
            versions 1.1
        }
    }
}

node-b.sandbox.example.com {
    reverse_proxy node-b.internal:7777 {
        transport http {
            versions 1.1
        }
    }
}
```

For a private development CA, add `tls internal` to each site and provide
Caddy's root certificate to both SDKs using the
[TLS client settings](sdk.md#https-endpoints). For public DNS names, Caddy can
manage the certificates. Keep the upstream listeners and mesh private.

The edge must pass HTTP/1.1 `Connection: Upgrade`, `Upgrade: silkd`, and the
101 response, then relay both byte streams without response buffering. A
WebSocket-only upgrade allowlist is insufficient. Agent connections must not
negotiate HTTP/2. Configure stream/idle timeouts to exceed the longest relay;
Caddy's default stream timeout is unlimited. Configuration reloads may close
active streams; set `stream_close_delay` when reloads need a drain window.
The Upgrade tunnel does not need `flush_interval -1`.

The pinned Caddy integration runs both SDKs against two real sandboxd HTTP
handlers and relays (with a fake VM/guest), with an unreachable internal owner
address. It checks redirects, lookup, exec, port forwarding, half-close,
release, and certificate rejection:

```bash
cd e2e
CADDY_BIN=/path/to/caddy GOWORK=off go test -race -run TestCaddyTLSCluster -v .
```

For hardware acceptance, run the same SDK sequence from outside the node
network, including a guest HTTP server through `proxy_port`, and confirm an
A-to-B claim never dials B's internal address. A client-side TLS bridge alone
does not translate returned owners or redirects; use it only when every
returned endpoint is deliberately mapped through a local bridge.

## Preview URLs

`preview_listen` starts a second HTTP server that serves a sandbox's guest
HTTP port under a signed, expiring shareable URL. The whole mechanism is in
sandboxd:

- **Minting** (`sb.PreviewURL(ctx, port, ttl)`): the owner node signs a token
  embedding `{sandbox, port, owner, exp}` with `preview_secret`; the URL's
  life is clamped to the claim's lease.
- **Serving**: any node's preview listener verifies the token (no shared
  state), then reverse-proxies to the guest port over the relay if it owns
  the sandbox, or forwards over HTTP to the owner node's `advertise_addr`
  otherwise. A released sandbox is gone from the claim map, so its URL stops
  resolving — revocation is the liveness lookup, not a list.
- **The public entry point is a commodity dumb proxy.** Because any node
  can accept and forward, front the nodes with whatever terminates TLS and
  round-robins: a cloud HTTPS load balancer with a managed wildcard cert
  (GCP/AWS) in production, or a plain nginx/Caddy for self-hosting. It
  understands nothing about tokens — not sandboxd's code. Dev and e2e hit
  `preview_listen` directly over HTTP.
- **Sharing scope**: a preview URL is a bearer link, and its token payload
  (sandbox id, port, owner `advertise_addr`) is readable by whoever holds
  it — keep `advertise_addr` off browser-routable networks. All URLs under
  one `preview_advertise` share one browser origin, so different claims'
  apps are same-origin to the browser; workloads needing browser-side
  isolation need per-sandbox subdomains on the fronting proxy.
