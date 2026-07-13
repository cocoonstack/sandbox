# Deploying sandboxd

sandboxd is a single static binary per node. It drives VM lifecycle through
the cocoon CLI and needs a template image with silkd baked in.

## Prerequisites

- Linux with KVM (`/dev/kvm`)
- [cocoon](https://github.com/cocoonstack/cocoon) installed and working
  (`cocoon vm run` boots a VM), with Cloud Hypervisor and/or Firecracker
- The sandbox boot artifact installed where cocoon finds it
  (`/boot/vmlinuz-sandbox`, `/boot/initrd.img-sandbox` — from
  `ghcr.io/cocoonstack/sandbox/boot:<kernel-ver>`)
- A silkd-baked template image, e.g. `ghcr.io/cocoonstack/sandbox/base:24.04`
  (pull via cocoon, or `cocoon image import` a tar)

Prebuilt static linux/amd64 binaries (`sandboxd`, `sandbox-mcp`, `silkd`,
with `checksums.txt`) ship with every
[GitHub release](https://github.com/cocoonstack/sandbox/releases). Build
from source with `make sandboxd` (produces `dist/sandboxd`); either way
`sandboxd -version` reports what you are running.

## Configuration

sandboxd reads one JSON file (`-config`, default
`/etc/sandboxd/config.json`):

```json
{
  "listen": ":7777",
  "data_dir": "/var/lib/sandboxd",
  "cocoon_bin": "cocoon",
  "advertise_addr": "10.0.0.5:7777",
  "bridge": "br0",
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

| field | default | meaning |
|---|---|---|
| `listen` | `:7777` | control- and data-plane HTTP listener |
| `data_dir` | `/var/lib/sandboxd` | golden snapshot exports, the claims journal, the usage/audit journals (`usage.jsonl`, `audit.jsonl` + `.1` backups), and `checkpoints/` by default |
| `cocoon_bin` | `cocoon` | cocoon CLI binary |
| `advertise_addr` | = `listen` | the host:port clients reach this node at; returned as a claim's owner address and gossiped to peers. Must be routable when `listen` is a wildcard |
| `bridge` / `network` | unset | egress-lane attachment: a host bridge device, or a CNI conflist name. Mutually exclusive; with neither set the node serves only the no-network lane. [Guarded egress](egress.md) needs the bridge form and rejects a CNI network at load |
| `egress_ca` | unset | [HTTPS-interception](egress.md#https-interception) PKI: `root_cert` (the cluster root baked into intercepted guests; may bundle old+new roots during rotation) plus this node's `intermediate_cert`/`intermediate_key` from `sandboxd ca issue-intermediate`. Required when any pool rule sets `intercept` |
| `api_token` | unset | the operator (root) credential: when set, guards the node-level endpoints (Bearer) with full access. Per-sandbox tokens guard sandbox-scoped calls regardless |
| `tenants` | unset | multi-tenant tokens next to `api_token`: `[{"name": "acme", "token": "…", "max_claims": 50}]`. A tenant token reaches the resource-creating verbs (claim, fork, promote, checkpoint, preview) and everything it creates is stamped with the tenant name; operator surfaces (`GET /v1/sandboxes`, `GET /v1/info`, `PUT /v1/pools`, `POST/DELETE /v1/drain`, `/metrics`) answer it 403. `max_claims` (0 = unlimited) caps that tenant's live claims next to the node-wide cap. Requires `api_token` set (operator surfaces need it). Names and tokens must be unique, tokens distinct from `api_token`. On a cluster all nodes must carry the same tenants set (the SDK replays a tenant token across a redirect; a peer missing that tenant answers 401), and per-node caps mean a tenant's effective cluster limit is `max_claims` × nodes. Empty = exactly the single-token behavior |
| `max_fork_count` | 16 | children a single `fork` may create; each is a full-RAM VM, so this bounds one request's memory blast radius to the node's capacity |
| `preview_listen` | (off) | address for a preview HTTP server that serves guest ports under signed URLs; needs `preview_secret` |
| `preview_secret` | — | cluster-shared HMAC secret signing preview tokens (all nodes share one) |
| `preview_advertise` | = `preview_listen` | the base URL a browser/proxy reaches this node's preview server at |
| `checkpoint_dir` | `<data_dir>/checkpoints` | where checkpoints and promoted templates live. Point it at a shared FUSE mount (JuiceFS over object storage, NFS) and every node sharing the mount can branch every checkpoint — the path's filesystem is the operator's choice. One contract on any shared root (mount or bucket): a template key has a single writer — promotes go to the sandbox's owner node, and operators must not race promotes of one name from different nodes (checkpoint ids are node-generated and never collide). A checkpoint deleted on one node while another is mid-branch from it fails that branch visibly |
| `checkpoint_store` | dir | checkpoint AND promoted-template backend (both live in one store root, id-namespaced ck_/tp_): `{"kind": "s3", "s3": {"bucket": "…", "prefix": "ck/", "endpoint": "…", "region": "…", "force_path_style": true}}` stores checkpoints in object storage (any node claims any checkpoint, no shared mount needed). Credentials come from the standard AWS chain (env/IAM role), never this file. A crash between upload and the meta.json commit marker leaves orphan objects invisible to listings — add an S3 lifecycle rule to reclaim them. Absent = the dir backend at `checkpoint_dir` |
| `checkpoint_ttl_hours` | 0 (keep forever) | ages out checkpoints older than this; the sweep runs hourly and at startup. Explicit deletes never wait for it |
| `warm_max` (pool entry) | 0 (static) | turns on the demand-adaptive watermark for that pool: the warm target rises from `warm` toward `warm_max` while claims arrive faster than the measured provision lead covers, and decays back over ~a minute of silence |
| `max_claims` | 0 (unlimited) | node-wide cap on live claims; claim/fork/branch requests beyond it answer 429 with the pool state unharmed (on a cluster, a claim is first redirected to a warm peer) |
| `audit_log` | false | append every relayed request frame's op + addressing fields (never payloads) to `<data_dir>/audit.jsonl`, size-rotated with one `.1` backup. Records are `{t, id, op}` plus whichever addressing fields the op carries (`argv`, `path`, `dest`, `from`, `to`, `url`, `session`, `port`); preview accesses record as op `preview_dial`. A request frame whose first line exceeds 4 KiB is skipped, never truncated |
| `idle_hibernate_seconds` | 0 (off) | node-wide idle policy for unpooled claims (template/checkpoint claims): a claim with no data-plane connection for this long is hibernated; the next call wakes it transparently. Per-pool `idle_hibernate_seconds` (in a pool entry) does the same for that pool's claims — pooled keys ignore the node-wide value. Opt-in deliberately: a wake costs latency and the snapshot, so callers with their own idle logic must not pay twice |
| `archive_after_seconds` | 0 (off) | tier below hibernation: a hibernated claim idle this long is checkpointed to the store and its local VM dropped, freeing the node entirely; the next call restores it transparently (a checkpoint restore's latency). Requires `idle_hibernate_seconds > 0` and must exceed it. Node-wide for unpooled keys; per-pool overrides for that pool |
| `archive_delete_after_seconds` | 0 (keep) | purge an archived claim's store checkpoint this long after it was archived, reclaiming storage; the claim is then gone for good. Same node-wide/per-pool split |
| `mesh` | unset | join a cluster ([Clusters](cluster.md)); unset = single node |
| `pools[]` | — | warm pools. `warm` defaults to 4; `net` is `none` or `egress`; `size` is a tier, below. Retune online without a restart via [`PUT /v1/pools`](sandboxd-api.md#put-v1pools) — omitted pools drain. This is the **first-boot seed**: once a node takes a `PUT /v1/pools`, the applied set persists to `<data_dir>/pools.json` and overrides this section on every later boot (a startup log notes it); delete `pools.json` to return to config-owned pools. Egress stays config-owned either way. See [state ownership](cluster.md#state-ownership) |

Size tiers (free-form CPU/memory is deliberately not accepted — it would
fragment the warm pools):

| size | CPU | memory |
|---|---|---|
| `small` | 1 | 512M |
| `medium` | 2 | 1G |
| `large` | 4 | 4G |
| `xlarge` | 4 | 8G |

### A fuller config

The block above is the minimum. A production node with tenants, guarded
egress + HTTPS interception, previews, an object-store checkpoint backend,
the idle→hibernate→archive tiers, and a mesh looks like this — every field
here validates on load:

```json
{
  "listen": ":7777",
  "data_dir": "/var/lib/sandboxd",
  "advertise_addr": "10.0.0.5:7777",
  "bridge": "br0",

  "api_token": "op-root-token",
  "tenants": [
    {"name": "acme", "token": "acme-token", "max_claims": 50}
  ],

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
  "idle_hibernate_seconds": 300,
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
     "idle_hibernate_seconds": 120, "archive_after_seconds": 900,
     "egress": {"allow": [
       {"host": "api.github.com", "methods": ["GET", "POST"], "secret": "gh", "intercept": true},
       {"host": "*.googleapis.com"}
     ]}}
  ]
}
```

Secret values come from the environment named by `value_env` (here
`GH_TOKEN`), never the file. The `egress_ca` files are provisioned with
`sandboxd ca` — see [Guarded egress](egress.md#https-interception). On a
cluster, `api_token`, `tenants`, `preview_secret`, and `cluster_key` must
match on every node.

### Auth model

Three token kinds. The root `api_token` has full access — operators and
single-tenant deployments need nothing else. Tenant tokens (the `tenants`
list) create and manage their own resources: claims, forks, checkpoints,
promoted templates, and preview URLs are stamped with the tenant name;
checkpoint listings filter to the caller's tenant, and a tenant can delete
only its own checkpoints and templates (root sees and deletes everything).
Operator surfaces stay root-only — a tenant token there is authenticated but
not authorized, so it answers 403 (a wrong token stays 401). Per-sandbox
tokens are unchanged: whoever holds a sandbox's token drives that sandbox.
Fork children inherit the parent's tenant and count against its
`max_claims`; a tenant at its cap gets 429 exactly like a node at
`max_claims`, and the usage journal's claim events carry the tenant for
per-tenant billing.

## Running

```bash
sandboxd -config /etc/sandboxd/config.json
```

On start the node reconciles: persisted claims whose VMs still run are
re-adopted, everything else `sbx-`-prefixed is removed. Then the refill loop
builds one golden snapshot per pool (a one-time cold boot + snapshot export,
tens of seconds) and keeps each pool topped up with claim-ready clones.
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
Environment=SANDBOXD_LOG_LEVEL=info

[Install]
WantedBy=multi-user.target
```

Stopping sandboxd leaves VMs alive; the next start reconciles them. Claimed
sandboxes are reaped when their TTL expires (default 5m, capped at 24h).

To empty a node for maintenance, cordon it first:
[`POST /v1/drain`](sandboxd-api.md#post-v1drain) (root) stops new claims and
drains the warm pools; poll `GET /v1/info` until `claimed` reaches zero (or
let the leases expire), then stop sandboxd. `DELETE /v1/drain` uncordons.
The drain is not persisted — a restarted node serves again.

## Verifying a node

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:7777/v1/info | jq .
```

The repository's `scripts/sandboxd-e2e.sh` runs the full loop on a real node
(golden build → warm pool → claim tiers → the complete verb smoke → reap →
restart reconcile); set `BRIDGE=<dev>` to include the egress lane.

## Preview URLs

`preview_listen` starts a second HTTP server that serves a sandbox's guest
HTTP port under a signed, expiring shareable URL. The whole mechanism is in
sandboxd:

- **Minting** (`sb.PreviewURL(port, ttl)`): the owner node signs a token
  embedding `{sandbox, port, owner, exp}` with `preview_secret`; the URL's
  life is clamped to the claim's lease.
- **Serving**: any node's preview listener verifies the token (no shared
  state), then reverse-proxies to the guest port over the relay if it owns
  the sandbox, or forwards to the owner node otherwise. A released sandbox
  is gone from the claim map, so its URL stops resolving — revocation is
  the liveness lookup, not a list.
- **The public entry point is a commodity dumb proxy.** Because any node
  can accept and forward, front the nodes with whatever terminates TLS and
  round-robins: a cloud HTTPS load balancer with a managed wildcard cert
  (GCP/AWS) in production, or a plain nginx/Caddy for self-hosting. It
  understands nothing about tokens — not sandboxd's code. Dev and e2e hit
  `preview_listen` directly over HTTP.
