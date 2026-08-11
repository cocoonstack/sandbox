# sandboxd HTTP API

All bodies are JSON. Three token kinds:

- **root** — the node-level `api_token`: full access to every endpoint.
- **tenant** — a token from the `tenants` config list: accepted by the
  resource-creating verbs (claim, fork, promote, checkpoint create/claim,
  preview mint) and the tenant-scoped listings/deletes below; everything a
  tenant creates is stamped with its name. Operator surfaces
  (`GET /v1/sandboxes` and the per-id reads under it, `GET /v1/info`,
  `PUT /v1/pools`, `POST/DELETE /v1/drain`, `GET /metrics`,
  `GET /v1/checkpoints/{id}/blob`)
  answer a tenant token `403` — authenticated but not authorized; an unknown
  token stays `401`.
- **sandbox** — every claimed sandbox carries its own bearer token guarding
  the sandbox-scoped endpoints, regardless of the other two.

Endpoints below that say "node API token" accept root or tenant unless
marked root-only. Errors are `{"error": "message"}` with the status codes
listed per endpoint.

## POST /v1/claim

Auth: `Authorization: Bearer <api_token>` (when configured).

```json
{"template": "base:24.04", "net": "none", "size": "small",
 "ttl_seconds": 300, "claim_ref": "namespace/workload", "no_redirect": false}
```

- `net` defaults to `none`, `size` to `small`
- `ttl_seconds` 0 means the server default (5 minutes); capped at 24h. The
  owning node reaps the sandbox after the TTL even if the client vanishes
- `claim_ref` is an optional opaque caller reference echoed by the root-only
  sandbox index; the aggregated apiserver uses `<namespace>/<name>`
- `no_redirect` is set by the SDK when retrying at a redirect target

Success:

```json
{"id": "sb_…", "token": "…", "deadline": "2026-07-06T00:05:00Z",
 "owner_addr": "10.0.0.5:7777", "template_digest": "sha256:…"}
```

A claim cloned from a promoted template carries `template_digest`, the exact
export generation fetched for that clone. It is absent for configured pools,
cold image boots, forks, checkpoints, and templates published by an older
sandboxd until they are re-promoted.

A claim branched from a checkpoint (fork children included) additionally
carries `"from_checkpoint": "ck_…"` — the lineage edge for reconstructing
the checkpoint tree.

Redirects (mutually exclusive with the fields above) name peers to retry
at — sent on a warm miss with warm peers, when the node lacks a golden for
the key but gossip names a template owner, and when the node is at
`max_claims` but a peer reports warm capacity:

```json
{"redirect": ["10.0.0.6:7777", "10.0.0.7:7777"]}
```

Retry the same body (+`no_redirect: true`) at each candidate until one
answers.

A tenant token claims the same way; the sandbox is stamped with the tenant
name (attributed in the usage journal and counted against the tenant's
`max_claims`).

Errors: 400 unknown template axis / bad body; 401 bad api token; 409 egress
requested on a node without an egress attachment; 429 node at `max_claims`,
the calling tenant at its own `max_claims`, or the node draining (a redirect
to a warm peer is tried first on a cluster); 500 provisioning failed.

## POST /v1/sandboxes/{id}/release

Auth: the sandbox's own token, or the root token for operator cleanup by id.
Destroys the VM. 204 on success, 404 for an unknown id or wrong token.
Releasing an already-gone sandbox is 404 — the SDK treats it as success.

## POST /v1/sandboxes/{id}/hibernate

Auth: the sandbox's own token, or the root token by id (operator, like
release). Atomically snapshots the VM and stops it,
freeing its memory; the next agent access restores it transparently
(sessions, processes, and memory state intact — cocoon's hibernate keeps the
snapshot point and the stop coincident). Idempotent on an already-hibernated
sandbox. The TTL keeps running: a hibernated sandbox is still reaped (VM and
snapshot) at its deadline. When to hibernate is the caller's policy — the
node only provides the transition. 204 on success, 404 unknown id or wrong
token, 409 on the egress lane (egress-lane sandboxes never hibernate; see
[egress](egress.md)).

## POST /v1/sandboxes/{id}/wake

Auth: the sandbox's own token, or the root token by id (operator). Restores
a hibernated (or archived) sandbox and leaves it running — waking is
otherwise only a side effect of the next agent access, so this is the
explicit form for warming a sandbox ahead of use. Idempotent on one already
running. 204 on success, 404 unknown id or wrong token.

## POST /v1/sandboxes/{id}/fork

Auth: the node `api_token` (Bearer) — forking creates node resources, like a
claim. The sandbox's own token rides in the body as the ownership proof:

```json
{"token": "…", "count": 2, "ttl_seconds": 300}
```

Clones the sandbox into `count` fresh claims (1 up to the node's
`max_fork_count`, default 16). Memory, disk, and guest
state (sessions, processes, tmpfs) duplicate at the fork point; cocoon's
clone reseed gives every child a distinct machine identity. Children get
their own lease — `ttl_seconds` (0 = server default), never the parent's
remainder. A running parent is snapshotted in a brief pause window; a
hibernated parent forks from its existing memory image without waking.
All-or-nothing: on error no child survived. 200 with one claim per child:

```json
{"children": [{"id": "sb_…", "token": "…", "deadline": "…", "owner_addr": "…"}]}
```

Children inherit the parent's tenant and count against its `max_claims`,
whoever calls. 400 invalid count or body, 401 bad api token, 404 unknown id
or wrong sandbox token, 409 egress-lane parent (the lane never forks,
checkpoints, or promotes; see [egress](egress.md)), 429 node or the parent's
tenant at `max_claims`, or the node draining.

## POST /v1/sandboxes/{id}/promote

Auth: like fork — node `api_token` in the header, the sandbox's own token in
the body:

```json
{"token": "…", "template": "myproj:v1"}
```

Publishes the sandbox's current state as a node-local template under
(template, this sandbox's net, its size): later claims for that key clone
from it, provision-on-demand — no warm pool unless the node config adds one.
Re-promoting to the same name replaces the template. A hibernated sandbox is
promoted from its memory image without waking. 200 returns the template's
full key and immutable content identity. On the default local-disk backend a
template is node-local, so a cluster client claims from and deletes on this
node (name-based calls route via gossip); a shared checkpoint store makes
every node resolve it. Under exactly this key:

```json
{"key": {"template": "myproj:v1", "net": "none", "size": "small"},
 "content_digest": "sha256:…"}
```

`content_digest` is SHA-256 over a versioned canonical stream of the published
export's regular files: slash-relative path, byte length, and bytes, ordered
lexically. Directory entries, modes, mtimes, and the template's ownership/
creation metadata do not affect it. The digest is computed once while
promoting, stored in `meta.json`, and therefore has identical semantics on the
directory and S3 backends. Re-promoting unchanged export bytes keeps the
digest; changing any exported path or bytes changes it.

400 invalid name, 401 bad api token, 409 when the name collides with a
configured pool, the template is owned by another tenant, or the sandbox is
on the egress lane (see [egress](egress.md)), 404 unknown id or wrong
sandbox token.

## DELETE /v1/templates?template=…&net=…&size=…

Auth: node API token. Removes a promoted template (the query parameters
default like a claim's: `net=none`, `size=small`). A tenant may delete only
templates it promoted — anything else is 404, root deletes anything. 204 on
success, 404 unknown template, 409 when the key belongs to a configured pool
(those goldens are owned by the node config). On a cluster, a node that does not
hold the template but sees an owner in gossip answers `200
{"redirect": [addrs]}` — the claim redirect shape — and the SDK retries the
delete at the owner. The retry carries `no_redirect=1`, mirroring the claim
protocol: a node answering a `no_redirect` delete speaks only for itself.

## PUT /v1/pools

Auth: root only (tenant tokens get 403). Replaces the node's desired warm
targets online — no restart, live claims untouched:

```json
{"pools": [{"template": "base:24.04", "net": "none", "size": "small",
            "warm": 4, "warm_max": 16, "idle_hibernate_seconds": 0}]}
```

Pools omitted from the list are drained: their unclaimed warm VMs are
destroyed and the pool entry retires. `net`/`size` default like a claim's.
Answers the fresh `GET /v1/info` payload. 400 bad key, negative warm/idle,
`warm_max` below `warm`, or duplicate pool; 401 bad api token; 409 egress
pool on a node without an egress attachment.

## POST /v1/drain

Auth: root only (tenant tokens get 403). Cordons the node for maintenance: claim/fork/branch answer
429 `node draining` (on a cluster the warm-peer redirect is tried first, and
gossip stops naming this node within a tick as its warm counts hit zero),
unclaimed warm VMs are destroyed, and live claims keep serving until release
or TTL. Pool ownership is untouched — no pools.json write, no config change.
Answers the fresh `GET /v1/info` payload; poll `claimed` to zero to know the
node is empty. Deliberately not persisted: a restarted node serves again.

## DELETE /v1/drain

Auth: root only (tenant tokens get 403). Lifts the drain and kicks an immediate refill. Answers the
fresh info payload.

## POST /v1/sandboxes/{id}/preview

Auth: like fork — node `api_token` in the header, the sandbox's own token
in the body. Mints a signed URL serving a
guest HTTP port from a browser: body `{"token": "...", "port": 8080,
"ttl_seconds": 0}` → `{"url": "http://<preview_advertise>/p/<token>/"}`.
The URL's life is clamped to the claim's remaining lease. 501 when the node
has no `preview_listen`. The signed token embeds the sandbox id, port, and
owner node, so any node's preview listener can serve it (forwarding to the
owner) and a released sandbox's URL simply stops resolving — no revocation
list. See [deploy](deploy.md#preview-urls).

## POST /v1/sandboxes/{id}/checkpoint

Auth: node API token; body `{"token": "<sandbox token>", "name": "..."}`
(name optional). Captures the sandbox's full state without stopping it and
answers `200 {"checkpoint": {id, name, sandbox_id, key, tenant?,
created_at}}` — `tenant` records the calling tenant, absent for root.
400 bad body or name, 401 bad api token, 404 unknown id or wrong sandbox
token, 409 egress-lane sandbox (see [egress](egress.md)).

## POST /v1/checkpoints/{id}/claim

Auth: node API token; body `{"ttl_seconds": 0, "no_redirect": false}`.
Claims a fresh sandbox branched from the checkpoint (a normal claim
response, attributed to the caller); the checkpoint's recorded key applies
— the unguessable id is the capability to branch.

Checkpoints are node-local (unless the store is shared — see
[Configuration](deploy.md#configuration)), so a miss here runs a tier order:

1. This node checks its own store first.
2. On a miss, it probes up to 3 peers directly — a parallel `HEAD` to each
   (authenticated on an encrypted mesh, see below) — and answers exactly like
   a warm-miss
   [`POST /v1/claim`](#post-v1claim): `200 {"redirect": ["10.0.0.6:7777",
   "10.0.0.7:7777"]}`, retry the same body (+`no_redirect: true`) at each
   candidate until one answers. The probe and the follow-up claim are not
   atomic: a peer can answer the probe, then lose the record — a delete's
   broadcast lands, or its own TTL sweep runs (below) — before the retry
   reaches it, so a redirect can go stale between the two calls.
3. If nothing answers the probe (or `no_redirect` is set), and the node has
   `checkpoint_peer_heal` enabled, it pulls the record from a probed peer
   itself, validates it, publishes it locally, and serves the claim from
   there — paid once per node. See the full
   [placement lifecycle](cluster.md#checkpoints-on-a-cluster) for how the
   three tiers fit together.

404 for an unknown checkpoint (locally, and after redirect and heal both
miss), 409 for an egress-lane checkpoint (see [egress](egress.md)), 429 node
or calling tenant at `max_claims` or the node draining, 503 when the node's
concurrent-heal cap is already full — retryable, and the response carries a
`Retry-After` hint.

## GET/HEAD /v1/checkpoints/{id}/blob

The peer-transfer route behind the probe and heal above — internal, not
part of the public API; an SDK caller has no reason to call it directly.

- `GET` streams the whole record — guest memory, disk, and meta — as a tar.
  Operator-token only: the stream carries no tenant scoping, and its only
  real caller is a peer's heal pull, which presents the fleet `api_token`.
  401 missing or unrecognized token, 403 a valid tenant token (authenticated
  but not the operator), 404 unknown checkpoint.
- `HEAD` is the ownership probe: 200 when this node holds a branchable
  (non-archive) copy, 404 otherwise. On a mesh with `cluster_key` set the
  request must carry `X-Cocoon-Probe`, an HMAC over the id and a coarse time
  bucket keyed off a probe-specific derivation of the cluster key — verified
  before any disk is touched, replayable for roughly a minute at most. On a
  keyless mesh (redirect-only fleets have no shared secret to sign with) the
  id itself remains the only capability, matching the mesh's own posture.
  Repeat probes for one id are answered from a short positive cache on the
  asking node, which a local delete evicts immediately.

## GET /v1/checkpoints

Auth: node API token. Lists this node's checkpoints, newest first. A tenant
sees only its own records; root sees everything.

## DELETE /v1/checkpoints/{id}

Auth: node API token. A tenant may delete only its own records — anything
else is 404, never a hint the id exists; root deletes anything. 204 on
success, 404 unknown.

**Delete removes the local record and then best-effort broadcasts to peers
so a healed replica does not outlive it — this is eventual best-effort
cleanup, not a fleet-wide revocation.** A peer that is offline or
partitioned during the broadcast keeps its copy until the checkpoint TTL
ages it out. A healed replica carries the source's original `CreatedAt`, so
it becomes eligible for expiry at the same instant on every node; the actual
removal is each node's own hourly sweep, which is independently phased and
retries on a later sweep if one fails. So a deleted checkpoint normally stops
being branchable within `checkpoint_ttl_hours` plus a sweep interval, but a
node whose sweeps keep failing holds its replica until one succeeds — the TTL
is the eligibility point, not a hard ceiling. The TTL must also match
fleet-wide, which the
[cluster-invariant config](cluster.md#cluster-invariant-config) digest
checks. A window always exists because `checkpoint_peer_heal` cannot be
enabled with `checkpoint_ttl_hours: 0` — a replica that can outlive a delete
must have a finite eligibility point. A shared
checkpoint store skips the broadcast: every node already resolves every
record directly, so there is no replica to chase. `?no_forward=1` marks a
delete already arriving from another node's own broadcast, so it is not
itself re-broadcast (loop prevention); it is an internal parameter, not one
an SDK caller should set.

## GET /v1/sandboxes

Auth: root only (tenant tokens get 403). The operator index: `{"sandboxes":
[{id, key, deadline, hibernated, archived?, from_checkpoint?, claim_ref?}]}` —
never tokens.

## GET /v1/sandboxes/{id}

Auth: root only. One live claim in the index-row shape above, so a
reconcile loop can read a single sandbox without scanning the whole node
listing. 404 unknown id.

## GET /v1/sandboxes/{id}/stats

Auth: root only. One sandbox's resource usage — the per-sandbox counterpart
to the node-scoped `/metrics`:

```json
{"id": "sb_…", "cpu_count": 2, "mem_total_bytes": 1073741824,
 "mem_used_bytes": 187654144, "mem_used_measured": true,
 "hibernated": false, "measured_at": "…"}
```

`cpu_count`/`mem_total_bytes` come from the size tier. `mem_used_bytes` is
the host VMM process's resident set — the only usage signal available
without a guest agent; `mem_used_measured` is false when there is no VMM
process to read (hibernated, or the PID is not yet known), so a zero is
never mistaken for idle. 404 unknown id.

## GET /metrics

Auth: root only (tenant tokens get 403). Prometheus text format,
hand-rendered: pool warm/target gauges, claimed/hibernated gauges, a
per-tenant live-claim gauge (`sandboxd_tenant_claims{tenant="…"}`,
configured tenants only), claims by tier (warm/clone/cold),
wake/hibernate/fork/checkpoint/promote/release/reap counters, and claim/wake
`*_seconds_total` for average latency. /metrics is a derived ops view; the
billing source of truth is the usage journal below.

## Usage journal (usage.jsonl)

Always on: every lifecycle transition appends one JSONL event to
`<data_dir>/usage.jsonl` — `{"t": <RFC3339>, "ev":
"claim|hibernate|wake|fork|checkpoint|promote|release|reap|archive|unarchive|archive_delete|egress",
"id": "sb_…", "vm": "sbx-…"}` plus `key` and `tenant` (the pool key and
owning tenant, claim events), `children` (fork) and `ref` (the promoted
template / checkpoint id, or the egress host). The file rotates at
64 MiB keeping one `.1` backup, so a tailing collector never loses a window
silently. Folding rules: billable compute seconds per sandbox =
Σ(claim→release/reap) − Σ(hibernate→wake); hibernated storage seconds =
Σ(hibernate→wake) minus the archived span; archived storage seconds =
Σ(archive→unarchive/archive_delete), a cheaper store tier than a hibernated
VM's RAM. An interval left open by a crash clamps to the claim's
deadline, and the next reconcile emits `reap` for claims it drops. The `vm`
name joins cocoon's machine-level metering ledger for audit cross-checks.

## GET /v1/sandboxes/{id}/agent

Auth: the sandbox's own token. Requires `Upgrade: silkd` +
`Connection: Upgrade`; answers `101 Switching Protocols` and from then on
the connection is a byte-for-byte relay to the guest's silkd (one silkd RPC
per connection — see [silkd](silkd.md)). 426 without the upgrade header, 404
unknown sandbox, 502 guest unreachable.

## GET /v1/sandboxes/{id}/owner

Auth: the sandbox's own token. Answers `{"owner_addr": "host:port"}` when
this node owns the sandbox, 404 otherwise. Used by the SDK's `Lookup`
scatter.

## GET /v1/info

Auth: root only (tenant tokens get 403). Node pools, claim count, and mesh
peers:

```json
{"pools": [{"key": {"template": "base:24.04", "net": "none", "size": "small"},
            "warm": 4, "refilling": 0, "target": 4, "golden": true}],
 "claimed": 2,
 "hibernated": 1,
 "archived": 0,
 "peers": ["10.0.0.6:7777"]}
```

`hibernated` counts claims whose VM is currently hibernated, `archived` those
checkpointed to the store with the local VM dropped (see
[archive tiers](deploy.md#configuration)); both are included in `claimed`.
A node cordoned via [`POST /v1/drain`](#post-v1drain) additionally reports
`"draining": true`.

`golden` reports whether the pool's snapshot exists (refill can clone);
`warm` at `target` with `golden: true` means warm claims are served in
sub-millisecond time.

## GET /v1/peers

Auth: node API token (root **or** tenant). Answers `{"peers": [addr, …]}`
— the cluster's other node addresses, for the SDK's redirect follow and
`Lookup` scatter. Cluster topology, not operator state, so a tenant token
may read it (unlike `GET /v1/info`).

## GET /healthz

Unauthenticated liveness probe; answers `ok`.

## Operational notes

- The HTTP server must keep `ReadTimeout`/`WriteTimeout` at zero if you
  front it with your own server: cold claims legitimately block for the cold
  probe window and relays stream indefinitely. sandboxd itself uses
  `ReadHeaderTimeout` for slowloris protection.
- Shutdown force-closes in-flight relays and leaves VMs running for the next
  reconcile.
