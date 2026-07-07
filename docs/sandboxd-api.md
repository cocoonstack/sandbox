# sandboxd HTTP API

All bodies are JSON. Three token kinds:

- **root** — the node-level `api_token`: full access to every endpoint.
- **tenant** — a token from the `tenants` config list: accepted by the
  resource-creating verbs (claim, fork, promote, checkpoint create/claim,
  preview mint) and the tenant-scoped listings/deletes below; everything a
  tenant creates is stamped with its name. Operator surfaces
  (`GET /v1/sandboxes`, `GET /v1/info`, `PUT /v1/pools`, `GET /metrics`)
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
 "ttl_seconds": 300, "no_redirect": false}
```

- `net` defaults to `none`, `size` to `small`
- `ttl_seconds` 0 means the server default (5 minutes); capped at 24h. The
  owning node reaps the sandbox after the TTL even if the client vanishes
- `no_redirect` is set by the SDK when retrying at a redirect target

Success:

```json
{"id": "sb_…", "token": "…", "deadline": "2026-07-06T00:05:00Z",
 "owner_addr": "10.0.0.5:7777"}
```

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
requested on a node without an egress attachment; 429 node at `max_claims`
or the calling tenant at its own `max_claims` (a redirect to a warm peer is
tried first on a cluster); 500 provisioning failed.

## POST /v1/sandboxes/{id}/release

Auth: the sandbox's own token. Destroys the VM. 204 on success, 404 for an
unknown id or wrong token. Releasing an already-gone sandbox is 404 — the
SDK treats it as success.

## POST /v1/sandboxes/{id}/hibernate

Auth: the sandbox's own token. Atomically snapshots the VM and stops it,
freeing its memory; the next agent access restores it transparently
(sessions, processes, and memory state intact — cocoon's hibernate keeps the
snapshot point and the stop coincident). Idempotent on an already-hibernated
sandbox. The TTL keeps running: a hibernated sandbox is still reaped (VM and
snapshot) at its deadline. When to hibernate is the caller's policy — the
node only provides the transition. 204 on success, 404 unknown id or wrong
token.

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
or wrong sandbox token, 429 node or the parent's tenant at `max_claims`.

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
full key — templates are node-local, so a cluster client must claim from and
delete on this node, under exactly this key:

```json
{"key": {"template": "myproj:v1", "net": "none", "size": "small"}}
```

400 invalid name, 401 bad api token, 409 when the name collides with a
configured pool, 404 unknown id or wrong sandbox token.

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
token.

## POST /v1/checkpoints/{id}/claim

Auth: node API token; body `{"ttl_seconds": 0}`. Claims a fresh sandbox
branched from the checkpoint (a normal claim response, attributed to the
caller); the checkpoint's recorded key applies — the unguessable id is the
capability to branch. 404 for an unknown checkpoint, 429 node or calling
tenant at `max_claims`.

## GET /v1/checkpoints

Auth: node API token. Lists this node's checkpoints, newest first. A tenant
sees only its own records; root sees everything.

## DELETE /v1/checkpoints/{id}

Auth: node API token. A tenant may delete only its own records — anything
else is 404, never a hint the id exists; root deletes anything. 204 on
success, 404 unknown.

## GET /v1/sandboxes

Auth: root only (tenant tokens get 403). The operator index: `{"sandboxes":
[{id, key, deadline, hibernated, from_checkpoint?}]}` — never tokens.

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
"claim|hibernate|wake|fork|checkpoint|promote|release|reap", "id": "sb_…",
"vm": "sbx-…"}` plus `key` and `tenant` (the pool key and owning tenant,
claim events), `children` (fork) and `ref` (the promoted template /
checkpoint id). The file rotates at
64 MiB keeping one `.1` backup, so a tailing collector never loses a window
silently. Folding rules: billable compute seconds per sandbox =
Σ(claim→release/reap) − Σ(hibernate→wake); hibernated storage seconds =
Σ(hibernate→wake); an interval left open by a crash clamps to the claim's
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
 "peers": ["10.0.0.6:7777"]}
```

`hibernated` counts claims whose VM is currently hibernated (included in
`claimed`).

`golden` reports whether the pool's snapshot exists (refill can clone);
`warm` at `target` with `golden: true` means warm claims are served in
sub-millisecond time.

## GET /healthz

Unauthenticated liveness probe; answers `ok`.

## Operational notes

- The HTTP server must keep `ReadTimeout`/`WriteTimeout` at zero if you
  front it with your own server: cold claims legitimately block for the cold
  probe window and relays stream indefinitely. sandboxd itself uses
  `ReadHeaderTimeout` for slowloris protection.
- Shutdown force-closes in-flight relays and leaves VMs running for the next
  reconcile.
