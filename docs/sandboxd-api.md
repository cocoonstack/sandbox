# sandboxd HTTP API

All bodies are JSON. Two token kinds: the node-level `api_token` guards
claim and info; every claimed sandbox carries its own bearer token guarding
the sandbox-scoped endpoints. Errors are `{"error": "message"}` with the
status codes listed per endpoint.

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

Warm miss on a cluster node with warm peers (mutually exclusive with the
fields above):

```json
{"redirect": ["10.0.0.6:7777", "10.0.0.7:7777"]}
```

Retry the same body (+`no_redirect: true`) at each candidate until one
answers.

Errors: 400 unknown template axis / bad body; 401 bad api token; 409 egress
requested on a node without an egress attachment; 500 provisioning failed.

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

Clones the sandbox into `count` fresh claims (1–16). Memory, disk, and guest
state (sessions, processes, tmpfs) duplicate at the fork point; cocoon's
clone reseed gives every child a distinct machine identity. Children get
their own lease — `ttl_seconds` (0 = server default), never the parent's
remainder. A running parent is snapshotted in a brief pause window; a
hibernated parent forks from its existing memory image without waking.
All-or-nothing: on error no child survived. 200 with one claim per child:

```json
{"children": [{"id": "sb_…", "token": "…", "deadline": "…", "owner_addr": "…"}]}
```

400 invalid count or body, 401 bad api token, 404 unknown id or wrong
sandbox token.

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
default like a claim's: `net=none`, `size=small`). 204 on success, 404
unknown template, 409 when the key belongs to a configured pool (those
goldens are owned by the node config). On a cluster, a node that does not
hold the template but sees an owner in gossip answers `200
{"redirect": [addrs]}` — the claim redirect shape — and the SDK retries the
delete at the owner. The retry carries `no_redirect=1`, mirroring the claim
protocol: a node answering a `no_redirect` delete speaks only for itself.

## POST /v1/sandboxes/{id}/checkpoint

Auth: node API token; body `{"token": "<sandbox token>", "name": "..."}`
(name optional). Captures the sandbox's full state without stopping it and
answers `200 {"checkpoint": {id, name, sandbox_id, key, created_at}}`.

## POST /v1/checkpoints/{id}/claim

Auth: node API token; body `{"ttl_seconds": 0}`. Claims a fresh sandbox
branched from the checkpoint (a normal claim response); the checkpoint's
recorded key applies. 404 for an unknown checkpoint.

## GET /v1/checkpoints

Auth: node API token. Lists this node's checkpoints, newest first.

## DELETE /v1/checkpoints/{id}

Auth: node API token. 204 on success, 404 unknown.

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

Auth: api token. Node pools, claim count, and mesh peers:

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
