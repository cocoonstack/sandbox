# sandboxd HTTP API

All bodies are JSON. Three token kinds:

- **root** — the node-level `api_token`: full access to every endpoint.
- **tenant** — a token the [tenant API](#tenants-v1tenants) added: accepted by the
  resource-creating verbs (claim, fork, promote, checkpoint create/claim,
  preview mint) and the tenant-scoped listings/deletes below; everything a
  tenant creates is stamped with its name. Operator surfaces
  (the per-id sandbox reads, `PUT /v1/sandboxes/{id}/instance-metadata`, `GET /v1/info`,
  `PUT /v1/pools`, `POST/DELETE /v1/drain`, `GET /metrics`,
  `GET /v1/checkpoints/{id}/blob`)
  answer a tenant token `403` — authenticated but not authorized; an unknown
  token stays `401`. On a node with a shared
  [`meta_store`](deploy.md#shared-tenant-database), a tenant token the node
  has not cached answers `503` while the database is unreachable; retry.
- **sandbox** — every claimed sandbox carries its own bearer token guarding
  the sandbox-scoped endpoints, regardless of the other two.

Endpoints below that say "node API token" accept root or tenant unless
marked root-only. Errors are `{"error": "message"}` with the status codes
listed per endpoint. Request bodies are capped at 1 MiB; a larger one is
rejected with `400 {"error": "invalid request body"}`.

## POST /v1/claim

Auth: `Authorization: Bearer <api_token>` (when configured).

```json
{"template": "base:24.04", "net": "none", "size": "small",
 "ttl_seconds": 300,
 "volumes": [{"name": "imagenet"}, {"name": "weights", "mount": "/models"},
             {"name": "scratch-db", "mode": "rw"}],
 "claim_ref": "namespace/workload", "metadata": {"team": "a"},
 "on_expire": "archive", "no_redirect": false, "require_promoted": false,
 "egress": true,
 "env": {"MODE": {"value": "prod"}, "GW_KEY": {"value": "Bearer …", "guest": false}}}
```

- `net` defaults to `none`, `size` to `small`; the pool key is
  `(template, net, size)`
- `ttl_seconds` 0 means the server default (5 minutes); capped at 24h. The
  owning node reaps the sandbox after the TTL even if the client vanishes
- `claim_ref` is an optional opaque caller reference echoed by the scoped
  sandbox index; the aggregated apiserver uses `<namespace>/<name>`
- `metadata` is an optional map of caller labels, echoed and filterable by
  the [sandbox index](#get-v1sandboxes): at most 16 pairs, each key 1 to 128
  bytes of printable ASCII without `=` or `&` (list filters encode pairs as
  `key=value`, and a query string joins pairs with `&`), each value at most
  512 bytes of printable UTF-8, and at most 4 KiB as JSON, so quotes and
  backslashes count twice. Labels are fixed at claim time; no verb edits them
- `on_expire` is what the node does when the lease ends: `destroy`, the
  default, or `archive`. `archive` hibernates a running sandbox and archives it
  to the checkpoint store, whatever the pool's `archive_after_seconds`. The
  archive is kept for `archive_delete_after_seconds`, or forever when that is
  0, and the next call that reaches the guest restores it with a fresh lease
  of the claim's length, so its next expiry archives it again; a call that
  wakes it between the hibernate and the export grants that fresh lease too.
  A failed hibernate or export at expiry keeps the claim and retries on every
  reap tick (5 s); it never falls back to destroy. A volume claim or an egress-lane claim
  cannot hibernate, so `archive` on one answers 409 with the same error as a
  hibernate
- `no_redirect` is set by the SDK when retrying at a redirect target
- `require_promoted` provisions only from a promoted template: a claim carrying
  it never takes a warm VM or cold-boots, and answers 404 unknown template when
  no record holds the key, a key a pool serves included. A redirect to a template's owner sets it; copy it into the
  `no_redirect` retry so a target whose gossip view is stale refuses instead of
  cold-booting an image named like the template
- `env` is the claim's own environment, at most 64 entries: each name a shell
  identifier of at most 128 bytes, each value at most 8 KiB with no control
  characters other than tab, and names plus values at most 64 KiB in total. An entry holds only `value`, `guest` and `inject`; any other
  member, or `"guest": null`, answers 400, so a mistyped flag never lands a
  host-only value in the guest. `guest` (default `true`) delivers the entry into the guest; a
  `guest: false` entry never enters it and only feeds the node's egress
  [secrets](egress.md#claim-env-and-secrets), or, with `inject`
  (`{"hosts": [...], "header": "...", "placeholder": "..."}` with `placeholder`
  optional, or `{"hosts": [...], "query": "...", "body": true, "placeholder": "..."}`
  with `body` optional), becomes a
  [claim credential](egress.md#claim-credentials) the proxy sends to those
  hosts. A bad entry answers 400, and an `inject` host the claim's egress does
  not intercept answers 400 with the claim's VM released; see
  [claim env](#getputpatch-v1sandboxesidenv)
- `egress` `false` claims with no [egress policy](egress.md), whatever the
  pool's (or, for a promoted template, its source pool's): the guest's doors are
  never bound. Omitted or `true` keeps the policy the claim would get
- `volumes` is an ordered list of at most eight unique catalog names. `mount`
  defaults to `/volumes/<name>`; a custom value must be absolute and clean,
  outside the guest OS tree, unique, and non-nesting within the request.
  `mode` is `"ro"` (default, omitted) or `"rw"`; `"rw"` requires the catalog
  entry's `writable: true` (see [deploy](deploy.md#dataset-volumes))
- `volumes_attach_only` (default `false`) attaches every requested volume
  without mounting it, handing the whole mount contract to the workload. It
  requires at least one volume, and rejects any entry carrying a `mount` —
  the path is meaningless when the caller mounts the device. Everything below
  describes the default, eager behaviour, which is completely unchanged; the
  attach-only contract is in [its own section](#attach-only-volumes)

Success:

```json
{"id": "sb_…", "token": "…", "deadline": "2026-07-06T00:05:00Z",
 "owner_addr": "10.0.0.5:7777", "template_digest": "sha256:…", "net_route": "relay",
 "volumes": [{"name": "imagenet", "mount": "/volumes/imagenet"},
             {"name": "weights", "mount": "/models"},
             {"name": "scratch-db", "mount": "/volumes/scratch-db", "mode": "rw"}]}
```

A claim cloned from a promoted template carries `template_digest`, the exact
export generation fetched for that clone. It is absent for configured pools,
cold image boots, forks, checkpoints, and templates published by an older
sandboxd until they are re-promoted.

`net_route` says how the guest reaches the network: `relay` through the host
proxy behind the guest's bound doors, `direct` over an egress-lane NIC the node
does not lock, or `none` (a claim with no effective egress policy, a claim with
`"egress": false`, or a locked egress-lane NIC with no policy). silkd hands its children the
`http_proxy`, `https_proxy` and `no_proxy` of its unit unless the lane is
direct; a guest agent of another kind reads `net_route` and hands its processes
`http://127.0.0.1:3128` when it is `relay`. `"egress": false` closes the doors, not an unlocked NIC, so
such a claim on a `direct` lane still reaches out directly.

A claim branched directly from a checkpoint additionally carries
`"from_checkpoint": "ck_…"` — the lineage edge for reconstructing the
checkpoint tree. Fork children do not repeat that edge: they branch from the
parent sandbox, not directly from its checkpoint.

`volumes` reports the names, effective mounts, and (`rw` only) mode applied
and persisted at finalization — `mode` is omitted from the echo for `ro`
entries, matching the request shape. sandboxd attaches each disk, waits for
the device settle — a serial match under `/sys/block`, then the `/dev/<name>`
node itself, since the kernel publishes sysfs before devtmpfs creates it — up
to 2 seconds total, and mounts the filesystem — read-only, unless the entry
requested and was granted `rw` — before returning. A custom mount may shadow
an existing populated guest directory for the claim's life.

### Attach-only volumes

`"volumes_attach_only": true` stops after the attach. The device appears in
the guest, nothing is mounted, and the echoed entries carry no `mount` key:

```json
{"id": "sb_…", "token": "…",
 "volumes": [{"name": "imagenet"}, {"name": "scratch-db", "mode": "rw"}]}
```

Find each device by its virtio serial, which is the catalog name — poll this,
not a one-shot lookup: the claim can return before the guest enumerates the
device (typically within ~100ms, never guaranteed), and the `/dev/<blk>` node
can lag the serial match the same way it does for an eager mount's device
settle. Confirm both before mounting:

```sh
device=
tries=0
while [ "$tries" -lt 200 ]; do
  for serial in /sys/block/*/serial /sys/block/*/device/serial; do
    [ -r "$serial" ] || continue
    [ "$(cat "$serial")" = scratch-db ] || continue
    block=${serial#/sys/block/}
    candidate="/dev/${block%%/*}"
    [ -b "$candidate" ] && { device="$candidate"; break 2; }
  done
  tries=$((tries + 1))
  sleep 0.01
done
[ -n "$device" ] || { echo "scratch-db device not ready" >&2; exit 1; }
printf '%s\n' "$device"
```

Then mount it however the workload needs. A `ro` entry is attached
`--readonly` and stays read-only at the guest block layer no matter who
mounts it, so the guarantee does not depend on your mount flags.

The whole consistency contract moves to the caller with the mount:

- sandboxd writes no dirty marker for an attach-only `rw` claim and clears
  none, because it cannot verify your unmount and therefore promises nothing.
  The flush is yours too: release runs no unmount and no `sync`, so whatever
  the workload left unsynced is discarded with the VM. Releasing without a
  clean unmount of your own leaves the image exactly as a filesystem crash
  would. The next *eager* `ro` claim then fails at mount time with a 500 —
  loud and attributable to the claim that skipped its unmount — instead of
  the marker's 409. Recovery is any `rw` cycle that replays the journal.
- a marker left by an earlier *eager* `rw` crash is not cleared by attach-only
  cycles, and still refuses eager `ro` claims with 409 until a `rw` claim
  releases cleanly.
- writes straight to the block device bypass the filesystem journal entirely
  and are outside marker protection in either mode.

What still protects other claims is unchanged: admission excludes an
attach-only `rw` claim against every other claim of the name (and readers
against a writer), an attach-only `ro` claim of a marker-bearing image is
refused with 409, and a claim with volumes still refuses checkpoint, fork and
hibernate. An attach-only claim costs one disk attach per volume — no device
settle, no mount round-trip — and release costs nothing at all: removing the
VM closes the devices.

Redirects (mutually exclusive with the fields above) name peers to retry
at — sent on a warm miss with warm peers, when the node lacks a golden for
the key but gossip names a template owner, and when the node is at
`max_claims` but a peer reports warm capacity:

```json
{"redirect": ["10.0.0.6:7777", "10.0.0.7:7777"],
 "require_promoted": true}
```

Retry the same body (+`no_redirect: true`) at each candidate until one
answers. Preserve `require_promoted: true` when the redirect carries it, as a
redirect to a template's owner does; warm-candidate redirects omit the field.

A volume claim may consume an ordinary warm VM; candidate ranking, the
promoted-template intersection, and the `no_redirect`/`require_promoted` retry
follow the fleet-wide rule in [cluster](cluster.md#volumes-and-placement).
Redirect responses never carry `volumes`.

A tenant token claims the same way; the sandbox is stamped with the tenant
name (attributed in the usage journal and counted against the tenant's
`max_claims`). A catalog access list may restrict an entry to named tenants;
an unknown and a forbidden volume return the same error text.

Errors: 400 unknown template axis, metadata over a bound (the message names
it), an unknown `on_expire`, invalid/duplicate volumes,
`volumes_attach_only` with no volumes or with an entry carrying a `mount`,
`mode: "rw"`
against a non-writable entry, or a volume that is unknown or forbidden (the
latter two are deliberately indistinguishable), or
bad body; 401 bad api token; 409 egress requested on a node without an egress
attachment, a writable name already claimed in a conflicting mode (volume
busy — a live writer excludes every other claim for that name, live readers
exclude a writer), or a `ro` claim against a writable image left dirty by an
unclean `rw` release (needs recovery — one `rw` claim must replay and cleanly
release before `ro` claims resume; see
[deploy](deploy.md#writable-dataset-volumes)); 429 node at `max_claims`, the
calling tenant at its own `max_claims`, or the node draining; 500
provisioning failed.

## GET /v1/volumes

Auth: node API token (root or tenant). Lists the fleet catalog entries the
caller may use, without host paths or holder addresses:

```json
{"volumes": [{"name": "imagenet", "default_mount": "/volumes/imagenet",
              "size_bytes": 214748364800, "available": true, "nodes": 3}]}
```

Root sees the gossiped union, including peer-only entries (`available: false`);
a tenant sees only entries this node declares locally and whose access list
permits it. `nodes` counts members advertising the name; `size_bytes` and
`available` are a best-effort stat of the answering node's image. Membership is
eventually consistent by one gossip tick.
`writable` is the entry's catalog configuration, fleet-uniform like the access
list; the field is emitted (as `true`) only for a writable entry this node
declares and omitted otherwise (peer-only rows never carry it), so a read-only
entry's response is byte-identical to v1.

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
snapshot) at its deadline, unless it claimed `on_expire: archive` or its pool
archives. When to hibernate is the caller's policy — the
node only provides the transition. 204 on success, 404 unknown id or wrong
token, 409 on the egress lane or when volumes are attached (neither kind of
sandbox hibernates; see [egress](egress.md)).

## POST /v1/sandboxes/{id}/wake

Auth: the sandbox's own token, or the root token by id (operator). Restores
a hibernated (or archived) sandbox and leaves it running — waking is
otherwise only a side effect of the next agent access, so this is the
explicit form for warming a sandbox ahead of use. Idempotent on one already
running. A hibernated sandbox retains its existing deadline. An archived
sandbox instead uses `archive_delete_after_seconds` as its retention deadline
while stored, and waking it starts a fresh lease of the length the claim was granted (the server default when it asked for none). A
[failed](deploy.md#dead-vmms-and-host-restarts) sandbox is cold-booted from
its own disk (memory is lost) and its restart budget resets. 204 on
success, 404 unknown id or wrong token, 409 a failed egress-lane or volume
sandbox, or one whose VM record is gone, 500 when the cold boot itself fails.

## POST /v1/sandboxes/{id}/renew

Auth: the sandbox's own token, or the root token by id (operator). Resets the
lease to `ttl_seconds` from now, so a client holding a sandbox longer than its
claim asked for does not have to claim a new one:

```json
{"ttl_seconds": 3600, "on_expire": "archive"}
```

→ `200 {"deadline": "2026-09-22T12:00:00Z"}`. The grant, not the request, is
authoritative: `ttl_seconds` 0 means the node default and anything past the
24 h cap is clamped, and the reply always carries what was granted. An absent
body means the same as `ttl_seconds` 0. The new lease also becomes the one a
wake from the archive grants again. A renew can shorten a lease as well as
extend it. `on_expire` switches the expiry action to `archive` or back to
`destroy`; absent keeps the current one. It is refused as on a claim for a
sandbox that cannot hibernate.

An archived sandbox is refused: off-node, its deadline is the store's retention
window rather than a lease, and a TTL written over it would schedule the
checkpoint for deletion. Wake it first. A claim whose [tenant was
removed](#tenants-v1tenants) runs to its deadline but cannot extend it: its own
token gets 403, the root token still renews. 400 a malformed body; 401 missing
bearer token; 403 the claim's tenant was removed; 404 unknown id or wrong
token; 409 archived, or being archived right now.

## POST /v1/sandboxes/{id}/fork

Auth: the node `api_token` (Bearer) — forking creates node resources, like a
claim. The sandbox's own token rides in the body as the ownership proof:

```json
{"token": "…", "count": 2, "ttl_seconds": 300, "claim_ref_prefix": "team-a/"}
```

`claim_ref_prefix`, when set, records each child under `prefix + child id` as
its `claim_ref`, so a control plane that names claims `<namespace>/<name>`
addresses the children by name; without it children carry no `claim_ref`.
Children inherit the parent's `metadata`, `on_expire` and `"egress": false`; an
`on_expire` in the body overrides the inherited action.
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
or wrong sandbox token, 409 egress-lane or volume parent (neither forks,
checkpoints, or promotes; see [egress](egress.md)) or an archived parent (an
exec or file call wakes it first), 429 node or the parent's
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
export's regular files: slash-relative path, byte length, and the SHA-256 of
each 16 MiB chunk of its bytes, ordered lexically. Directory entries, modes,
mtimes, and the template's ownership/creation metadata do not affect it. The
digest is computed once while promoting, stored beside `meta.json` (a sibling
digest file on the directory backend, object metadata on S3), and therefore has
identical semantics on both. Re-promoting unchanged export bytes keeps the
digest; changing any exported path or bytes changes it. A re-promote starts the
template with no [labels](#put-v1templateslabelstemplatenetsize): they describe
the export they were set on, which the new one replaces.

400 invalid name, 401 bad api token, 409 when the name collides with a
configured pool, the template is owned by another tenant, or the sandbox is
on the egress lane, has volumes attached (see [egress](egress.md)), or is
archived (an exec or file call wakes it first), 404 unknown id or wrong
sandbox token.

## DELETE /v1/templates?template=…&net=…&size=…

Auth: node API token. Removes a promoted template (the query parameters
default like a claim's: `net=none`, `size=small`). A tenant may delete only
templates it promoted — anything else is 404, root deletes anything. 204 on
success, 404 unknown template, 409 when the key belongs to a configured pool
(those goldens are owned by the node config). `digest=<content digest>` deletes
only while the record still has that digest and answers 412 otherwise, so a
caller that observed an older build never removes the newer one that replaced
it. On a cluster, a node that does not
hold the template but sees an owner in gossip answers `200
{"redirect": [addrs]}` — the claim redirect shape — and the SDK retries the
delete at the owner. The retry carries `no_redirect=1`, mirroring the claim
protocol: a node answering a `no_redirect` delete speaks only for itself.

## PUT /v1/templates/labels?template=…&net=…&size=…

Auth: node API token. Replaces a promoted template's labels on this node (the
query parameters default like a claim's):

```json
{"labels": {"v1": "sha256:…", "team": "a"}}
```

Labels follow the [claim `metadata`](#post-v1claim) rules — at most 16 pairs,
the same key and value grammar, at most 4 KiB as JSON — and replace the whole
map; `{}` clears it. `digest=<content digest>` writes only while the record
still has that digest and answers 412 otherwise, so a writer that observed an
older build never relabels the newer one. They are stored beside the template's record, so they
survive a restart, are listed by the node that promoted the template or loaded
it at startup, and go with the template when it is deleted; a re-promote starts with none. A tenant may label
only templates it promoted — anything else is 404, root labels anything. 204 on
success, 400 bad labels or body, 404 unknown template, 409 when the key belongs
to a configured pool. The call speaks only for the node it reaches; it does not
follow a redirect.
## PUT /v1/pools

Auth: root only (tenant tokens get 403). Replaces the node's desired warm
targets online — no restart, and no live claim's VM is touched. On a node with egress policies,
whether a key is pooled decides which [policy layers](egress.md) a claim of it
gets; that is settled when the claim is made, so adding or dropping a pool here
changes only claims made afterwards — a key that gains a pool with no `egress`
block leaves new tenant claims without egress, a key that loses such a pool
gives new tenant claims their egress class's policy alone:

```json
{"pools": [{"template": "base:24.04", "net": "none", "size": "small",
            "warm": 4, "warm_max": 16, "idle_hibernate_seconds": 0}]}
```

Pools omitted from the list are drained: their unclaimed warm VMs are
destroyed and the pool entry retires. `net`/`size` default like a claim's.
Answers the fresh `GET /v1/info` payload. 400 bad key, negative warm/idle,
`warm_max` below `warm`, `idle_hibernate_seconds` on an egress pool, a
negative archive duration or an `archive_after_seconds` not above the pool's
`idle_hibernate_seconds`, duplicate pool, or a config-owned `egress`/`warmup`/`capture_trim`/`storage`/`egress_upstream_env`
field; 401 bad api
token; 409 egress pool on a node without an egress attachment.

## Tenants (/v1/tenants)

Auth: root only (tenant tokens get 403); a node without `api_token` answers 400,
since tenants need it. The API is the only way in: `config.json` has no
`tenants` field, and a node starts with no tenants. The set changes at
runtime, with no restart:

- `PUT /v1/tenants/{name}` `{"token": "…", "max_claims": 50, "egress_class": "desk"}`
  adds a tenant or changes one; an omitted `token` keeps the tenant's stored
  one, so a cap change never resends it, while `max_claims` and `egress_class`
  take the values sent. 204.
- `DELETE /v1/tenants/{name}` removes one. 204; 404 unknown tenant.
- `PUT /v1/tenants` `{"tenants": [{"name": "…", "token": "…", "max_claims": 0, "egress_class": "desk"}]}`
  replaces the whole set: a tenant left out is removed, and an entry without
  `token` keeps that tenant's. It answers the `GET` body. Use it to reconcile
  and to import many tenants at once, since every change rewrites the whole
  set; a sign-up is a single-tenant `PUT`, so concurrent sign-ups never
  overwrite each other. The 1 MiB body cap holds roughly 8,000 entries; import
  a larger set with single-tenant `PUT`s.
- `GET /v1/tenants?after=<name>&limit=<n>` → `{"tenants": [{"name": "…", "max_claims": 50, "egress_class": "desk", "claims": 3},
  {"name": "…", "claims": 1, "removed": true}], "digest": "…", "next": "…"}`.
  - **Paging:**
    - one page of tenants in name order, each with its live claims on this node;
    - `limit` defaults to 1000, at most 10000 (400 outside);
    - pass `next` as the following page's `after`; it is absent on the last page.
  - **Removed tenants:** the first page also lists each removed tenant that
    still owns claims here.
  - **Digest:** two nodes with the same `digest` hold the same set. A node on a
    shared [`meta_store`](deploy.md#shared-tenant-database) reports none.
  - It never carries a token.

A name follows the claim name grammar (`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$`;
escape `/` as `%2F` in the path) and is the tenant's identity: claims,
checkpoints and promoted templates belong to a tenant by name, so a name reused
after a removal inherits what the old tenant left. Use an id the control plane
never reissues, such as a user UUID. Tokens must be unique and differ from
`api_token`. `egress_class` names one of the node's `egress_classes` (400 when
unknown) and gives the tenant's claims that egress layer; without one they
reach nothing on a pool with a policy. The API references a class and never
defines one. A claim keeps the class its tenant had when it was made.

A change applies to the next request: a removed or rotated token gets 401 at
once. Lowering `max_claims` below the live count refuses new claims and evicts
none. A removed tenant's claims keep running and stay listed for root
(`GET /v1/sandboxes?tenant=<name>`) until they expire or root releases them,
but they cannot extend: renew and fork answer 403 to the sandbox's own token,
a hibernated one is never archived, one archived before the removal is not
woken (403) and is deleted when its retention ends, and at the deadline they
are destroyed even under `on_expire: archive`. The verbs that need a tenant
token (promote, checkpoint, preview, branch claims) are closed to them already.

The node writes the applied set to `<data_dir>/tenants.json` (0600, each token
kept only as its SHA-256) before it serves, and loads that file at boot. Each
change is an `op:"tenants"` audit record naming the
tenants added, changed and removed. On a cluster every node needs the change
(see [cluster](cluster.md#tenants)). 400 a bad entry, a duplicate name or token,
a token equal to `api_token`, or a config-owned field.

## POST /v1/config/reload

Auth: root only (tenant tokens get 403). Re-reads the node's config file and
applies its reloadable settings at once, or nothing. SIGHUP runs the same
reload and logs the result. →

```json
{"changed": ["egress_internal_allow", "pools[desktop:v13 none small].warmup"],
 "ignored": ["pools[desktop:v13 none small] targets (PUT /v1/pools owns them)"]}
```

`changed` lists the settings that now apply. `ignored` lists differences the
file holds for parts another API owns: pool targets and tenant identities.
Which fields reload, and what happens to live claims and goldens, is described
in [deploy](deploy.md#reloading-the-config). The status codes:

- **400**: the file does not parse or does not validate.
- **409**: a field needs a restart (the message names it), or intercept is
  turned on for a key this node has served or without a CA loaded at boot.

A refused reload changes nothing.

## POST /v1/drain

Auth: root only (tenant tokens get 403). Cordons the node for maintenance: claim/fork/branch answer
429 `node draining` (on a cluster a non-volume claim tries a warm-peer redirect first, and
gossip stops naming this node within a tick as its warm counts hit zero),
unclaimed warm VMs are destroyed, and live claims keep serving until release
or TTL. Pool ownership is untouched — no pools.json write, no config change.
Answers the fresh `GET /v1/info` payload; poll `claimed` to zero to know the
node is empty. Deliberately not persisted: a restarted node serves again.

## DELETE /v1/drain

Auth: root only (tenant tokens get 403). Lifts the drain and kicks an immediate refill. Answers the
fresh info payload.

## POST /v1/sandboxes/{id}/preview

Auth: node `api_token` in the header, the sandbox's own token in the body.
Unlike fork, promote and checkpoint — where the root token with an empty body
token acts by id as the operator — preview has no operator path: the body
token is required. Mints a signed URL serving a
guest HTTP port from a browser: body `{"token": "...", "port": 8080,
"ttl_seconds": 0}` → `{"url": "http://<preview_advertise>/p/<token>/"}`.
The URL's life is clamped to the claim's remaining lease; an archived claim
kept forever (`archive_delete_after_seconds: 0`) has no lease, so there the
requested `ttl_seconds` stands unclamped and `ttl_seconds: 0` mints a one-hour
URL; releasing the sandbox ends it early either way.
400 a port of 0, 501 when the node
has no `preview_listen`. The signed token embeds the sandbox id, port, and
owner `advertise_addr`, so any node's preview listener can serve it (forwarding
to the owner's main listener) and a released sandbox's URL simply stops
resolving — no revocation list. See [deploy](deploy.md#preview-urls).

## POST /v1/sandboxes/{id}/checkpoint

Auth: node API token; body `{"token": "<sandbox token>", "name": "..."}`
(name optional). Captures the sandbox's full state without stopping it and
answers `200 {"checkpoint": {id, name, sandbox_id, key, tenant?,
created_at}}` — `tenant` records the calling tenant, absent for root.
400 bad body or name, 401 bad api token, 404 unknown id or wrong sandbox
token, 409 egress-lane sandbox, one with volumes attached (see
[egress](egress.md)), or an archived one (an exec or file call wakes it
first).

## POST /v1/checkpoints/{id}/claim

Auth: node API token; body `{"ttl_seconds": 0, "no_redirect": false,
"metadata": {}, "on_expire": "destroy", "env": {}}`. Claims a fresh sandbox branched from the checkpoint (a
normal claim response, attributed to the caller); the checkpoint's recorded
key applies — the unguessable id is the capability to branch. The branch
carries this request's `metadata`, `on_expire` and `env`, under the claim rules,
never the source sandbox's. 400 metadata over a bound, an unknown
`on_expire`, or a bad `env` entry.

Checkpoints are node-local (unless the store is shared — see
[Configuration](deploy.md#configuration)), so a miss here runs a tier order:

1. This node checks its own store first.
2. On a miss, it probes every mesh peer directly — a parallel `HEAD` to each
   (authenticated on an encrypted mesh, see below) — and answers exactly like
   a warm-miss
   [`POST /v1/claim`](#post-v1claim): `200 {"redirect": ["10.0.0.6:7777",
   "10.0.0.7:7777"]}`, retry the same body (+`no_redirect: true`) at each
   candidate until one answers. The answer is capped at 3 addresses: a hint,
   not an exhaustive list of every owner. The probe and the follow-up claim
   are not atomic: a peer can answer the probe, then lose the record — a
   delete's broadcast lands, or its own TTL sweep runs (below) — before the
   retry reaches it, so a redirect can go stale between the two calls.
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
  (non-archive) copy, 404 otherwise, and 401 on a keyed mesh when
  `X-Cocoon-Probe` is absent or expired. On a mesh with `cluster_key` set the
  request must carry `X-Cocoon-Probe`, an HMAC over the id and a coarse time
  bucket keyed off a probe-specific derivation of the cluster key — verified
  before any disk is touched, replayable for roughly ninety seconds at most. On a
  keyless mesh (redirect-only fleets have no shared secret to sign with) the
  id itself remains the only capability, matching the mesh's own posture.
  Repeat probes for one id are answered from a short positive cache on the
  asking node, which a local delete evicts immediately.

## GET /v1/checkpoints

Auth: node API token. Lists this node's checkpoints, newest first. A tenant
sees only its own records; root sees everything. A checkpoint backing an
archived claim is that claim's wake image, not a listable record.

## DELETE /v1/checkpoints/{id}

Auth: node API token. A tenant may delete only its own records — anything
else is 404, never a hint the id exists; root deletes anything. 204 on
success, 404 unknown — including the checkpoint behind an archived claim,
which only that claim's release or retention window removes.

**Delete removes the local record, then best-effort broadcasts to peers so a
healed replica does not outlive it — eventual cleanup, not a fleet-wide
revocation.** A peer offline during the broadcast keeps its copy until the
checkpoint TTL ages it out, so an id-holder can still branch it for that
window; [placement lifecycle](cluster.md#delete-is-eventual-not-a-fleet-wide-revocation)
has the bound and why heal requires a nonzero, fleet-matching TTL. A shared
checkpoint store skips the broadcast: every node already resolves every
record directly, so there is no replica to chase. `?no_forward=1` marks a
delete already arriving from another node's own broadcast, so it is not
itself re-broadcast (loop prevention); it is an internal parameter, not one
an SDK caller should set.

## GET /v1/sandboxes

Auth: node API token. Root sees every live claim; a tenant sees only its own.
The index is `{"sandboxes": [{id, key, tenant?, deadline, claimed_at?, hibernated,
archived?, from_checkpoint?, claim_ref?, metadata?, on_expire?, cpu_count, mem_total_bytes,
volumes?: [{name, mount, mode?}], restarts?, restarted_at?, failed?}]}` — `cpu_count` and `mem_total_bytes` are the
size tier's allocation; `restarts` counts the cold boots that replaced an
exited VMM and `restarted_at` stamps the last one (each loses guest memory and
the instance-metadata document, so a caller re-establishes guest state when
`restarts` moves; the claim's env is written to the guest again); `failed` names why the VMM is down and was not restarted
(see [dead VMMs](deploy.md#dead-vmms-and-host-restarts));
`mode` is omitted for `ro`, matching the claim echo; never sandbox tokens,
volume host paths, or catalog access lists. `claimed_at` is the first grant
(renew and wake move `deadline` only) and is absent for a claim recorded before
the field existed. `?claim_ref=<ref>` keeps only the claims recorded under
exactly that reference, so a caller that named its claim reads its row without
the rest of the index; an empty value lists everything. `?metadata=key=value`,
repeatable, keeps only the claims holding every pair; 400 a pair without `=`,
with an empty key, or with a key repeated. `tenant` names the owning tenant
(absent for root's own claims); `?tenant=<name>` lets root list one tenant's
claims, a removed tenant's included, and is ignored for a tenant token.

## GET /v1/sandboxes/{id}

Auth: root only. One live claim in the index-row shape above, so a reconcile
loop can read a single sandbox without scanning the whole node listing. When
the caller presents the configured `api_token` the row also carries `token`,
the sandbox's own bearer token, so a control plane can hand a returning client
its data-plane credential without keeping a copy; a node without an
`api_token` never grants per-sandbox authority and answers without it. 404
unknown id.

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

## GET/PUT/PATCH /v1/sandboxes/{id}/env

Auth: node API token (root or tenant). A tenant reaches only claims it owns;
anything else is 404, like an unknown id. Each call speaks only for the node
it reaches; on a cluster, send it to the claim's `owner_addr`.

`PUT` replaces the claim's whole env with the body's map, under the claim
rules; `{"env": {}}` clears it. `PATCH` merges: each entry in the body is set,
an entry of `null` removes that name, and every other entry is kept as stored,
host-only values included, so a control plane that cannot read those values
back never has to send them again:

```json
{"env": {"CB_META_wake": {"value": "w2"}, "OLD": null}}
```

The claim rules apply to the merged env, so a patch that takes it past 64
entries or 64 KiB answers 400 and changes nothing. `GET` answers the same shape with every
`guest: false` value blanked, so a control plane sees the names without the
values:

```json
{"env": {"MODE": {"value": "prod"}, "GW_KEY": {"value": "", "guest": false}}}
```

An entry with `inject` comes back with its hosts, header or query, body flag and placeholder, value blanked.
An `inject` host the claim's pool does not intercept, or its tenant class does
not allow, and a header the pool rule's own secret already sets on that host,
answer 400 and store nothing.

The env is persisted with the claim. A `guest: false` change takes effect on
the egress proxy's next request at any time, a hibernated or archived claim
included, and never touches the guest. A change to the guest entries is
written to the guest at once, so it needs a running guest: on a hibernated,
archived, failed or transitioning sandbox it answers 409 (this verb never wakes one).
A `PUT` or `PATCH` whose result equals the stored env rewrites a running
guest's file, so resending repairs a failed write, including a removal or
a change from guest to host-only. This also applies to an unchanged
host-only patch; the file contains only the guest entries. An unchanged
result does not rewrite the claims journal. A 500
after the store step means the env is stored and its `guest: false` values
are already live, but the guest write failed; resend the same request to
deliver it. The guest side is described in [silkd](silkd.md#claim-env).
`PUT`/`PATCH` answer 204, `GET` 200; 400 a bad entry, an unknown body
field or an `inject` outside the claim's intercepted hosts; 404 unknown id or another tenant's claim; 409 a guest change on a
paused or failed sandbox.

## PUT /v1/sandboxes/{id}/instance-metadata

Auth: root only. Sets the sandbox's instance-metadata document: the JSON a guest
agent reads from `169.254.169.254` in the IMDSv2 shape, as cloud-init and
similar agents do (`PUT /latest/api/token`, then `GET /` with the
`X-metadata-token` header). It is unrelated to the claim's `metadata` labels.
The body is one JSON object of at most 4 KiB, stored and served verbatim; a
guest with no document answers `{}`, and a fork child, checkpoint branch or
template clone starts with the document its source held. The node writes it to
`/run/silkd/instance-metadata.json` in the guest atomically, so repeating a call
is a no-op. silkd serves the address only in images that alias it on `lo`.

204 on success; 400 when the body is not one JSON object within the bound; 404
unknown id; 409 when the sandbox is hibernated, mid-transition or archived (this
verb never wakes one) or its image serves no instance metadata.

## GET /metrics

Auth: root only (tenant tokens get 403). Prometheus text format,
hand-rendered: pool warm/target gauges, claimed/hibernated/archived/failed/draining
gauges, a per-tenant live-claim gauge (`sandboxd_tenant_claims{tenant="…"}`,
tenants holding live claims only, so the label set is bounded by the node's claims), `sandboxd_config_digest_mismatch` on a mesh, claims
by tier (warm/clone/cold),
wake/hibernate/fork/checkpoint/promote/release/reap counters plus
archive/unarchive/archive-delete counters, and claim/wake `*_seconds_total`
for average latency. /metrics is a derived ops view; the billing source of
truth is the usage journal below.

## Usage journal (usage.jsonl)

Always on: every lifecycle transition appends one JSONL event to
`<data_dir>/usage.jsonl` — `{"t": <RFC3339>, "ev":
"claim|hibernate|wake|fork|checkpoint|promote|release|reap|archive|unarchive|archive_delete|egress|egress_bytes|vmm_exit|vmm_restart|vmm_failed",
"id": "sb_…", "vm": "sbx-…"}` plus `key` (the pool key's stable hash, claim
events), `tenant` (the owning tenant, on claim, egress, archive, unarchive and
archive_delete), `children` (fork) and `ref` (the promoted
template / checkpoint id, the egress host, or a `vmm_failed` reason). An egress event that injected credentials names
them in `secret` (`gh,claim:<entry>`, never a value). An egress event routed through an
[upstream proxy](egress.md#upstream-proxies) carries `upstream` (its `host:port`), and with
`egress_usage_bytes` on, `egress_bytes` events add the connection's payload `tx`/`rx` bytes. A volume claim also carries
`volumes`, the applied catalog names, and — omitted when empty — `volumes_rw`,
the subset of those names claimed `rw`, so billing can discriminate write
access (mounts and host paths are not billing dimensions). The file rotates at
64 MiB keeping one `.1` backup, so a tailing collector never loses a window
silently. Folding rules: billable compute seconds per sandbox =
Σ(claim→release/reap) − Σ(hibernate→wake) − Σ(vmm_exit→vmm_restart/release/reap);
`vmm_exit` is stamped when the node first finds the VMM gone, so an outage
before sandboxd runs again (a host that was down) stays billed up to that
point, and `vmm_failed` is informational; hibernated storage seconds =
Σ(hibernate→wake) minus the archived span; archived storage seconds =
Σ(archive→unarchive/archive_delete), a cheaper store tier than a hibernated
VM's RAM. An interval left open by a crash clamps to the claim's
deadline; reconcile records nothing for the claims it drops (those whose VM
record is gone), so a restart leaves those intervals without a terminal event. The `vm` name joins
cocoon's machine-level metering ledger for audit cross-checks.

## GET /v1/sandboxes/{id}/agent

Auth: the sandbox's own token. Requires `Upgrade: silkd` +
`Connection: Upgrade`; answers `101 Switching Protocols` and from then on
the connection is a byte-for-byte relay to the guest's silkd, carrying RPCs
back to back (see [silkd](silkd.md)). An open relay holds the sandbox's idle
clock, so a client that keeps a connection warm must close it when idle for
`idle_hibernate_seconds` to apply. 426 without the upgrade header, 404
unknown sandbox, 409 a failed sandbox, 502 guest unreachable.

## GET /v1/sandboxes/{id}/ports/{port}

Auth: the sandbox's own token, or the root `api_token` for a control-plane
read (below). Send `Upgrade: tcp` + `Connection: Upgrade`
(the gate reads `Upgrade` alone, as `/agent`'s does); answers
`101 Switching Protocols` and from then on
the connection is a raw byte relay to `127.0.0.1:{port}` inside the guest,
over the same vsock `port_forward` preview uses. Nothing of ours frames the
stream, so HTTP/1.1, HTTP/2 and any other protocol pass through unchanged.
This is how an edge proxy reaches a guest listener on a `net=none` sandbox,
which has no NIC.

Half-close works in **both directions**: a client write shutdown reaches the
guest socket, and a guest write shutdown reaches the client without ending
the client's writes. Each direction stays open until its own EOF. A relay
copy error closes both connections and releases the sandbox hold.

With the sandbox's token, an open relay holds the sandbox's idle clock exactly
like the silkd relay, and a hibernated sandbox wakes transparently — which is
why the upgrade header is mandatory: a bare `GET` must not consume a hibernate
snapshot. With the root `api_token` the relay is passive, for control-plane
reads such as metrics: it never wakes the sandbox and never stamps activity,
so a poll neither undoes a pause nor keeps a sandbox from idling. A hibernated
or archived sandbox, or one with a hibernate, wake, checkpoint, promote or
fork in flight, answers `409` at once; an open passive relay still holds the sandbox against the idle sweep.
400 a port outside 1-65535; 401 missing bearer token; 404 unknown sandbox or
wrong token; 409 a failed sandbox or a passive relay to a paused one; 426 without the upgrade
header; 502 when the guest cannot be reached on that port — no listener, or a
hibernated sandbox whose restore failed. The node log carries which.

## POST /v1/sandboxes/{id}/exec

Auth: the sandbox's own token. A buffered exec for clients that cannot hold
an upgraded connection — plain JSON in, plain JSON out, so it multiplexes over
HTTP/2 through a TLS proxy:

```json
{"argv": ["node", "-v"], "cwd": "/work", "env": {"CI": "1"}, "timeout_seconds": 60}
```

→ `200 {"exit_code": 0, "stdout": "v22.23.2\n", "stderr": ""}`. The command
runs to completion (no stdin, no streaming, no detach — use the relay for
those); `timeout_seconds` 0 means no limit beyond the request itself. When
the node gives up on a started command — timeout, client gone, output cap —
it kills the child through silkd before answering, so nothing keeps running
behind a 504. Output comes back as JSON strings: bytes that are not valid
UTF-8 are replaced with U+FFFD, so binary output belongs on the relay. 400
empty `argv`, negative `timeout_seconds`, an unknown field, or a silkd
`bad_request` — a command that is missing or not executable and a `cwd` that
does not exist are the caller's, not 502s; 401 missing bearer token; 404 unknown sandbox or wrong token;
413 when stdout+stderr exceed 8 MiB; 503 with `Retry-After: 1` when the
node's buffered-exec memory is exhausted (64 MiB of output across every
buffered exec in flight; the command is killed); 502 guest unreachable or any other
silkd error; 409 a failed sandbox; 504 when the command outlives `timeout_seconds`. A client that hangs up first gets no reply and the command is killed. A hibernated sandbox wakes transparently like on the relay.

## GET /v1/sandboxes/{id}/owner

Auth: the sandbox's own token. Answers `{"owner_addr": "host:port"}` when
this node owns the sandbox, 404 otherwise. With `client_advertise` configured,
`owner_addr` is that node's full HTTP(S) origin instead; the same contract applies
to claim and fork responses. Used by the SDK's `Lookup` scatter.

## GET /v1/info

Auth: root only (tenant tokens get 403). Node pools, promoted templates, claim
count, the node's own address, and mesh peers:

```json
{"pools": [{"key": {"template": "base:24.04", "net": "none", "size": "small"},
            "warm": 4, "refilling": 0, "target": 4, "golden": true}],
 "templates": [{"key": {"template": "myproj:v1", "net": "none", "size": "small"},
                "content_digest": "sha256:…", "tenant": "acme",
                "created_at": "2026-07-06T00:00:00Z", "cpu_count": 1,
                "mem_total_bytes": 536870912, "labels": {"v1": "sha256:…"}}],
 "claimed": 2,
 "hibernated": 1,
 "archived": 0,
 "failed": 0,
 "at_capacity": true,
 "at_capacity_reason": "exchange full",
 "advertise_addr": "10.0.0.5:7777",
 "peers": ["10.0.0.6:7777"]}
```

`advertise_addr` is the address the node hands clients: its `client_advertise`
when set, else its `advertise_addr`. It is the same value a claim reports as
`owner_addr`, and it is omitted when the node advertises no host.

`hibernated` counts claims whose VM is currently hibernated, `archived` those
checkpointed to the store with the local VM dropped (see
[archive tiers](deploy.md#configuration)), `failed` those whose VMM is down
and was not restarted (omitted at zero); all are included in `claimed`.
A node cordoned via [`POST /v1/drain`](#post-v1drain) additionally reports
`"draining": true`. When refill is parked because the node cannot start
another VM, `"at_capacity": true` distinguishes a full node from one still
filling its warm target, and `at_capacity_reason` carries the engine's capacity
reason. Both capacity fields are omitted while refill is not capacity-blocked.

`golden` reports whether the pool's snapshot exists (refill can clone);
`warm` at `target` with `golden: true` means warm claims are served in
sub-millisecond time.

`templates` lists the [promoted templates](#post-v1sandboxesidpromote) this
node holds: each one's full key, the `content_digest` its promote returned,
`created_at` of its last promote, the `cpu_count` and `mem_total_bytes` of its
size tier, its [`labels`](#put-v1templateslabelstemplatenetsize) when any are
set, and `tenant` when a tenant promoted it (omitted for the operator). A key
a configured pool now owns is left out, since claims of it clone the pool's
golden. On a shared checkpoint store a node lists what it promoted plus what
was in the store when it started. A template published by an older sandboxd is
listed once it is re-promoted.

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
