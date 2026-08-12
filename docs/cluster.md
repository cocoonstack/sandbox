# Clusters

A cluster is a set of sandboxd nodes joined through a
[hashicorp/memberlist](https://github.com/hashicorp/memberlist) SWIM mesh.
Gossip carries only placement hints — per-pool warm counts, promoted-template
hashes, available volume names, and each node's data-plane address.
Per-sandbox state never leaves its owning node, so a stale view costs at most
one extra redirect, never correctness. A single node with no seeds is a valid
mesh of one.

## Joining

Give every node a `mesh` block:

```json
"mesh": {
  "node_id": "node-b",
  "bind": "10.0.0.6:7946",
  "join": ["10.0.0.5:7946"],
  "cluster_key": "MDEyMzQ1Njc4OWFiY2RlZg=="
}
```

- `node_id` — unique name; defaults to `bind`
- `bind` — memberlist host:port. Needs an explicit routable host (a wildcard
  would advertise an unroutable address). Open **TCP and UDP** on this port
  between nodes
- `join` — any existing members to contact at startup; an empty list starts
  a new mesh
- `cluster_key` — optional base64 key (16/24/32 bytes) enabling gossip
  encryption; all members must share it

Two constraints, fine for a homogeneous cluster: all nodes share the same
`api_token` and `tenants` set (the SDK replays whichever token authorized a
call across a redirect; a peer missing that tenant answers 401), and only
egress-capable nodes redirect egress claims.

## How placement works

A claim always enters at whatever node the client dialed:

1. **Warm hit** — the node has a warm sandbox for the pool key: ownership
   transfers in sub-millisecond time.
2. **Warm miss with peers** — the node answers with a MOVED-style redirect:
   up to two peer addresses that gossip says hold warm sandboxes, chosen
   power-of-two-choices to avoid herding. The SDK retries there with
   `no_redirect` set, so the target warms-or-provisions locally and a stale
   view can never ping-pong. The same redirect fires when the node lacks a
   golden for the key but gossip names a template owner, and when the node
   sits at `max_claims` while a peer reports warm capacity.
3. **No candidates** — the node provisions locally: a golden clone (tens of
   ms) or a cold boot for an unpooled key.

The data plane is never proxied between nodes: the claim response carries
`owner_addr` and all sandbox traffic dials the owner directly.

Node death is honest: a dead node's sandboxes die with it (memory state is
node-local by design). SWIM detects the death and peers stop redirecting to
it.

### Volumes and placement

A volume name has one fleet-wide meaning and access list, while catalog
membership is node-local and deliberately excluded from the cluster config
digest. Nodes gossip only their currently available catalog names: host paths
and access lists never leave the node. After config load the set appears on the
next gossip tick; later image distribution or removal is detected the same way.
The node epoch bumps only when the advertised name set changes.

A writable name (`writable: true`) is expected to have exactly one holder
fleet-wide — the operator contract in
[deploy](deploy.md#writable-dataset-volumes), not a mechanism this layer
enforces. Because a node only ever advertises catalog names it actually
holds, every claim for that name — `ro` or `rw` — already resolves to the
single node that has it through the ordinary redirect logic below; there is
no new gossip field or admission message for writable routing. Configuring
the same writable name on two nodes is an operator error the fleet has no way
to detect.

A volume claim may consume an ordinary warm VM because attach happens after the
pop and before finalization. Warm candidates retain their normal ranking, but a
candidate must advertise every requested volume. If the entry node cannot serve
all requested names, it redirects once to a peer advertising their intersection.
A promoted-template claim uses the intersection of template owners and volume
owners first. If that advertised intersection is empty, a volume holder gets one
chance to prove it can resolve the template from a shared store. The target
retries with `no_redirect` plus the carried `require_promoted` intent and
validates both resources before provisioning, so a node-local template still
fails without a second hop even while template gossip is one tick stale.

`GET /v1/volumes` and the SDK discovery calls return the gossiped union filtered
through the answering node's fleet-uniform access lists. `nodes` counts members
advertising each name, while `available` and `size_bytes` describe only the
answering node's image, and `writable` is the entry's catalog configuration,
uniform fleet-wide. No node address or dataset-to-host mapping is returned;
claim placement resolves the holder.

## Querying members

`GET /v1/info` (root `api_token`) reports this node's pools plus the peer
data-plane addresses it currently sees:

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://node-a:7777/v1/info | jq .
```

```json
{
  "pools": [
    {"key": {"template": "base:24.04", "net": "none", "size": "small"},
     "warm": 4, "refilling": 0, "target": 4, "golden": true}
  ],
  "claimed": 2,
  "hibernated": 0,
  "archived": 0,
  "peers": ["10.0.0.6:7777"]
}
```

`peers`, served by `GET /v1/peers` (root or tenant token), is what the SDK
scatters across in `Lookup`. An empty/absent `peers` means a mesh of one.

## Relocating a handle

If a client kept a sandbox's `id` and `token` but lost the owner address
(process restart, handle passed between services), `Client.Lookup` asks the
entry node, then queries every peer concurrently
(`GET /v1/sandboxes/{id}/owner` — the token is both authorization and
ownership proof) and returns a handle bound to whichever node confirms
ownership first.

```go
// Persist sb.ID and sb.Token() before the process exits; dial any node to
// rebind — Lookup finds the owner wherever the sandbox actually lives.
client, _ := sandbox.Connect("10.0.0.6:7777", sandbox.WithAPIToken(token))
sb, err := client.Lookup(ctx, savedID, savedToken)
```

## Templates on a cluster

A promoted template (see the [SDK guide](sdk.md#promoting-to-a-template))
lives in its node's checkpoint store. On the default local-disk backend
that means **only on its owner node**, and the mesh gossips each node's
template set alongside its warm counts; on a shared store (a FUSE-mounted
`checkpoint_dir` or the s3 backend) every node resolves every template
directly, so the gossip routing simply never fires. An export carries only
the sandbox's writable state — the base image blobs resolve from the
claiming node's local store, pulled by digest when missing (`vm clone
--pull`). Name-based calls route cluster-wide:

- `Client.New("tpl")` at any node redirects to the template's owner when the
  entry node has no golden for the key (warm peers still win first).
- `Client.DeleteTemplate("tpl")` at any node follows the same gossip to the
  owner and deletes there.

Gossip is eventually consistent: a template promoted a moment ago may be
invisible to name-based calls for about a gossip tick (the claim fails
cold — retry), and one deleted a moment ago may still redirect to a 404.
Correctness is never violated; only the name-based convenience lags. The
handle `Sandbox.Promote` returns is still owner-bound. `template.Delete` and a
`template.New` without volumes dial the owner directly; a volume claim may use
one placement redirect to a node that can resolve both resources.

## Checkpoints on a cluster

A checkpoint lives on whichever node captured it (the default
[`checkpoint_dir`](deploy.md#configuration)) unless the store is shared (a
FUSE mount or the s3 backend), in which case every node resolves every
checkpoint directly and nothing below applies.

On the default per-node store, a branch claim (`Checkpoint.New` /
[`POST /v1/checkpoints/{id}/claim`](sandboxd-api.md#post-v1checkpointsidclaim))
at a node that does not hold the record runs a tier order:

1. **Local claim** — the fast path when the claiming node already holds it.
2. **Probe + redirect** — a live `HEAD` fan-out (HMAC-signed when the mesh
   carries a `cluster_key`) to every mesh peer in parallel, redirecting to
   the first owners that answer — the same claim-redirect contract a warm
   miss already uses. The answer is capped at 3 addresses: a hint, not an
   exhaustive list of every owner. The probe and
   the follow-up claim are not atomic: a peer can answer the probe, then
   lose the record — a delete's broadcast lands, or its own TTL sweep runs
   (below) — before the retry reaches it, so a redirect can go stale
   between the two calls.
3. **Heal** — when `checkpoint_peer_heal` is on and neither of the above
   answered, the node pulls the record from a probed peer, validates its
   shape and id before trusting it, and publishes it locally so later
   branches are local — paid once per node. The pull is bounded to one
   overall time budget across however many peers it tries (`healBudget` in
   `store/peer`), and a node accepts only so many heals at once
   (`maxConcurrentHeals` in `pool`) — past that cap it answers `503` rather
   than queuing indefinitely.

### Delete is eventual, not a fleet-wide revocation

`Checkpoint.Delete` (`DELETE /v1/checkpoints/{id}`) removes the local
record, then best-effort broadcasts the delete to every peer this node
currently sees, so a replica a heal pulled earlier is cleaned up too, not
just the original. A peer that is offline or partitioned during that
broadcast keeps its copy until the checkpoint TTL ages it out. A healed
replica carries the source checkpoint's original `CreatedAt`, so it becomes
eligible for expiry at the same instant everywhere; each node then removes
it on its own hourly sweep, which is independently phased and retries on
failure. The window in which a deleted checkpoint stays branchable by an
id-holder is therefore normally `checkpoint_ttl_hours` plus the wait for the
next hourly sweep; a sweep that fails retries on a later one, so persistent
sweep failure extends retention until one succeeds — the TTL is the eligibility
point, not a hard ceiling. This is why heal *requires* a nonzero, fleet-matching
TTL (see [cluster-invariant config](#cluster-invariant-config)).
With TTL disabled it could never close at all, so that combination is
rejected at config load.

## State ownership

Each kind of state has exactly one source of truth, and a restart rebuilds
fully from it:

| state | source of truth | survives restart |
|---|---|---|
| operator config (`tenants`, `volumes`, `secrets`, egress policies, `bridges`/`networks`, `mesh`, `preview_secret`, `egress_ca`) | `config.json` (human/deploy-tool owned) | re-read at boot |
| API-applied pool targets (`PUT /v1/pools`) | `<data_dir>/pools.json` (machine owned) | yes |
| claims | the claims journal + `Reconcile` | yes |
| placement hints (warm counts, template and volume sets) | gossip | rebuilt |
| checkpoint ownership | a live per-request probe (no gossip); a healed replica is this node's own persisted copy, aged out by `checkpoint_ttl_hours` | yes |

Pools are managed API-first. The first time a node takes `PUT /v1/pools`, it
writes the applied set to `<data_dir>/pools.json` and from then on **that file
seeds the pools at boot**, overriding the `pools` section of `config.json` (a
loud log line notes this; if the two disagree because someone edited
`config.json` afterward, a second warning names it). The file's presence is the
ownership marker — delete `pools.json` to return the node to config-owned pools.
Egress stays config-owned regardless: the API rejects egress specs, so
`pools.json` never carries them, and egress policies re-merge from `config.json`
by pool key at boot.

Cluster-wide pool changes are a client-side fan-out, not a gossiped desired
state (pools are legitimately heterogeneous per node): `Client.SetPoolsCluster`
PUTs the set to the entry node and every peer and returns a per-node result;
retrying the failed nodes is the whole protocol, because the apply is an
idempotent declarative replace. A non-nil error means peer discovery itself
failed and the fan-out reached only the entry node — an incomplete apply to
retry, kept distinct from a genuine single-node cluster (nil error). It fits a
homogeneous cluster — a spec set that names an egress-lane pool is refused on a
node without an attachment, so nodes that differ in capacity or attachment take
a per-node `Client.SetPools`.

### Cluster-invariant config

Some config must match on every node or the cluster fails in confusing ways:

| config | what breaks on mismatch |
|---|---|
| `api_token`, `tenants` | the SDK replays the authorizing token across a redirect; a mid-rotation peer missing that tenant/token answers 401, which the SDK treats as transient — the claim falls back to the origin node (which already authorized it) and resolves there |
| `preview_secret` | a preview URL signed on one node fails verification on another |
| `mesh.cluster_key` | nodes cannot join / decrypt gossip at all |
| `egress_ca` cluster root | a guest checkpointed/redirected across nodes trusts the root; a divergent root fails interception |

Each node gossips a digest of these (HMAC-keyed by `cluster_key` when set;
otherwise a token-free digest of tenant names + the CA root, so nothing
brute-forceable rides cleartext gossip). A mismatch logs a warning at the moment
the divergent node appears — not at the first unlucky redirect — and raises the
`sandboxd_config_digest_mismatch` gauge. It is warn-only: a rolling credential
rotation is a legitimate transient mismatch, so a divergence never partitions
the mesh.

## Cluster checklist

- memberlist port (e.g. 7946) open node-to-node, TCP **and** UDP
- `advertise_addr` set to a routable address on every node (never loopback,
  never a wildcard)
- same `api_token`, `tenants`, `preview_secret`, and `egress_ca` root everywhere
  (a mismatch warns and shows in `sandboxd_config_digest_mismatch`)
- `cluster_key` set if the gossip network is not otherwise trusted
- pool changes via `Client.SetPoolsCluster` (or per-node `SetPools`); the applied
  set persists to `pools.json` and survives restart
- keep each volume name's dataset identity and access list identical across
  the fleet; distribute a read-only image to every node meant to advertise
  it, but a writable (`writable: true`) image belongs on exactly one node —
  verify the fleet view and holder count with `GET /v1/volumes`
