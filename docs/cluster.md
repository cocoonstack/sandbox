# Clusters

A cluster is a set of sandboxd nodes joined through a
[hashicorp/memberlist](https://github.com/hashicorp/memberlist) SWIM mesh.
Gossip carries only placement hints — per-pool warm counts, promoted-template
hashes, available volume names, each node's data-plane address, and a digest
of the cluster-invariant config.
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
  a new mesh. A node that reaches none of them exits at start (`start mesh:
  join mesh: … connection refused`), so two nodes that name only each other
  cannot cold-start together: list the node's own `bind` alongside its peers
  and every node comes up in any order, joining whoever is already there
- `cluster_key` — optional base64 key (16/24/32 bytes) enabling gossip
  encryption; all members must share it

Two constraints, fine for a homogeneous cluster: all nodes share the same
`api_token` and tenant set (the SDK replays whichever token authorized a
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
Behind a TLS edge, set each node's `client_advertise` to its own public HTTPS
origin. Owner, redirect, and peer responses use these origins; gossip, peer
probes, and preview forwarding retain the internal `advertise_addr`. See
[TLS termination](deploy.md#tls-for-sdk-clients) for the edge setup.

Node death is honest: a dead node's sandboxes die with it (memory state is
node-local by design). SWIM detects the death and peers stop redirecting to
it.

### Volumes and placement

A volume name has one fleet-wide meaning and access list, while catalog
membership is node-local and deliberately excluded from the cluster config
digest. Nodes gossip only their currently available catalog names: host paths
and access lists never leave the node. After config load the set appears on the
next gossip tick; later image distribution or removal is detected the same way.
The node epoch bumps only when one of the gossiped sets — warm counts,
template hashes, volume names — actually changes.

A writable name (`writable: true`) still needs its catalog entry — name,
access list, and the `writable` flag — declared identically on every node
meant to serve it, the same rule the read-only case already needs: gossip
carries only currently-available names, never ACL or `writable` metadata, so
a tenant claim landing on a node with no local entry for that name is a hard
error (unknown/forbidden), not a gossip redirect. Root tokens carry no ACL to
enforce and are exempt — they can still redirect off gossip alone. What a
writable entry adds is a constraint on the backing *image file*, not the
entry: exactly one of those nodes should actually have the file present at
the configured path (the operator contract in
[deploy](deploy.md#writable-dataset-volumes)); the others log a missing-path
warning and never advertise the name, so an ordinary claim still funnels to
that one holder. Two nodes both holding the writable file is the operator
error the fleet cannot detect — two nodes both declaring the entry is normal
and expected.

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

One key should be pooled or promoted fleet-wide, not both — the same class of
operator error as two nodes holding one writable image. `promote` refuses a key
the answering node pools, but nothing coordinates that across the mesh, so a
peer can promote what another node pools and the two resolve to different
content. Each node then answers with its own: a node holding a pool golden
serves it to every claim for that key and ignores peer template
advertisements, so volume and non-volume claims there never disagree. A claim
that sets `require_promoted`, volume or plain, is refused with 404 instead of
being handed the pool golden or a cold boot, which lets the SDK walk on to a
node that has the template.

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
ownership first. With `archive_failover`, a node may adopt a claim whose
owner's node lease expired and answer for it (see
[archived claims across node loss](#archived-claims-across-node-loss)).

```go
// Persist sb.ID and sb.Token() before the process exits; dial any node to
// rebind — Lookup finds the owner wherever the sandbox actually lives.
client, _ := sandbox.Connect("10.0.0.6:7777", sandbox.WithAPIToken(token))
sb, err := client.Lookup(ctx, savedID, savedToken)
```

## Archived claims across node loss

A claim lives on one node, so a lost node takes its running and hibernated
claims with it, unless the claim is archived or [replicated](#running-claims-across-node-loss).
An archived claim keeps only a pointer on the node; with
[`archive_failover`](deploy.md#configuration) on every node (it needs
`meta_store` with a `cell`, an `s3` `checkpoint_store` and a `mesh`), the
archive also writes one row to the shared PostgreSQL, scoped by the cell: id,
tenant, token SHA-256, owner node, epoch, a consumed flag, checkpoint id,
retention deadline and the claim record without its token. Only archives and
replication rounds write it, so the claim path is unchanged.

- **Adoption.** `Lookup` asks the entry node, then every peer. A node that does
  not hold the claim reads the row; when the token hash matches, it takes the
  row with one compare-and-set on the epoch. The same statement requires the
  owner's [node lease](#node-lease) to be expired, unless the owner is this
  node (with its record gone). The node records the claim as archived and
  answers as the owner. Two nodes adopting at once leave one owner; the other
  answers 404. A tombstone is never adopted. The next exec or file call wakes
  the claim from the store as usual. An owner with a live lease answers for
  itself, and a node that tries to take its row is refused with a 404.
  A draining node never adopts. Adoption checks no tenant `max_claims` or node
  `max_claims`: the claim already exists, and a refusal could strand it.
- **Fencing.** Take, consume and delete are compare-and-sets on the row's
  epoch, and so is the write of a replica round and of the archive of a claim
  that holds a replica (`Replace`, which also requires this node as owner and a
  live row): an absent row at a held epoch refuses it, so a round that outlived
  an adoption cannot recreate the row. The writes that remain owner-guarded
  upserts are the first write of a row, when this node holds no epoch (the first
  replica, the archive of a claim with no row), and the re-arm of this node's
  own tombstone. Every epoch comes from one database sequence, so none is ever
  reused. A wake takes
  the row before it restores and consumes it before it commits: the row turns
  into a tombstone, deleted only once the checkpoint is gone. A release or a
  retention purge consumes the row before the checkpoint goes; with failover
  on, a release holds the claim's transition lock, so an archive cannot commit
  a row under it. An old owner that comes back finds the row moved on (another
  owner, a tombstone of another owner, or no row at all): its wake and release
  answer unknown sandbox (the client looks it up again), its restart drops the
  record, and none of them deletes the checkpoint, which belongs to the new
  owner; the drop clears the old owner's delete marker first. Only a tombstone
  of its own means this node's commit never happened: the wake or the restart
  re-arms it and keeps the claim. Orphan reclaim at restart never deletes a
  checkpoint a row references. A restart also settles an archive a crash
  interrupted: a live row it left for a claim it still holds is deleted with
  its checkpoint, and a claim whose row another node holds is dropped, its VM
  and snapshot swept as stale. An archive journals its intent (the checkpoint
  it is about to register) before it writes the row. A restart that finds the
  intent and no row keeps the claim while that checkpoint is still in the
  store, since the row never committed, and drops it once the checkpoint is
  gone, since only a node that consumed the row deletes it.
  A put whose outcome is unknown leaves the intent in place, and a release,
  the next archive and the hourly sweep settle it by the same rule before
  they act on the claim.
- **Departed owners and tombstones.** Every hour, on the store retention sweep,
  each node reads the rows past their retention deadline whose owner's lease
  expired, the consumed rows, and its own rows. An expired row of such an owner
  is purged checkpoint first, then the row. No node touches another node's tombstone:
  the owner settles it when it returns, through its sweep or its restart.
  The epoch compare-and-set lets exactly one node win each,
  and `archive_delete` is recorded only for a row that was still archived.
  Each node also settles its own rows against its claims, which converges a
  registry write whose reply was lost: a tombstone its archived claim still
  names is re-armed, a live row next to a claim that names another checkpoint
  is an archive that never committed and goes with its export, and a
  tombstone no claim holds is purged checkpoint first. Retention is a lower
  bound: a purge can run up to an hour late. An archive kept forever (no
  `archive_delete_after_seconds`) stays until a client adopts and releases it.
- **Usage journal.** The journal stays per node: an adopted claim's `archive`
  is in the old owner's `usage.jsonl`, and its `unarchive`, `release` or
  `archive_delete` is in the adopter's or the purging node's. Fold the journals
  of all nodes by sandbox id.

The node lease decides adoption, so a partitioned owner can lose an archived
claim it still lists once its lease runs out; the epoch fences it out. A claim
that has entered its wake counts as running. Running and hibernated claims of a
pool without `replicate_seconds` are lost with their node, and an owner that
never returns leaves behind only the tombstones and checkpoints of the claims it
was waking or releasing when it died.

### Node lease

With `archive_failover` every node renews one row of `sandboxd_nodes (cell,
node, lease_until)` every 5 seconds, with a single upsert on the database clock
that covers the whole node: `lease_until = now() + 15 s`. A renewal that fails
once does not lose the lease or trip the fence, as long as the next one
answers within 2 seconds of its start. The table is created at start when it is missing, and a node
with no row counts as expired. Adoption and the departed-owner sweep consult the
lease, never mesh membership, so a node the mesh lost but the database still
hears keeps its claims, and a node the mesh still lists but the database no
longer hears loses them. The cost is one statement per node every 5 seconds; no
claim path or data plane reads it.

### Running claims across node loss

A pool entry with [`replicate_seconds`](deploy.md#configuration) (a `none`-lane
pool on a node with `archive_failover`) snapshots its running claims to the
checkpoint store and keeps each claim's registry row on the latest snapshot:

- **Round.** Every interval, each running claim of the pool is checkpointed:
  cocoon pauses the guest, captures memory and the writable disk, and resumes
  it, so the capture is consistent for any disk format and needs no guest
  cooperation. The record is published as a replica: hidden like an archive's
  wake image, so it is not listed, branched or deleted through the API. Then
  the claim's row is put with this node as owner and the replica as checkpoint,
  and the previous replica is deleted. A round that fails logs a warning and
  retries at the next interval; the old replica and row stay as they were, and
  the claim keeps running. The round journals the checkpoint it is about to
  register before it writes the row, and deletes it only when the write is
  refused. A write whose outcome is unknown leaves the checkpoint in place, and
  the next round, a release, an archive, a wake, the hourly sweep and a restart
  settle it by reading the row: a row on that checkpoint promotes it, another
  owner's row drops the claim, and an absent row means the adopter finished
  when the checkpoint is gone and a write that never landed when it is still
  there. A refused write reads the row: one this node still owns only had a
  stale epoch and is refreshed, and an absent or foreign row drops the claim and
  its VM. A claim gets its first replica one interval after it is claimed (or
  after a restart). A hibernated, archived or failed claim,
  one whose tenant was removed, and one with volumes (capture refuses them;
  logged once per claim) is skipped; a replicated claim that gains volumes
  drops its row and replica. The claim's policy pool, not its own template key,
  picks the interval, so a claim cloned from a template promoted out of a
  replicating pool follows that pool. Rounds share the maintenance budget with
  refill and never take the share refill keeps. A node whose lease is overdue
  starts none, and a round whose registry is down fails before it captures.
- **Restore.** A lost node's replica is adopted like an archive: the first
  `Lookup` that reaches another node after the lease expires takes the row,
  records the claim as archived on the replica with the adopter pool's
  retention window, and the next call wakes it
  through the archive wake, which restores the guest with its memory and
  processes. The claim resumes from its last snapshot: everything after it, and
  the connections open at the time, are lost, so the data loss is at most the
  time since the last successful round: one interval while rounds succeed, more
  while they fail and the older replica stands.
- **Lifecycle.** A release consumes the row, then deletes the replica and the
  tombstone. A hibernate leaves the row on the last replica. An archive puts its
  own checkpoint over the row, with its retention deadline, and deletes the
  replica. A lease expiry
  with `on_expire` release drops the row and the replica first, and the next reap
  destroys the claim; with `on_expire` archive, the archive puts its checkpoint
  over the row. Turning `replicate_seconds` off takes a restart, since a reload
  that changes it is refused; the restarted node drops the rows and replicas of
  the pool's claims on its first pass. A claim that holds a replica when the
  node restarts with replication off is still treated as replicated by the fence
  until the next restart, so during a registry outage it answers 503 like a
  replicated claim; that costs availability only, never a second copy or data. A restart keeps the node's own live row for a running claim
  and takes the checkpoint a crashed round left on it; a claim whose row another
  node holds, or whose row a finished adopter consumed, is dropped with its VM.
  A replica row has no retention deadline: the hourly sweep never purges it,
  because no round refreshes it while the claim is hibernated, and retention
  starts when the claim is archived or adopted. The rows of a node that never
  returns therefore stay until a client adopts them, a bounded residue of one
  row and one checkpoint per claim. Apart from that and the lease rule the
  sweep is unchanged.
- **Self-fencing.** A node whose renewal is overdue by 12 seconds (the lease
  length minus a 3 second margin, counted from the start of the last renewal
  that answered) stops serving every replicated claim it holds: relay, exec,
  ports and preview answer `503 lease lost; retry`, and `/owner` answers 404 so
  a `Lookup` moves on to the other nodes. A claim is replicated when its policy
  pool has `replicate_seconds`, from the moment it is claimed, adopted or
  loaded, whether or not a round has run, so the fence covers it before its
  first replica; the setting is restart-only so that decision cannot go stale.
  The guest has no NIC on the `none` lane, so it is unreachable except through
  the node, and the node destroys nothing. Claims of pools that do not
  replicate stay served. A node starts fenced and lifts the fence after its
  first renewal that is followed by a confirmation. The confirmation runs beside the renew loop, which keeps
  renewing: for each replicated claim the node takes the row back for itself in
  one compare-and-set, which the row lock serializes against adopters, and
  retries once on a stale epoch. A claim whose row another node took, or took
  and finished, is forgotten at once and its VM removed in the background; a
  claim whose row is still this node's but keeps refusing the write leaves the
  node fenced and is retried at the next renewal. The lift replaces the fenced
  state this renewal saw, only if it is still current, and trips again at once
  if more than 12 seconds passed since the start of that renewal; a slower
  confirmation therefore leaves the node fenced for the next renewal to repeat. The margin makes the fence close 3 seconds before
  another node may adopt, so no moment has two serving copies, partitions
  included. This holds while the node's clock and the database's run at the same
  rate to within that margin over 12 seconds, and ignores skew between them,
  because both sides count from a renewal's own start. The guest of a claim the
  fence closed keeps running without a NIC until the node returns and removes
  it, and its state is discarded. When the fence closes, every open
  stream of a replicated claim on the node is closed too, one opened before the
  claim's first replica included: the agent relay
  (including parked keep-alive relays, whose later RPCs never pass the
  resolution gate), the exec call, port relays and preview requests. A client
  retries and meets the 503 until the lease is back or the claim moved.
  Streams of claims that are not replicated stay open.
- **Availability.** The design chooses consistency over availability for
  replicated claims. A registry outage longer than the fence threshold (12
  seconds) stops service of replicated claims on every node until PostgreSQL
  returns, because no node can prove it still owns them. Claims of pools that
  do not replicate are unaffected. An archived claim of a replicating pool is
  gated like a running one while the node is fenced, so its wake waits too.

A replica costs, per claim per interval, one capture pause, one upload, one
registry read, one write and one delete; the node's own cost is one scan of its
claims every 5 seconds. Resolving a claim for a stream of a pool that does not
replicate reads one plain field and no atomic; a replicated claim adds one
atomic load of the fence state and its stream one watcher goroutine. Adding a
claim to a failover node gains one map lookup. Nothing runs per frame.

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
| operator config (`volumes`, `secrets`, egress policies, `bridges`/`networks`, `mesh`, `preview_secret`, `egress_ca`) | `config.json` (human/deploy-tool owned) | re-read at boot; its reloadable part on a per-node [reload](deploy.md#reloading-the-config) |
| API-applied pool targets (`PUT /v1/pools`) | `<data_dir>/pools.json` (machine owned), or the cell's row in the shared `meta_store` when the node sets `meta_store.cell` | yes |
| API-applied tenants (`/v1/tenants`) | `<data_dir>/tenants.json` (machine owned), or the shared `meta_store` table | yes |
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

With [`meta_store.cell`](deploy.md#shared-pool-set), the set belongs to the
node's cell: one `PUT /v1/pools` on any node of the cell reaches every node in
it, and no fan-out is needed; every node of the cell runs the same targets. This
pool cell is only a name for nodes that share one pool set in the database. It
is not a mesh cell (see [scale](#scale)): a pool cell may cover part of a mesh
or nodes of several meshes, and gossip, redirects and heals ignore it.

Without it, cluster-wide pool changes are a client-side fan-out, not a gossiped desired
state (pools are legitimately heterogeneous per node): `Client.SetPoolsCluster`
PUTs the set to the entry node and every peer and returns a per-node result;
retrying the failed nodes is the whole protocol, because the apply is an
idempotent declarative replace. A non-nil error means peer discovery itself
failed and the fan-out reached only the entry node — an incomplete apply to
retry, kept distinct from a genuine single-node cluster (nil error). It fits a
homogeneous cluster — a spec set that names an egress-lane pool is refused on a
node without an attachment, so nodes that differ in capacity or attachment take
a per-node `Client.SetPools`.

### Tenants

Tenants are API-only: `config.json` has no `tenants` field, a node starts
with an empty set, and each
[tenant API](sandboxd-api.md#tenants-v1tenants) change rewrites
`<data_dir>/tenants.json`, which the node loads at boot. Unlike pools, the set
must be equal on every node,
because the SDK replays the authorizing token across a redirect. The change is
still a client-side fan-out: `Client.PutTenantCluster`,
`Client.DeleteTenantCluster` and `Client.SetTenantsCluster` apply it to the
entry node and every peer and return a per-node result, and retrying the failed
nodes converges because each apply is idempotent (a cluster delete counts a
node that no longer holds the tenant as done). Until every node has it, a
redirect to a node that lacks the tenant answers 401, which the SDK treats as
transient and resolves at the origin. `GET /v1/tenants` reports each node's set
`digest`, and the gossiped config digest below covers the live set, so a node
that missed a change shows in `sandboxd_config_digest_mismatch`. A node that is
down during a fan-out has left the peer list, so it is absent from the results
rather than failed: when it rejoins with its old `tenants.json`, that mismatch
names the divergence, and one apply to that node closes it.

Without a shared store, every node holds the whole tenant set in memory and
rewrites `tenants.json` on each change, so the set suits thousands to tens of
thousands of tenants: control planes, organisations, services. Map end users
onto them in the control plane (`claim_ref`, `metadata`) rather than giving
each user a tenant.

With [`meta_store`](deploy.md#shared-tenant-database), the tenant set lives in
one PostgreSQL table that every node reads, so one tenant per end user works:
- **Writes:** a `/v1/tenants` write on any node commits there with a `NOTIFY`
  that evicts the name on every node. There is no fan-out, no `tenants.json`,
  and the set is not part of the gossiped digest; `GET /v1/tenants` reports no
  `digest`.
- **Cache:** each node caches the tenants it has seen. A tenant's first request
  on a node is one indexed query; later ones are a map hit. Unknown tokens are
  cached as misses for 10 s, and a node sends at most 200 cold lookups per
  second, so random tokens cannot load the database.
- **Database down:**
  - cached tenants keep authenticating and their claims keep running;
  - an uncached tenant answers 503, and writes fail;
  - a node whose invalidation listener reconnects drops its cache, so it never
    serves a change it missed while disconnected;
  - a change made while a node's listener was down reaches that node at the
    listener's next retry. Keepalive finds a dead connection in about 25 s and
    retries back off to 30 s, so the change lands within about a minute of the
    database returning;
  - renew, fork and wake ask the database whenever the cache lacks the
    tenant, so they never outlive a removal. While the database is down, they
    answer 503 for a tenant the node has not cached.

### Cluster-invariant config

Some config must match on every node or the cluster fails in confusing ways:

| config | what breaks on mismatch |
|---|---|
| `api_token`, the tenant set | the SDK replays the authorizing token across a redirect; a mid-rotation peer missing that tenant/token answers 401, which the SDK treats as transient — the claim falls back to the origin node (which already authorized it) and resolves there |
| `preview_secret` | a preview URL signed on one node fails verification on another |
| `mesh.cluster_key` | nodes cannot join / decrypt gossip at all |
| `egress_ca` cluster root | a guest checkpointed/redirected across nodes trusts the root; a divergent root fails interception |
| egress: `egress_classes`, each pool's `egress` and `egress_upstream_env`, `secrets` names, headers and `value_env`, `egress_internal_allow`, `egress_upstream` | a claim or tenant redirected to a divergent node reaches different hosts, and its [claim credentials](egress.md#claim-credentials) are refused or not injected there |
| the engine root path (cocoon's `root_dir`) | a checkpoint or template export pins the base image blobs' absolute path under the root that captured it; a node whose root sits at another path refuses to clone a record it healed or pulled (`untrusted storage path in snapshot metadata`) — the heal succeeds, the branch fails. Not part of the gossiped digest below |

Each node gossips a digest of these over its live tenant set and quotas (HMAC-keyed by
`cluster_key` when set, covering each tenant token's SHA-256; otherwise a
token-free digest of tenant names and quotas + the CA root + `checkpoint_ttl_hours`, so
nothing brute-forceable rides cleartext gossip) plus the egress config above in
an order-free form, never a secret value, and republishes it after every tenant
change and every config reload. A mismatch logs a warning at the moment
the divergent node appears — not at the first unlucky redirect — and raises the
`sandboxd_config_digest_mismatch` gauge. It is warn-only: a rolling credential
rotation is a legitimate transient mismatch, so a divergence never partitions
the mesh.

## Scale

Every memberlist push/pull carries the whole cluster's placement state: each
node's warm counts, template set and volume set, as one JSON document. Its
size grows with nodes × templates per node:

| nodes | templates per node | state per push/pull |
|---|---|---|
| 1,000 | 5 | 0.6 MB |
| 10,000 | 5 | 6.3 MB |
| 10,000 | 500 | 353 MB |

memberlist refuses a push/pull state over 20 MiB, and above 32 nodes it
stretches the push/pull interval with log2 of the cluster size (10 s at 10,000
nodes), so a change takes minutes to reach every node while warm counts move
with every claim.

- **Size one mesh in the hundreds of nodes.** Split a larger fleet into
  independent meshes (cells), each with its own `mesh.join`, and route users
  to a cell in front of them. Redirects, heals and template lookups stay
  inside the cell. A mesh cell is not configured by `meta_store.cell`, which
  only names the nodes that share a pool set (see
  [state ownership](#state-ownership)).
- **Keep per-user data out of the gossip.** A promoted template is gossiped by
  every node that holds it, so templates belong to pools or tenants, not to
  end users. At 500 templates per node the state reaches the 20 MiB limit near
  600 nodes. Checkpoints are not gossiped: their owner is probed per request.

## Cluster checklist

- memberlist port (e.g. 7946) open node-to-node, TCP **and** UDP
- `mesh.join` on every node lists the node's own `bind` plus its peers, so a
  fleet-wide restart comes up in any order
- `advertise_addr` set to a routable address on every node (never loopback,
  never a wildcard)
- the same engine root path (cocoon's `root_dir`) on every node; it is not in
  the digest, so a mismatch shows only at the first cross-node branch
- same `api_token`, tenant set, `preview_secret`, and `egress_ca` root everywhere
- with `meta_store`, every node's DSN points at the same database: the tenant
  set is not in the gossiped digest, so nodes on different databases serve
  diverging tenants without a warning
  (a mismatch warns and shows in `sandboxd_config_digest_mismatch`)
- `cluster_key` set if the gossip network is not otherwise trusted
- one mesh sized in the hundreds of nodes, and promoted templates per pool or
  tenant rather than per end user (see [scale](#scale))
- pool changes via `Client.SetPoolsCluster` (or per-node `SetPools`); the applied
  set persists to `pools.json` and survives restart. With `meta_store.cell`,
  one `SetPools` on any node reaches every node of the cell
- tenant changes via `Client.PutTenantCluster` / `DeleteTenantCluster` /
  `SetTenantsCluster`, retrying the nodes that failed; the applied set persists
  to `tenants.json`. With `meta_store`, one write to any node reaches all of them
- declare each volume's catalog entry — name, access list, and `writable`
  — identically on every node meant to serve it (a tenant claim gets a hard
  error, not a redirect, on a node missing the entry); put the actual image
  file on every one of those nodes for a read-only name, but keep a writable
  name's file on exactly one of them — verify the fleet view and holder
  count with `GET /v1/volumes`
