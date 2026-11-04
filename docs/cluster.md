# Clusters

A cluster is a set of sandboxd nodes joined through a
[hashicorp/memberlist](https://github.com/hashicorp/memberlist) SWIM mesh.
Gossip carries only placement hints — per-pool warm counts, promoted-template
hashes, available volume names, each node's data-plane address, and a digest
of the cluster-invariant config.
Gossip carries no per-sandbox state, so a stale view costs at most
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
   sits at `max_claims` or `max_running`: a warm peer first, else a peer that
   serves the pool. A node that would refuse a claim stops gossiping its pools
   within a tick. A stale view can still name one; it answers 429, and the SDK
   then tries the next one and the origin.
3. **No candidates** — the node provisions locally: a golden clone (tens of
   ms) or a cold boot for an unpooled key.

The data plane is never proxied between nodes: the claim response carries
`owner_addr` and all sandbox traffic dials the owner directly.
Behind a TLS edge, set each node's `client_advertise` to its own public HTTPS
origin. Owner, redirect, and peer responses use these origins; gossip, peer
probes, and preview forwarding retain the internal `advertise_addr`. See
[TLS termination](deploy.md#tls-for-sdk-clients) for the edge setup.

Node death is honest: a dead node's sandboxes die with it (memory state is
node-local by design), except
[archived](#archived-claims-across-node-loss) and
[replicated](#running-claims-across-node-loss) claims. SWIM detects the death and peers stop redirecting to
it.

### Volumes and placement

A volume name has one fleet-wide meaning and access list, while catalog
membership is node-local and deliberately excluded from the cluster config
digest. Nodes gossip only their currently available catalog names: host paths
and access lists never leave the node. After config load the set appears on the
next gossip tick; later image distribution or removal is detected the same way.
The node epoch bumps only when one of the gossiped sets — warm counts,
template hashes, volume names — or the config digest actually changes.

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
retention deadline and the claim record without its token. Archives,
replication rounds, and env writes and renews of a claim whose row must follow
it while it sleeps write it, so the claim path is unchanged.

- **Adoption.** `Lookup` asks the entry node, then every peer. A node that does
  not hold the claim reads the row; when the token hash matches, it takes the
  row with one compare-and-set on the epoch. The same statement requires the
  owner's [node lease](#node-lease) to be expired, unless the owner is this
  node (with its record gone) or the row has no owner, since its owner
  released it in an [evacuation](#evacuating-a-node). The node records the claim as archived and
  answers as the owner. Two nodes adopting at once leave one owner; the other
  answers 404. A tombstone is never adopted. The next call that reaches the
  guest wakes the claim from the store as usual. An owner with a live lease answers for
  itself, and a node that tries to take its row is refused with a 404.
  A draining node never adopts, nor does a fenced node or one that lifted its
  fence less than 8 seconds ago (one renewal interval plus one registry query),
  so after a registry outage every healthy owner renews before a peer can take
  its rows. Adoption checks no tenant `max_claims` or node
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
  re-arms it and keeps the claim. Orphan reclaim at restart deletes only archive
  checkpoints this node published, and never one a row references. A restart
  also settles an archive a crash interrupted: a live row it left for a claim it
  still holds is deleted with its checkpoint, and a claim whose row another node
  holds is dropped, its VM and snapshot swept as stale. An archive journals its
  intent (the checkpoint it is about to register) before it writes the row. A
  restart that finds the intent and no row keeps the claim while that
  checkpoint is still in the store, since the row never committed, and drops it
  once the checkpoint is gone, since only a node that consumed the row deletes
  it. A replicated claim's row stays: one the archive moved goes back to the
  replica, so the row is never absent while the claim is held.
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
  A checkpoint the claims journal may still name is never deleted: after a
  failed journal write its delete waits for the next write that lands, and a
  replica retirement keeps its tombstone until the hourly sweep after that.
- **Usage journal.** The journal stays per node: an adopted claim's `archive`
  is in the old owner's `usage.jsonl`, and its `unarchive`, `release` or
  `archive_delete` is in the adopter's or the purging node's. Fold the events
  of all nodes by sandbox id. With `meta_store.usage` every node lands its
  journal in the cell's [shared usage table](deploy.md#shared-usage-table), and
  [`GET /v1/usage`](sandboxd-api.md#get-v1usage) on any node returns the
  events of all of them.

The node lease decides adoption, so a partitioned owner can lose an archived
claim it still lists once its lease runs out; the epoch fences it out. A claim
that has entered its wake counts as running. Running and hibernated claims of a
pool without `replicate_seconds` are lost with their node, and an owner that
never returns leaves behind only the tombstones and checkpoints of the claims it
was waking or releasing when it died, plus what a replicated claim leaves (see
[running claims](#running-claims-across-node-loss)).

### Evacuating a node

[`POST /v1/drain?evacuate=1`](sandboxd-api.md#post-v1drain) empties a node for
maintenance or retirement without losing its claims. The node cordons as a
drain does, then moves each claim with bounded concurrency, on the maintenance
budget, archived claims first and running ones last: it hibernates a running claim, archives a hibernated one through the
archive path above, releases the row, and asks a peer to adopt it. A release is
one compare-and-set: the row this node owns at the epoch it holds moves to no
owner at a new epoch. A row with no owner has no lease to wait out, so a peer
takes it with the same epoch compare-and-set as any adoption, and two peers
adopting at once still leave one owner. This node keeps the claim listed until
a peer answers `GET /v1/sandboxes/{id}/owner`, so a root listing finds the claim
on at least one node at every moment, then forgets it under the claim's transition lock
and answers 404 for it; the checkpoint stays, since it belongs to the adopter.
With no adopter (no peer, every peer draining or unreachable) the node takes the
row back and the claim stays here. Until the hand-off, a wake of a claim the
evacuation still has to move answers 503 with `Retry-After`, so a client call
cannot wake it back onto the node. A running claim keeps serving until its turn.
The run takes the budget one batch at a time, so crash recovery and reaps of the
claims that stay keep their turn while it runs.

The release keeps the fencing intact. Every later write of the old owner is
refused: a put is owner-guarded, and a replace, consume or second release
needs the epoch the release moved on. A release that finds its row moved
reads it: a row with no owner is one an outage cut short or a later owner
released, so the node asks a peer to adopt it and never takes it back; a row another node took means
the claim moved already, and the node drops it; a row this node still owns at
another epoch keeps the claim, which is skipped. A crash between the release and a peer's answer leaves a row no node
lists: a restart drops the claim, and the next `Lookup` with its token adopts it,
so a holder that reads the token back with root's `GET /v1/sandboxes/{id}` loses that claim. A
released row past its retention deadline is purged by any
node's hourly sweep, like the row of a departed owner.

An archive the sweeps already run for a claim is waited out and the claim
retried every 5 seconds. Claims that cannot move are skipped and keep serving
here; once `evacuating` is gone `claimed` counts them, and the log names each
one with its reason. A client that holds a handle gets 404 from this node after
the move, the same answer as for a released claim, or a connection error once
the node is retired, and only a `Lookup` tells these apart: the SDKs' `Close`
does this itself. A reader-served connection
gets the retry as an error frame, not a 503, and a preview URL minted before
the move stops working until it is minted again, except behind a shared
`preview_advertise`, where it answers when it reaches the new owner. `DELETE /v1/drain` stops the evacuation: claims not yet started
stay as they are, and one mid-archive finishes its archive but keeps its row.

### Moving a running claim

`POST /v1/sandboxes/{id}/move` (root only) hands one running claim to a named
peer with cocoon's live migration, so its guest keeps its processes and
memory. Evacuation stays the cold path:

| | Cold: `POST /v1/drain?evacuate=1` | Hot: `POST /v1/sandboxes/{id}/move` |
|---|---|---|
| What moves | every claim of a draining node | one running claim |
| How | hibernate, archive to the checkpoint store, release the row, a peer adopts | memory and disk rounds straight to the target, which restores and starts the guest |
| Guest after the move | stopped until the next call wakes it | running, with its processes and memory |
| Target | any peer that answers | the one the caller names |
| When it cannot move | the claim is skipped and keeps serving | the call fails and the claim keeps running here |

A move needs `meta_store` with a `cell`, which holds its record, and
`mesh.cluster_key`. It does not need `archive_failover` or an S3
`checkpoint_store`, since its rounds go straight to the peer.

Only a none-lane claim without volumes moves; a hibernated, archived, failed or
waking claim, an egress-lane claim and a claim with volumes answer 409. Open
exec and pty streams, port forwards and egress connections end; the client
meets a 404 on the source and runs `Lookup`.

**The record.** Each move has a record of its own in the meta store (table
`sandboxd_moves`), separate from the archive rows: `id`, `from`, `to`, `state`
and `epoch`, each change a compare-and-set at a new epoch.

| From | To | Who | When |
|---|---|---|---|
| (none) | `pending` | source | after the target admitted the move, before the baseline round |
| `pending` | `final` | source | the final round paused the guest |
| `final` | `committed` | target | after the receive; the point of no return |
| `committed` | `started` | target, or a later source that holds the claim | after it journaled that it runs the claim |
| `pending` or `final` | `aborted` | source, or a target that holds a received claim or finds the record stale | before the guest resumes |

A new move of the claim overwrites only a `started` or `aborted` record; a
`committed` one refuses it with 409 unless the node making the new move holds the
claim, which proves the target started it, so that node marks it `started` first.
Until a received claim starts, its target gates it like a claim in a move: sweeps
skip it, calls that reach the guest or change the claim answer 503, and operator
calls answer 409.

The target commits only from `final`, so it never starts a guest restored from
a round the source kept running after. The source resumes its guest only on
proof the move stayed here: a record it wrote, at or past the epoch in its
journal, in `pending` or `final` (it writes `aborted` first) or `aborted`. A
`committed` or `started` record, a missing one, or another node's means the
claim moved on, and the source drops it and its VM. While the meta store is
down, a guest paused in the final round stays paused and the call answers 503;
the abort is retried every 2 seconds.

The record outlives each side's need for it. A target with a received claim
starts it on `committed` or `started` and drops it on `aborted` or a missing
record, which is safe because no record is deleted while the target still
journals that claim: it clears its journal first and only then writes `started`.
An abort runs in the order `aborted`, resume, target drop, journal clear, record
delete, so a source holding a journal never finds its own abort missing. The
source deletes a record only in `started` or `aborted`, never `committed`. Each
node's hourly sweep handles its records older than an hour that no move in
flight holds: a target writes `started` over a `committed` record, either side
writes `aborted` over a `pending` or `final` one, and the source deletes the
ended ones.

**Rounds.** The source sends a baseline round, then up to 4 deltas, each over
the control port with the root token. Once the last delta's bytes on the wire at the rate
the rounds reached, plus 400 ms for the receive and the start, fits
`max_downtime_ms` (default 2000, and above 400 since those 400 ms alone would
never fit), the source pauses the guest for the final
round and the target receives the series and starts the guest. A move whose
deltas never fit answers 503 and the guest runs on; from the second delta on, one
that shrinks by under 10% and is still too large answers at once. `downtime_ms` in the answer
is the pause the source measured, from the final capture to the target's
commit. Each round has one minute plus its size at 10 MB/s to arrive, so a
target that stops reading fails the move. The final capture, its transfer and the target's commit have 2 minutes
together; past that the source resolves the move through the record, and the
guest resumes on the source unless the target committed.

**Counting.** The target admits the move like a new claim: node `max_claims`,
`max_running`, the tenant's `max_claims`, `memory_limit` and
`move_concurrency`, and holds a reservation from admission to its commit. The
reservation counts in every cap but is no claim: listings, reaps and gossip
never see it. At the commit the reservation becomes the claim under one lock,
so the target never counts it twice. The source counts the claim until it drops
it after the commit, so the cluster counts a moving claim twice, which can make
a node refuse earlier but never over-admit. `memory_limit` is checked again
before the receive. A new admission from the source that holds the reservation
replaces it and resets its staged files, so a move retried after a source crash
does not wait out the idle drop; one from another source answers 409. A
reservation with no round arriving and none finished or
commit asked for 2 minutes is dropped with its staged files.

**Same paths.** Both nodes need the same path strings in cocoon's `root_dir` and
`run_dir`. The requirement is on the string, not the real directory: a symlink
of one name can lead to different disks on each node, and `data_dir` may differ.
The string has to be set before the VM starts, because the VM's record keeps it.
A move between nodes with different strings fails at the receive, after the
guest paused: cocoon refuses the series with `untrusted storage path in snapshot
metadata`, the call answers 502 and the guest resumes on the source.

**Known limits.**

- A `committed` record whose target never returns stays as one row until a node
  holding the claim moves it.
- Without `sync_claims` a host crash can roll back a node's journal. A source that
  rolled back to a journal whose record is gone drops the claim, since a missing
  record reads as moved on.
- A move call on a claim that moved further than the peer it named answers 200
  with that peer as `owner_addr`; `Lookup` finds the claim.

**Replicated claims.** Before the final round the source consumes the claim's
registry row, since no node adopts a tombstone, then clears the claim's replica
fields and deletes the row, so no third node can adopt a stale replica while
the target runs the claim. It keeps the replica until the move ends. The target's
first replica round puts a new row one interval after the start, and an aborted
move lets the source's next round put it again. A source cut off from the
registry changes nothing and its move fails: it keeps the row's epoch, so the
fence still finds a peer that adopted the claim during the outage, and the source
drops it. A consume whose reply was lost leaves the source's own tombstone, which
the hourly sweep puts back on the replica.

**Failures.** Any failure before the commit aborts: the source resumes the
guest, the target drops its reservation, files and any received VM, and the
call answers the error. A target answer lost after its commit is resolved
through the record, and the call answers 200. A restarted node resolves a move
in its journal by the same rules, so a source back from an outage never resumes
a claim that moved on, and a target that crashed after its commit starts the
guest. A
guest the target cannot start fails there through the
[failed path](#running-claims-across-node-loss); it never goes back.

### Node lease

With `archive_failover` every node renews one row of `sandboxd_nodes (cell,
node, lease_until)` every 5 seconds, with a single upsert on the database clock
that covers the whole node: `lease_until = now() + 15 s`. A renewal that fails
once does not lose the lease or trip the fence, as long as the next one
answers within 2 seconds of its start. The table is [created at start](deploy.md#shared-archive-registry) when it is missing, and a node
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
  when the checkpoint is gone or this node confirmed an earlier row, and a
  write that never landed otherwise. A refused write reads the row: one this node still owns only had a
  stale epoch and is refreshed, and an absent or foreign row drops the claim and
  its VM. A claim gets its first replica one interval after it is claimed, after a
  restart, or after it wakes from an archive or an adoption; until a claim woken
  that way commits its first replica it has no row, and it is lost with its node. A hibernated, archived or failed claim,
  one whose tenant was removed, and one with volumes (capture refuses them) is
  skipped; a replicated claim that gains volumes
  drops its row and replica. The claim's policy pool, not its own template key,
  picks the interval, so a claim cloned from a template promoted out of a
  replicating pool follows that pool. Rounds share the maintenance budget with
  refill and never take the share refill keeps. A node whose lease is overdue
  starts none, and a round whose registry is down fails before it captures.
- **Restore.** A lost node's replica is adopted like an archive: the first
  `Lookup` that reaches another node after the lease expires takes the row,
  records the claim as archived on the replica with the row's retention deadline,
  or the adopter pool's window when it has none, and the next call wakes it
  through the archive wake, which restores the guest with its memory and
  processes. The claim resumes from its last committed replica: everything
  after it, and the connections open at the time, are lost: up to the longer of
  the interval and one replication pass, plus the 5-second tick, plus one more
  pass, since a claim can run first in one pass and last in the next. A pass
  covers all the node's due claims of the replicating pools with bounded
  concurrency, each round a capture, upload and registry write, and a new pass
  cannot start before the previous one ends, so a pass grows with claim count
  and guest memory; for example, an interval of 30 s with a 90 s pass can lose
  about 185 s. The first round of a claim after a wake or adoption uploads the
  whole claim, so that pass is the long pass in this bound.
  While rounds fail it is the time since the last one that committed.
- **Lifecycle.** A release consumes the row, then deletes the tombstone and the
  replica. A hibernate leaves the row on the last replica. An archive puts its
  own checkpoint over the row, with its retention deadline, and deletes the
  replica. A lease expiry with `on_expire` destroy drops the row and the replica
  first, and the next reap destroys the claim; with `on_expire` archive, the
  archive puts its checkpoint over the row. Turning `replicate_seconds` off
  takes a restart, since a reload that changes it is refused; the restarted node
  drops the rows and replicas of the pool's claims on its first pass. A claim
  that holds a replica when the node restarts with replication off is still
  treated as replicated by the fence until the next restart, so during a
  registry outage it answers 503 like a replicated claim; that costs
  availability only, never a second copy or data. Turning `archive_failover`
  off stops every registry read and write: a claim that holds a replica then
  expires and is released like any other, and leaves its row and replica
  checkpoint behind, the same residue as a node that never returns.
  A restart keeps the node's own live row for a running claim and takes the checkpoint a crashed round left on
  it; a claim whose row another node holds, or whose row a finished adopter
  consumed, is dropped with its VM. A live claim's replica row has no retention
  deadline: the hourly sweep never purges it, because no round refreshes it while
  the claim is hibernated, and retention starts when the claim is archived or
  adopted. A row the node holds with no claim, as after reconcile dropped it
  because the VM record was gone, gains the retention deadline on that sweep: the claim stays adoptable
  until it passes, and `archive_delete_after_seconds` 0 keeps the row. The
  rows of a node that never returns therefore stay until a client adopts them, a
  bounded residue of one row and up to two checkpoints per claim: the row's, and
  a committed pending replica or a previous replica the node died before
  deleting. Their metas stay live, so offline reclamation keeps them.
  They are archive checkpoints (`"archive": true` in their meta) that no
  registry row names, which an operator can delete. Apart from these and the
  lease rule the sweep is unchanged.
- **Self-fencing.** A node whose renewal is overdue by 12 seconds (the lease
  length minus a 3 second margin, counted from the start of the last renewal
  that answered) stops serving every replicated claim it holds: relay, exec,
  ports, preview, fork, checkpoint and promote answer `503 lease lost; retry` (a read through a
  [reader](deploy.md#reading-sleeping-claims-without-waking-them) after the `101`
  gets an `error` frame instead), and `/owner` answers 404 so
  a `Lookup` moves on to the other nodes. A claim is replicated when its policy
  pool has `replicate_seconds`, from the moment it is claimed, adopted or
  loaded, whether or not a round has run, so the fence covers it before its
  first replica; the setting is restart-only so that decision cannot go stale.
  The guest has no NIC on the `none` lane, so it is unreachable except through
  the node, and the node destroys nothing. Claims of pools that do not
  replicate stay served. A node starts fenced and lifts the fence after its
  first renewal that is followed by a confirmation. The confirmation runs beside the renew loop, which keeps
  renewing: the node takes back every live row it holds in one statement, a
  compare-and-set on each row's epoch that the row locks serialize against
  adopters, and records the new epochs in one journal write; a claim whose row
  that statement did not move takes it in its own compare-and-set and retries
  once on a stale epoch. A claim whose row another node took, or took
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
  retries and meets the 503 until the lease is back or the claim moved. Neither
  an API call nor the idle sweep starts a hibernate or a wake of such a claim
  while the fence is closed, so an idle claim stays billed until the fence lifts.
  Streams of claims that are not replicated stay open.
- **Availability.** The design chooses consistency over availability for
  replicated claims. A registry outage longer than the fence threshold (12
  seconds) stops service of replicated claims on every node until PostgreSQL
  returns, because no node can prove it still owns them. Claims of pools that
  do not replicate are unaffected. An archived claim of a replicating pool is
  gated like a running one while the node is fenced, so its wake waits too.

A replica costs, per claim per interval, one capture pause, an upload of the
changed chunks plus a manifest and a meta, one registry read, one write and one
delete of the previous replica; the node's own cost is one scan of its claims
every 5 seconds. The delete is soft: the chunks only the previous replica names
stay in the store until the operator's
[offline reclamation](deploy.md#reclaiming-s3-storage) removes them. With replication this
is the fastest-growing part of the store: roughly the per-round churn times the
rounds since the last reclamation, per replicated claim, on top of the live replica.
Later rounds dedup; the first after a wake or adoption does not, because the
wake deletes the record it restored from. Resolving a
claim for a stream of a pool that does not replicate reads one plain field and
no atomic; a replicated claim adds one atomic load of the fence state and its
stream one watcher goroutine. Adding a claim to a failover node gains one map
lookup. Nothing runs per frame.

## Templates on a cluster

A promoted template (see the [SDK guide](sdk.md#promoting-to-a-template))
lives in its node's checkpoint store. On the default local-disk backend
that means **only on its owner node**, and the mesh gossips each node's
template set alongside its warm counts; on a shared store (a FUSE-mounted
`checkpoint_dir` or the s3 backend) every node resolves every template
directly, so the gossip routing fires only for a template another node deleted
after this one started, which stays listed until this node restarts. An export carries only
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
just the original. A peer that is offline, partitioned or not yet joined during
that broadcast keeps its copy until the checkpoint TTL ages it out. A healed
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
rejected at config load. A node later run with healing off and that TTL back at
0 keeps such a replica until an explicit delete.

## State ownership

Each kind of state has exactly one source of truth, and a restart rebuilds
fully from it:

| state | source of truth | survives restart |
|---|---|---|
| operator config (`volumes`, `secrets`, egress policies, `bridges`/`networks`, `mesh`, `preview_secret`, `egress_ca`) | `config.json` (human/deploy-tool owned) | re-read at boot; its reloadable part on a per-node [reload](deploy.md#reloading-the-config) |
| API-applied pool targets (`PUT /v1/pools`) | `<data_dir>/pools.json` (machine owned), or the cell's row in the shared `meta_store` when the node sets `meta_store.cell` | yes |
| API-applied tenants (`/v1/tenants`) | `<data_dir>/tenants.json` (machine owned), or the shared `meta_store` table | yes |
| API-registered volumes (`PUT /v1/volumes/{name}`) | `<data_dir>/runtime-volumes.json` (machine owned, per node) | yes |
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
  - renew, fork and wake from the archive ask the database whenever the cache
    lacks the tenant, so they never outlive a removal. While the database is down, they
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
| `checkpoint_ttl_hours` | a healed replica a delete broadcast missed ages out at a different time on each node, and nodes report a digest mismatch until every node has reloaded the same value |
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
  (a mismatch warns and shows in `sandboxd_config_digest_mismatch`; only with
  `cluster_key` set does that cover `api_token`, `preview_secret` and tenant
  tokens)
- with `meta_store`, every node's DSN points at the same database: the tenant
  set is not in the gossiped digest, so nodes on different databases serve
  diverging tenants without a warning
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
