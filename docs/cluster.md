# Clusters

A cluster is a set of sandboxd nodes joined through a
[hashicorp/memberlist](https://github.com/hashicorp/memberlist) SWIM mesh.
Gossip carries only placement hints — per-pool warm counts and each node's
data-plane address. Per-sandbox state never leaves its owning node, so a
stale view costs at most one extra redirect, never correctness. A single
node with no seeds is a valid mesh of one.

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

Two v1 constraints, fine for a homogeneous cluster: all nodes share the same
`api_token` (the SDK replays it across a redirect), and only egress-capable
nodes redirect egress claims.

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

## Querying members

`GET /v1/info` (api token) reports this node's pools plus the peer
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
  "peers": ["10.0.0.6:7777"]
}
```

`peers` lists the *other* members' advertise addresses — it is what the SDK
scatters across in `Lookup`. An empty/absent `peers` means a mesh of one.

## Relocating a handle

If a client kept a sandbox's `id` and `token` but lost the owner address
(process restart, handle passed between services), `Client.Lookup` asks the
entry node, then queries every peer concurrently
(`GET /v1/sandboxes/{id}/owner` — the token is both authorization and
ownership proof) and returns a handle bound to whichever node confirms
ownership first.

## Templates on a cluster

A promoted template (see the [SDK guide](sdk.md#promoting-to-a-template))
lives in its node's checkpoint store. On the default local-disk backend
that means **only on its owner node**, and the mesh gossips each node's
template set alongside its warm counts; on a shared store (a FUSE-mounted
`checkpoint_dir` or the s3 backend) every node resolves every template
directly, so the gossip routing simply never fires. Name-based calls route cluster-wide:

- `Client.New("tpl")` at any node redirects to the template's owner when the
  entry node has no golden for the key (warm peers still win first).
- `Client.DeleteTemplate("tpl")` at any node follows the same gossip to the
  owner and deletes there.

Gossip is eventually consistent: a template promoted a moment ago may be
invisible to name-based calls for about a gossip tick (the claim fails
cold — retry), and one deleted a moment ago may still redirect to a 404.
Correctness is never violated; only the name-based convenience lags. The
handle `Sandbox.Promote` returns is still owner-bound — `template.New` and
`template.Delete` dial the owner directly, no gossip involved — and is the
race-free choice immediately after a promote.

## Cluster checklist

- memberlist port (e.g. 7946) open node-to-node, TCP **and** UDP
- `advertise_addr` set to a routable address on every node (never loopback,
  never a wildcard)
- same `api_token` everywhere
- `cluster_key` set if the gossip network is not otherwise trusted
