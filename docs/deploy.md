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

Build from source: `make sandboxd` produces `dist/sandboxd`.

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
| `data_dir` | `/var/lib/sandboxd` | golden snapshot exports and the claims journal |
| `cocoon_bin` | `cocoon` | cocoon CLI binary |
| `advertise_addr` | = `listen` | the host:port clients reach this node at; returned as a claim's owner address and gossiped to peers. Must be routable when `listen` is a wildcard |
| `bridge` / `network` | unset | egress-lane attachment: a host bridge device, or a CNI conflist name. Mutually exclusive; with neither set the node serves only the no-network lane |
| `api_token` | unset | when set, guards `POST /v1/claim` and `GET /v1/info` (Bearer). Per-sandbox tokens guard sandbox-scoped calls regardless |
| `max_fork_count` | 16 | children a single `fork` may create; each is a full-RAM VM, so this bounds one request's memory blast radius to the node's capacity |
| `idle_hibernate_seconds` | 0 (off) | node-wide idle policy for unpooled claims (template/checkpoint claims): a claim with no data-plane connection for this long is hibernated; the next call wakes it transparently. Per-pool `idle_hibernate_seconds` (in a pool entry) does the same for that pool's claims — pooled keys ignore the node-wide value. Opt-in deliberately: a wake costs latency and the snapshot, so callers with their own idle logic must not pay twice |
| `mesh` | unset | join a cluster ([Clusters](cluster.md)); unset = single node |
| `pools[]` | — | warm pools. `warm` defaults to 4; `net` is `none` or `egress`; `size` is a tier, below |

Size tiers (free-form CPU/memory is deliberately not accepted — it would
fragment the warm pools):

| size | CPU | memory |
|---|---|---|
| `small` | 1 | 512M |
| `medium` | 2 | 1G |
| `large` | 4 | 4G |

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

A minimal systemd unit:

```ini
[Unit]
Description=sandboxd
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

## Verifying a node

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:7777/v1/info | jq .
```

The repository's `scripts/sandboxd-e2e.sh` runs the full loop on a real node
(golden build → warm pool → claim tiers → the complete verb smoke → reap →
restart reconcile); set `BRIDGE=<dev>` to include the egress lane.
