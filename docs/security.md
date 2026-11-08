# Security model

What the stack defends, where the trust boundaries sit, what the operator
must provide, and what it deliberately does not defend. Read this before
exposing any part of a deployment beyond a single trusted host.

## Trust boundaries

```
  agent workload (untrusted code)
      │ syscalls
  guest kernel + virtio drivers
      │
══ VM boundary (KVM) ═══════════════ the isolation unit
      │ vsock (none lane) / nft-locked NIC (egress lane)
  host: sandboxd + cocoon            trusted
      │ HTTP control/data plane, bearer tokens
  clients: SDK / MCP / operators     trusted per token scope
```

- **The VM boundary is the security boundary.** Everything inside the
  guest — the workload, the guest kernel, and silkd itself — is untrusted
  by the host. silkd is a convenience daemon, not a defense: a compromised
  guest can lie to its own client about its own state, but gains nothing
  toward the host, siblings, or the network beyond its lanes.
- **What a compromised guest can reach.** On the none lane: nothing but
  vsock — the relay back to its own client and the guarded-egress proxy,
  plus, on a policy that opts into `socks5`, a SOCKS5 door to the same
  allow-list that carries arbitrary TCP rather than HTTP only, and on one
  that opts into `dns`, a rate-limited resolver whose answers carry no `A`
  or `AAAA` record the dial guard refuses; names reach any domain. On a `tun`
  pool the guest also has an L3 path to the exits ([advanced network
  mode](advanced-network.md)), carrying only flows the policy's bare
  `host: "*"` rules and the internal-address guard allow, under per-claim
  rate, flow and quota limits.
  On the egress lane: the same vsock paths plus a NIC whose every
  outgoing packet, replies and ARP included, is dropped by an nftables lock in the host
  root netns ([egress](egress.md)); the lock is fail-closed and applied
  before the claim is handed out. On both lanes the proxy refuses
  loopback, private, link-local (cloud metadata), CGN, the
  IPv4-embedding IPv6 ranges and the node's own interface addresses, so an
  allow-listed name that resolves or rebinds to an internal address cannot
  reach the host or a sibling. A prefix the operator re-admits with
  `egress_internal_allow` is an exception: scope it with ports
  ([egress](egress.md)). The upstream proxies `egress_upstream.allow` names
  are the other: sandboxd dials them unguarded, so list only proxies.
- **What never enters the guest.** Egress credentials: the proxy injects
  the secret host-side, so prompt injection can exfiltrate at most the
  proxy's answers, and every credentialed call is journaled. Git auth
  tokens travel as in-memory headers, never guest disk. Secrets come from
  the host environment or a claim's `guest: false` env, never the config
  file. That env stays in the node-local claim record, except in the
  `archive_failover` row ([assumption 4](#deployment-assumptions)). With
  [`host_env_memory_only`](deploy.md#configuration), `guest: false` values
  stay in sandboxd's memory: `claims.json` and the rows written with it on hold
  none, and a hibernating claim drops them. The control plane sends them again
  after a hibernate, a restart or an adoption.
- **Sandbox identity.** Every clone is reseeded (entropy, machine-id), so
  branches and forks never share an identity with their source.

## Deployment assumptions

These are hard constraints, not suggestions; the guarantees above assume
all of them.

1. **The control/data plane runs on a trusted private network.** sandboxd
   serves plain HTTP; `api_token`, tenant tokens, and per-sandbox tokens
   are cleartext bearers on the wire, and on a cluster the SDK dials every
   node it is redirected to. Keep nodes and clients inside a VPC, WireGuard
   mesh, or equivalent, or put a TLS edge in front of every node
   ([TLS for SDK clients](deploy.md#tls-for-sdk-clients)). The only surface
   designed to face a browser is the preview listener, and it belongs behind a
   TLS-terminating proxy ([deploy](deploy.md#preview-urls)). Never expose
   `listen` itself publicly.
2. **One sandboxd per host** ([why](egress.md#deployment-constraints)).
3. **A cell is the boundary of a host compromise.** Every node of a
   [cell](cluster.md#cells) holds the cell's credentials, so host root on one
   node reaches every claim, checkpoint and template of its cell. Cells share
   no credential, database, store or network path ([cells](#cells)). Set
   `mesh.cluster_key` unless the gossip network is itself trusted, and open
   the memberlist port to the cell's nodes only.
4. **Treat the checkpoint store like backups.** A checkpoint, an archive and
   a replica each embed full guest memory, including any secret the workload
   held at capture. Restrict store access; use S3 server-side encryption or
   an encrypted mount where that matters. With `archive_failover`, treat the
   shared PostgreSQL the same way: its rows hold claim env in plaintext,
   `guest: false` values included unless `host_env_memory_only` is on.
5. **No custom NAT64 prefix routed on a sandboxd host** — the SSRF guard
   cannot see through an operator-specific translator prefix
   ([egress](egress.md#deployment-constraints)).

## Cells

A host compromise on one node is bounded to its cell, and operators can cut a
node off without its cooperation. Both hold only when the deployment meets
this contract.

**Per cell**, each with its own permissions, not only its own name:

- a PostgreSQL database and role that cannot reach another cell's database;
- a store credential that the store's policy limits to the cell's bucket or
  prefix;
- a cloud identity that cannot read another cell's secrets or change IAM,
  network or firewall rules;
- its own `api_token`, `mesh.cluster_key`, `preview_secret` and egress root;
- its own tenant tokens: the control plane never gives one tenant token to
  two cells;
- on `tun` pools, its own exit, with its own netns, FDB and neighbor table,
  admitting only the cell's nodes. A separate VNI is not enough: host root can
  send any VNI.

**Per node**, recommended: its own PostgreSQL role and store key with the
cell's grants, so one node can be cut off without a restart of the rest.

**Ingress**: the control, preview and mesh ports, PostgreSQL and the store
admit only the cell's node addresses, plus the control plane where it calls
in. The backends refuse the cell's credentials from anywhere else.

**Anti-spoofing** outside the host: VPC rules on a cloud; port security or
routed ports on bare metal.

[Cutting off a node](deploy.md#cutting-off-a-node),
[rotating a cell's credentials](deploy.md#rotating-a-cells-credentials) and
[recovering a cell](deploy.md#recovering-a-cell) are in the runbook.

Check the boundary when a cell is built and after each change to it:

- cell A's credentials reach none of cell B's control API, previews,
  database, store or cloud resources;
- a cutoff ends established and new access within the measured bound,
  without the node's help;
- credentials copied off a host do not work from outside the cell;
- a node of cell A cannot inject into or draw traffic from cell B's overlay;
- cell B's guests do not trust a certificate issued under cell A's egress
  root;
- a rotation drill follows the restart requirements.

## Token model

Four credentials in descending scope: the root `api_token`, the optional
`operator_token` (root's authority on the claim lifecycle, tenant and node-read
routes only, for a public control plane), tenant tokens (isolated at the API
layer) and per-sandbox tokens (that sandbox only, nothing node-level); what each
may call is in the [API reference](sandboxd-api.md#sandboxd-http-api). The root,
operator and tenant tokens
are valid on every node of their [cell](#cells) and nowhere else. A preview URL
is a capability on top, HMAC-signed with the cell's `preview_secret` and no
revocation list. A checkpoint branches
only for its own tenant, and a delete does not remove it fleet-wide the
instant it runs
([why](cluster.md#delete-is-eventual-not-a-fleet-wide-revocation)).

## Dataset volumes

The volume catalog is an operator-owned data boundary. A volume name and its
access list must mean the same thing fleet-wide, although membership is
node-local. Entry `tenants` lists follow the [access-list
rule](sandboxd-api.md#volume-access-lists). Claim lookup returns byte-identical
errors for an unknown and a forbidden name, and catalog discovery filters before
replying, so a tenant cannot enumerate restricted entries by probing. Gossip
carries names only.
Neither gossip, discovery, persisted claims, usage events, nor the sandbox index
exposes host image paths or access lists.

Runtime [registration and unregistration](sandboxd-api.md#put-v1volumesname)
require a configured root `api_token`;
these endpoints return 403 when the token is unset. New catalog records use
mode 0600.
Registration uses static configuration's path validation plus, when set, the
[`volume_roots`](deploy.md#configuration) bound. It does not verify backing type
or pin backing identity. Without
`volume_roots` the root token can register any absolute host path, writable if
requested, for every scope its [access list](sandboxd-api.md#volume-access-lists)
permits. Protect the root token like host root, or set `volume_roots`.

Read-only is integrity protection for the shared image, not confidentiality.
Mounting a dataset into an egress-lane sandbox gives that sandbox an export path
to every destination its tenant egress policy permits; review the volume access
list and egress policy together. With `directio=off`, concurrent readers share
the host page cache, which improves reuse but lets one tenant's large scan evict
another's cached pages — the same holds for a writer: a large `rw` write can
evict cached pages backing another volume's readers exactly like a large scan
would. `directio=on` is the per-volume mitigation; there is no per-tenant cache
quota or accounting. Operators must keep a read-only entry's attached image
immutable and publish a new name/path for new content.

Changing a running claim's volumes needs the root token or the owning tenant's
API token. A sandbox token cannot, so a guest holding its own sandbox token
cannot attach another dataset, and every addition passes the same
[access list](sandboxd-api.md#volume-access-lists) as a claim.

A writable entry (`writable: true`) adds a channel the read-only model does
not have: whichever tenant is permitted to claim it `rw` changes what every
other permitted tenant reads next, and any of them can be the writer in
turn — a multi-tenant access list on a writable entry is a bidirectional
channel between those tenants, not just a shared read. Recommend a writable
entry's `tenants` name exactly one tenant — an empty list permits every
authenticated scope, which for a writable entry means every tenant can write
to every other tenant's next read. A dataset that genuinely needs multiple
writers needs an out-of-band process for who writes when, which the catalog
ACL does not provide.

## Known limitations

Facts to plan around, stated so the boundary is honest:

- **The VMM processes are not additionally sandboxed.** cloud-hypervisor
  runs as an ordinary host process under cocoon; a VMM
  escape lands on the host with the VMM's privileges (upstream:
  cocoonstack/cocoon#83). Compensate with dedicated sandbox nodes and a
  minimal host.
- **HTTPS interception is HTTP/1.1 only and breaks certificate-pinning
  clients** — scope `intercept` rules to hosts you control, or use
  `"inject"` so only claims holding a credential for a host are intercepted
  ([egress](egress.md#https-interception)).
- **The audit and usage journals are local JSONL** with size rotation —
  no tamper-evidence. Ship them off-node if integrity against a host
  compromise matters (at which point the journals are the least of it). With
  `meta_store.usage` the usage journal also lands in the
  [shared usage table](deploy.md#shared-usage-table); the audit journal stays
  local.
- **API rate limiting is capacity-based only** (`max_claims`, `max_running`,
  `disk_reserve`, `memory_limit` and per-tenant caps answer 429). The API assumes callers inside the trust boundary;
  front it with your own limiter if semi-trusted automation can reach it.
- **A dead node's running sandboxes die with it** unless their pool
  replicates them (`replicate_seconds`): another node then restores the last
  replica, losing what ran after it. Archived claims are adopted from the
  checkpoint store ([clusters](cluster.md#archived-claims-across-node-loss));
  durability is the checkpoint store's job, not the node's.
