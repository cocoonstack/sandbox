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
  vsock — the relay back to its own client and the guarded-egress proxy.
  On the egress lane: the same vsock paths plus a NIC whose every packet
  except IPv4 broadcast DHCP is dropped by an nftables lock in the host
  root netns ([egress](egress.md)); the lock is fail-closed and applied
  before the claim is handed out. On both lanes the proxy refuses
  loopback, private, link-local (cloud metadata), CGN, and the
  IPv4-embedding IPv6 ranges, so an allow-listed name that resolves or
  rebinds to an internal address cannot reach the host or a sibling.
- **What never enters the guest.** Egress credentials: the proxy injects
  the secret host-side, so prompt injection can exfiltrate at most the
  proxy's answers, and every credentialed call is journaled. Git auth
  tokens travel as in-memory headers, never guest disk. Secrets come from
  the host environment, never the config file.
- **Sandbox identity.** Every clone is reseeded (entropy, machine-id), so
  branches and forks never share an identity with their source.

## Deployment assumptions

These are hard constraints, not suggestions; the guarantees above assume
all of them.

1. **The control/data plane runs on a trusted private network.** sandboxd
   serves plain HTTP; `api_token`, tenant tokens, and per-sandbox tokens
   are cleartext bearers on the wire, and on a cluster the SDK dials every
   node it is redirected to. Keep nodes and clients inside a VPC, WireGuard
   mesh, or equivalent. The only surface designed to face a browser is the
   preview listener, and it belongs behind a TLS-terminating proxy
   ([deploy](deploy.md#preview-urls)). Never expose `listen` publicly.
2. **One sandboxd per host.** The restart sweep owns the whole
   `sandbox_egress_*` nftables namespace; a second daemon would clear the
   first's locks.
3. **The egress bridge shares no broadcast domain with untrusted
   listeners.** The DHCP lock exception is matched by header shape, so a
   packet in that shape can reach the local L2 segment.
4. **Set `mesh.cluster_key` unless the gossip network is itself trusted**,
   and open the memberlist port node-to-node only.
5. **Treat the checkpoint store like backups.** A checkpoint embeds full
   guest memory — including any secret the workload held at capture.
   Restrict store access; use S3 server-side encryption or an encrypted
   mount where that matters.
6. **No custom NAT64 prefix routed on a sandboxd host** — the SSRF guard
   cannot see through an operator-specific translator prefix
   ([egress](egress.md#deployment-constraints)).

## Token model

Three credentials, in descending scope ([API reference](sandboxd-api.md)):
the root `api_token` (operator surfaces, full access), tenant tokens
(resource-creating verbs, everything stamped and quota'd per tenant), and
per-sandbox tokens (that sandbox only — holding a handle amplifies to
nothing node-level). Two capability tokens ride on top: preview URLs are
HMAC-signed, expire with the claim's lease, and die with the sandbox (no
revocation list to leak); a checkpoint id is the unguessable capability to
branch it. On a cluster, deleting a checkpoint does not revoke that
capability fleet-wide the instant it runs: the delete is best-effort —
broadcast to every peer the node currently sees — so a peer that is offline
or partitioned at that moment keeps its own replica branchable until
`checkpoint_ttl_hours` ages it out ([placement lifecycle](cluster.md#checkpoints-on-a-cluster)).
Tenants are isolated at the API layer — listings filter, deletes answer 404
rather than confirming existence, and operator surfaces answer tenants 403.

## Known limitations

Facts to plan around, stated so the boundary is honest:

- **The VMM processes are not additionally sandboxed.** cloud-hypervisor
  runs as an ordinary host process under cocoon; a VMM
  escape lands on the host with the VMM's privileges (upstream:
  cocoonstack/cocoon#83). Compensate with dedicated sandbox nodes and a
  minimal host.
- **HTTPS interception is HTTP/1.1 only and breaks certificate-pinning
  clients** — scope `intercept` rules to hosts you control
  ([egress](egress.md#https-interception)).
- **The audit and usage journals are local JSONL** with size rotation —
  no tamper-evidence. Ship them off-node if integrity against a host
  compromise matters (at which point the journals are the least of it).
- **Rate limiting is capacity-based only** (`max_claims`, per-tenant
  caps answer 429). The API assumes callers inside the trust boundary;
  front it with your own limiter if semi-trusted automation can reach it.
- **A dead node's sandboxes die with it** — memory state is node-local by
  design ([clusters](cluster.md)); durability is the checkpoint store's
  job, not the node's.
