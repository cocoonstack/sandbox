# Guarded egress

A sandbox gets the **effect** of a credential, never the credential. Outbound
access is an allow-listed, audited resource enforced on the host: the secret
lives host-side and enters no guest memory, so prompt injection can exfiltrate
at most the proxy's answers, and every credentialed call lands in the audit and
usage journals keyed by sandbox and tenant. Default-deny.

The same host proxy also serves the **none lane** (a NIC-less Firecracker
guest) over vsock, so a network-less sandbox can call approved APIs with
injected credentials — no NIC required in the guest.

## How it works (none lane)

The guest has no network device. silkd binds `127.0.0.1:3128` and relays each
connection to sandboxd over vsock (`CID2:2049` → the host UDS
`<vsock_socket>_2049`). sandboxd serves a per-sandbox forward proxy there,
bound to that sandbox's identity by the socket path — no in-band token. The
proxy evaluates the policy, injects the matched rule's secret host-side, dials
the origin, and relays the response.

A sandbox reaches it as a standard HTTP proxy:

```sh
curl -x http://127.0.0.1:3128 https://api.github.com/user   # allowed + credentialed
curl -x http://127.0.0.1:3128 https://evil.example/         # 403 egress denied: evil.example
```

## How it works (egress lane)

A `net:"egress"` guest owns a real NIC on the host bridge and reaches the same
proxy over the same vsock path. To stop it bypassing the proxy, sandboxd locks
the NIC at claim: an nftables netdev table (`sandbox_egress_<tap>`) with an
ingress hook on the guest's tap drops every guest-initiated packet except IPv4
broadcast DHCP, so the guest's only routed egress is the audited vsock proxy. The
lock is fail-closed — a claim whose NIC cannot be locked is rejected, not handed
out unlocked, and no policy still means a locked NIC (default-deny), never a free
one. It lives in the host root netns and is removed once the VM is gone (a failed
remove keeps an existing lock in place; the next restart retries the remove). A
lock that never applied plus a failed remove leaves the VM unguarded until a
later remove succeeds.

Egress-lane sandboxes do not hibernate, archive, fork, checkpoint, or promote:
cocoon resumes a guest before its fresh tap can be re-locked, so any resume from
a snapshot would open an unlocked-NIC window. Keeping the lane live holds the
lock unbroken from claim to release; those operations are refused (409) on the
lane.

The proxy also refuses to connect to internal addresses — every IANA
special-purpose range that is not globally reachable (loopback, link-local
incl. cloud metadata, private, carrier-grade NAT, benchmarking, documentation,
reserved) plus the IPv4-embedding IPv6 forms (NAT64, 6to4, Teredo) — so an
allow-listed host that resolves, or is rebound, to one cannot reach the
sandboxd host or a sibling VM.

### Deployment constraints

- **One sandboxd per host.** The restart sweep owns the whole `sandbox_egress_*`
  table namespace in the root netns; a second daemon would clear the first's locks.
- **No shared broadcast domain with untrusted peers.** The DHCP exception is
  matched by header fields, not payload, so a broadcast in that shape can still
  reach the local L2 segment (never routed off-link). Give egress-lane VMs a
  bridge they do not share with an untrusted listener.
- **Bridge lane only (egress lane).** A CNI network's tap lives in the VM netns,
  out of reach of the root-netns lock, so a guarded egress *lane* needs a bridge
  and is rejected on a CNI `network`. None-lane policies ride the proxy and work
  on either. A bridge egress lane locks every NIC default-deny, even with no
  policy configured.
- **No custom NAT64/DNS64 prefix routed to the host.** The SSRF guard folds the
  standard NAT64 forms (RFC 6052 well-known `64:ff9b::/96`, RFC 8215 local-use
  `64:ff9b:1::/48`), but an operator-specific network-specific prefix (RFC 6052
  allows any /32–/96) is opaque — its embedded IPv4 reads as public. If the host
  routes such a translator, an allow-listed or DNS-rebound host could reach an
  internal IPv4 through it; do not route a custom NAT64 prefix on a sandboxd host.

## Configuration

Policy is per pool and per tenant; the effective policy is their intersection
(a request must pass both, and the pool rule's secret wins on a double allow).
Secrets are registered separately and referenced by name — the value comes from
the environment, never the config file.

```jsonc
{
  "bridge": "sbxbr0",                       // egress-lane pools only; none-lane needs no attachment
  "secrets": [
    { "name": "gh", "header": "Authorization", "value_env": "GH_TOKEN" }
  ],
  "pools": [
    { "template": "rt:24.04", "net": "none", "size": "small", "warm": 2,
      "egress": { "allow": [
        { "host": "api.github.com", "methods": ["GET", "POST"], "secret": "gh" },
        { "host": "*.googleapis.com" }
      ] } },
    { "template": "rt:24.04", "net": "egress", "size": "small", "warm": 2,
      "egress": { "allow": [{ "host": "api.github.com", "secret": "gh" }] } }
  ],
  "tenants": [
    { "name": "acme", "token": "…", "egress": { "allow": [{ "host": "api.github.com" }] } }
  ]
}
```

Both pools serve the proxy the same way; the egress-lane pool additionally gets
its NIC locked at claim, so its policy governs the only route out just like the
none lane's.

- `host`: an exact name, a `*.`-prefixed suffix wildcard, or `*`. Case-insensitive.
- `methods`: empty means any.
- `secret`: injects the named registered secret's header (plaintext requests
  only; HTTPS injection needs TLS interception, below). A guest-supplied value
  for the same header is overwritten.
- No policy on a claim ⇒ no egress at all (the proxy is not started).

Each decision is written to `audit.jsonl` (`op:"egress"`, host, allow/deny, the
secret **name**) and metered as an `egress` usage event.

## Lanes and status

| Capability | Status |
|---|---|
| none lane — proxy, policy, plaintext injection, audit | **shipped** |
| egress lane — nftables lock on the tap, forcing the NIC through the proxy | **shipped** |
| HTTPS credential injection (per-node ephemeral-CA TLS interception) | planned |

HTTPS requests are gated by host (CONNECT allow/deny) and audited on both lanes,
but credential injection into an HTTPS request awaits TLS interception. Use
plaintext or CONNECT-gated HTTPS for the credentialed-egress guarantees above.
