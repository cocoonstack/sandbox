# Guarded egress

A sandbox gets the **effect** of a credential, never the credential. Outbound
access is an allow-listed, audited resource enforced on the host: the secret
lives host-side and enters no guest memory, so prompt injection can exfiltrate
at most the proxy's answers, and every credentialed call lands in the usage
journal keyed by sandbox and tenant (and in the audit journal when `audit_log`
is on). Default-deny.

The same host proxy also serves the **none lane** (a NIC-less Cloud Hypervisor
guest) over vsock, so a network-less sandbox can call approved APIs with
injected credentials — no NIC required in the guest.

## How it works (none lane)

The guest has no network device. silkd binds `127.0.0.1:3128` and relays each
connection to sandboxd over vsock (`CID2:2049` → the host UDS
`<vsock_socket>_2049`). sandboxd serves a per-sandbox forward proxy there,
bound to that sandbox's identity by the socket path — no in-band token. The
proxy evaluates the policy, injects the matched rule's secret host-side, dials
the origin, and relays the response.

The base image sets `http_proxy`/`https_proxy`/`no_proxy` on silkd's unit, and
silkd forwards exactly those variables into every exec — whenever nothing
routes directly: the guest has no NIC beyond `lo`/`sit0`, or the host has
locked the NIC it has (the egress lane below). A lane with a working NIC is
never steered into a relay whose host side is closed. An unconfigured client
just works, and `-x` still does:

```sh
curl https://api.github.com/user                            # allowed + credentialed
curl https://evil.example/                                  # 403 egress denied: evil.example
curl -x http://127.0.0.1:3128 https://api.github.com/user   # explicit form, same path
```

### Non-HTTP traffic (SOCKS5)

Clients that speak no HTTP proxy for their own protocol — IMAP and SMTP, SSH
via `ProxyCommand`, database drivers, `rclone` and friends over `ALL_PROXY` —
use the SOCKS5 listener silkd binds at `127.0.0.1:1080` and relays over
`CID2:2050` (`<vsock_socket>_2050`). It is the same per-sandbox proxy behind a
second door: CONNECT only (BIND and UDP ASSOCIATE answer *command not
supported*), no authentication (the socket path already carries the sandbox's
identity), and a `DOMAINNAME` destination is resolved host-side, so the guest
still needs no resolver. Each door serves at most 256 connections per sandbox
at a time; later ones sit unserved in the socket backlog until one closes, and
the proxy keeps at most 64 idle upstream connections per sandbox — 64 more for
decrypted traffic when the pool intercepts — so a guest cannot turn its proxy
into a host descriptor sink.

A SOCKS5 tunnel takes the decision an HTTP `CONNECT` to the same host takes,
through the same code: a rule with a nonempty `methods` list that omits
`CONNECT` denies it, and a rule with a `secret` opens it without injecting anything, on
both ports. The one difference is `intercept`: on 3128 such a rule terminates
the TLS and filters the requests inside, on 1080 there is no HTTP to filter, so
the tunnel is refused. The door is opt-in: the pool policy sets
`"socks5": true`, otherwise the host side stays unwired and the guest's dial
is refused like the HTTP one with no policy. The tenant policy's rules gate
every tunnel through the door as they gate `CONNECT`, so the tenant does not
opt in separately; its own `socks5` counts only on a claim outside any
configured pool, where the tenant policy is the whole policy. A policy that
opts in with rules that all carry a nonempty `methods` list without
`CONNECT`, or all `intercept`, is rejected at load. A pool whose policy does
not opt in pays
nothing for the door on the claim path, whatever its tenants' policies say.
Audit lines (with `audit_log` on) carry `"method":"SOCKS5"`.

```sh
curl --socks5-hostname 127.0.0.1:1080 imaps://imap.example.com/   # allowed: {"socks5": true, "allow": [{"host": "imap.example.com"}]}
```

## How it works (egress lane)

A `net:"egress"` guest owns a real NIC on the host bridge and reaches the same
proxy over the same vsock path. To stop it bypassing the proxy, sandboxd locks
the NIC at claim — until then a warm VM of the pool, and the pool's golden
build, sit on the bridge unlocked, running only the image's own services, never
tenant code: an nftables netdev table (`sandbox_egress_<tap>`) with an
ingress hook on the guest's tap drops every guest-initiated packet except IPv4
broadcast DHCP, so the guest's only routed egress is the audited vsock proxy. The
lock is fail-closed — a claim whose NIC cannot be locked is rejected, not handed
out unlocked, and no policy still means a locked NIC (default-deny), never a free
one. It lives in the host root netns and is removed once the VM is gone (a failed
remove keeps an existing lock in place; the next restart retries the remove). A
lock that never applied plus a failed remove leaves the VM unguarded until a
later remove succeeds.

Because the lock is invisible from inside the guest, sandboxd writes its
verdict into `/etc/silkd-lane` before the golden snapshot (or on a cold boot):
`relay` on a locked bridge lane, `direct` on a CNI lane. Where silkd reads
`relay` it hands the proxy variables to every exec exactly as on the none lane,
and the image's units wait for the same file instead of guessing from NIC
presence; git keeps running as on any lane with a NIC. A golden built under
another verdict is rebuilt, not adopted.

Egress-lane sandboxes never archive and do not hibernate, fork, checkpoint, or
promote: cocoon resumes a guest before its fresh tap can be re-locked, so any
resume from a snapshot would open an unlocked-NIC window. Keeping the lane live
holds the lock unbroken from claim to release; the idle sweep skips the lane and
those four verbs are refused (409) on it.

The proxy also refuses to connect to internal addresses — every IANA
special-purpose range that is not globally reachable (loopback, link-local
incl. cloud metadata, private, carrier-grade NAT, benchmarking, documentation,
reserved, per the registry snapshot in `sandboxd/pool/egress.go`) plus the IPv4-embedding
IPv6 forms (NAT64, 6to4, Teredo, IPv4-compatible) — so an allow-listed host
that resolves, or is rebound, to one cannot reach the sandboxd host or a
sibling VM.

`egress_internal_allow` re-admits named prefixes through that guard for nodes
whose sandboxes legitimately need internal services:

```jsonc
{ "egress_internal_allow": ["10.8.0.0/16", "fdc8::/16"] }
```

It is node-wide (every pool and tenant on the node gets the same re-admission)
and checked after the well-known `64:ff9b::/96` unwrap, so an IPv4 embedded in
that prefix matches as the IPv4 it is; an address in the local-use
`64:ff9b:1::/48` is blocked as a whole and needs that prefix itself in the list.
Name service prefixes, never the whole private space: the guest bridges are
themselves ULA/RFC1918, so a blanket permit would open sandbox-to-sandbox and
the host's own gateway. `0.0.0.0/0` + `::/0` turns the guard off entirely — for
fleets with a policy-enforcing proxy in front — and requests still pass the
domain policy first; the allow-list widens the IP gate only.

### Deployment constraints

- **One sandboxd per host.** The restart sweep owns the whole `sandbox_egress_*`
  table namespace in the root netns; a second daemon would clear the first's locks.
- **No shared broadcast domain with untrusted peers.** The DHCP exception is
  matched by header fields, not payload, so a broadcast in that shape can still
  reach the local L2 segment (never routed off-link). Give egress-lane VMs a
  bridge they do not share with an untrusted listener.
- **Bridge lane only (egress lane).** A CNI network's tap lives in the VM netns,
  out of reach of the root-netns lock, so a guarded egress *lane* needs the
  `bridges` form (those taps stay in the root netns) and is rejected on CNI
  `networks`. A none-lane *pool* policy rides the proxy and works on either; a
  tenant policy does not, because it could land on an egress-lane claim, so
  `networks` plus any tenant `egress` block is rejected at load. A bridge
  egress lane locks every NIC default-deny, even with no policy configured.
- **No custom NAT64/DNS64 prefix routed to the host.** The SSRF guard folds the
  standard NAT64 forms (RFC 6052 well-known `64:ff9b::/96`, RFC 8215 local-use
  `64:ff9b:1::/48`), but an operator-specific network-specific prefix (RFC 6052
  allows any /32–/96) is opaque — its embedded IPv4 reads as public. If the host
  routes such a translator, an allow-listed or DNS-rebound host could reach an
  internal IPv4 through it; do not route a custom NAT64 prefix on a sandboxd host.

## Configuration

Policy is per pool and per tenant; the effective policy is their intersection
(a request must pass both, and the pool rule's secret wins on a double allow).
A missing policy on either side is an empty allow-list, not a pass: a tenant
without its own `egress` block reaches nothing — upgrading, a tenant that
relied on inheriting its pool's policy must now declare one — so granting a tenant egress —
and the secret injection that rides it — is always an explicit act. Root
claims have no tenant layer and take the pool's policy whole; a key with no
configured pool — a promoted template, or an image cold-booted on demand — has
no pool layer, so a tenant's claim of one takes the tenant's policy alone (a
root claim of one stays denied). Which layers a claim has is settled when it
is made and kept for its life: a `PUT /v1/pools` that adds or drops a pool
changes the claims made after it, never a live one (a record from before this
rule resolves against the live pool set). Secrets are registered separately and
referenced by name — the value comes from the environment, never the config
file.

```jsonc
{
  "bridges": ["sbxbr0"],                    // egress-lane pools only; none-lane needs no attachment
  "secrets": [
    { "name": "gh", "header": "Authorization", "value_env": "GH_TOKEN" }
  ],
  "pools": [
    { "template": "rt:24.04", "net": "none", "size": "small", "warm": 2,
      "egress": { "socks5": true, "allow": [
        { "host": "api.github.com", "methods": ["GET", "POST"], "secret": "gh", "intercept": true },
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

Both pools serve the proxy the same way, and the first also opens the SOCKS5
door; the egress-lane pool additionally gets its NIC locked at claim, so its
policy governs the only route out just like the none lane's. sandboxd binds a
warm clone's doors when it joins the pool and starts serving them at claim, so
the bind is off the claim path (a refill-time bind failure is logged and that
clone binds at claim); a door the guest reached while the clone was warm is
bound afresh at claim, which resets what waited in it, and an unpooled or
woken sandbox binds at arm time.

- `host`: an exact name, a `*.`-prefixed suffix wildcard, or `*`. Case-insensitive.
- `methods`: empty means any. Enforced on plaintext and on intercepted HTTPS. A
  non-intercepted CONNECT tunnel is opaque — the method cannot be checked — so a
  rule with a nonempty `methods` list that omits `CONNECT`, and no `intercept`,
  denies the tunnel outright rather than tunneling unchecked.
- `ports`: empty means any; otherwise the destination port must be listed. The
  port is the tunnel's target on CONNECT and SOCKS5, the URL's port (or the
  scheme's default) on the forward path, and the CONNECT's port for every
  request inside an intercepted tunnel. Exact ports only; `0` and repeats are
  rejected at load.
- `secret`: injects the named registered secret's header. A guest-supplied value
  for the same header is overwritten. Inside an HTTPS `CONNECT` tunnel the
  injection needs `intercept`; an absolute-form `https://` request on the
  forward door needs none — the proxy dials the origin over verified TLS
  itself, checks the method, and injects.
- `intercept`: terminate a matched HTTPS CONNECT so the request is filtered by
  method and the secret injected (see below). Only a pool rule may set it.
- No policy on a claim ⇒ no egress at all (the proxy is not started).

Each decision is metered as an `egress` usage event and, when `audit_log` is
on, written to `audit.jsonl` (`op:"egress"`, `dest`, `port`, `method`,
`decision`, and the secret **name** in `secret`); an intercepted `CONNECT` is
recorded as a decision of its own before its inner requests.

## HTTPS interception

A rule with `intercept: true` makes the proxy terminate that host's TLS with a
leaf it signs, so it sees the request's method and path and can inject the
secret into HTTPS — the same guarantees plaintext already has. Upstream is
re-originated and verified against the host's real root store; the proxy never
trusts an unverified origin.

Trust is a two-tier PKI. One **cluster root CA** is the trust anchor: its
certificate (public) is baked into every interception guest's store, so a leaf
from any node validates. Each node signs leaves with its **own intermediate
CA**, issued from the root, and presents `[leaf, intermediate]`; the guest
builds `leaf → intermediate → cluster root`. The **root private key never
reaches a node** — a node holds only its intermediate.

### Provisioning the cluster PKI

Run the `sandboxd ca` tool on an **operator machine, not a node** — the root
key never touches a worker. Mint the root once, then issue one intermediate
per node:

```bash
# 1. Once per cluster. root.key stays here, offline, forever.
sandboxd ca init -out ca -cn "acme sandbox root"
#   → ca/root.crt (public)   ca/root.key (0600, never leaves this host)

# 2. One intermediate per node, all signed by the same root.
for n in node-a node-b node-c; do
  sandboxd ca issue-intermediate \
    -root-cert ca/root.crt -root-key ca/root.key \
    -node "$n" -out "dist/$n"
done
#   dist/node-a/node-a.crt  dist/node-a/node-a.key   (0600)
#   dist/node-b/…  dist/node-c/…
```

Both subcommands refuse to overwrite an existing `.crt`/`.key`; pass
`-force` to replace one deliberately.

That gives one shared root and a per-node signing key:

```
ca/root.crt          → copied to EVERY node (public trust anchor)
ca/root.key          → stays offline on the operator machine
dist/node-a/node-a.* → node-a only
dist/node-b/node-b.* → node-b only
dist/node-c/node-c.* → node-c only
```

Distribute to each node — the root cert plus **that node's** intermediate,
never another node's key, never `root.key`:

```bash
scp ca/root.crt dist/node-a/node-a.crt dist/node-a/node-a.key \
    node-a:/etc/sandboxd/egress-ca/
```

then point that node's config at the root cert and its own intermediate:

```jsonc
"egress_ca": {
  "root_cert":         "/etc/sandboxd/egress-ca/root.crt",
  "intermediate_cert": "/etc/sandboxd/egress-ca/node-a.crt",
  "intermediate_key":  "/etc/sandboxd/egress-ca/node-a.key"
}
```

`openssl verify -CAfile ca/root.crt dist/node-a/node-a.crt` confirms the chain.
Adding a node later is step 2 for the new node only — the root is untouched,
so existing guests keep validating. Losing a node's intermediate key compromises
only that node: re-issue it (step 2) and roll the node; the cluster root, and
every other node, is unaffected.

`egress_ca` is required whenever a pool has an intercept rule. The root cert is
baked into a guest **when the guest is created** — at golden build, or at a
pre-golden cold claim's provision (both via silkd; only the golden-build
install is off the claim path). It is
**not** re-installed on re-claim: a clone, checkpoint restore, archive wake, or
reconcile adopts the guest with whatever root it was born with. Each golden's
`.stamp` file records what it baked (the root fingerprint, the lane verdict,
and the warmup argv), and a golden is adopted only when its stamp exists and
matches, so a changed root rebuilds goldens; a golden from before the stamp
has none and is rebuilt once. Nothing rebuilds an existing checkpoint,
archive, promoted template, or live/hibernated claim. Because the baked cert
is the shared cluster root, promote/checkpoint/archive carry no node-private
material and stay unrestricted.

**Intermediate rotation is seamless.** Issue a fresh intermediate from the same
root, point the node's config at it, restart: leaves still chain to the root
every guest already trusts, and the root fingerprint in each golden's `.stamp`
is unchanged, so no golden rebuilds.

**Root rotation needs a drain, not a hot swap.** A guest verifies leaves only
against the root(s) it was born with, so the node's leaves must chain to a root
every *live* guest trusts. `root_cert` may bundle several CA certs (every block
must be a CA cert; the fingerprint covers the exact file bytes, so any edit
rebuilds goldens), and `LoadCA` validates the node's intermediate against the
**first** cert in the bundle. That fixes the order:

1. Bundle `old root, new root` (old first) and keep the **old** intermediate.
   Rebuilt goldens bake both roots, so new guests trust old and new while the
   node still signs under the old root that every live guest trusts.
2. Recreate or drain every old-root guest — checkpoints, archives, promoted
   templates, and live/hibernated claims.
3. Atomically swap to `new root` first and the **new** intermediate.

Swapping the intermediate while old-root guests are alive breaks interception
for them, with no re-claim path to fix it.

Limitations: interception is HTTP/1.1 only and breaks clients that pin
certificates, so scope it to hosts you control. A host that speaks a non-HTTP
protocol over TLS must not be given an intercept rule.
