# Guarded egress

A sandbox gets the **effect** of a credential, never the credential. Outbound
access is an allow-listed, audited resource enforced on the host: the secret
lives host-side and enters no guest memory, so prompt injection can exfiltrate
at most the proxy's answers, and every credentialed call lands in the audit and
usage journals keyed by sandbox and tenant. Default-deny.

The wedge: the same host proxy serves the **none lane** (a NIC-less Firecracker
guest) over vsock, so a network-less sandbox can call approved APIs with
injected credentials — a capability that assumes a NIC everywhere else.

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

## Configuration

Policy is per pool and per tenant; the effective policy is their intersection
(a request must pass both, and the pool rule's secret wins on a double allow).
Secrets are registered separately and referenced by name — the value comes from
the environment, never the config file.

```jsonc
{
  "secrets": [
    { "name": "gh", "header": "Authorization", "value_env": "GH_TOKEN" }
  ],
  "pools": [
    { "template": "rt:24.04", "net": "none", "size": "small", "warm": 2,
      "egress": { "allow": [
        { "host": "api.github.com", "methods": ["GET", "POST"], "secret": "gh" },
        { "host": "*.googleapis.com" }
      ] } }
  ],
  "tenants": [
    { "name": "acme", "token": "…", "egress": { "allow": [{ "host": "api.github.com" }] } }
  ]
}
```

- `host`: an exact name, a `*.`-prefixed suffix wildcard, or `*`. Case-insensitive.
- `methods`: empty means any.
- `secret`: injects the named registered secret's header (plaintext requests
  only; HTTPS injection needs TLS interception, below). A guest-supplied value
  for the same header is overwritten.
- No policy on a claim ⇒ no egress at all (the proxy is not started).

Each decision is written to `audit.jsonl` (`op:"egress"`, host, allow/deny, the
secret **name**) and metered as an `egress` usage event.

## Lanes and roadmap

| Slice | Status |
|---|---|
| none lane — proxy, policy, plaintext injection, audit | **shipped** |
| HTTPS credential injection (per-node ephemeral-CA TLS interception) | planned |
| egress lane — nftables default-deny on the tap, forcing the NIC through the proxy | planned |

Today the **egress lane** (a NIC-backed guest) is not yet forced through the
proxy; a policy on an `net:"egress"` pool is accepted but enforced only once the
nftables slice lands. HTTPS requests on the none lane are gated by host
(CONNECT allow/deny) and audited, but credential injection into an HTTPS request
awaits TLS interception. Use the none lane with plaintext or CONNECT-gated
HTTPS for the credentialed-egress guarantees above.
