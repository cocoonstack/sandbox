# Advanced network mode

The **default network mode** of a none-lane guest is the proxy path: silkd
relays `127.0.0.1:3128` and `127.0.0.1:1080` to the host's
[guarded-egress](egress.md) proxy over vsock. The **advanced network mode**
adds a full L3 path beside it for clients the proxy cannot carry (UDP, QUIC,
WebRTC, clients that ignore proxy variables): silkd creates `tun0` in the
guest and carries raw IP packets over one vsock stream to the host. The guest
still has no NIC, so hibernate, archive, wake and move work as before.

This page is the contract between silkd and the host. The host side design is
[#471](https://github.com/cocoonstack/sandbox/issues/471).

## Guest side

| Item | Value |
| --- | --- |
| Device | `tun0`, `IFF_TUN \| IFF_NO_PI \| IFF_VNET_HDR`, owned by silkd |
| Address | `100.64.0.2/30` in every guest; the host tells guests apart by their vsock stream, never by address |
| Gateway | `100.64.0.1`, default route via `tun0` |
| MTU | 1500 until the host hello names another |
| IPv6 | none; silkd turns IPv6 off on `tun0` |
| Packet stream | guest dials host vsock port **2051** (`<vsock_socket>_2051` on the host) |

Both modes run at once. silkd keeps the proxy variables in every exec, so a
client that honours them still reaches `127.0.0.1:3128` over loopback; all
other traffic takes the default route through `tun0`. Loopback never enters
`tun0`, so the guest DNS stub of #472 is unaffected; the tun path does not
intercept DNS.

### Turning it on and off

- **`tun_up`** (control channel, port 2048): creates `tun0` with its address
  and route, starts the packet stream, answers `done` or an `error` saying why
  `tun0` could not be created. On a guest whose `tun0` is already up it ends
  the current stream and dials again. sandboxd sends it once a claim of a
  tun-enabled pool is live, and again after every wake, unarchive and move,
  so the guest drops its old stream at once whatever the VMM did with it (on
  Cloud Hypervisor the guest also notices by itself within about 200 ms of a
  restore). Warm VMs never get it, so goldens are unchanged.
- **`tun_down`** (control channel): stops the packet stream and removes `tun0`
  with its routes before answering `done`, so clients fail at once with
  "network unreachable".
- **Boot and reconnect**: silkd dials 2051 at boot and after every stream it
  sees end, and lets the host hello decide. At boot a refused dial or a failed
  hello means off. Once `tun0` is up, a refused dial or a failed hello is
  retried with backoff (50 ms doubling to 1 s); an off answer removes `tun0`.

The guest switch is advisory. The guest user can be root, create its own TUN
and dial 2051 directly, so the host enforces: it accepts a packet stream only
for a tun-enabled claim, rewrites every source address, and closes the stream
to revoke.

## Wire protocol (version 1)

All integers are little-endian.

### Hello

Each side sends one 12-byte hello first; the guest sends first, the host
answers.

| Offset | Size | Field | Guest | Host |
| --- | --- | --- | --- | --- |
| 0 | 4 | magic | `SLKT` | `SLKT` |
| 4 | 2 | version | highest it speaks (1) | the version both use; ≤ the guest's |
| 6 | 2 | offloads | offloads its tun accepts on write | offloads its tun accepts on write |
| 8 | 2 | mtu | 0 | MTU for `tun0`; 0 keeps the current one |
| 10 | 2 | flags | 0 | bit 0 `ON`: packet mode is on for this box |

Flag bits 1 to 15 are reserved and sent as 0; a peer ignores a bit it does
not know. A later revision uses one to open several streams per guest, one
per `tun0` queue.

A host that answers without `ON` closes the stream after the hello; the guest
removes `tun0`. A host that refuses a guest answers without `ON` rather than
closing silently. A guest that does not speak the host's version closes the
stream. The guest gives up on a hello that does not arrive in 5 seconds.

Offload bits are the kernel's `TUN_F_*` values: `0x01` CSUM, `0x02` TSO4,
`0x04` TSO6, `0x08` TSO_ECN, `0x20` USO4, `0x40` USO6. Each side probes its
kernel (USO needs Linux 6.2) and then enables, with `TUNSETOFFLOAD`, only the
offloads both hellos carry; segmentation offloads are dropped when CSUM is not
common. A side never builds a frame whose GSO type the peer did not offer;
packets its kernel queued before a reconnect keep their old type, and a
receiver may drop one it cannot take.

### Frames

After the hellos, both directions carry frames back to back:

```
u32 len | virtio_net_hdr (10 bytes) | IP packet
```

`len` counts the header and the packet: 10 to 65545 bytes. A length outside
that range is a protocol error and ends the stream. The header is the plain
10-byte `struct virtio_net_hdr` (`flags`, `gso_type`, `hdr_len`, `gso_size`,
`csum_start`, `csum_offset`) exactly as `IFF_VNET_HDR` reads and writes it, so
a GSO packet of up to 64 KiB crosses vsock as one frame and is segmented, if
at all, by the receiving kernel. A packet the receiving kernel refuses is
dropped like a packet lost on a link.

Writers batch: every frame ready at the moment goes out in one stream write.
Readers parse every complete frame one stream read returns; the io_uring path
hands them to the TUN in batched submissions.

### Reconnect

There is no session state beyond the hello. After any stream loss the guest
dials again and both sides renegotiate offloads and MTU, so a move to a host
with a different kernel or MTU is safe. Packets in flight during the gap are
lost; TCP retransmits.

## Host side

sandboxd needs no TUN, no kernel routes and no NAT rules on the node. It
carries each claim's packets over VXLAN, on an ordinary UDP socket, to an
**exit**: plain Linux configuration on the same host or on a gateway machine.
The design is [#471](https://github.com/cocoonstack/sandbox/issues/471).

```
guest -> tun0 -> silkd --vsock 2051--> sandboxd --VXLAN--> exit (vxlan0, forward, NAT) -> uplink
```

- **Overlay address**: each tun-enabled claim gets a unique address from
  `tun.subnet`, kept across hibernate, archive and move and freed at release.
  A single node allocates it in memory and keeps it in `claims.json`, with no
  disk write on the claim path; each release is appended to
  `<data_dir>/tun-freed.log` so its reuse window outlives a restart. A node
  with a `mesh` must have `meta_store.cell` and allocates it in the cell's
  table, one round trip per claim, and stays off (proxy mode only) while the
  meta store is unreachable. A freed address is reused after
  `tun.reuse_after_seconds`, so the subnet's size over that window bounds the
  sustained claim rate: a `/8` with the 5-day default carries about 38 new tun
  claims a second across the cell. The claim's MAC is `02:00:` followed by the
  address bytes.
- **Outbound**: the source address becomes the claim's overlay address
  whatever the guest wrote; a GSO frame is cut into wire-sized packets with
  full checksums; each gets an Ethernet header (claim MAC to the exit's MAC)
  and a VXLAN header and leaves for the exit picked by the claim's profile
  label (`default` when it has none). The outer source port follows a hash of
  the inner 5-tuple.
- **Inbound**: a VXLAN packet from an exit's VTEP whose inner destination is
  a claim on this node goes back to `100.64.0.2` and onto that claim's stream;
  anything else is dropped. Inbound passes only for a flow the claim opened,
  except that a TCP segment other than a bare SYN reopens its flow when the
  claim's policy allows the peer, so a stream crosses a move, a wake or a
  restart. In-order TCP segments of one flow whose checksums
  verify are merged into one GSO frame for the guest. An ICMP error gets the
  quoted packet's source rewritten to `100.64.0.2` too, so path MTU discovery
  and port-unreachable reach the guest's socket.
- **ARP**: sandboxd answers ARP from the exits for its own claims only,
  announces a claim with a gratuitous ARP when it is armed (claim, wake,
  move-in), after a reload of `tun.exits` and every 45 seconds, and never
  forwards a frame the guest built.
- **Policy**: a flow table keyed by 5-tuple; the first packet of a flow is
  judged by `EvalFlow` (bare `host: "*"` rules only) and the internal-address
  guard, which also refuses the overlay subnet and `100.64.0.0/30`.
  Per-claim byte, packet and new-flow rates, a flow cap and a byte quota apply
  before any further work on a packet; a GSO frame is charged for every packet
  and header it puts on the wire. The limits and flows belong to the claim, so
  a guest that reconnects its stream starts with what it had used.
- **Untrusted input**: frames, virtio-net headers and IPv4 headers are bounds
  checked; non-IPv4, IP options, fragments, a packet or GSO segment larger
  than the MTU the stream's hello announced, a GSO frame of more than 128
  packets, a GSO type that does not match the protocol and a partial checksum
  outside the L4 checksum field are dropped and counted per reason. A stream
  whose guest stops reading for 10 seconds is closed, freeing its queue.
- **Enforcement**: the packet port answers `ON` only for an armed,
  tun-enabled claim; disabling a claim ends its stream, drops its flows and
  stops its ARP answers. A guest of a `tun` pool that wakes, moves or reloads
  into a state without a door (disabled, no address, the pool's `tun` off) is
  sent `tun_down` in the background. The claim switch and the profile label change only
  with the operator token, through
  [`PUT /v1/sandboxes/{id}/tun`](sandboxd-api.md#put-v1sandboxesidtun).
- **Audit and usage**: with `audit_log` on, each flow writes one
  `op:"tun_flow"` record when it ends ([fields](deploy.md#configuration)); with
  `egress_usage_bytes` on, each allowed flow also appends an `egress_bytes`
  usage event with its IP bytes both ways, the destination `ip:port` as its
  reference, and `upstream` `tun/<label>`. A clean stop ends every live flow,
  so its records cover the bytes up to the stop, and persists the quota
  counter, which is also persisted when the claim is disarmed or moved; a
  crash loses both since the last of these.
- **Cost**: a claim of a pool without `tun` pays one map lookup. A tun claim
  adds, on the claim path, an in-memory address (or one meta-store round
  trip), a socket bind and one gratuitous ARP; `tun_up` is sent in the
  background. Per packet: one parse, one flow-table read, the rate checks, an
  incremental checksum update and one copy into the send buffer; a 64 KiB GSO
  frame costs about 9 µs to segment and checksum (`BenchmarkForwardGSO`), a
  small packet about 100 ns, a delivered packet about 250 ns, all without
  allocation.

### Configuration

The node's `tun` block turns the mode on; a pool opts in with `"tun": true`
(the `none` lane only). Like `egress`, the flag is config-owned: `PUT /v1/pools` rejects it.

```json
"tun": {
  "subnet": "100.80.0.0/12",
  "listen": "10.0.0.5:4789",
  "exits": {"default": {"vtep": "10.0.0.20", "mac": "02:ee:00:00:00:01"}},
  "limits": {"bytes_per_second": 125000000, "quota_bytes": 107374182400}
}
```

| key | default | meaning |
|---|---|---|
| `subnet` | required | the overlay addresses, `/4` to `/24`, outside `100.64.0.0/30`; every node of a cell sets the same one; a change needs a restart, and claims that still hold an old address keep it until released, so drain tun claims first |
| `listen` | required | the node's VXLAN socket, a specific IPv4 `ip:port`; the exits send to it |
| `vni` | `1` | the VXLAN network identifier |
| `port` | `4789` | the exits' VXLAN port |
| `mtu` | `1450` | the guest `tun0` MTU, 576 to 9000 |
| `senders` | `4` | send sockets; the outer source port follows the flow across them |
| `exits` | required | profile label to `{vtep, mac}`; a `default` entry is required |
| `limits` | `max_flows` 16384, `new_flows_per_second` 1000 | per claim: `bytes_per_second`, `packets_per_second`, `new_flows_per_second`, `max_flows`, `quota_bytes`; an unset `bytes_per_second`, `packets_per_second` or `quota_bytes` is unlimited |
| `reuse_after_seconds` | `432000` | how long a freed address waits; match the exits' `nf_conntrack_tcp_timeout_established` |

### Single-host exit

The exit's VXLAN socket binds the wildcard address, so on the node itself
the exit lives in its own network namespace, reached over a veth pair. This
is the layout the CI test runs:

```sh
ip netns add exit
ip link add vh type veth peer name ve netns exit
ip addr add 10.99.0.1/24 dev vh && ip link set vh up          # tun.listen 10.99.0.1:4789
ip -n exit addr add 10.99.0.2/24 dev ve && ip -n exit link set ve up
# a veth leaves the inner checksum partial; the exit must compute it
ip netns exec exit ethtool -K ve tx off
ip -n exit link add vxlan0 address 02:ee:00:00:00:01 \
    type vxlan id 1 dstport 4789 local 10.99.0.2 dev ve learning
# the exit's own address sits outside tun.subnet, so no claim can be handed it
ip -n exit addr add 192.0.2.1/32 dev vxlan0 && ip -n exit link set vxlan0 up
ip -n exit route add 100.80.0.0/12 dev vxlan0
ip netns exec exit sysctl -w net.ipv4.ip_forward=1 \
    net.ipv4.conf.vxlan0.arp_accept=1 \
    net.ipv4.neigh.vxlan0.base_reachable_time_ms=300000
# uplink: route the exit namespace out through a second veth or a NIC moved into it
ip netns exec exit nft -f - <<'NFT'
table ip exit {
  chain overlay_nat { type nat hook postrouting priority srcnat; ip saddr 100.80.0.0/12 oifname != "vxlan0" masquerade; }
  chain overlay_forward { type filter hook forward priority filter; iifname "vxlan0" oifname "vxlan0" drop; }
  chain overlay_input { type filter hook input priority filter; iifname "vxlan0" drop; }
  chain underlay_input { type filter hook input priority filter; udp dport 4789 ip saddr != 10.99.0.1 drop; }
}
NFT
ip netns exec exit sysctl -w net.netfilter.nf_conntrack_max=1048576
```

- `overlay_forward` keeps claims from reaching each other through the exit.
- `overlay_input` drops everything from `vxlan0` addressed to the exit itself;
  ARP is not IP traffic and is unaffected.
- `underlay_input` accepts VXLAN only from node addresses, because a sender
  that reaches the port could claim an overlay address with a gratuitous ARP.
  sandboxd in turn accepts VXLAN only from the exits' VTEP addresses; the
  underlay must not let other hosts spoof those addresses toward `tun.listen`.
- Watch `nf_conntrack_count` against `nf_conntrack_max`; a full table drops
  new flows for every claim.
- A host without `nft` carries the same four rules in `iptables` inside the
  exit's namespace.
- A gateway machine uses the same configuration with its own addresses, and
  its NIC computes inner checksums, so the `ethtool` line is only for veth.
- MTU: VXLAN adds 50 bytes; on a 1500-byte underlay `tun.mtu` is 1450, on a
  1460-byte cloud underlay 1410.

## Limits

- IPv4 only.
- One stream per guest; a guest kernel without io_uring, or `SILKD_TUN_IO=epoll`
  in silkd's environment, uses the epoll path.
- Windows guests run no silkd and get no advanced network mode.
