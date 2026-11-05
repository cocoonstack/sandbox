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
| Device | `tun0`, `IFF_TUN | IFF_NO_PI | IFF_VNET_HDR`, owned by silkd |
| Address | `100.64.0.2/30` in every guest; the host tells guests apart by their vsock stream, never by address |
| Gateway | `100.64.0.1`, default route via `tun0` |
| MTU | 1500 until the host hello names another |
| IPv6 | none |
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
  tun-enabled pool is live, and again after every wake, unarchive and move:
  a restored VMM never resets the guest's old stream, so without it an idle
  guest keeps waiting on a dead one. Warm VMs never get it, so goldens are
  unchanged.
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
hands them to the TUN in one submission.

### Reconnect

There is no session state beyond the hello. After any stream loss the guest
dials again and both sides renegotiate offloads and MTU, so a move to a host
with a different kernel or MTU is safe. Packets in flight during the gap are
lost; TCP retransmits.

## Host side

The host side is sandboxd; its design is
[#471](https://github.com/cocoonstack/sandbox/issues/471). Whatever it does
with the packets, it owes the guest this contract:

- **One stream per claim.** The packet stream arrives on the claim's own
  vsock socket, so the stream, not any address in a packet, names the claim.
  Every guest uses `100.64.0.2`; the host must not trust a source address the
  guest wrote and must deliver only the claim's own return traffic on its
  stream, with the destination set back to `100.64.0.2`.
- **Enforcement on the host.** The host answers `ON` only for a claim that is
  allowed packet mode, and answers off, closes the stream or refuses the dial
  otherwise; revocation closes the stream. Nothing depends on the guest
  obeying `tun_down`.
- **Checksums after a rewrite.** A host that changes an address updates the
  IPv4 header checksum and the L4 checksum incrementally. For a frame with
  `VIRTIO_NET_HDR_F_NEEDS_CSUM` the L4 checksum sits at
  `csum_start + csum_offset` and holds the uncomplemented pseudo-header sum;
  a UDP checksum of 0 stays 0. With GSO this happens once per frame.
- **Offloads it offered.** The host sends only GSO types the guest's hello
  offered and segments the rest itself.

## Limits

- IPv4 only.
- One stream per guest; a guest kernel without io_uring, or `SILKD_TUN_IO=epoll`
  in silkd's environment, uses the epoll path.
- Windows guests run no silkd and get no advanced network mode.
