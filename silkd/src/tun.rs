//! Packet mode: tun0 carries the guest's non-proxy traffic as raw IP frames over one vsock stream to the host.

use std::io;
use std::net::Ipv4Addr;

/// Host vsock port of the packet stream.
pub const HOST_VSOCK_PORT: u32 = 2051;
/// The guest's address, the same in every box; the host tells boxes apart by their vsock stream.
pub const GUEST_ADDR: Ipv4Addr = Ipv4Addr::new(100, 64, 0, 2);
/// The guest's gateway, and the resolver address the host answers.
pub const PEER_ADDR: Ipv4Addr = Ipv4Addr::new(100, 64, 0, 1);
pub const PREFIX: u8 = 30;
pub const VERSION: u16 = 1;
pub const HELLO_LEN: usize = 12;
/// The virtio-net header that leads every frame's packet.
pub const VNET_HDR_LEN: usize = 10;
/// The largest frame body: a virtio-net header and a 64 KiB GSO packet.
pub const MAX_FRAME: usize = VNET_HDR_LEN + 65535;
/// Host hello flag: packet mode is on for this box.
pub const FLAG_ON: u16 = 0x01;

/// Offload bits in a hello, equal to the kernel's `TUN_F_*` values.
pub const OFFLOAD_CSUM: u16 = 0x01;
pub const OFFLOAD_TSO4: u16 = 0x02;
pub const OFFLOAD_TSO6: u16 = 0x04;
pub const OFFLOAD_TSO_ECN: u16 = 0x08;
pub const OFFLOAD_USO4: u16 = 0x20;
pub const OFFLOAD_USO6: u16 = 0x40;

const MAGIC: [u8; 4] = *b"SLKT";

/// The 12 bytes each side sends first: magic, version, the offloads its tun accepts, the MTU and flags (host only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hello {
    pub version: u16,
    pub offloads: u16,
    pub mtu: u16,
    pub flags: u16,
}

impl Hello {
    pub fn encode(&self) -> [u8; HELLO_LEN] {
        let mut b = [0u8; HELLO_LEN];
        b[..4].copy_from_slice(&MAGIC);
        b[4..6].copy_from_slice(&self.version.to_le_bytes());
        b[6..8].copy_from_slice(&self.offloads.to_le_bytes());
        b[8..10].copy_from_slice(&self.mtu.to_le_bytes());
        b[10..12].copy_from_slice(&self.flags.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8; HELLO_LEN]) -> io::Result<Hello> {
        if b[..4] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tun hello: bad magic",
            ));
        }
        let field = |i: usize| u16::from_le_bytes([b[i], b[i + 1]]);
        Ok(Hello {
            version: field(4),
            offloads: field(6),
            mtu: field(8),
            flags: field(10),
        })
    }
}

#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(target_os = "linux")]
mod linux {
    use std::io;
    use std::net::Ipv4Addr;
    use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::io::unix::AsyncFd;

    use super::*;
    use crate::{sysutil, tun_uring};

    const IFNAME: &str = "tun0";
    const DEFAULT_MTU: u16 = 1500;
    /// Per-direction buffer; the sender batches every packet its tun has ready into one stream write.
    const BATCH: usize = 256 << 10;
    const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
    const REDIAL_MIN: Duration = Duration::from_millis(50);
    const REDIAL_MAX: Duration = Duration::from_secs(1);
    const FULL_OFFLOADS: u16 =
        OFFLOAD_CSUM | OFFLOAD_TSO4 | OFFLOAD_TSO6 | OFFLOAD_TSO_ECN | OFFLOAD_USO4 | OFFLOAD_USO6;
    /// The set a kernel older than 6.2 accepts, which predates UDP segmentation.
    const BASE_OFFLOADS: u16 = OFFLOAD_CSUM | OFFLOAD_TSO4 | OFFLOAD_TSO6 | OFFLOAD_TSO_ECN;

    /// The link thread that owns tun0, if one runs.
    static LINK: Mutex<Option<Arc<Link>>> = Mutex::new(None);

    /// How a session moves packets.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Io {
        Uring,
        Epoll,
    }

    impl Io {
        /// io_uring unless `SILKD_TUN_IO=epoll`; a kernel that refuses io_uring falls back per session.
        pub fn from_env() -> Io {
            match std::env::var("SILKD_TUN_IO").as_deref() {
                Ok("epoll") => Io::Epoll,
                _ => Io::Uring,
            }
        }
    }

    /// How a session ended.
    #[derive(Debug, PartialEq, Eq)]
    pub enum Flow {
        /// The host turned packet mode off for this box.
        Off,
        /// The stream closed; the link dials again.
        Closed,
    }

    /// A tun device with a virtio-net header on every packet, and the offloads its kernel accepts.
    pub struct Tun {
        fd: OwnedFd,
        accepts: u16,
        configured: bool,
    }

    impl Tun {
        /// Opens tun `name` with every offload off until a peer states what it accepts.
        pub fn open(name: &str) -> io::Result<Tun> {
            let fd = sysutil::tun_open(name)?;
            let raw = fd.as_raw_fd();
            let accepts = [FULL_OFFLOADS, BASE_OFFLOADS]
                .into_iter()
                .find(|&m| sysutil::tun_set_offload(raw, m.into()).is_ok())
                .unwrap_or(0);
            sysutil::tun_set_offload(raw, 0)?;
            Ok(Tun {
                fd,
                accepts,
                configured: false,
            })
        }

        pub fn accepts(&self) -> u16 {
            self.accepts
        }

        /// Turns on the offloads both kernels accept; segmentation offloads need checksum offload.
        pub fn agree(&self, peer: u16) -> io::Result<()> {
            let both = self.accepts & peer;
            let on = if both & OFFLOAD_CSUM == 0 { 0 } else { both };
            sysutil::tun_set_offload(self.fd.as_raw_fd(), on.into())
        }

        /// Gives tun0 the fixed guest address and the default route; idempotent.
        pub fn configure_guest(&mut self) -> io::Result<()> {
            if self.configured {
                return Ok(());
            }
            sysutil::if_set_mtu(IFNAME, DEFAULT_MTU)?;
            sysutil::if_up(IFNAME, GUEST_ADDR, PREFIX)?;
            sysutil::route_add(IFNAME, Ipv4Addr::UNSPECIFIED, 0, Some(PEER_ADDR))?;
            self.configured = true;
            Ok(())
        }
    }

    /// Handles `tun_up`: brings tun0 up now and starts its link; the host's hello may still turn it off.
    pub fn up() -> io::Result<()> {
        let mut slot = sysutil::lock(&LINK);
        if slot.is_some() {
            return Ok(());
        }
        let mut tun = Tun::open(IFNAME)?;
        tun.configure_guest()?;
        *slot = Some(spawn_link(tun)?);
        Ok(())
    }

    /// Handles `tun_down`: stops the link and removes tun0 with its routes before returning; enforcement stays on the host.
    pub fn down() {
        let Some(link) = sysutil::lock(&LINK).take() else {
            return;
        };
        link.stop.store(true, Ordering::Relaxed);
        if let Some(stream) = sysutil::lock(&link.stream).as_ref() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        if let Some(thread) = sysutil::lock(&link.thread).take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }

    /// Asks the host at boot; a refused dial or an off answer leaves no tun0.
    pub fn ask_at_boot() {
        let mut slot = sysutil::lock(&LINK);
        if slot.is_some() {
            return;
        }
        match Tun::open(IFNAME).and_then(spawn_link) {
            Ok(link) => *slot = Some(link),
            Err(e) => eprintln!("silkd tun: {e}"),
        }
    }

    /// Runs one guest connection: hello both ways, then pumps until the stream ends; configures tun0 on the first on.
    pub fn session<S: AsFd>(tun: &mut Tun, stream: &S, io: Io) -> io::Result<Flow> {
        let fd = stream.as_fd();
        sysutil::set_nonblocking(fd, false)?;
        let hello = Hello {
            version: VERSION,
            offloads: tun.accepts(),
            mtu: 0,
            flags: 0,
        };
        sysutil::write_all_fd(fd, &hello.encode())?;
        let mut b = [0u8; HELLO_LEN];
        sysutil::read_exact_fd(fd, &mut b, HELLO_TIMEOUT)?;
        let host = Hello::decode(&b)?;
        if host.version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("tun hello: host speaks version {}", host.version),
            ));
        }
        if host.flags & FLAG_ON == 0 {
            return Ok(Flow::Off);
        }
        tun.configure_guest()?;
        tun.agree(host.offloads)?;
        if host.mtu != 0 {
            sysutil::if_set_mtu(IFNAME, host.mtu)?;
        }
        pump(tun, fd, io)?;
        Ok(Flow::Closed)
    }

    /// Moves frames between `tun` and `stream` until either fails or the stream ends; blocks the calling thread.
    pub fn pump(tun: &Tun, stream: BorrowedFd<'_>, io: Io) -> io::Result<()> {
        let tun_fd = tun.fd.as_fd();
        if io == Io::Uring {
            match tun_uring::Ring::new() {
                Ok(ring) => {
                    sysutil::set_nonblocking(tun_fd, false)?;
                    sysutil::set_nonblocking(stream, false)?;
                    return ring.pump(tun_fd.as_raw_fd(), stream.as_raw_fd());
                }
                Err(e) => eprintln!("silkd tun: io_uring unavailable, using epoll: {e}"),
            }
        }
        sysutil::set_nonblocking(tun_fd, true)?;
        sysutil::set_nonblocking(stream, true)?;
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()?
            .block_on(epoll_pump(tun_fd, stream))
    }

    /// A link thread's stop signal and the stream it may block on.
    struct Link {
        stop: AtomicBool,
        stream: Mutex<Option<Arc<vsock::VsockStream>>>,
        thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    }

    fn spawn_link(tun: Tun) -> io::Result<Arc<Link>> {
        let link = Arc::new(Link {
            stop: AtomicBool::new(false),
            stream: Mutex::new(None),
            thread: Mutex::new(None),
        });
        let me = Arc::clone(&link);
        let thread = std::thread::Builder::new()
            .name("silkd-tun".into())
            .spawn(move || run_link(&me, tun))?;
        *sysutil::lock(&link.thread) = Some(thread);
        Ok(link)
    }

    fn run_link(me: &Arc<Link>, mut tun: Tun) {
        let io = Io::from_env();
        let mut wait = REDIAL_MIN;
        while !me.stop.load(Ordering::Relaxed) {
            match vsock::VsockStream::connect_with_cid_port(libc::VMADDR_CID_HOST, HOST_VSOCK_PORT)
            {
                Ok(stream) => {
                    wait = REDIAL_MIN;
                    let stream = Arc::new(stream);
                    {
                        let mut slot = sysutil::lock(&me.stream);
                        if me.stop.load(Ordering::Relaxed) {
                            break;
                        }
                        *slot = Some(Arc::clone(&stream));
                    }
                    let flow = session(&mut tun, &*stream, io);
                    *sysutil::lock(&me.stream) = None;
                    match flow {
                        Ok(Flow::Off) => break,
                        Ok(Flow::Closed) => {}
                        Err(e) if !me.stop.load(Ordering::Relaxed) => eprintln!("silkd tun: {e}"),
                        Err(_) => {}
                    }
                }
                Err(_) if !tun.configured => break,
                Err(_) => {}
            }
            std::thread::park_timeout(wait);
            wait = (wait * 2).min(REDIAL_MAX);
        }
        let mut slot = sysutil::lock(&LINK);
        drop(tun);
        if slot.as_ref().is_some_and(|l| Arc::ptr_eq(l, me)) {
            *slot = None;
        }
    }

    async fn epoll_pump(tun: BorrowedFd<'_>, stream: BorrowedFd<'_>) -> io::Result<()> {
        let tun = sysutil::watch_fd(tun)?;
        let stream = sysutil::watch_fd(stream)?;
        tokio::select! {
            res = tun_to_stream(&tun, &stream) => res,
            res = stream_to_tun(&tun, &stream) => res,
        }
    }

    async fn tun_to_stream(
        tun: &AsyncFd<BorrowedFd<'_>>,
        stream: &AsyncFd<BorrowedFd<'_>>,
    ) -> io::Result<()> {
        let mut buf = vec![0u8; BATCH];
        loop {
            let mut guard = tun.readable().await?;
            let mut len = 0;
            while len + 4 + MAX_FRAME <= BATCH {
                let body = &mut buf[len + 4..len + 4 + MAX_FRAME];
                match guard.try_io(|fd| nix::unistd::read(fd.get_ref(), body).map_err(Into::into)) {
                    Ok(Ok(n)) if n >= VNET_HDR_LEN => {
                        buf[len..len + 4].copy_from_slice(&(n as u32).to_le_bytes());
                        len += 4 + n;
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Err(e),
                    Err(_) => break,
                }
            }
            let mut sent = 0;
            while sent < len {
                let mut guard = stream.writable().await?;
                match guard.try_io(|fd| {
                    nix::unistd::write(fd.get_ref(), &buf[sent..len]).map_err(Into::into)
                }) {
                    Ok(Ok(n)) => sent += n,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {}
                }
            }
        }
    }

    async fn stream_to_tun(
        tun: &AsyncFd<BorrowedFd<'_>>,
        stream: &AsyncFd<BorrowedFd<'_>>,
    ) -> io::Result<()> {
        let mut buf = vec![0u8; BATCH];
        let mut end = 0;
        loop {
            let mut start = 0;
            while let Some(len) = next_frame(&buf[start..end])? {
                send_packet(tun, &buf[start + 4..start + 4 + len]).await;
                start += 4 + len;
            }
            buf.copy_within(start..end, 0);
            end -= start;
            let mut guard = stream.readable().await?;
            match guard
                .try_io(|fd| nix::unistd::read(fd.get_ref(), &mut buf[end..]).map_err(Into::into))
            {
                Ok(Ok(0)) => return Ok(()),
                Ok(Ok(n)) => end += n,
                Ok(Err(e)) => return Err(e),
                Err(_) => {}
            }
        }
    }

    async fn send_packet(tun: &AsyncFd<BorrowedFd<'_>>, pkt: &[u8]) {
        loop {
            let Ok(mut guard) = tun.writable().await else {
                return;
            };
            // a packet the kernel refuses is dropped, as a lossy link drops it.
            if guard
                .try_io(|fd| nix::unistd::write(fd.get_ref(), pkt).map_err(Into::into))
                .is_ok()
            {
                return;
            }
        }
    }
}

/// The body length of the complete frame at the head of `buf`, or None while it is still partial.
pub fn next_frame(buf: &[u8]) -> io::Result<Option<usize>> {
    let Some(head) = buf.first_chunk::<4>() else {
        return Ok(None);
    };
    let len = u32::from_le_bytes(*head) as usize;
    if !(VNET_HDR_LEN..=MAX_FRAME).contains(&len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("tun frame length {len}"),
        ));
    }
    Ok((buf.len() >= 4 + len).then_some(len))
}

/// Packet mode needs a Linux tun device.
#[cfg(not(target_os = "linux"))]
pub fn up() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "silkd tun requires linux",
    ))
}

#[cfg(not(target_os = "linux"))]
pub fn down() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_round_trips() {
        let h = Hello {
            version: VERSION,
            offloads: OFFLOAD_CSUM | OFFLOAD_TSO4,
            mtu: 1460,
            flags: FLAG_ON,
        };
        let b = h.encode();
        assert_eq!(&b[..4], b"SLKT");
        assert_eq!(Hello::decode(&b).unwrap(), h);
    }

    #[test]
    fn hello_rejects_bad_magic() {
        let mut b = Hello {
            version: VERSION,
            offloads: 0,
            mtu: 0,
            flags: 0,
        }
        .encode();
        b[0] = b'X';
        assert_eq!(
            Hello::decode(&b).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn next_frame_waits_for_the_whole_body() {
        let mut buf = (12u32).to_le_bytes().to_vec();
        buf.extend_from_slice(&[0; 11]);
        assert_eq!(next_frame(&buf).unwrap(), None);
        buf.push(0);
        assert_eq!(next_frame(&buf).unwrap(), Some(12));
        assert_eq!(next_frame(&buf[..3]).unwrap(), None);
    }

    #[test]
    fn next_frame_rejects_lengths_out_of_range() {
        for len in [0u32, 9, (MAX_FRAME + 1) as u32] {
            let err = next_frame(&len.to_le_bytes()).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{len}");
        }
    }
}
