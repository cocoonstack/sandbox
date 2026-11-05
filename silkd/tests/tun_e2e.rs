//! Packet mode end to end: guest tun0 and a host tun in two network namespaces, joined over a stream pair; root only.
#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use nix::sched::{CloneFlags, unshare};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use silkd::sysutil;
use silkd::tun::{
    self, FLAG_ON, Flow, HELLO_LEN, Hello, Io, PEER_ADDR, Tun, VERSION, VNET_HDR_LEN,
};

const CLAIM_ADDR: Ipv4Addr = Ipv4Addr::new(100, 65, 0, 7);
const UPLOAD: u16 = 5001;
const ECHO: u16 = 5002;
const DOWNLOAD: u16 = 5003;
const SINK: u16 = 5004;
const PPS_SINK: u16 = 5005;
const PPS_SOURCE: u16 = 5006;
const GUEST_PPS_PORT: u16 = 6000;
const BULK: u64 = 256 << 20;
const GSO_NONE: u8 = 0;
const GSO_UDP_L4: u8 = 5;
const NEEDS_CSUM: u8 = 1;
const UDP_SEGMENT: libc::c_int = 103;
const UDP_DATAGRAMS: usize = 32;
const UDP_SEG: usize = 1000;
const SMALL: usize = 64;
const FLOOD: Duration = Duration::from_secs(2);

#[derive(Default)]
struct Stats {
    frames: AtomicU64,
    gso: AtomicU64,
    uso: AtomicU64,
    largest: AtomicU64,
}

struct Seen {
    upload_peer: String,
    echo_peers: Vec<String>,
    downloaded: u64,
    sink_datagrams: u64,
}

#[test]
#[ignore = "needs root for network namespaces and /dev/net/tun"]
fn tcp_udp_and_gso_cross_a_rewriting_host() {
    for io in [Io::Uring, Io::Epoll] {
        let up = Arc::new(Stats::default());
        let down = Arc::new(Stats::default());
        let (seen, accepts) = with_link(io, true, Some((up.clone(), down.clone())), |_| {
            guest_workload()
        });
        assert_eq!(seen.upload_peer, CLAIM_ADDR.to_string(), "{io:?}");
        assert_eq!(seen.echo_peers, vec![CLAIM_ADDR.to_string(); 16], "{io:?}");
        assert_eq!(seen.downloaded, BULK, "{io:?}");
        assert!(
            up.gso.load(Ordering::Relaxed) > 0,
            "{io:?}: no guest GSO frame"
        );
        assert!(
            down.gso.load(Ordering::Relaxed) > 0,
            "{io:?}: no host GSO frame"
        );
        assert!(
            up.largest.load(Ordering::Relaxed) > 16_000,
            "{io:?}: MTU-sized frames"
        );
        if accepts & tun::OFFLOAD_USO4 != 0 {
            assert_eq!(seen.sink_datagrams, UDP_DATAGRAMS as u64, "{io:?}");
            assert!(
                up.uso.load(Ordering::Relaxed) > 0,
                "{io:?}: no guest USO frame"
            );
        }
        eprintln!(
            "{io:?}: accepts {accepts:#x}; up frames {} gso {} uso {} largest {}; down frames {} gso {} largest {}",
            up.frames.load(Ordering::Relaxed),
            up.gso.load(Ordering::Relaxed),
            up.uso.load(Ordering::Relaxed),
            up.largest.load(Ordering::Relaxed),
            down.frames.load(Ordering::Relaxed),
            down.gso.load(Ordering::Relaxed),
            down.largest.load(Ordering::Relaxed),
        );
    }
}

#[test]
#[ignore = "needs root for network namespaces and /dev/net/tun"]
fn full_checksums_cross_when_offloads_are_off() {
    for io in [Io::Uring, Io::Epoll] {
        let up = Arc::new(Stats::default());
        let down = Arc::new(Stats::default());
        let (seen, _) = with_link(io, false, Some((up.clone(), down.clone())), |_| {
            guest_workload()
        });
        assert_eq!(seen.upload_peer, CLAIM_ADDR.to_string(), "{io:?}");
        assert_eq!(seen.echo_peers, vec![CLAIM_ADDR.to_string(); 16], "{io:?}");
        assert_eq!(seen.downloaded, BULK, "{io:?}");
        assert_eq!(up.gso.load(Ordering::Relaxed), 0, "{io:?}");
        assert_eq!(down.gso.load(Ordering::Relaxed), 0, "{io:?}");
    }
}

#[test]
#[ignore = "needs root for network namespaces and /dev/net/tun"]
fn host_off_answer_leaves_no_tun() {
    in_netns(|| {
        let mut tun = Tun::open("tun0").unwrap();
        let (guest, host) = UnixStream::pair().unwrap();
        let answer = std::thread::spawn(move || {
            let mut b = [0u8; HELLO_LEN];
            sysutil::read_exact_fd(
                std::os::fd::AsFd::as_fd(&host),
                &mut b,
                Duration::from_secs(5),
            )
            .unwrap();
            assert_eq!(Hello::decode(&b).unwrap().version, VERSION);
            let off = Hello {
                version: VERSION,
                offloads: 0,
                mtu: 0,
                flags: 0,
            };
            sysutil::write_all_fd(std::os::fd::AsFd::as_fd(&host), &off.encode()).unwrap();
        });
        assert_eq!(
            tun::session(&mut tun, &guest, Io::Uring).unwrap(),
            Flow::Off
        );
        answer.join().unwrap();
        drop(tun);
        assert!(Tun::open("tun0").is_ok(), "tun0 still held after off");
    })
    .join()
    .unwrap();
}

#[test]
#[ignore = "needs root; prints throughput, asserts nothing about speed"]
fn throughput_and_per_packet_cost() {
    let order = [
        (Io::Uring, true),
        (Io::Epoll, true),
        (Io::Uring, false),
        (Io::Epoll, false),
        (Io::Epoll, false),
        (Io::Uring, false),
        (Io::Epoll, true),
        (Io::Uring, true),
    ];
    for (io, offloads) in order {
        let (r, _) = with_link(io, offloads, None, |pump_tid| {
            runtime().block_on(async move {
                let t = Instant::now();
                upload(SocketAddr::from((PEER_ADDR, UPLOAD)), 4 * BULK).await;
                let up = gbits(4 * BULK, t.elapsed());
                let t = Instant::now();
                let got = download(SocketAddr::from((PEER_ADDR, DOWNLOAD))).await;
                let down = gbits(got, t.elapsed());
                let cpu = pump_cpu_ns(pump_tid);
                let sent = flood_up().await;
                let up_pkts = query(PPS_SINK).await;
                let up_cpu = pump_cpu_ns(pump_tid) - cpu;
                let cpu = pump_cpu_ns(pump_tid);
                let down_pkts = flood_down().await;
                let down_cpu = pump_cpu_ns(pump_tid) - cpu;
                (up, down, sent, up_pkts, up_cpu, down_pkts, down_cpu)
            })
        });
        let (up, down, sent, up_pkts, up_cpu, down_pkts, down_cpu) = r;
        let secs = FLOOD.as_secs_f64();
        eprintln!(
            "{io:?} offloads={offloads}: tcp up {up:.2} Gbit/s, down {down:.2} Gbit/s; \
             udp{SMALL} up {:.0} kpps ({:.0} ns/pkt, {sent} sent), down {:.0} kpps ({:.0} ns/pkt)",
            up_pkts as f64 / secs / 1e3,
            up_cpu as f64 / up_pkts.max(1) as f64,
            down_pkts as f64 / secs / 1e3,
            down_cpu as f64 / down_pkts.max(1) as f64,
        );
    }
}

/// Runs `guest` in the guest namespace while the session runs; `relay` rewrites the guest address the way the host's shared TUN does.
fn with_link<T: Send + 'static>(
    io: Io,
    offloads: bool,
    relay: Option<(Arc<Stats>, Arc<Stats>)>,
    guest: impl FnOnce(i32) -> T + Send + 'static,
) -> (T, u16) {
    let (guest_end, host_end, relay_threads) = match relay {
        Some((up, down)) => {
            let (g, rg) = UnixStream::pair().unwrap();
            let (rh, h) = UnixStream::pair().unwrap();
            let threads = vec![
                forward(
                    rg.try_clone().unwrap(),
                    rh.try_clone().unwrap(),
                    up,
                    12,
                    tun::GUEST_ADDR,
                    CLAIM_ADDR,
                ),
                forward(rh, rg, down, 16, CLAIM_ADDR, tun::GUEST_ADDR),
            ];
            (g, h, threads)
        }
        None => {
            let (g, h) = UnixStream::pair().unwrap();
            (g, h, Vec::new())
        }
    };
    let stop = Arc::new(AtomicBool::new(false));
    let host_stop = guest_end.try_clone().unwrap();
    let (host, accepts) = spawn_host(
        host_end,
        io,
        offloads,
        relay_threads.is_empty(),
        stop.clone(),
    );
    let out = in_netns(move || {
        let mut tun = Tun::open("tun0").unwrap();
        tun.configure_guest().unwrap();
        let shut = guest_end.try_clone().unwrap();
        let (tid_tx, tid_rx) = std::sync::mpsc::channel();
        let session = std::thread::Builder::new()
            .name("guest-pump".into())
            .spawn(move || {
                // SAFETY: gettid takes no arguments.
                tid_tx.send(unsafe { libc::gettid() }).unwrap();
                tun::session(&mut tun, &guest_end, io)
            })
            .unwrap();
        let tid = tid_rx.recv().unwrap();
        let out = guest(tid);
        shut.shutdown(std::net::Shutdown::Both).unwrap();
        assert_eq!(session.join().unwrap().unwrap(), Flow::Closed);
        out
    })
    .join()
    .unwrap();
    stop.store(true, Ordering::Relaxed);
    let _ = host_stop.shutdown(std::net::Shutdown::Both);
    host.join().unwrap();
    for t in relay_threads {
        t.join().unwrap();
    }
    (out, accepts)
}

fn spawn_host(
    stream: UnixStream,
    io: Io,
    offloads: bool,
    direct: bool,
    stop: Arc<AtomicBool>,
) -> (JoinHandle<()>, u16) {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let handle = in_netns(move || {
        let tun = Tun::open("tunh").unwrap();
        if direct {
            sysutil::if_up("tunh", PEER_ADDR, tun::PREFIX).unwrap();
        } else {
            sysutil::if_up("tunh", PEER_ADDR, 32).unwrap();
            sysutil::route_add("tunh", Ipv4Addr::new(100, 65, 0, 0), 16, None).unwrap();
        }
        let shut = stream.try_clone().unwrap();
        let (bound_tx, bound_rx) = std::sync::mpsc::channel();
        let servers = std::thread::spawn(move || {
            runtime().block_on(async move {
                let upload = TcpListener::bind((PEER_ADDR, UPLOAD)).await.unwrap();
                let download = TcpListener::bind((PEER_ADDR, DOWNLOAD)).await.unwrap();
                let echo = UdpSocket::bind((PEER_ADDR, ECHO)).await.unwrap();
                let sink = UdpSocket::bind((PEER_ADDR, SINK)).await.unwrap();
                let pps = UdpSocket::bind((PEER_ADDR, PPS_SINK)).await.unwrap();
                let source = TcpListener::bind((PEER_ADDR, PPS_SOURCE)).await.unwrap();
                tokio::spawn(serve_upload(upload));
                tokio::spawn(serve_download(download));
                tokio::spawn(serve_echo(echo));
                tokio::spawn(serve_count(sink, SINK, Some(UDP_SEG)));
                tokio::spawn(serve_count(pps, PPS_SINK, None));
                tokio::spawn(serve_flood(source));
                bound_tx.send(()).unwrap();
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
        });
        bound_rx.recv().unwrap();
        ready_tx.send(tun.accepts()).unwrap();
        let fd = std::os::fd::AsFd::as_fd(&stream);
        let mut b = [0u8; HELLO_LEN];
        sysutil::read_exact_fd(fd, &mut b, Duration::from_secs(5)).unwrap();
        let guest = Hello::decode(&b).unwrap();
        assert_eq!(guest.version, VERSION);
        let mine = if offloads { tun.accepts() } else { 0 };
        let hello = Hello {
            version: VERSION,
            offloads: mine,
            mtu: 1500,
            flags: FLAG_ON,
        };
        sysutil::write_all_fd(fd, &hello.encode()).unwrap();
        tun.agree(guest.offloads & mine).unwrap();
        let _ = tun::pump(&tun, fd, io);
        drop(shut);
        servers.join().unwrap();
    });
    let accepts = ready_rx.recv().unwrap();
    (handle, accepts)
}

fn guest_workload() -> Seen {
    runtime().block_on(async {
        let upload_peer = upload(SocketAddr::from((PEER_ADDR, UPLOAD)), BULK).await;
        let echo = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await.unwrap();
        echo.connect((PEER_ADDR, ECHO)).await.unwrap();
        let mut echo_peers = Vec::new();
        for i in 0..16u8 {
            echo.send(&[i; 1200]).await.unwrap();
            let mut buf = [0u8; 64];
            let n = tokio::time::timeout(Duration::from_secs(2), echo.recv(&mut buf))
                .await
                .unwrap()
                .unwrap();
            echo_peers.push(String::from_utf8_lossy(&buf[..n]).into_owned());
        }
        let downloaded = download(SocketAddr::from((PEER_ADDR, DOWNLOAD))).await;
        let sink = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
        let seg = UDP_SEG as libc::c_int;
        // SAFETY: seg is a live c_int and the length matches its size.
        let rc = unsafe {
            libc::setsockopt(
                sink.as_raw_fd(),
                libc::SOL_UDP,
                UDP_SEGMENT,
                (&seg as *const libc::c_int).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "UDP_SEGMENT: {}", std::io::Error::last_os_error());
        sink.send_to(&[7u8; UDP_DATAGRAMS * UDP_SEG], (PEER_ADDR, SINK))
            .unwrap();
        let sink_datagrams = query(SINK).await;
        Seen {
            upload_peer,
            echo_peers,
            downloaded,
            sink_datagrams,
        }
    })
}

fn thread_cpu_ns(tid: i32) -> u64 {
    std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat"))
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

fn pump_cpu_ns(pump_tid: i32) -> u64 {
    let workers = format!("iou-wrk-{pump_tid}");
    let mut total = thread_cpu_ns(pump_tid);
    for entry in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
        let comm = std::fs::read_to_string(entry.path().join("comm")).unwrap_or_default();
        if comm.trim() == workers
            && let Ok(tid) = entry.file_name().to_string_lossy().parse::<i32>()
        {
            total += thread_cpu_ns(tid);
        }
    }
    total
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn in_netns<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> JoinHandle<T> {
    std::thread::spawn(move || {
        unshare(CloneFlags::CLONE_NEWNET).unwrap();
        f()
    })
}

fn gbits(bytes: u64, took: Duration) -> f64 {
    bytes as f64 * 8.0 / took.as_secs_f64() / 1e9
}

async fn upload(target: SocketAddr, bytes: u64) -> String {
    let mut s = TcpStream::connect(target).await.unwrap();
    let chunk = vec![0x5au8; 1 << 20];
    let mut left = bytes;
    while left > 0 {
        let n = left.min(chunk.len() as u64) as usize;
        s.write_all(&chunk[..n]).await.unwrap();
        left -= n as u64;
    }
    s.shutdown().await.unwrap();
    let mut r = BufReader::new(s);
    assert_eq!(r.read_u64().await.unwrap(), bytes);
    let mut peer = String::new();
    r.read_line(&mut peer).await.unwrap();
    peer
}

async fn download(target: SocketAddr) -> u64 {
    let mut s = TcpStream::connect(target).await.unwrap();
    let mut buf = vec![0u8; 1 << 20];
    let mut got = 0u64;
    loop {
        let n = s.read(&mut buf).await.unwrap();
        if n == 0 {
            return got;
        }
        got += n as u64;
    }
}

async fn query(port: u16) -> u64 {
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut s = TcpStream::connect((PEER_ADDR, port)).await.unwrap();
    s.read_u64().await.unwrap()
}

async fn flood_up() -> u64 {
    tokio::task::spawn_blocking(|| {
        let s = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
        s.connect((PEER_ADDR, PPS_SINK)).unwrap();
        let payload = [1u8; SMALL];
        let deadline = Instant::now() + FLOOD;
        let mut sent = 0u64;
        while Instant::now() < deadline {
            for _ in 0..64 {
                if s.send(&payload).is_ok() {
                    sent += 1;
                }
            }
        }
        sent
    })
    .await
    .unwrap()
}

async fn flood_down() -> u64 {
    let sink = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, GUEST_PPS_PORT)).unwrap();
    sink.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let mut s = TcpStream::connect((PEER_ADDR, PPS_SOURCE)).await.unwrap();
    s.write_u8(1).await.unwrap();
    tokio::task::spawn_blocking(move || {
        let mut buf = [0u8; 2048];
        let mut got = 0u64;
        while sink.recv(&mut buf).is_ok() {
            got += 1;
        }
        got
    })
    .await
    .unwrap()
}

async fn serve_upload(l: TcpListener) {
    loop {
        let (mut s, peer) = l.accept().await.unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1 << 20];
            let mut got = 0u64;
            while let Ok(n) = s.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                got += n as u64;
            }
            let _ = s.write_u64(got).await;
            let _ = s.write_all(peer.ip().to_string().as_bytes()).await;
        });
    }
}

async fn serve_download(l: TcpListener) {
    loop {
        let (mut s, _) = l.accept().await.unwrap();
        tokio::spawn(async move {
            let chunk = vec![0xa5u8; 1 << 20];
            for _ in 0..BULK / chunk.len() as u64 {
                if s.write_all(&chunk).await.is_err() {
                    return;
                }
            }
        });
    }
}

async fn serve_echo(s: UdpSocket) {
    let mut buf = [0u8; 2048];
    loop {
        let (_, peer) = s.recv_from(&mut buf).await.unwrap();
        let _ = s.send_to(peer.ip().to_string().as_bytes(), peer).await;
    }
}

async fn serve_count(s: UdpSocket, port: u16, size: Option<usize>) {
    let report = TcpListener::bind((PEER_ADDR, port)).await.unwrap();
    let got = Arc::new(AtomicU64::new(0));
    tokio::spawn({
        let got = Arc::clone(&got);
        async move {
            let mut buf = [0u8; 2048];
            loop {
                let (n, _) = s.recv_from(&mut buf).await.unwrap();
                if size.is_none_or(|want| want == n) {
                    got.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    loop {
        let (mut c, _) = report.accept().await.unwrap();
        let _ = c.write_u64(got.swap(0, Ordering::Relaxed)).await;
    }
}

async fn serve_flood(l: TcpListener) {
    loop {
        let (mut c, peer) = l.accept().await.unwrap();
        let _ = c.read_u8().await;
        let target = SocketAddr::from((peer.ip(), GUEST_PPS_PORT));
        tokio::task::spawn_blocking(move || {
            let s = std::net::UdpSocket::bind((PEER_ADDR, 0)).unwrap();
            s.connect(target).unwrap();
            let payload = [2u8; SMALL];
            let deadline = Instant::now() + FLOOD;
            while Instant::now() < deadline {
                for _ in 0..64 {
                    let _ = s.send(&payload);
                }
            }
        });
    }
}

/// Copies frames one way, rewriting the IPv4 address at header offset `at` from `from` to `to`.
fn forward(
    r: UnixStream,
    w: UnixStream,
    stats: Arc<Stats>,
    at: usize,
    from: Ipv4Addr,
    to: Ipv4Addr,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut r = std::io::BufReader::with_capacity(256 << 10, r);
        let mut w = w;
        let mut hello = [0u8; HELLO_LEN];
        if r.read_exact(&mut hello).is_err() || w.write_all(&hello).is_err() {
            return;
        }
        let mut out = Vec::with_capacity(256 << 10);
        let mut frame = vec![0u8; tun::MAX_FRAME];
        loop {
            let mut head = [0u8; 4];
            if r.read_exact(&mut head).is_err() {
                let _ = w.shutdown(std::net::Shutdown::Write);
                return;
            }
            let len = u32::from_le_bytes(head) as usize;
            if r.read_exact(&mut frame[..len]).is_err() {
                return;
            }
            let body = &mut frame[..len];
            rewrite(body, at, from.octets(), to.octets());
            stats.frames.fetch_add(1, Ordering::Relaxed);
            stats.largest.fetch_max(len as u64, Ordering::Relaxed);
            if body[1] != GSO_NONE {
                stats.gso.fetch_add(1, Ordering::Relaxed);
            }
            if body[1] == GSO_UDP_L4 {
                stats.uso.fetch_add(1, Ordering::Relaxed);
            }
            out.extend_from_slice(&head);
            out.extend_from_slice(body);
            let buffered = frame_buffered(r.buffer());
            if (!buffered || out.len() > 192 << 10) && {
                let res = w.write_all(&out);
                out.clear();
                res.is_err()
            } {
                return;
            }
        }
    })
}

fn frame_buffered(buf: &[u8]) -> bool {
    tun::next_frame(buf).ok().flatten().is_some()
}

/// Swaps an IPv4 address at header offset `at`, patching the IP checksum and the L4 checksum (partial or full).
fn rewrite(frame: &mut [u8], at: usize, from: [u8; 4], to: [u8; 4]) {
    let partial = frame[0] & NEEDS_CSUM != 0;
    let csum_at = usize::from(u16::from_le_bytes([frame[6], frame[7]]))
        + usize::from(u16::from_le_bytes([frame[8], frame[9]]));
    let ip = &mut frame[VNET_HDR_LEN..];
    if ip[0] >> 4 != 4 || ip[at..at + 4] != from {
        return;
    }
    ip[at..at + 4].copy_from_slice(&to);
    let check = u16::from_be_bytes([ip[10], ip[11]]);
    ip[10..12].copy_from_slice(&(!adjust(!check, from, to)).to_be_bytes());
    let l4 = usize::from(ip[0] & 0x0f) * 4;
    let field = if partial {
        csum_at
    } else {
        match ip[9] {
            6 => l4 + 16,
            17 => l4 + 6,
            _ => return,
        }
    };
    let sum = u16::from_be_bytes([ip[field], ip[field + 1]]);
    let next = if partial {
        adjust(sum, from, to)
    } else if ip[9] == 17 && sum == 0 {
        0
    } else {
        !adjust(!sum, from, to)
    };
    ip[field..field + 2].copy_from_slice(&next.to_be_bytes());
}

/// Moves a ones'-complement sum from one address to another (RFC 1624).
fn adjust(sum: u16, from: [u8; 4], to: [u8; 4]) -> u16 {
    let mut s = u32::from(sum);
    for i in [0, 2] {
        s += u32::from(!u16::from_be_bytes([from[i], from[i + 1]]));
        s += u32::from(u16::from_be_bytes([to[i], to[i + 1]]));
    }
    while s >> 16 != 0 {
        s = (s & 0xffff) + (s >> 16);
    }
    s as u16
}
