//! Small OS helpers and syscall wrappers.

use std::borrow::Cow;
use std::io::Read;
#[cfg(target_os = "linux")]
use std::net::Ipv4Addr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use nix::fcntl::{FcntlArg, OFlag, fcntl};
use tokio::io::unix::AsyncFd;
use tokio::process::Command;

/// Forwarded into every exec that has no direct route: the loopback proxy relay is its only way out.
const FORWARDED: [&str; 6] = [
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
];

/// The claim's environment the host writes, in systemd EnvironmentFile syntax.
const CLAIM_ENV_PATH: &str = "/run/silkd.env";

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// The FORWARDED values in silkd's own environment, read once: the unit environment is fixed.
static PROXY_VARS: LazyLock<Vec<(&'static str, String)>> =
    LazyLock::new(|| snapshot_proxy(|key| std::env::var(key).ok()));
static DIRECT: LazyLock<Vec<(&'static str, &'static str)>> =
    LazyLock::new(|| compose_env(true, &PROXY_VARS));
static RELAYED: LazyLock<Vec<(&'static str, &'static str)>> =
    LazyLock::new(|| compose_env(false, &PROXY_VARS));

/// A suffix unique within this process, enough to name a temp file; not cryptographic.
pub fn tmp_suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// A 128-bit CSPRNG hex token, used where an in-sandbox command must not forge the value.
pub fn rand_token() -> std::io::Result<String> {
    let mut b = [0u8; 16];
    if !fill_random(&mut b) {
        return Err(std::io::Error::other(
            "no entropy source for a session marker",
        ));
    }
    Ok(format!("{:032x}", u128::from_be_bytes(b)))
}

/// Sends `sig` to the group led by `pgid`, so a command's children share its fate; ESRCH is ignored.
pub fn signal_group(pgid: u32, sig: i32) {
    if is_valid_pid(pgid) {
        send_signal(-(pgid as libc::pid_t), sig);
    }
}

/// The environment every exec starts from; the proxy snapshot rides only where nothing routes directly.
pub fn base_env() -> &'static [(&'static str, &'static str)] {
    if crate::net::routes_directly() {
        &DIRECT
    } else {
        &RELAYED
    }
}

/// The claim's environment file every exec, session and pty parses after base_env; empty when the host wrote none.
pub fn claim_env() -> String {
    std::fs::read_to_string(CLAIM_ENV_PATH).unwrap_or_default()
}

pub fn apply_claim_env(cmd: &mut Command) {
    let claim = claim_env();
    cmd.envs(parse_env_file(&claim).iter().map(|(k, v)| (*k, &**v)));
}

/// Parses the subset the host writes: one `KEY="value"` per line, with systemd's double-quote escapes.
pub fn parse_env_file(text: &str) -> Vec<(&str, Cow<'_, str>)> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key, unquote(value)))
        .collect()
}

/// Applies base_env's lane rule to an env-inheriting child: a routed NIC drops the proxy variables.
pub fn align_proxy_env(cmd: &mut Command) {
    align_proxy_env_for(cmd, crate::net::routes_directly());
}

/// Sets the command's cwd and, with a user, de-escalates onto it with the user's groups before entering cwd; returns that user's uid.
pub fn apply_cwd_user(
    cmd: &mut Command,
    cwd: Option<&str>,
    user: Option<&str>,
) -> Result<Option<u32>, String> {
    let Some(user) = user else {
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }
        return Ok(None);
    };
    let (uid, gid, home) = lookup_user(user)?;
    de_escalate(cmd, user, uid, gid, cwd)?;
    cmd.env("HOME", home);
    cmd.env("USER", user);
    Ok(Some(uid))
}

pub fn apply_owner(cmd: &mut Command, path: &str) {
    let Ok(meta) = std::fs::metadata(path) else {
        return;
    };
    if meta.uid() == 0 || !nix::unistd::geteuid().is_root() {
        return;
    }
    cmd.uid(meta.uid()).gid(meta.gid());
    if let Ok(Some(pw)) = nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(meta.uid())) {
        cmd.env("HOME", pw.dir).env("USER", pw.name);
    }
}

pub fn start_in(cmd: &mut Command, dir: &OwnedFd) {
    let fd = dir.as_raw_fd();
    // SAFETY: fchdir is async-signal-safe and `dir` outlives the spawn that runs this hook.
    unsafe {
        cmd.pre_exec(move || {
            nix::unistd::fchdir(BorrowedFd::borrow_raw(fd)).map_err(std::io::Error::from)
        })
    };
}

/// Opens a pty, returning the (master, slave) fds; both are close-on-exec from birth and the master is non-blocking.
pub fn openpty(cols: u16, rows: u16) -> std::io::Result<(OwnedFd, OwnedFd)> {
    let master = nix::pty::posix_openpt(OFlag::O_RDWR | OFlag::O_NOCTTY | OFlag::O_CLOEXEC)?;
    nix::pty::grantpt(&master)?;
    nix::pty::unlockpt(&master)?;
    let slave = nix::fcntl::open(
        pty_name(&master)?.as_str(),
        OFlag::O_RDWR | OFlag::O_NOCTTY | OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )?;
    let master = OwnedFd::from(master);
    set_winsize(master.as_raw_fd(), cols, rows)?;
    set_nonblocking(master.as_fd(), true)?;
    Ok((master, slave))
}

pub fn watch_fd<T: AsFd + AsRawFd>(fd: T) -> std::io::Result<AsyncFd<T>> {
    // SAFETY: AsFd's I/O-safety contract keeps the raw fd open while `fd`, and so the AsyncFd, lives.
    unsafe { AsyncFd::register(fd) }.map_err(|e| e.into_parts().1)
}

/// Resizes a pty via TIOCSWINSZ on its master fd.
pub fn set_winsize(fd: RawFd, cols: u16, rows: u16) -> std::io::Result<()> {
    let ws = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: fd is an open pty master (held by the Proc); ws is initialized.
    let rc = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &ws) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Makes the child a session leader with fd 0 as its controlling terminal.
pub fn adopt_controlling_tty(cmd: &mut Command) {
    // SAFETY: make_controlling_tty runs only async-signal-safe syscalls, the contract for a post-fork pre_exec hook.
    unsafe {
        cmd.pre_exec(make_controlling_tty);
    }
}

/// Locks a std mutex, panicking on poisoning: silkd's critical sections never panic.
#[allow(clippy::unwrap_used)]
pub fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap()
}

/// Maps a wait() status to the shell convention (128 + signal when killed).
pub fn exit_code(status: ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    status.signal().map_or(-1, |s| 128 + s)
}

/// Unwraps a piped child stdio handle; absent means the spawn contract broke.
pub fn take_pipe<T>(pipe: Option<T>, name: &str) -> std::io::Result<T> {
    pipe.ok_or_else(|| std::io::Error::other(format!("child {name} not piped")))
}

/// Opens tun device `name` with a virtio-net header leading every packet.
#[cfg(target_os = "linux")]
pub fn tun_open(name: &str) -> std::io::Result<OwnedFd> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")?;
    let mut req = ifreq(name)?;
    req.ifr_ifru.ifru_flags =
        (libc::IFF_TUN | libc::IFF_NO_PI | libc::IFF_VNET_HDR) as libc::c_short;
    // SAFETY: file is an open tun control fd; req is an initialized ifreq that outlives the call.
    check(unsafe { libc::ioctl(file.as_raw_fd(), libc::TUNSETIFF, &mut req) })?;
    Ok(file.into())
}

#[cfg(target_os = "linux")]
pub fn vsock_connect_start(cid: u32, port: u32) -> std::io::Result<OwnedFd> {
    // SAFETY: socket(2) takes no pointers.
    let raw = unsafe {
        libc::socket(
            libc::AF_VSOCK,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        )
    };
    check(raw)?;
    // SAFETY: raw is the fresh fd socket(2) just returned, owned by nothing else.
    let fd = unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(raw) };
    // SAFETY: sockaddr_vm is plain data and all-zero is a valid value.
    let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
    addr.svm_cid = cid;
    addr.svm_port = port;
    // SAFETY: addr is an initialized sockaddr_vm and the length passed is its size.
    let rc = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            (&raw const addr).cast(),
            std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        )
    };
    match check(rc) {
        Err(e) if e.raw_os_error() != Some(libc::EINPROGRESS) => Err(e),
        _ => Ok(fd),
    }
}

#[cfg(target_os = "linux")]
pub fn vsock_accept(listener: BorrowedFd<'_>) -> std::io::Result<(OwnedFd, u32)> {
    // SAFETY: sockaddr_vm is plain data and all-zero is a valid value.
    let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t;
    // SAFETY: addr and len are live locals sized for a sockaddr_vm.
    let raw = unsafe {
        libc::accept4(
            listener.as_raw_fd(),
            (&raw mut addr).cast(),
            &raw mut len,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
        )
    };
    check(raw)?;
    // SAFETY: raw is the fresh fd accept4(2) just returned, owned by nothing else.
    let fd = unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(raw) };
    Ok((fd, addr.svm_cid))
}

/// Sets the `TUN_F_*` offloads the tun reader accepts; the kernel refuses a bit it does not know.
#[cfg(target_os = "linux")]
pub fn tun_set_offload(fd: RawFd, flags: u32) -> std::io::Result<()> {
    // SAFETY: TUNSETOFFLOAD takes its argument by value.
    check(unsafe { libc::ioctl(fd, libc::TUNSETOFFLOAD, libc::c_ulong::from(flags)) })
}

/// Assigns `addr/prefix` to interface `name` and brings it up.
#[cfg(target_os = "linux")]
pub fn if_up(name: &str, addr: Ipv4Addr, prefix: u8) -> std::io::Result<()> {
    let sock = ctl_socket()?;
    let fd = sock.as_raw_fd();
    let mut req = ifreq(name)?;
    req.ifr_ifru.ifru_addr = inet_sockaddr(addr);
    if_ioctl(fd, libc::SIOCSIFADDR, &mut req)?;
    req.ifr_ifru.ifru_netmask = inet_sockaddr(prefix_mask(prefix));
    if_ioctl(fd, libc::SIOCSIFNETMASK, &mut req)?;
    if_ioctl(fd, libc::SIOCGIFFLAGS, &mut req)?;
    // SAFETY: SIOCGIFFLAGS just filled ifru_flags.
    let flags = unsafe { req.ifr_ifru.ifru_flags };
    req.ifr_ifru.ifru_flags = flags | libc::IFF_UP as libc::c_short;
    if_ioctl(fd, libc::SIOCSIFFLAGS, &mut req)
}

#[cfg(target_os = "linux")]
pub fn if_set_mtu(name: &str, mtu: u16) -> std::io::Result<()> {
    let sock = ctl_socket()?;
    let mut req = ifreq(name)?;
    req.ifr_ifru.ifru_mtu = libc::c_int::from(mtu);
    if_ioctl(sock.as_raw_fd(), libc::SIOCSIFMTU, &mut req)
}

#[cfg(target_os = "linux")]
pub fn route_add(
    dev: &str,
    dst: Ipv4Addr,
    prefix: u8,
    gateway: Option<Ipv4Addr>,
) -> std::io::Result<()> {
    let sock = ctl_socket()?;
    let name = std::ffi::CString::new(dev)?;
    // SAFETY: rtentry is plain data and all-zero is a valid value, a null rt_dev included.
    let mut rt: libc::rtentry = unsafe { std::mem::zeroed() };
    rt.rt_dst = inet_sockaddr(dst);
    rt.rt_genmask = inet_sockaddr(prefix_mask(prefix));
    rt.rt_flags = libc::RTF_UP;
    if let Some(gw) = gateway {
        rt.rt_gateway = inet_sockaddr(gw);
        rt.rt_flags |= libc::RTF_GATEWAY;
    }
    rt.rt_dev = name.as_ptr().cast_mut();
    // SAFETY: rt is initialized and rt_dev points into `name`, which outlives the call.
    check(unsafe { libc::ioctl(sock.as_raw_fd(), libc::SIOCADDRT as _, &mut rt) })
}

pub fn set_nonblocking(fd: BorrowedFd<'_>, on: bool) -> std::io::Result<()> {
    let flags = OFlag::from_bits_retain(fcntl(fd, FcntlArg::F_GETFL)?);
    let next = if on {
        flags | OFlag::O_NONBLOCK
    } else {
        flags - OFlag::O_NONBLOCK
    };
    if next != flags {
        fcntl(fd, FcntlArg::F_SETFL(next))?;
    }
    Ok(())
}

pub fn write_all_fd(fd: BorrowedFd<'_>, mut buf: &[u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        match nix::unistd::write(fd, buf) {
            Ok(n) => buf = &buf[n..],
            Err(nix::errno::Errno::EINTR) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Fills `buf` from blocking `fd`, failing with TimedOut once `timeout` passes.
pub fn read_exact_fd(
    fd: BorrowedFd<'_>,
    buf: &mut [u8],
    timeout: std::time::Duration,
) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    let mut got = 0;
    while got < buf.len() {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        let mut pfd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ms = libc::c_int::try_from(left.as_millis()).unwrap_or(libc::c_int::MAX);
        // SAFETY: pfd is one initialized pollfd that outlives the call.
        match unsafe { libc::poll(&mut pfd, 1, ms) } {
            0 => return Err(std::io::ErrorKind::TimedOut.into()),
            r if r < 0 => {
                let e = std::io::Error::last_os_error();
                if e.kind() != std::io::ErrorKind::Interrupted {
                    return Err(e);
                }
                continue;
            }
            _ => {}
        }
        match nix::unistd::read(fd, &mut buf[got..]) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => got += n,
            Err(nix::errno::Errno::EINTR) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn make_controlling_tty() -> std::io::Result<()> {
    // SAFETY: setsid and ioctl take no pointers, so no memory precondition applies.
    unsafe {
        if libc::setsid() < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Fills `b` from the OS CSPRNG without blocking a tokio worker; /dev/urandom covers the fallback.
fn fill_random(b: &mut [u8]) -> bool {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: getrandom writes at most b.len() bytes into b's live buffer.
        let n = unsafe { libc::getrandom(b.as_mut_ptr().cast(), b.len(), libc::GRND_NONBLOCK) };
        if n == b.len() as isize {
            return true;
        }
    }
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(b))
        .is_ok()
}

/// Rejects pid 0, which kill(2) reads as silkd's own process group, and anything that would go negative through the pid_t cast.
fn is_valid_pid(id: u32) -> bool {
    id != 0 && id <= i32::MAX as u32
}

#[cfg(target_os = "linux")]
fn pty_name(master: &nix::pty::PtyMaster) -> nix::Result<String> {
    nix::pty::ptsname_r(master)
}

#[cfg(not(target_os = "linux"))]
fn pty_name(master: &nix::pty::PtyMaster) -> nix::Result<String> {
    static PTSNAME: Mutex<()> = Mutex::new(());
    let _serial = lock(&PTSNAME);
    // SAFETY: PTSNAME serializes every ptsname call in silkd, so its static buffer is copied before another call reuses it.
    unsafe { nix::pty::ptsname(master) }
}

fn send_signal(target: libc::pid_t, sig: i32) {
    // SAFETY: kill(2) takes no pointers; every caller vets the target with is_valid_pid first.
    unsafe { libc::kill(target, sig) };
}

fn align_proxy_env_for(cmd: &mut Command, direct: bool) {
    if !direct {
        return;
    }
    for key in FORWARDED {
        cmd.env_remove(key);
    }
}

fn compose_env<'a>(
    direct: bool,
    proxy: &'a [(&'static str, String)],
) -> Vec<(&'static str, &'a str)> {
    let mut env = vec![
        (
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ),
        ("TERM", "xterm-256color"),
    ];
    if !direct {
        env.extend(proxy.iter().map(|(k, v)| (*k, v.as_str())));
    }
    env
}

fn snapshot_proxy(get: impl Fn(&str) -> Option<String>) -> Vec<(&'static str, String)> {
    FORWARDED
        .iter()
        .filter_map(|&key| match get(key) {
            Some(v) if !v.is_empty() => Some((key, v)),
            _ => None,
        })
        .collect()
}

fn unquote(value: &str) -> Cow<'_, str> {
    let Some(inner) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return Cow::Borrowed(value);
    };
    if !inner.contains('\\') {
        return Cow::Borrowed(inner);
    }
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some(e @ ('"' | '\\' | '$' | '`')) => out.push(e),
            Some(e) => {
                out.push('\\');
                out.push(e);
            }
            None => out.push('\\'),
        }
    }
    Cow::Owned(out)
}

#[cfg(target_os = "linux")]
fn de_escalate(
    cmd: &mut Command,
    user: &str,
    uid: u32,
    gid: u32,
    cwd: Option<&str>,
) -> Result<(), String> {
    use nix::unistd::{Gid, Uid};
    if !nix::unistd::geteuid().is_root() {
        cmd.uid(uid).gid(gid);
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }
        return Ok(());
    }
    let name = std::ffi::CString::new(user).map_err(|e| format!("user {user:?}: {e}"))?;
    let groups = nix::unistd::getgrouplist(&name, Gid::from_raw(gid))
        .map_err(|e| format!("look up groups of {user:?}: {e}"))?;
    let dir = cwd
        .map(std::ffi::CString::new)
        .transpose()
        .map_err(|e| format!("cwd: {e}"))?;
    // SAFETY: setgroups, setgid, setuid and chdir are async-signal-safe, the contract for a post-fork pre_exec hook.
    unsafe {
        cmd.pre_exec(move || {
            nix::unistd::setgroups(&groups)?;
            nix::unistd::setgid(Gid::from_raw(gid))?;
            nix::unistd::setuid(Uid::from_raw(uid))?;
            if let Some(dir) = &dir {
                nix::unistd::chdir(dir.as_c_str())?;
            }
            Ok(())
        })
    };
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn de_escalate(
    cmd: &mut Command,
    _user: &str,
    uid: u32,
    gid: u32,
    cwd: Option<&str>,
) -> Result<(), String> {
    cmd.uid(uid).gid(gid);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    Ok(())
}

fn lookup_user(user: &str) -> Result<(u32, u32, PathBuf), String> {
    match nix::unistd::User::from_name(user) {
        Ok(Some(pw)) => Ok((pw.uid.as_raw(), pw.gid.as_raw(), pw.dir)),
        Ok(None) => Err(format!("unknown user {user:?}")),
        Err(e) => Err(format!("look up user {user:?}: {e}")),
    }
}

#[cfg(target_os = "linux")]
fn ctl_socket() -> std::io::Result<std::net::UdpSocket> {
    std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
}

#[cfg(target_os = "linux")]
fn ifreq(name: &str) -> std::io::Result<libc::ifreq> {
    if name.len() >= libc::IFNAMSIZ || name.as_bytes().contains(&0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid interface name {name:?}"),
        ));
    }
    // SAFETY: ifreq is plain data and all-zero is a valid value.
    let mut req: libc::ifreq = unsafe { std::mem::zeroed() };
    for (d, s) in req.ifr_name.iter_mut().zip(name.bytes()) {
        *d = libc::c_char::from_ne_bytes([s]);
    }
    Ok(req)
}

#[cfg(target_os = "linux")]
fn if_ioctl(fd: RawFd, request: libc::c_ulong, req: &mut libc::ifreq) -> std::io::Result<()> {
    // SAFETY: fd is an open socket; req is an initialized ifreq that outlives the call.
    check(unsafe { libc::ioctl(fd, request as _, req) })
}

#[cfg(target_os = "linux")]
fn inet_sockaddr(addr: Ipv4Addr) -> libc::sockaddr {
    let mut sa_data = [0; 14];
    for (d, s) in sa_data[2..6].iter_mut().zip(addr.octets()) {
        *d = libc::c_char::from_ne_bytes([s]);
    }
    libc::sockaddr {
        sa_family: libc::AF_INET as libc::sa_family_t,
        sa_data,
    }
}

#[cfg(target_os = "linux")]
fn prefix_mask(prefix: u8) -> Ipv4Addr {
    Ipv4Addr::from(u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0))
}

#[cfg(target_os = "linux")]
fn check(rc: libc::c_int) -> std::io::Result<()> {
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openpty_fds_are_close_on_exec_and_the_master_non_blocking() {
        let (master, slave) = openpty(80, 24).unwrap();
        for fd in [&master, &slave] {
            let fd_flags =
                nix::fcntl::FdFlag::from_bits_retain(fcntl(fd, FcntlArg::F_GETFD).unwrap());
            assert!(fd_flags.contains(nix::fcntl::FdFlag::FD_CLOEXEC));
        }
        let fl = OFlag::from_bits_retain(fcntl(&master, FcntlArg::F_GETFL).unwrap());
        assert!(fl.contains(OFlag::O_NONBLOCK));
    }

    #[test]
    fn base_env_has_path_and_term() {
        let env = base_env();
        assert!(env.iter().any(|(k, _)| *k == "PATH"));
        assert!(env.iter().any(|(k, _)| *k == "TERM"));
    }

    #[test]
    fn base_env_forwards_only_the_proxy_variables() {
        let proxy = snapshot_proxy(|key| match key {
            "http_proxy" => Some("http://127.0.0.1:3128".to_string()),
            "HTTPS_PROXY" => Some(String::new()),
            _ => Some("2048".to_string()),
        });
        let env = compose_env(false, &proxy);
        assert_eq!(
            env.iter()
                .find(|(k, _)| *k == "http_proxy")
                .map(|(_, v)| *v),
            Some("http://127.0.0.1:3128"),
        );
        assert!(
            !env.iter().any(|(k, _)| *k == "SILKD_PORT"),
            "silkd's own settings must not reach the guest's commands",
        );
        assert!(
            !env.iter().any(|(k, _)| *k == "HTTPS_PROXY"),
            "an empty variable must not be forwarded",
        );
    }

    #[test]
    fn base_env_forwards_nothing_when_the_nic_routes() {
        let proxy = snapshot_proxy(|_| Some("http://127.0.0.1:3128".to_string()));
        let env = compose_env(true, &proxy);
        assert!(
            !env.iter().any(|(k, _)| *k == "http_proxy"),
            "proxy variables must stay off a lane that routes directly",
        );
    }

    #[test]
    fn align_proxy_env_scrubs_only_where_the_nic_routes() {
        let removed = |cmd: &Command| cmd.as_std().get_envs().filter(|(_, v)| v.is_none()).count();
        let mut direct = Command::new("true");
        align_proxy_env_for(&mut direct, true);
        assert_eq!(removed(&direct), FORWARDED.len());

        let mut relayed = Command::new("true");
        align_proxy_env_for(&mut relayed, false);
        assert_eq!(removed(&relayed), 0);
    }

    #[test]
    fn parse_env_file_reads_what_the_host_writes() {
        let text = "A=\"plain\"\nB=\"a \\\"quoted\\\" \\$HOME \\\\ path \\`x\\`\"\nC=bare\nnot-an-entry\nD=\"keep \\n\"\n";
        let want = vec![
            ("A", Cow::from("plain")),
            ("B", Cow::from("a \"quoted\" $HOME \\ path `x`")),
            ("C", Cow::from("bare")),
            ("D", Cow::from("keep \\n")),
        ];
        assert_eq!(parse_env_file(text), want);
    }

    #[test]
    fn parse_env_file_of_an_empty_file_is_empty() {
        assert!(parse_env_file("").is_empty());
    }

    #[test]
    fn unknown_user_rejected() {
        assert!(lookup_user("definitely-not-a-user-xyz").is_err());
    }

    #[test]
    fn root_resolves() {
        let (uid, _, home) = lookup_user("root").expect("root must exist");
        assert_eq!(uid, 0);
        assert!(!home.as_os_str().is_empty());
    }
}
