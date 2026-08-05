//! Small OS helpers: the base environment for spawned commands, best-effort
//! user de-escalation, and the one signal syscall — the crate's unsafe work
//! lives here (pty.rs holds the one other unsafe block, its pre_exec
//! registration).

use std::fmt::Write as _;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use tokio::process::Command;

/// Serializes getpwnam, whose result points into a process-global static
/// buffer the next call clobbers — not safe across tokio's worker threads.
static NSS_LOCK: Mutex<()> = Mutex::new(());

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Forwarded from silkd's own environment into every exec: the loopback proxy
/// relay is the no-NIC lane's only way out, and nothing else may leak.
const FORWARDED: [&str; 6] = [
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
];

/// A suffix unique within this process (pid + monotonic counter), enough to
/// name a temp file a concurrent write in the same directory won't collide
/// with. Not cryptographic — just unique.
pub fn tmp_suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// A 128-bit unpredictable hex token from the OS CSPRNG. Used where an
/// in-sandbox command must not be able to guess or forge the value (the
/// session-command sentinel). Falls back to the predictable tmp_suffix only if
/// /dev/urandom is unreadable, which never happens on a normal guest.
pub fn rand_token() -> String {
    let mut b = [0u8; 16];
    match std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b)) {
        Ok(()) => {
            let mut s = String::with_capacity(32);
            for x in b {
                let _ = write!(s, "{x:02x}");
            }
            s
        }
        Err(_) => tmp_suffix(),
    }
}

/// SIGKILLs the process group led by `pgid` — a session's shell plus its
/// foreground children, so tearing down a session that is running an external
/// command actually stops the command (killing only the shell would leave the
/// child holding the stdout pipe open). The shell is spawned as its own group
/// leader (pgid == its pid).
pub fn kill_group(pgid: u32) {
    // kill(-0) targets the CALLER's group (silkd itself). Synthetic ids pass
    // the guard: synth_pid() keeps them ≤ i32::MAX and above pid_max, so they
    // reach kill() and miss with ESRCH.
    if !valid_pid(pgid) {
        return;
    }
    // SAFETY: kill(2) takes no pointers; the guards above keep the pid_t cast
    // in range and away from silkd's own group.
    unsafe { libc::kill(-(pgid as libc::pid_t), libc::SIGKILL) };
}

/// Sends `sig` to `pid`, ignoring the result: an ESRCH against a
/// just-exited pid already satisfies the caller's goal (the process is gone).
pub fn signal_pid(pid: u32, sig: i32) {
    // pid 0 means "my whole process group" to kill(2) — signalling it would
    // take down silkd and every child (see kill_group for synthetic ids).
    if !valid_pid(pid) {
        return;
    }
    // SAFETY: kill(2) takes no pointers; the guards above keep the pid_t cast
    // in range and away from silkd's own group.
    unsafe { libc::kill(pid as libc::pid_t, sig) };
}

/// Rejects pid 0 and anything that would go negative through the pid_t cast
/// (and so hit a process group instead of a pid).
fn valid_pid(id: u32) -> bool {
    id != 0 && id <= i32::MAX as u32
}

/// The environment every exec starts from before the request's env is layered
/// on — a sane PATH and TERM, plus the proxy snapshot on the no-network lane
/// (a lane with its own NIC would be steered into a closed relay).
pub fn base_env() -> Vec<(&'static str, &'static str)> {
    compose_env(crate::net::has_egress(), proxy_vars())
}

/// Applies base_env's lane rule to an env-inheriting child (git, language
/// servers): with a network of its own, the proxy variables come off.
pub fn align_proxy_env(cmd: &mut Command) {
    align_proxy_env_for(cmd, crate::net::has_egress());
}

fn align_proxy_env_for(cmd: &mut Command, nic: bool) {
    if !nic {
        return;
    }
    for key in FORWARDED {
        cmd.env_remove(key);
    }
}

fn compose_env<'a>(nic: bool, proxy: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    let mut env = vec![
        (
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ),
        ("TERM", "xterm-256color"),
    ];
    if !nic {
        env.extend(proxy.iter().map(|(k, v)| (*k, v.as_str())));
    }
    env
}

/// The FORWARDED values present in silkd's own environment, read once: the
/// unit environment is fixed at service start and silkd never mutates it.
fn proxy_vars() -> &'static [(&'static str, String)] {
    static PROXY_VARS: OnceLock<Vec<(&'static str, String)>> = OnceLock::new();
    PROXY_VARS.get_or_init(|| snapshot_proxy(|key| std::env::var(key).ok()))
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

/// Resolves a username to uid/gid via getpwnam and sets them on the command.
/// silkd runs as root, so this de-escalates; unknown users are rejected.
pub fn apply_user(cmd: &mut Command, user: &str) -> Result<(), String> {
    let (uid, gid, home) = lookup_user(user)?;
    cmd.uid(uid).gid(gid);
    cmd.env("HOME", home);
    cmd.env("USER", user);
    Ok(())
}

fn lookup_user(user: &str) -> Result<(u32, u32, String), String> {
    use std::ffi::CString;
    let cname = CString::new(user).map_err(|_| format!("invalid user {user:?}"))?;
    // Hold NSS_LOCK across the call and every read of the returned static
    // buffer, so a concurrent lookup on another worker thread cannot clobber
    // it mid-read (which would de-escalate to the wrong uid).
    let _guard = lock(&NSS_LOCK);
    // SAFETY: cname is a live NUL-terminated CString for the call.
    let pw = unsafe { libc::getpwnam(cname.as_ptr()) };
    if pw.is_null() {
        return Err(format!("unknown user {user:?}"));
    }
    // SAFETY: pw is non-null (checked) and, with NSS_LOCK still held, points to
    // a valid passwd whose pw_dir is a NUL-terminated string it owns.
    let pw = unsafe { &*pw };
    // SAFETY: pw_dir is NUL-terminated and stays valid while NSS_LOCK is held.
    let home = unsafe { std::ffi::CStr::from_ptr(pw.pw_dir) }
        .to_string_lossy()
        .into_owned();
    Ok((pw.pw_uid, pw.pw_gid, home))
}

/// Opens a pseudo-terminal, returning the (master, slave) fds. The master is
/// set non-blocking for async I/O; the slave becomes the child's controlling
/// terminal after setsid (see `pty::open`).
pub fn openpty(cols: u16, rows: u16) -> std::io::Result<(OwnedFd, OwnedFd)> {
    let mut master: libc::c_int = 0;
    let mut slave: libc::c_int = 0;
    let mut ws = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: openpty writes two valid fds into master/slave on success; ws is a
    // fully-initialized winsize (openpty only reads it). We take ownership of
    // both fds immediately. A raw `*mut` pointer for winp works across libc's
    // platform-varying signature (*mut on macOS, *const on Linux) without the
    // &mut that clippy flags as unnecessary on Linux.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::addr_of_mut!(ws),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: openpty just handed us these fds; wrapping transfers ownership so
    // they close on drop.
    let master = unsafe { OwnedFd::from_raw_fd(master) };
    // SAFETY: same transfer for the slave end.
    let slave = unsafe { OwnedFd::from_raw_fd(slave) };
    set_nonblocking(master.as_raw_fd())?;
    Ok((master, slave))
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

/// Makes the calling process a session leader and adopts fd 0 as its
/// controlling terminal. Called in the child between fork and exec, so it must
/// touch nothing but the two syscalls.
///
/// # Safety
/// Only async-signal-safe syscalls; valid in a post-fork child.
pub unsafe fn make_controlling_tty() -> std::io::Result<()> {
    // SAFETY: setsid and ioctl are async-signal-safe and take no pointers; the
    // caller guarantees a post-fork child (see # Safety).
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

/// Reads from a raw fd (the pty master), returning bytes read; a WouldBlock
/// error is surfaced to the caller's readiness loop.
pub fn read_fd(fd: RawFd, buf: &mut [u8]) -> std::io::Result<usize> {
    // SAFETY: fd is a live open fd; buf is a valid mutable slice of buf.len().
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(n as usize)
}

/// Writes to a raw fd (the pty master), returning bytes written.
pub fn write_fd(fd: RawFd, buf: &[u8]) -> std::io::Result<usize> {
    // SAFETY: fd is a live open fd; buf is a valid slice of buf.len().
    let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(n as usize)
}

fn set_nonblocking(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: fd is open; F_GETFL/F_SETFL only read and set its flags.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: same fd; F_SETFL only sets the just-read flags plus O_NONBLOCK.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Locks a std mutex, panicking on poisoning: silkd's critical sections never
/// panic, so a poisoned lock is unreachable.
#[allow(clippy::unwrap_used)]
pub fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap()
}

/// Maps a wait() status to the shell convention (128 + signal when killed).
pub fn exit_code(status: ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    if let Some(code) = status.code() {
        return code;
    }
    status.signal().map_or(-1, |s| 128 + s)
}

/// Reaps the child and maps its status via `exit_code`; -1 when the wait
/// itself fails.
pub async fn wait_code(child: &mut tokio::process::Child) -> i32 {
    child.wait().await.map_or(-1, exit_code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_env_has_path_and_term() {
        let env = base_env();
        assert!(env.iter().any(|(k, _)| *k == "PATH"));
        assert!(env.iter().any(|(k, _)| *k == "TERM"));
    }

    /// The no-NIC lane only reaches out if the proxy variables arrive without
    /// anyone asking; nothing else of silkd's environment is the guest's.
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

    /// A guest with a working NIC must not be steered into the loopback relay,
    /// which is closed unless the host armed a proxy for this sandbox.
    #[test]
    fn base_env_forwards_nothing_when_a_nic_is_present() {
        let proxy = snapshot_proxy(|_| Some("http://127.0.0.1:3128".to_string()));
        let env = compose_env(true, &proxy);
        assert!(
            !env.iter().any(|(k, _)| *k == "http_proxy"),
            "proxy variables must stay off a lane with its own NIC",
        );
    }

    /// Env-inheriting children (git, language servers) follow the same lane
    /// rule as cleared ones: scrubbed with a NIC, inherited without.
    #[test]
    fn align_proxy_env_scrubs_only_on_a_nic_lane() {
        let removed = |cmd: &Command| cmd.as_std().get_envs().filter(|(_, v)| v.is_none()).count();
        let mut with_nic = Command::new("true");
        align_proxy_env_for(&mut with_nic, true);
        assert_eq!(removed(&with_nic), FORWARDED.len());

        let mut without_nic = Command::new("true");
        align_proxy_env_for(&mut without_nic, false);
        assert_eq!(removed(&without_nic), 0);
    }

    #[test]
    fn unknown_user_rejected() {
        assert!(lookup_user("definitely-not-a-user-xyz").is_err());
    }

    #[test]
    fn root_resolves() {
        let (uid, _, home) = lookup_user("root").expect("root must exist");
        assert_eq!(uid, 0);
        assert!(!home.is_empty()); // /root on Linux, /var/root on macOS
    }
}
