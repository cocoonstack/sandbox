//! Small OS helpers: the base environment for spawned commands, best-effort
//! user de-escalation, and the one signal syscall — all the crate's unsafe
//! lives here.

use std::fmt::Write as _;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::process::ExitStatus;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::process::Command;

/// Serializes getpwnam, whose result points into a process-global static
/// buffer the next call clobbers — not safe across tokio's worker threads.
static NSS_LOCK: Mutex<()> = Mutex::new(());

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

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

/// The environment every exec starts from before the request's env is
/// layered on — a sane PATH and TERM, nothing inherited from silkd.
pub fn base_env() -> [(&'static str, &'static str); 2] {
    [
        (
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ),
        ("TERM", "xterm-256color"),
    ]
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
    status.signal().map(|s| 128 + s).unwrap_or(-1)
}

/// Reaps the child and maps its status via `exit_code`; -1 when the wait
/// itself fails.
pub async fn wait_code(child: &mut tokio::process::Child) -> i32 {
    child.wait().await.map(exit_code).unwrap_or(-1)
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
