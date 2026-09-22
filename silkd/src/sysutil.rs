//! Small OS helpers; the crate's unsafe work lives here.

use std::io::Read;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use tokio::process::Command;

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Forwarded into every exec that has no direct route: the loopback proxy relay is its only way out.
const FORWARDED: [&str; 6] = [
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
];

const HEX: [u8; 16] = *b"0123456789abcdef";

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
    Ok(b.iter()
        .flat_map(|byte| [HEX[(byte >> 4) as usize], HEX[(byte & 0xf) as usize]])
        .map(char::from)
        .collect())
}

/// SIGKILLs the group led by `pgid`, so a session's external command dies with its shell; a synthetic id misses with ESRCH.
pub fn kill_group(pgid: u32) {
    if is_valid_pid(pgid) {
        send_signal(-(pgid as libc::pid_t), libc::SIGKILL);
    }
}

/// Sends `sig` to `pid`, ignoring ESRCH against a just-exited pid.
pub fn signal_pid(pid: u32, sig: i32) {
    if is_valid_pid(pid) {
        send_signal(pid as libc::pid_t, sig);
    }
}

/// The environment every exec starts from; the proxy snapshot rides only where nothing routes directly.
pub fn base_env() -> &'static [(&'static str, &'static str)] {
    static DIRECT: LazyLock<Vec<(&'static str, &'static str)>> =
        LazyLock::new(|| compose_env(true, proxy_vars()));
    static RELAYED: LazyLock<Vec<(&'static str, &'static str)>> =
        LazyLock::new(|| compose_env(false, proxy_vars()));
    if crate::net::routes_directly() {
        &DIRECT
    } else {
        &RELAYED
    }
}

/// Applies base_env's lane rule to an env-inheriting child: a routed NIC drops the proxy variables.
pub fn align_proxy_env(cmd: &mut Command) {
    align_proxy_env_for(cmd, crate::net::routes_directly());
}

/// Resolves a username via getpwnam and de-escalates the command onto it.
pub fn apply_user(cmd: &mut Command, user: &str) -> Result<(), String> {
    let (uid, gid, home) = lookup_user(user)?;
    cmd.uid(uid).gid(gid);
    cmd.env("HOME", home);
    cmd.env("USER", user);
    Ok(())
}

/// Opens a pty, returning the (master, slave) fds; the master is non-blocking for async I/O.
pub fn openpty(cols: u16, rows: u16) -> std::io::Result<(OwnedFd, OwnedFd)> {
    let ws = nix::pty::Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = nix::pty::openpty(&ws, None)?;
    set_nonblocking(pty.master.as_raw_fd())?;
    Ok((pty.master, pty.slave))
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
        cmd.pre_exec(|| make_controlling_tty());
    }
}

/// Reads from a raw fd, surfacing WouldBlock to the caller's readiness loop.
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

/// Reaps the child and maps its status via `exit_code`; -1 when the wait fails.
pub async fn wait_code(child: &mut tokio::process::Child) -> i32 {
    child.wait().await.map_or(-1, exit_code)
}

/// Makes the calling process a session leader and adopts fd 0 as its controlling terminal.
///
/// # Safety
/// Only async-signal-safe syscalls; valid in a post-fork child.
unsafe fn make_controlling_tty() -> std::io::Result<()> {
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

/// The FORWARDED values in silkd's own environment, read once: the unit environment is fixed.
fn proxy_vars() -> &'static [(&'static str, String)] {
    static PROXY_VARS: LazyLock<Vec<(&'static str, String)>> =
        LazyLock::new(|| snapshot_proxy(|key| std::env::var(key).ok()));
    &PROXY_VARS
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

fn lookup_user(user: &str) -> Result<(u32, u32, String), String> {
    match nix::unistd::User::from_name(user) {
        Ok(Some(pw)) => Ok((
            pw.uid.as_raw(),
            pw.gid.as_raw(),
            pw.dir.to_string_lossy().into_owned(),
        )),
        Ok(None) => Err(format!("unknown user {user:?}")),
        Err(e) => Err(format!("look up user {user:?}: {e}")),
    }
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
    fn unknown_user_rejected() {
        assert!(lookup_user("definitely-not-a-user-xyz").is_err());
    }

    #[test]
    fn root_resolves() {
        let (uid, _, home) = lookup_user("root").expect("root must exist");
        assert_eq!(uid, 0);
        assert!(!home.is_empty());
    }
}
