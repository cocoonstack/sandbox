//! Small OS helpers: the base environment for spawned commands, best-effort
//! user de-escalation, and the one signal syscall — all the crate's unsafe
//! lives here.

use std::io::Read;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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
        Ok(()) => b.iter().map(|x| format!("{x:02x}")).collect(),
        Err(_) => tmp_suffix(),
    }
}

/// SIGKILLs the process group led by `pgid` — a session's shell plus its
/// foreground children, so tearing down a session that is running an external
/// command actually stops the command (killing only the shell would leave the
/// child holding the stdout pipe open). The shell is spawned as its own group
/// leader (pgid == its pid).
pub fn kill_group(pgid: u32) {
    // kill(-0) targets the CALLER's group (silkd itself); guard it, and the
    // synthetic-id range whose pid_t cast would go negative.
    if pgid == 0 || pgid > i32::MAX as u32 {
        return;
    }
    unsafe { libc::kill(-(pgid as libc::pid_t), libc::SIGKILL) };
}

/// Sends `sig` to `pid`, ignoring the result: an ESRCH against a
/// just-exited pid already satisfies the caller's goal (the process is gone).
pub fn signal_pid(pid: u32, sig: i32) {
    // pid 0 means "my whole process group" to kill(2) — signalling it would
    // take down silkd and every child. Above i32::MAX is only a synthetic
    // fallback id (no real process); casting it to pid_t would go negative and
    // hit a process group. Neither is ever a real target.
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    unsafe { libc::kill(pid as libc::pid_t, sig) };
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
    let _guard = NSS_LOCK.lock().unwrap();
    let pw = unsafe { libc::getpwnam(cname.as_ptr()) };
    if pw.is_null() {
        return Err(format!("unknown user {user:?}"));
    }
    let pw = unsafe { &*pw };
    let home = unsafe { std::ffi::CStr::from_ptr(pw.pw_dir) }
        .to_string_lossy()
        .into_owned();
    Ok((pw.pw_uid, pw.pw_gid, home))
}

/// Maps a wait() status to the shell convention (128 + signal when killed).
pub fn exit_code(status: ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    if let Some(code) = status.code() {
        return code;
    }
    status.signal().map(|s| 128 + s).unwrap_or(-1)
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
