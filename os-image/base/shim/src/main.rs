//! sandbox-shim: PID 1 for one blink. Forks cocoon-agent before systemd so
//! agent-ready never waits on systemd's exec + dynamic-link + unit chain,
//! then execs systemd, which takes over as PID 1 and boots the full system
//! exactly as it would have. cocoon-agent.service skips itself via
//! ExecCondition when the shim-spawned agent is already running.
//!
//! sandbox-init hands off here when /sbin/sandbox-shim exists in the
//! assembled rootfs (see boot/init in the sandbox repo).

use std::ffi::CString;
use std::path::Path;
use std::ptr;

const AGENT: &str = "/usr/local/bin/cocoon-agent";
const INIT_CANDIDATES: [&str; 2] = ["/sbin/init", "/lib/systemd/systemd"];
const PATH_ENV: &str = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

fn main() {
    spawn_agent();
    for init in INIT_CANDIDATES {
        exec_init(init); // returns only on failure
    }
    // Nothing to hand over to: power off loudly instead of panicking PID 1.
    eprintln!("sandbox-shim: FATAL: could not exec any of {INIT_CANDIDATES:?}");
    unsafe {
        libc::sync();
        libc::reboot(libc::RB_POWER_OFF);
    }
    unreachable!();
}

/// Fork + execve the agent in its own session. The child is reparented to
/// systemd once the shim execs it; systemd reaps but does not manage it.
/// Failure is non-fatal — the systemd unit's ExecCondition then finds no
/// running agent and starts a managed instance instead.
fn spawn_agent() {
    if !Path::new(AGENT).exists() {
        eprintln!("sandbox-shim: {AGENT} not present, leaving it to systemd");
        return;
    }
    let prog = cstr(AGENT);
    let arg = cstr("serve");
    let path_env = cstr(PATH_ENV);
    let log_env = cstr("AGENT_LOG_LEVEL=info");
    let argv = [prog.as_ptr(), arg.as_ptr(), ptr::null()];
    let envp = [path_env.as_ptr(), log_env.as_ptr(), ptr::null()];
    // Safety: single-threaded process; fork+execve is the whole point.
    unsafe {
        match libc::fork() {
            0 => {
                libc::setsid();
                libc::execve(prog.as_ptr(), argv.as_ptr(), envp.as_ptr());
                libc::_exit(127);
            }
            -1 => eprintln!("sandbox-shim: fork failed, leaving agent to systemd"),
            _ => {}
        }
    }
}

fn exec_init(path: &str) {
    let prog = cstr(path);
    let path_env = cstr(PATH_ENV);
    let term_env = cstr("TERM=linux");
    let argv = [prog.as_ptr(), ptr::null()];
    let envp = [path_env.as_ptr(), term_env.as_ptr(), ptr::null()];
    unsafe { libc::execve(prog.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
    eprintln!("sandbox-shim: exec {path} failed");
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("static string contains no NUL")
}
