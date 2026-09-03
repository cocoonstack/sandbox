//! Persistent shell sessions: each session owns a long-lived bash whose cwd,
//! environment, and shell state survive across `exec {session}` calls.
//! A command is injected into the shell and
//! delimited by a unique sentinel that also carries its exit code; stderr is
//! merged into stdout (`exec 2>&1`) so one stream frames cleanly without a
//! pipe deadlock, which is the conventional interactive-shell behaviour.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::proto::{self, ErrorKind, READ_CHUNK, Response};
use crate::sysutil;

/// Idle sessions (no command run within this window) are reaped so an
/// abandoned session's shell + fds don't accumulate.
pub const IDLE_TTL: Duration = Duration::from_secs(30 * 60);
pub const REAP_INTERVAL: Duration = Duration::from_secs(60);

/// Bounds the post-marker scan for the exit-code line's newline: the line is
/// tiny and atomic with the marker, so this only caps a backgrounded child
/// flooding stdout without a newline.
const EXIT_TAIL_MAX: usize = 64 * 1024;

/// Registry of live shell sessions, addressed by id across connections.
#[derive(Clone, Default)]
pub struct Table {
    inner: Arc<Mutex<HashMap<String, Arc<Session>>>>,
}

impl Table {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawns a bash session, applies cwd/env, and registers it. A blank id
    /// yields a generated one; a duplicate id is refused.
    pub async fn create(
        &self,
        id: Option<String>,
        cwd: Option<String>,
        env: &HashMap<String, String>,
    ) -> io::Result<String> {
        let id = match id {
            Some(s) if !s.is_empty() => s,
            _ => format!("sess-{}", sysutil::tmp_suffix()),
        };
        let mut child = Command::new("bash")
            // The same sanitized baseline as exec/pty ("nothing inherited
            // from silkd"); the request's cwd/env layer on via the init line.
            .env_clear()
            .envs(sysutil::base_env())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Null, not piped: nothing ever reads a stderr pipe, and the init
            // line `exec 2>&1` repoints fd 2 at the stdout pipe anyway.
            .stderr(Stdio::null())
            // Own process group (leader pgid == pid) so teardown can group-kill
            // the shell together with whatever external command it is running.
            .process_group(0)
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().unwrap_or(0);
        let stdin = take_pipe(child.stdin.take(), "stdin")?;
        let stdout = take_pipe(child.stdout.take(), "stdout")?;
        let session = Arc::new(Session {
            pid,
            io: tokio::sync::Mutex::new(Io {
                stdin,
                stdout,
                _child: child,
                cmd_buf: String::new(),
                buf: vec![0u8; READ_CHUNK],
                acc: Vec::new(),
                frame: Vec::new(),
            }),
            last_active: Mutex::new(Instant::now()),
        });

        // Merge stderr, then apply cwd/env; sync on a sentinel so the first
        // real command reads a clean stream.
        let mut init = String::from("exec 2>&1\n");
        if let Some(dir) = cwd {
            init.push_str("cd ");
            shell_quote_into(&mut init, &dir);
            init.push_str(" || exit 1\n");
        }
        for (k, v) in env {
            init.push_str("export ");
            init.push_str(k);
            init.push('=');
            shell_quote_into(&mut init, v);
            init.push('\n');
        }
        {
            let mut io = session.io.lock().await;
            io.converse::<tokio::io::Sink>(&init, None).await?;
        }

        // reserve the id atomically: a concurrent create must not orphan the loser's shell.
        let mut map = sysutil::lock(&self.inner);
        if map.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "session id already exists",
            ));
        }
        map.insert(id.clone(), session);
        Ok(id)
    }

    pub fn get(&self, id: &str) -> Option<Arc<Session>> {
        sysutil::lock(&self.inner).get(id).cloned()
    }

    pub fn list(&self) -> Vec<String> {
        let mut ids: Vec<String> = sysutil::lock(&self.inner).keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Removes and kills a session's shell. The explicit kill (not just
    /// kill_on_drop) unwedges a session whose command is blocked while a
    /// run() still holds the io lock and its Arc.
    pub fn remove(&self, id: &str) -> bool {
        let removed = sysutil::lock(&self.inner).remove(id);
        if let Some(s) = &removed {
            sysutil::kill_group(s.pid);
        }
        removed.is_some()
    }

    pub fn len(&self) -> usize {
        sysutil::lock(&self.inner).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes and kills sessions idle longer than `ttl`. A session currently
    /// running a command (io lock held) is never idle, so it is skipped even
    /// if its last stamp is old (a long-running command). Returns the count.
    pub fn reap_idle(&self, ttl: Duration) -> usize {
        let mut map = sysutil::lock(&self.inner);
        let before = map.len();
        map.retain(|_, s| {
            let idle = s.io.try_lock().is_ok() && sysutil::lock(&s.last_active).elapsed() > ttl;
            if idle {
                sysutil::kill_group(s.pid);
            }
            !idle
        });
        before - map.len()
    }
}

/// One persistent shell. `io` serializes commands so a session runs one at a
/// time; the child is killed on drop, and `pid` allows an explicit kill to
/// unwedge a session whose command is blocked while the io lock is held.
pub struct Session {
    pid: u32,
    io: tokio::sync::Mutex<Io>,
    last_active: Mutex<Instant>,
}

impl Session {
    /// Runs `argv` in the session shell, shell-quoted so `cd`/`export` persist; false once the shell is dead.
    pub async fn run<W: AsyncWrite + Unpin>(&self, argv: &[String], w: &mut W) -> bool {
        let mut cmdline = String::new();
        for (i, a) in argv.iter().enumerate() {
            if i > 0 {
                cmdline.push(' ');
            }
            shell_quote_into(&mut cmdline, a);
        }
        let mut io = self.io.lock().await;
        let outcome = io.converse(&cmdline, Some(w)).await;
        // Stamp while io is still held: the reaper skips a session whose io lock
        // is held, so it can't mis-reap in the gap between the command finishing
        // and the fresh stamp landing (a long-running command isn't idle either
        // — io stays held for its whole duration).
        *sysutil::lock(&self.last_active) = Instant::now();
        drop(io);
        match outcome {
            Ok(code) => {
                let _ = proto::write_frame(w, &Response::Exit { code }).await;
                true
            }
            Err(e) => {
                let _ = proto::error_frame(w, ErrorKind::Internal, format!("session: {e}")).await;
                false
            }
        }
    }
}

/// The shell's pipes plus converse's scratch buffers, which are per-session
/// and reused across commands rather than reallocated per call.
struct Io {
    stdin: ChildStdin,
    stdout: ChildStdout,
    _child: Child,
    cmd_buf: String,
    buf: Vec<u8>,
    acc: Vec<u8>,
    frame: Vec<u8>,
}

impl Io {
    /// Runs `cmd` up to a unique sentinel, streaming the bytes before it to `out` and returning the exit code.
    async fn converse<W: AsyncWrite + Unpin>(
        &mut self,
        cmd: &str,
        mut out: Option<&mut W>,
    ) -> io::Result<i32> {
        // The marker is unforgeable: the shell runs untrusted code that must
        // not be able to fake the sentinel and desync the stream. `{ …; }
        // </dev/null` keeps cd/export in the current shell and stops a
        // stdin-reading command (a REPL, `read`) from swallowing the printf.
        let body = match cmd.trim_end() {
            "" => ":",
            c => c,
        };
        let marker = format!("__SILK_{}__", sysutil::rand_token());
        self.cmd_buf.clear();
        let _ = write!(
            self.cmd_buf,
            "{{ {body} ; }} </dev/null\nprintf '{marker} %s\\n' \"$?\"\n"
        );
        self.stdin.write_all(self.cmd_buf.as_bytes()).await?;
        self.stdin.flush().await?;

        let mb = marker.as_bytes();
        let keep = mb.len().saturating_sub(1);
        self.acc.clear();
        loop {
            let n = self.stdout.read(&mut self.buf).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "shell exited"));
            }
            self.acc.extend_from_slice(&self.buf[..n]);
            if let Some(pos) = memchr::memmem::find(&self.acc, mb) {
                emit(&mut out, &mut self.frame, &self.acc[..pos]).await;
                self.acc.drain(..pos + mb.len());
                while !self.acc.contains(&b'\n') && self.acc.len() < EXIT_TAIL_MAX {
                    let m = self.stdout.read(&mut self.buf).await?;
                    if m == 0 {
                        break;
                    }
                    self.acc.extend_from_slice(&self.buf[..m]);
                }
                // Parse only the exit-code line; a command that left a
                // background writer can land bytes after the newline, which
                // must not corrupt the code.
                let end = self
                    .acc
                    .iter()
                    .position(|&b| b == b'\n')
                    .unwrap_or(self.acc.len());
                return Ok(String::from_utf8_lossy(&self.acc[..end])
                    .trim()
                    .parse()
                    .unwrap_or(-1));
            }
            if self.acc.len() > keep {
                let upto = self.acc.len() - keep;
                emit(&mut out, &mut self.frame, &self.acc[..upto]).await;
                self.acc.drain(..upto);
            }
        }
    }
}

/// Periodically reaps idle sessions until the process exits.
pub async fn reap_loop(table: Table, ttl: Duration, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let n = table.reap_idle(ttl);
        if n > 0 {
            eprintln!("silkd: reaped {n} idle session(s)");
        }
    }
}

/// Best-effort emit of a stdout frame. A client-write failure drops the
/// client (`*out = None`) but does not abort the caller, so converse keeps
/// draining the shell to its sentinel — a mid-command disconnect must not
/// leave the persistent shell out of sync for the next exec.
async fn emit<W: AsyncWrite + Unpin>(out: &mut Option<&mut W>, frame: &mut Vec<u8>, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let failed = match out.as_deref_mut() {
        Some(w) => proto::write_chunk_frame(w, frame, "stdout", data)
            .await
            .is_err(),
        None => false,
    };
    if failed {
        *out = None;
    }
}

/// Unwraps a piped child stdio handle; absent means the spawn contract broke.
fn take_pipe<T>(pipe: Option<T>, name: &str) -> io::Result<T> {
    pipe.ok_or_else(|| io::Error::other(format!("child {name} not piped")))
}

/// POSIX single-quote quoting: wraps `s` in '…' onto the end of `out`,
/// closing/reopening around any embedded single quote.
fn shell_quote_into(out: &mut String, s: &str) {
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_into_pins_exact_output() {
        let cases = [
            ("plain", "'plain'"),
            ("a b", "'a b'"),
            ("it's", "'it'\\''s'"),
            ("", "''"),
        ];
        for (input, want) in cases {
            let mut out = String::new();
            shell_quote_into(&mut out, input);
            assert_eq!(out, want, "quoting {input:?}");
        }
    }
}
