//! Persistent shell sessions: each session owns a long-lived bash whose cwd,
//! environment, and shell state survive across `exec {session}` calls
//! (Cloudflare/Daytona semantics). A command is injected into the shell and
//! delimited by a unique sentinel that also carries its exit code; stderr is
//! merged into stdout (`exec 2>&1`) so one stream frames cleanly without a
//! pipe deadlock, which is the conventional interactive-shell behaviour.

use std::collections::HashMap;
use std::io;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::proto::{self, ErrorKind, Response};
use crate::sysutil;

const READ_CHUNK: usize = 32 * 1024;
/// Idle sessions (no command run within this window) are reaped so an
/// abandoned session's shell + fds don't accumulate.
pub const IDLE_TTL: Duration = Duration::from_secs(30 * 60);
pub const REAP_INTERVAL: Duration = Duration::from_secs(60);

/// Registry of live shell sessions, addressed by id across connections.
#[derive(Clone, Default)]
pub struct Table {
    inner: Arc<Mutex<HashMap<String, Arc<Session>>>>,
}

/// One persistent shell. `io` serializes commands so a session runs one at a
/// time; the child is killed on drop, and `pid` allows an explicit kill to
/// unwedge a session whose command is blocked while the io lock is held.
pub struct Session {
    pub id: String,
    pid: u32,
    io: tokio::sync::Mutex<Io>,
    last_active: Mutex<Instant>,
}

struct Io {
    stdin: ChildStdin,
    stdout: ChildStdout,
    _child: Child,
}

impl Table {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawns a bash session, applies cwd/env, and registers it. A blank or
    /// duplicate id yields a generated one.
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().unwrap_or(0);
        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");
        let session = Arc::new(Session {
            id: id.clone(),
            pid,
            io: tokio::sync::Mutex::new(Io {
                stdin,
                stdout,
                _child: child,
            }),
            last_active: Mutex::new(Instant::now()),
        });

        // Merge stderr, then apply cwd/env; sync on a sentinel so the first
        // real command reads a clean stream.
        let mut init = String::from("exec 2>&1\n");
        if let Some(dir) = cwd {
            init.push_str(&format!("cd {} || exit 1\n", shell_quote(&dir)));
        }
        for (k, v) in env {
            init.push_str(&format!("export {}={}\n", k, shell_quote(v)));
        }
        {
            let mut io = session.io.lock().await;
            io.converse::<tokio::io::Sink>(&init, None).await?;
        }

        // Reserve the id atomically: a concurrent create with the same id
        // must not silently replace (and orphan) the loser. Dropping the loser
        // Arc here fires kill_on_drop on its shell.
        let mut map = self.inner.lock().unwrap();
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
        self.inner.lock().unwrap().get(id).cloned()
    }

    pub fn list(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.inner.lock().unwrap().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Removes and kills a session's shell. The explicit kill (not just
    /// kill_on_drop) unwedges a session whose command is blocked while a
    /// run() still holds the io lock and its Arc.
    pub fn remove(&self, id: &str) -> bool {
        let removed = self.inner.lock().unwrap().remove(id);
        if let Some(s) = &removed {
            sysutil::kill(s.pid);
        }
        removed.is_some()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes and kills sessions idle longer than `ttl`. A session currently
    /// running a command (io lock held) is never idle, so it is skipped even
    /// if its last stamp is old (a long-running command). Returns the count.
    pub fn reap_idle(&self, ttl: Duration) -> usize {
        let mut map = self.inner.lock().unwrap();
        let dead: Vec<String> = map
            .iter()
            .filter(|(_, s)| {
                s.io.try_lock().is_ok() && s.last_active.lock().unwrap().elapsed() > ttl
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dead {
            if let Some(s) = map.remove(id) {
                sysutil::kill(s.pid);
            }
        }
        dead.len()
    }
}

impl Session {
    /// Runs `argv` in the session shell, streaming stdout frames to `w` and
    /// ending with an exit frame. argv is shell-quoted and joined so a fresh
    /// `cd`/`export` persists to later calls while ordinary argv still runs.
    /// Returns whether the session shell is still alive; a dead shell is
    /// removed by the caller so its id stops resolving.
    pub async fn run<W: AsyncWrite + Unpin>(&self, argv: &[String], w: &mut W) -> bool {
        let cmdline = argv
            .iter()
            .map(|a| shell_quote(a))
            .collect::<Vec<_>>()
            .join(" ");
        let mut io = self.io.lock().await;
        let outcome = io.converse(&cmdline, Some(w)).await;
        // Stamp while io is still held: the reaper skips a session whose io lock
        // is held, so it can't mis-reap in the gap between the command finishing
        // and the fresh stamp landing (a long-running command isn't idle either
        // — io stays held for its whole duration).
        *self.last_active.lock().unwrap() = Instant::now();
        drop(io);
        match outcome {
            Ok(code) => {
                let _ = proto::write_frame(w, &Response::Exit { code }).await;
                true
            }
            Err(e) => {
                let _ = proto::write_frame(
                    w,
                    &Response::error(ErrorKind::Internal, format!("session: {e}")),
                )
                .await;
                false
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

impl Io {
    /// Writes `cmd` to the shell followed by a unique sentinel printf, then
    /// reads output up to the sentinel — emitting the preceding bytes as
    /// stdout frames on `out` (None discards, used for init) and returning the
    /// command's exit code. A tiny tail is held back so a marker split across
    /// reads is still found.
    async fn converse<W: AsyncWrite + Unpin>(
        &mut self,
        cmd: &str,
        mut out: Option<&mut W>,
    ) -> io::Result<i32> {
        // Unpredictable marker: the shell runs untrusted code that must not be
        // able to forge the sentinel and desync the stream. Redirect the
        // command's stdin from /dev/null so a command that reads stdin (a REPL,
        // `read`) can't swallow the sentinel printf and wedge the read. The
        // `{ …; }` group runs in the current shell, so cd/export still persist.
        // Trim so a trailing newline doesn't put `;` on its own line inside the
        // group; empty becomes a no-op so `{ }` stays valid.
        let body = match cmd.trim_end() {
            "" => ":",
            c => c,
        };
        let marker = format!("__SILK_{}__", sysutil::rand_token());
        self.stdin
            .write_all(
                format!("{{ {body} ; }} </dev/null\nprintf '{marker} %s\\n' \"$?\"\n").as_bytes(),
            )
            .await?;
        self.stdin.flush().await?;

        let mb = marker.as_bytes();
        let keep = mb.len().saturating_sub(1);
        let mut acc: Vec<u8> = Vec::new();
        let mut buf = vec![0u8; READ_CHUNK];
        loop {
            let n = self.stdout.read(&mut buf).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "shell exited"));
            }
            acc.extend_from_slice(&buf[..n]);
            if let Some(pos) = find(&acc, mb) {
                emit(&mut out, &acc[..pos]).await;
                let mut rest = acc[pos + mb.len()..].to_vec();
                while !rest.contains(&b'\n') {
                    let m = self.stdout.read(&mut buf).await?;
                    if m == 0 {
                        break;
                    }
                    rest.extend_from_slice(&buf[..m]);
                }
                return Ok(String::from_utf8_lossy(&rest).trim().parse().unwrap_or(-1));
            }
            if acc.len() > keep {
                let upto = acc.len() - keep;
                emit(&mut out, &acc[..upto]).await;
                acc.drain(..upto);
            }
        }
    }
}

/// Best-effort emit of a stdout frame. A client-write failure drops the
/// client (`*out = None`) but does not abort the caller, so converse keeps
/// draining the shell to its sentinel — a mid-command disconnect must not
/// leave the persistent shell out of sync for the next exec.
async fn emit<W: AsyncWrite + Unpin>(out: &mut Option<&mut W>, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let failed = match out.as_deref_mut() {
        Some(w) => proto::write_frame(
            w,
            &Response::Stdout {
                data: data.to_vec(),
            },
        )
        .await
        .is_err(),
        None => false,
    };
    if failed {
        *out = None;
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// POSIX single-quote quoting: wrap in '…', closing/reopening around any
/// embedded single quote.
fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}
