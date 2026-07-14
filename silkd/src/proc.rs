//! Process table: every exec is registered so `ps`/`kill`/`attach`/`logs`
//! work against a guest pid regardless of which connection started it.
//! Detached processes keep a bounded output ring so a later `logs`/`attach`
//! can replay what already streamed.

use std::collections::{HashMap, VecDeque};
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::AsyncWrite;
use tokio::sync::broadcast;

use crate::proto::{self, ErrorKind, ProcInfo, Response};
use crate::sysutil;

const LOG_RING_BYTES: usize = 256 * 1024;
const OUTPUT_FANOUT: usize = 256;

/// Monotonic fallback pids for a spawned child with no reported OS pid; kept
/// below i32::MAX so a later signal cast to pid_t stays a valid pid.
static SYNTH: AtomicU64 = AtomicU64::new(1 << 30);

/// A chunk of process output tagged by stream, shared with live attachers.
#[derive(Clone)]
pub enum Chunk {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
}

impl Chunk {
    /// The response frame that carries this chunk to a client.
    pub fn into_response(self) -> Response {
        match self {
            Chunk::Stdout(data) => Response::Stdout { data },
            Chunk::Stderr(data) => Response::Stderr { data },
            Chunk::Exit(code) => Response::Exit { code },
        }
    }
}

/// Registry of running and recently-exited processes.
#[derive(Clone, Default)]
pub struct Table {
    inner: Arc<Mutex<HashMap<u32, Arc<Proc>>>>,
}

impl Table {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, pid: u32, argv: Vec<String>, detached: bool, now: u64) -> Arc<Proc> {
        let (tx, _) = broadcast::channel(OUTPUT_FANOUT);
        let proc = Arc::new(Proc {
            pid,
            argv,
            detached,
            started_at_epoch_secs: now,
            tx,
            ring: Mutex::new(Ring {
                buf: VecDeque::new(),
                tags: VecDeque::new(),
            }),
            state: Mutex::new(State::Running),
            pty_master: Mutex::new(None),
        });
        sysutil::lock(&self.inner).insert(pid, Arc::clone(&proc));
        proc
    }

    pub fn get(&self, pid: u32) -> Option<Arc<Proc>> {
        sysutil::lock(&self.inner).get(&pid).cloned()
    }

    /// Looks up a pid, writing a NotFound frame and returning None on a miss —
    /// the shared prelude of kill/logs/attach/pty.resize.
    pub async fn get_or_not_found<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        pid: u32,
    ) -> std::io::Result<Option<Arc<Proc>>> {
        match self.get(pid) {
            Some(proc) => Ok(Some(proc)),
            None => {
                proto::error_frame(w, ErrorKind::NotFound, "no such pid").await?;
                Ok(None)
            }
        }
    }

    pub fn list(&self) -> Vec<ProcInfo> {
        sysutil::lock(&self.inner)
            .values()
            .map(|p| p.info())
            .collect()
    }

    /// Removes the entry only if it is still `proc`. OS pids are recycled, so
    /// a deferred cleanup that removed by pid alone could evict a newer
    /// process that reused the number.
    pub fn remove_if(&self, pid: u32, proc: &Arc<Proc>) {
        let mut map = sysutil::lock(&self.inner);
        if map.get(&pid).is_some_and(|cur| Arc::ptr_eq(cur, proc)) {
            map.remove(&pid);
        }
    }

    pub fn len(&self) -> usize {
        sysutil::lock(&self.inner).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One tracked process. `tx` fans out live output; `ring` retains a bounded
/// tail for replay; `state` flips to exited when the child is reaped.
/// `pty_master` holds a dup of a pty's master fd (None for a plain exec) so
/// `pty.resize` can ioctl it without racing the I/O task's own fd.
pub struct Proc {
    pub pid: u32,
    pub argv: Vec<String>,
    pub detached: bool,
    pub started_at_epoch_secs: u64,
    tx: broadcast::Sender<Chunk>,
    ring: Mutex<Ring>,
    state: Mutex<State>,
    pty_master: Mutex<Option<OwnedFd>>,
}

impl Proc {
    /// Records output for replay and fans it out to live attachers, holding
    /// the ring lock across the broadcast send so an `attach` racing this
    /// emit sees the chunk in exactly one of replay or the live stream.
    /// The broadcast clone is skipped when nobody listens (the common case
    /// on the output hot path); `attach_stream` subscribes under this same
    /// ring lock, so the receiver count cannot change mid-emit.
    pub fn emit(&self, chunk: &Chunk) {
        let mut ring = sysutil::lock(&self.ring);
        if let Chunk::Stdout(d) | Chunk::Stderr(d) = chunk {
            ring.push(matches!(chunk, Chunk::Stderr(_)), d);
        }
        if self.tx.receiver_count() > 0 {
            let _ = self.tx.send(chunk.clone());
        }
    }

    /// `emit` for produced bytes: the ring takes the borrowed slice, and the
    /// owned Chunk is built only when an attacher is actually listening — a
    /// pty or detached exec pays no allocation on its output path otherwise.
    pub fn emit_bytes(&self, stderr: bool, data: &[u8]) {
        let mut ring = sysutil::lock(&self.ring);
        ring.push(stderr, data);
        if self.tx.receiver_count() > 0 {
            let chunk = if stderr {
                Chunk::Stderr(data.to_vec())
            } else {
                Chunk::Stdout(data.to_vec())
            };
            let _ = self.tx.send(chunk);
        }
    }

    pub fn mark_exited(&self, code: i32) {
        *sysutil::lock(&self.state) = State::Exited(code);
    }

    /// Records a pty master fd (a dup, owned here) so `resize` can reach it.
    pub fn set_pty_master(&self, fd: OwnedFd) {
        *sysutil::lock(&self.pty_master) = Some(fd);
    }

    /// Resizes the pty's window; errors if this proc is a plain exec.
    pub fn resize(&self, cols: u16, rows: u16) -> std::io::Result<()> {
        match &*sysutil::lock(&self.pty_master) {
            Some(fd) => crate::sysutil::set_winsize(fd.as_raw_fd(), cols, rows),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "not a pty",
            )),
        }
    }

    /// The exit code if the process has already exited; None while running.
    /// Lets `logs`/`attach`/`kill` act on terminal state instead of waiting
    /// on (or signalling against) a pid whose child is already reaped.
    pub fn exit_code(&self) -> Option<i32> {
        match *sysutil::lock(&self.state) {
            State::Running => None,
            State::Exited(code) => Some(code),
        }
    }

    /// Snapshot of retained output for `logs`.
    pub fn replay(&self) -> Vec<Chunk> {
        sysutil::lock(&self.ring).drain_view()
    }

    /// Atomically snapshots retained output and subscribes to live output
    /// under one lock, so a chunk emitted concurrently lands in the replay or
    /// the receiver but never both (no duplicate in the client's stream).
    pub fn attach_stream(&self) -> (Vec<Chunk>, broadcast::Receiver<Chunk>) {
        let mut ring = sysutil::lock(&self.ring);
        (ring.drain_view(), self.tx.subscribe())
    }

    fn info(&self) -> ProcInfo {
        let (state, exit_code) = match *sysutil::lock(&self.state) {
            State::Running => ("running".to_string(), None),
            State::Exited(c) => ("exited".to_string(), Some(c)),
        };
        ProcInfo {
            pid: self.pid,
            argv: self.argv.clone(),
            detached: self.detached,
            state,
            exit_code,
            started_at_epoch_secs: self.started_at_epoch_secs,
        }
    }
}

struct Ring {
    buf: VecDeque<u8>,
    tags: VecDeque<(bool, usize)>, // (is_stderr, len) preserving stream boundaries
}

impl Ring {
    fn push(&mut self, stderr: bool, data: &[u8]) {
        self.buf.extend(data);
        self.tags.push_back((stderr, data.len()));
        self.trim();
    }

    /// Drops whole oldest segments until the buffer fits the cap. VecDeque
    /// front-drains are O(dropped), not O(remaining), so a chatty process
    /// does not pay a full-buffer memmove per chunk.
    fn trim(&mut self) {
        let mut over = self.buf.len().saturating_sub(LOG_RING_BYTES);
        while over > 0 {
            let Some((_, len)) = self.tags.pop_front() else {
                break;
            };
            self.buf.drain(..len);
            over = over.saturating_sub(len);
        }
    }

    fn drain_view(&mut self) -> Vec<Chunk> {
        let bytes = self.buf.make_contiguous();
        let mut out = Vec::with_capacity(self.tags.len());
        let mut off = 0;
        for &(stderr, len) in &self.tags {
            let slice = bytes[off..off + len].to_vec();
            out.push(if stderr {
                Chunk::Stderr(slice)
            } else {
                Chunk::Stdout(slice)
            });
            off += len;
        }
        out
    }
}

#[derive(Clone, Copy)]
enum State {
    Running,
    Exited(i32),
}

pub fn synth_pid() -> u32 {
    (SYNTH.fetch_add(1, Ordering::Relaxed) & (i32::MAX as u64)) as u32
}
