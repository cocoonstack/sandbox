//! Process table: every exec is registered so `ps`/`kill`/`attach`/`logs` work across connections.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::io::AsyncWrite;
use tokio::sync::broadcast;

use crate::proto::{self, ErrorKind, ProcInfo, Response};
use crate::sysutil;

const LOG_RING_BYTES: usize = 256 * 1024;
const OUTPUT_FANOUT: usize = 256;

/// Fallback pids for a child with no OS pid, masked below i32::MAX so a pid_t cast stays valid.
static SYNTH: AtomicU64 = AtomicU64::new(1 << 30);

/// A chunk of process output tagged by stream, shared with live attachers.
#[derive(Clone)]
pub enum Chunk {
    Stdout(Arc<[u8]>),
    Stderr(Arc<[u8]>),
    Exit(i32),
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
        let proc = Arc::new(Proc {
            pid,
            argv,
            detached,
            started_at_epoch_secs: now,
            tx: OnceLock::new(),
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

    pub(crate) fn get(&self, pid: u32) -> Option<Arc<Proc>> {
        sysutil::lock(&self.inner).get(&pid).cloned()
    }

    /// Looks up a pid, writing a NotFound frame and returning None on a miss.
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

    /// Removes the entry only if it is still `proc`, because a recycled pid could name a newer process.
    pub fn remove_if(&self, pid: u32, proc: &Arc<Proc>) {
        let mut map = sysutil::lock(&self.inner);
        if map.get(&pid).is_some_and(|cur| Arc::ptr_eq(cur, proc)) {
            map.remove(&pid);
        }
    }

    pub(crate) fn len(&self) -> usize {
        sysutil::lock(&self.inner).len()
    }
}

/// One tracked process; `pty_master` is a dup, so `resize` cannot race the I/O task's fd.
pub struct Proc {
    pub pid: u32,
    pub argv: Vec<String>,
    pub detached: bool,
    pub started_at_epoch_secs: u64,
    tx: OnceLock<broadcast::Sender<Chunk>>,
    ring: Mutex<Ring>,
    state: Mutex<State>,
    pty_master: Mutex<Option<OwnedFd>>,
}

impl Proc {
    /// Records output and fans it out; the ring lock spans the send, so a racing `attach` cannot double a chunk.
    pub fn emit(&self, chunk: &Chunk) {
        let mut ring = sysutil::lock(&self.ring);
        if let Chunk::Stdout(d) | Chunk::Stderr(d) = chunk {
            ring.push(matches!(chunk, Chunk::Stderr(_)), d);
        }
        if let Some(tx) = self.attached() {
            let _ = tx.send(chunk.clone());
        }
    }

    /// `emit` for borrowed bytes; the owned Chunk is built only when an attacher listens.
    pub fn emit_bytes(&self, stderr: bool, data: &[u8]) {
        let mut ring = sysutil::lock(&self.ring);
        ring.push(stderr, data);
        if let Some(tx) = self.attached() {
            let chunk = if stderr {
                Chunk::Stderr(data.into())
            } else {
                Chunk::Stdout(data.into())
            };
            let _ = tx.send(chunk);
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
            Some(fd) => sysutil::set_winsize(fd.as_raw_fd(), cols, rows),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "not a pty",
            )),
        }
    }

    /// The exit code if the process has already exited; None while running.
    pub fn exit_code(&self) -> Option<i32> {
        match *sysutil::lock(&self.state) {
            State::Running => None,
            State::Exited(code) => Some(code),
        }
    }

    /// Snapshot of retained output and the exit state under one lock, for `logs`.
    pub fn replay(&self) -> (Vec<Chunk>, Option<i32>) {
        let mut ring = sysutil::lock(&self.ring);
        (ring.snapshot(), self.exit_code())
    }

    /// Snapshots retained output and the exit state and subscribes to live output under one lock.
    pub fn attach_stream(&self) -> (Vec<Chunk>, broadcast::Receiver<Chunk>, Option<i32>) {
        let mut ring = sysutil::lock(&self.ring);
        let tx = self.tx.get_or_init(|| broadcast::channel(OUTPUT_FANOUT).0);
        (ring.snapshot(), tx.subscribe(), self.exit_code())
    }

    fn attached(&self) -> Option<&broadcast::Sender<Chunk>> {
        self.tx.get().filter(|tx| tx.receiver_count() > 0)
    }

    fn info(&self) -> ProcInfo {
        let (state, exit_code) = match *sysutil::lock(&self.state) {
            State::Running => (Cow::Borrowed("running"), None),
            State::Exited(c) => (Cow::Borrowed("exited"), Some(c)),
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

#[derive(Clone, Copy)]
enum State {
    Running,
    Exited(i32),
}

struct Ring {
    buf: VecDeque<u8>,
    tags: VecDeque<(bool, usize)>, // (is_stderr, len) preserving stream boundaries
}

impl Ring {
    fn push(&mut self, stderr: bool, data: &[u8]) {
        self.buf.extend(data);
        match self.tags.back_mut() {
            Some((run, len)) if *run == stderr => *len += data.len(),
            _ => self.tags.push_back((stderr, data.len())),
        }
        self.trim();
    }

    /// Drops the oldest bytes past the cap; the oldest run shrinks or goes entirely.
    fn trim(&mut self) {
        let mut over = self.buf.len().saturating_sub(LOG_RING_BYTES);
        self.buf.drain(..over);
        while over > 0 {
            let Some((stderr, len)) = self.tags.pop_front() else {
                break;
            };
            if len > over {
                self.tags.push_front((stderr, len - over));
                break;
            }
            over -= len;
        }
    }

    /// Retained output, one chunk per stream run.
    fn snapshot(&mut self) -> Vec<Chunk> {
        let bytes = self.buf.make_contiguous();
        let mut off = 0;
        self.tags
            .iter()
            .map(|&(stderr, len)| {
                let seg: Arc<[u8]> = bytes[off..off + len].into();
                off += len;
                if stderr {
                    Chunk::Stderr(seg)
                } else {
                    Chunk::Stdout(seg)
                }
            })
            .collect()
    }
}

/// Stdout/Stderr ride the reused-buffer bulk path; serde's per-chunk allocations dominate replay otherwise.
pub async fn write_chunk<W: AsyncWrite + Unpin>(
    w: &mut W,
    buf: &mut Vec<u8>,
    chunk: Chunk,
) -> std::io::Result<()> {
    match chunk {
        Chunk::Stdout(data) => proto::write_chunk_frame(w, buf, "stdout", &data).await,
        Chunk::Stderr(data) => proto::write_chunk_frame(w, buf, "stderr", &data).await,
        Chunk::Exit(code) => proto::write_frame(w, &Response::Exit { code }).await,
    }
}

pub fn synth_pid() -> u32 {
    (SYNTH.fetch_add(1, Ordering::Relaxed) & (i32::MAX as u64)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fan_out_is_built_by_the_first_attacher() {
        let table = Table::new();
        let proc = table.register(7, vec!["true".to_string()], true, 0);
        proc.emit_bytes(false, b"before");
        assert!(
            proc.tx.get().is_none(),
            "an unattached exec owns no fan-out ring"
        );

        let (replay, mut rx, _) = proc.attach_stream();
        assert_eq!(replay.len(), 1);
        assert!(proc.tx.get().is_some());
        proc.emit_bytes(true, b"after");
        assert!(matches!(rx.try_recv(), Ok(Chunk::Stderr(d)) if &*d == b"after"));

        let (_, mut second, _) = proc.attach_stream();
        proc.emit(&Chunk::Exit(3));
        assert!(matches!(rx.try_recv(), Ok(Chunk::Exit(3))));
        assert!(matches!(second.try_recv(), Ok(Chunk::Exit(3))));
    }

    #[test]
    fn attach_after_exit_replays_the_tail_with_the_code() {
        let table = Table::new();
        let proc = table.register(8, vec!["true".to_string()], true, 0);
        proc.emit_bytes(false, b"tail");
        proc.mark_exited(5);
        proc.emit(&Chunk::Exit(5));

        let (replay, _rx, exit) = proc.attach_stream();
        assert_eq!(exit, Some(5));
        assert!(matches!(replay.as_slice(), [Chunk::Stdout(d)] if &**d == b"tail"));
        let (logs, exit) = proc.replay();
        assert_eq!(exit, Some(5));
        assert!(matches!(logs.as_slice(), [Chunk::Stdout(d)] if &**d == b"tail"));
    }

    #[test]
    fn attach_before_exit_receives_the_tail_then_the_exit() {
        let table = Table::new();
        let proc = table.register(9, vec!["true".to_string()], true, 0);
        let (replay, mut rx, exit) = proc.attach_stream();
        assert!(replay.is_empty());
        assert_eq!(exit, None);
        assert_eq!(proc.replay().1, None);

        proc.emit_bytes(false, b"tail");
        proc.mark_exited(5);
        proc.emit(&Chunk::Exit(5));
        assert!(matches!(rx.try_recv(), Ok(Chunk::Stdout(d)) if &*d == b"tail"));
        assert!(matches!(rx.try_recv(), Ok(Chunk::Exit(5))));
    }

    #[test]
    fn ring_keeps_one_tag_per_stream_run() {
        let mut ring = Ring {
            buf: VecDeque::new(),
            tags: VecDeque::new(),
        };
        for i in 0..LOG_RING_BYTES + 1000 {
            ring.push(false, &[(i % 251) as u8]);
        }
        assert_eq!(ring.buf.len(), LOG_RING_BYTES);
        assert_eq!(
            ring.tags.len(),
            1,
            "one stdout run is one tag, whatever the write size"
        );
        assert_eq!(ring.tags.iter().map(|t| t.1).sum::<usize>(), ring.buf.len());
        ring.push(true, b"err");
        ring.push(false, b"out");
        assert_eq!(ring.tags.len(), 3);
        assert_eq!(ring.snapshot().len(), 3);
    }

    #[test]
    fn ring_overflow_keeps_the_exact_newest_bytes() {
        let mut ring = Ring {
            buf: VecDeque::new(),
            tags: VecDeque::new(),
        };
        ring.push(false, &vec![b'a'; 200 * 1024]);
        ring.push(false, &vec![b'b'; 100 * 1024]);
        assert_eq!(
            ring.buf.len(),
            LOG_RING_BYTES,
            "a big oldest segment is thinned, not dropped"
        );
        assert_eq!(ring.tags.iter().map(|t| t.1).sum::<usize>(), ring.buf.len());
        let snap = ring.snapshot();
        assert_eq!(snap.len(), 1);
        let Chunk::Stdout(kept) = &snap[0] else {
            panic!("stdout run")
        };
        assert_eq!(kept.len(), LOG_RING_BYTES);
        assert_eq!(
            &kept[kept.len() - 100 * 1024..],
            &vec![b'b'; 100 * 1024][..]
        );
    }
}
