//! Search verbs: a pattern travels as data, never shell-quoted through exec.

use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use regex::{Captures, Regex, Replacer};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite};
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, SemaphorePermit, mpsc};

use crate::proto::{self, ErrorKind, Response, err_frame};

/// Size above which find skips a file unread and replace reports zero replacements.
pub const FIND_MAX_FILE: u64 = 8 * 1024 * 1024;

/// Match frames in flight between the walking thread and the writer.
const MATCH_QUEUE: usize = 256;

/// Bytes of match content in flight: above one size-bound match, so a reserve never blocks forever, far below 256 of them.
const MATCH_QUEUE_BYTES: usize = 2 * FIND_MAX_FILE as usize;

const ANCHORS: [&str; 4] = ["^", "$", "\\A", "\\z"];

struct MatchBudget {
    bytes: Semaphore,
    rt: Handle,
}

impl MatchBudget {
    fn new(cap: usize, rt: Handle) -> Self {
        Self {
            bytes: Semaphore::new(cap),
            rt,
        }
    }

    /// Blocks until n bytes fit or the budget closes.
    fn reserve(&self, n: usize) -> bool {
        let n = n as u32;
        if let Ok(permit) = self.bytes.try_acquire_many(n) {
            permit.forget();
            return true;
        }
        self.rt
            .block_on(self.bytes.acquire_many(n))
            .map(SemaphorePermit::forget)
            .is_ok()
    }

    fn release(&self, n: usize) {
        self.bytes.add_permits(n);
    }

    fn close(&self) {
        self.bytes.close();
    }
}

struct Walk<'a> {
    re: &'a Regex,
    name_re: Option<&'a Regex>,
    tx: &'a mpsc::Sender<Response>,
    budget: &'a MatchBudget,
    prefilter: bool,
}

impl Walk<'_> {
    /// Walks `root` depth-first; a failure below the root skips only that directory.
    fn run(&self, root: PathBuf) -> std::io::Result<bool> {
        let mut stack = vec![root];
        let mut at_root = true;
        let mut body = String::new();
        while let Some(dir) = stack.pop() {
            let rd = match std::fs::read_dir(&dir) {
                Ok(rd) => rd,
                Err(e) if at_root => return Err(e),
                Err(_) => continue,
            };
            for ent in rd {
                if self.tx.is_closed() {
                    return Ok(false);
                }
                let ent = match ent {
                    Ok(ent) => ent,
                    Err(e) if at_root => return Err(e),
                    Err(_) => break,
                };
                let Ok(ft) = ent.file_type() else {
                    continue;
                };
                let p = ent.path();
                if ft.is_dir() {
                    stack.push(p);
                } else if ft.is_file()
                    && name_matches(&p, self.name_re)
                    && !self.scan_file(&p, &mut body)
                {
                    return Ok(false);
                }
            }
            at_root = false;
        }
        Ok(true)
    }

    /// Scans one file; an unreadable or oversized file is skipped, not an error.
    fn scan_file(&self, path: &Path, body: &mut String) -> bool {
        if !matches!(read_bounded(path, body), Ok(true))
            || (self.prefilter && !self.re.is_match(body))
        {
            return true;
        }
        let mut name: Option<Arc<str>> = None;
        for (i, line) in body.lines().enumerate() {
            if self.re.is_match(line) {
                let frame = Response::Match {
                    file: name
                        .get_or_insert_with(|| path.to_string_lossy().into())
                        .clone(),
                    line: i as u64 + 1,
                    content: line.to_string(),
                };
                if !self.budget.reserve(match_cost(&frame)) || self.tx.blocking_send(frame).is_err()
                {
                    return false;
                }
            }
        }
        true
    }
}

/// Counts replacements in the same pass that expands `$n` groups, at the price of the literal fast path.
struct Counting<'a> {
    replacement: &'a str,
    count: u64,
}

impl Replacer for Counting<'_> {
    fn replace_append(&mut self, caps: &Captures<'_>, dst: &mut String) {
        self.count += 1;
        caps.expand(self.replacement, dst);
    }
}

/// Streams `match` frames for every line under `path` matching `pattern`, terminated by `done`.
pub async fn find<R, W>(
    reader: &mut R,
    w: &mut W,
    path: String,
    pattern: String,
    glob: Option<&str>,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    find_bounded(reader, w, path, pattern, glob, MATCH_QUEUE_BYTES).await
}

/// Rewrites `pattern` to `replacement` in each of `files`, one `replaced` frame each; per-file atomic, not per-list.
pub async fn replace<W: AsyncWrite + Unpin>(
    w: &mut W,
    files: Vec<String>,
    pattern: String,
    replacement: String,
) -> std::io::Result<()> {
    let re = match compile(pattern).await {
        Ok(re) => re,
        Err(e) => return proto::error_frame(w, ErrorKind::BadRequest, e).await,
    };
    let replacement: Arc<str> = replacement.into();
    for file in files {
        let (re, replacement, path) = (re.clone(), Arc::clone(&replacement), PathBuf::from(&file));
        let replaced = tokio::task::spawn_blocking(move || {
            let mut body = String::new();
            if !read_bounded(&path, &mut body)? {
                return Ok((0, None));
            }
            let mut counting = Counting {
                replacement: &replacement,
                count: 0,
            };
            let new = re.replace_all(&body, counting.by_ref());
            Ok::<_, std::io::Error>((
                counting.count,
                (counting.count > 0).then(|| new.into_owned()),
            ))
        })
        .await;
        let (count, new) = match replaced {
            Ok(Ok(replaced)) => replaced,
            Ok(Err(e)) => return err_frame(w, &e, "read").await,
            Err(e) => return err_frame(w, &std::io::Error::other(e), "read").await,
        };
        if let Some(new) = new
            && let Err(e) = crate::fs::write_atomic(Path::new(&file), new.as_bytes()).await
        {
            return err_frame(w, &e, "write").await;
        }
        proto::write_frame(
            w,
            &Response::Replaced {
                file,
                replacements: count,
            },
        )
        .await?;
    }
    proto::write_frame(w, &Response::Done).await
}

async fn compile(pattern: String) -> Result<Regex, String> {
    match tokio::task::spawn_blocking(move || Regex::new(&pattern)).await {
        Ok(compiled) => compiled.map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    }
}

async fn find_bounded<R, W>(
    reader: &mut R,
    w: &mut W,
    path: String,
    pattern: String,
    glob: Option<&str>,
    budget_bytes: usize,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let prefilter = !ANCHORS.iter().any(|a| pattern.contains(a));
    let re = match compile(pattern).await {
        Ok(re) => re,
        Err(e) => return proto::error_frame(w, ErrorKind::BadRequest, e).await,
    };
    let name_re = match glob.filter(|g| !g.is_empty()).map(glob_regex) {
        None => None,
        Some(Ok(re)) => Some(re),
        Some(Err(e)) => return proto::error_frame(w, ErrorKind::BadRequest, e.to_string()).await,
    };
    // one blocking-pool dispatch for the whole tree, not three per file.
    let (tx, mut rx) = mpsc::channel::<Response>(MATCH_QUEUE);
    let budget = Arc::new(MatchBudget::new(budget_bytes, Handle::current()));
    let walker_budget = Arc::clone(&budget);
    let walk = tokio::task::spawn_blocking(move || {
        Walk {
            re: &re,
            name_re: name_re.as_ref(),
            tx: &tx,
            budget: &walker_budget,
            prefilter,
        }
        .run(PathBuf::from(path))
    });
    let mut batch = Vec::new();
    let mut buf = Vec::new();
    let mut failed = None;
    let mut gone = false;
    let mut pipelined = false;
    loop {
        tokio::select! {
            biased;
            // a hung-up client ends the walk; a request pipelined behind the find waits for done
            r = reader.fill_buf(), if !pipelined => {
                if matches!(r, Ok(buf) if !buf.is_empty()) {
                    pipelined = true;
                } else {
                    gone = true;
                    break;
                }
            }
            n = rx.recv_many(&mut batch, MATCH_QUEUE) => {
                if n == 0 {
                    break;
                }
                let sent = proto::write_frames(w, &mut buf, &batch).await;
                budget.release(batch.iter().map(match_cost).sum());
                batch.clear();
                if let Err(e) = sent {
                    failed = Some(e);
                    break;
                }
            }
        }
    }
    drop(rx);
    budget.close();
    let walked = walk.await.map_err(std::io::Error::other)?;
    if gone {
        return Ok(());
    }
    if let Some(e) = failed {
        if e.kind() == std::io::ErrorKind::InvalidData {
            return err_frame(w, &e, "find").await;
        }
        return Err(e);
    }
    match walked {
        Ok(_) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "read_dir").await,
    }
}

/// Reads a file into `body` through one handle, stopping at FIND_MAX_FILE; `Ok(false)` when the file is above the bound.
fn read_bounded(path: &Path, body: &mut String) -> std::io::Result<bool> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)?;
    let len = file.metadata()?.len();
    if len > FIND_MAX_FILE {
        return Ok(false);
    }
    body.clear();
    body.reserve(len as usize);
    file.take(FIND_MAX_FILE).read_to_string(body)?;
    Ok(true)
}

fn match_cost(frame: &Response) -> usize {
    let Response::Match { file, content, .. } = frame else {
        return 0;
    };
    file.len() + content.len()
}

fn name_matches(path: &Path, name_re: Option<&Regex>) -> bool {
    let Some(re) = name_re else { return true };
    path.file_name()
        .is_some_and(|n| re.is_match(&n.to_string_lossy()))
}

/// Compiles a `*`/`?` glob into an anchored regex over the whole file name.
fn glob_regex(glob: &str) -> Result<Regex, regex::Error> {
    let pat = regex::escape(glob).replace(r"\*", ".*").replace(r"\?", ".");
    Regex::new(&format!("^{pat}$"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, BufReader, DuplexStream};
    use tokio::task::JoinHandle;

    use super::*;

    async fn tree(files: usize, lines: usize) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for f in 0..files {
            let body: String = (0..lines).map(|l| format!("needle {f}-{l}\n")).collect();
            tokio::fs::write(dir.path().join(format!("f{f}.txt")), body)
                .await
                .expect("write");
        }
        dir
    }

    fn sink() -> (DuplexStream, JoinHandle<String>) {
        let (w, mut r) = tokio::io::duplex(1 << 16);
        let reader = tokio::spawn(async move {
            let mut out = String::new();
            r.read_to_string(&mut out).await.expect("read");
            out
        });
        (w, reader)
    }

    fn silent_client() -> (DuplexStream, BufReader<DuplexStream>) {
        let (peer, r) = tokio::io::duplex(64);
        (peer, BufReader::new(r))
    }

    async fn run(w: &mut DuplexStream, dir: &Path, budget: usize) -> std::io::Result<()> {
        let (_peer, mut reader) = silent_client();
        find_bounded(
            &mut reader,
            w,
            dir.display().to_string(),
            "needle".into(),
            None,
            budget,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_delivers_every_match_under_a_byte_budget() {
        let dir = tree(40, 25).await;
        let (mut w, reader) = sink();
        run(&mut w, dir.path(), 512).await.expect("find");
        drop(w);
        let out = reader.await.expect("join");
        assert_eq!(out.matches("\"type\":\"match\"").count(), 1000);
        assert!(out.trim_end().ends_with("{\"type\":\"done\"}"), "{out}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_ends_the_walk_when_the_writer_is_gone() {
        let dir = tree(40, 25).await;
        let (mut w, r) = tokio::io::duplex(64);
        drop(r);
        let found = tokio::time::timeout(Duration::from_secs(10), run(&mut w, dir.path(), 512))
            .await
            .expect("walk must end once the writer is gone");
        assert!(found.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_stops_without_done_when_the_client_disconnects() {
        let dir = tree(2000, 1).await;
        let (mut w, reader) = sink();
        let (peer, mut client) = silent_client();
        drop(peer);
        let found = tokio::time::timeout(
            Duration::from_secs(10),
            find_bounded(
                &mut client,
                &mut w,
                dir.path().display().to_string(),
                "needle".into(),
                None,
                MATCH_QUEUE_BYTES,
            ),
        )
        .await
        .expect("walk must end once the client is gone");
        found.expect("find");
        drop(w);
        let out = reader.await.expect("join");
        assert!(!out.contains("\"type\":\"done\""), "{out}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn walk_stops_at_the_next_entry_once_the_receiver_is_gone() {
        let dir = tree(50, 1).await;
        let re = Regex::new("needle").expect("regex");
        let (tx, rx) = mpsc::channel::<Response>(MATCH_QUEUE);
        drop(rx);
        let budget = MatchBudget::new(MATCH_QUEUE_BYTES, Handle::current());
        let walk = Walk {
            re: &re,
            name_re: None,
            tx: &tx,
            budget: &budget,
            prefilter: true,
        };
        let completed =
            tokio::task::block_in_place(|| walk.run(dir.path().to_path_buf())).expect("walk");
        assert!(!completed);
    }

    #[test]
    fn read_bounded_does_not_wait_on_a_fifo_with_an_idle_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fifo = dir.path().join("fifo");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRWXU).expect("mkfifo");
        let _writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fifo)
            .expect("open writer");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(read_bounded(&fifo, &mut String::new()).is_err());
        });
        let failed = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("read_bounded must not wait for data");
        assert!(failed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_anchored_pattern_matches_a_line_start_inside_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        tokio::fs::write(dir.path().join("a.txt"), "bar\nfoo here\nbaz foo\n")
            .await
            .expect("write");
        let (mut w, reader) = sink();
        let (_peer, mut client) = silent_client();
        find_bounded(
            &mut client,
            &mut w,
            dir.path().display().to_string(),
            "^foo".into(),
            None,
            MATCH_QUEUE_BYTES,
        )
        .await
        .expect("find");
        drop(w);
        let out = reader.await.expect("join");
        assert_eq!(out.matches("\"type\":\"match\"").count(), 1, "{out}");
        assert!(out.contains("\"line\":2"), "{out}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn find_reports_an_oversized_match_as_an_error_frame() {
        let dir = tempfile::tempdir().expect("tempdir");
        let line = "\u{1}".repeat(proto::MAX_FRAME / 4);
        tokio::fs::write(dir.path().join("blob.txt"), format!("needle {line}\n"))
            .await
            .expect("write");
        let (mut w, reader) = sink();
        run(&mut w, dir.path(), MATCH_QUEUE_BYTES)
            .await
            .expect("find");
        drop(w);
        let out = reader.await.expect("join");
        assert_eq!(out.matches("\"type\":\"match\"").count(), 0);
        assert!(
            out.trim_end()
                .rsplit('\n')
                .next()
                .is_some_and(|last| last.contains("\"type\":\"error\"")),
            "{out}"
        );
    }
}
