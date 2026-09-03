//! Search verbs: `fs.find` walks a tree and streams regex matches; `fs.replace`
//! rewrites matches in named files. Regex handling lives here so agents pass a
//! pattern as data rather than shell-quoting it through exec.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use regex::Regex;
use tokio::fs;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite};
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, SemaphorePermit, mpsc};

use crate::proto::{self, ErrorKind, Response, err_frame};

/// The number of bytes a file may have before find skips it as binary/huge —
/// grep-scale line scanning is for source trees, not blobs.
const FIND_MAX_FILE: u64 = 8 * 1024 * 1024;

/// Match frames in flight between the walking thread and the writer.
const MATCH_QUEUE: usize = 256;

/// Bytes of match content in flight; 256 size-bound single-line frames would pin gigabytes.
const MATCH_QUEUE_BYTES: usize = 2 * FIND_MAX_FILE as usize;

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
}

impl Walk<'_> {
    /// Walks `root` depth-first, sending a `match` frame per matching line. A
    /// failure on the root propagates; deeper ones skip only that directory.
    fn run(&self, root: PathBuf) -> std::io::Result<()> {
        let mut stack = vec![root];
        let mut root = true;
        while let Some(dir) = stack.pop() {
            let rd = match std::fs::read_dir(&dir) {
                Ok(rd) => rd,
                Err(e) if root => return Err(e),
                Err(_) => continue,
            };
            for ent in rd {
                let ent = match ent {
                    Ok(ent) => ent,
                    Err(e) if root => return Err(e),
                    Err(_) => break,
                };
                let Ok(ft) = ent.file_type() else {
                    continue;
                };
                let p = ent.path();
                if ft.is_dir() {
                    stack.push(p);
                } else if ft.is_file() && name_matches(&p, self.name_re) && !self.scan_file(&p) {
                    return Ok(());
                }
            }
            root = false;
        }
        Ok(())
    }

    /// Scans one file, reporting whether the receiver is still listening. The size
    /// bound comes off the open handle, so the check and the read see one file.
    fn scan_file(&self, path: &Path) -> bool {
        let Ok(mut file) = std::fs::File::open(path) else {
            return true;
        };
        if file.metadata().is_ok_and(|m| m.len() > FIND_MAX_FILE) {
            return true;
        }
        let mut body = String::new();
        if file.read_to_string(&mut body).is_err() {
            return true;
        }
        let name: Arc<str> = path.to_string_lossy().into();
        for (i, line) in body.lines().enumerate() {
            if self.re.is_match(line) {
                let frame = Response::Match {
                    file: name.clone(),
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

/// Streams `match` frames for every line under `path` matching `pattern`,
/// terminated by `done`. `glob` narrows the walk to file names matching it
/// (`*` and `?` wildcards); an invalid pattern is a bad-request error.
pub async fn find<R, W>(
    reader: &mut R,
    w: &mut W,
    path: String,
    pattern: String,
    glob: Option<String>,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    find_bounded(reader, w, path, pattern, glob, MATCH_QUEUE_BYTES).await
}

/// Rewrites every `pattern` match to `replacement` in each of `files`,
/// streaming one `replaced` frame per file (with its match count) and a
/// terminal `done`. A file over the find size bound is skipped with a zero
/// count rather than read into memory. A read/write failure on one file ends
/// the stream with an error — files whose `replaced` frame already went out
/// are committed (each file is atomic; the list is not). An invalid pattern is
/// rejected before any file is touched.
pub async fn replace<W: AsyncWrite + Unpin>(
    w: &mut W,
    files: Vec<String>,
    pattern: String,
    replacement: String,
) -> std::io::Result<()> {
    let re = match Regex::new(&pattern) {
        Ok(re) => re,
        Err(e) => return proto::error_frame(w, ErrorKind::BadRequest, e.to_string()).await,
    };
    for file in files {
        let mut count: u64 = 0;
        if !oversized(&file).await {
            let body = match fs::read_to_string(&file).await {
                Ok(body) => body,
                Err(e) => return err_frame(w, &e, "read").await,
            };
            // expand keeps the &str replacer's $n capture-group semantics.
            let new = re.replace_all(&body, |caps: &regex::Captures| {
                count += 1;
                let mut expanded = String::new();
                caps.expand(&replacement, &mut expanded);
                expanded
            });
            if count > 0
                && let Err(e) = crate::fs::write_atomic(Path::new(&file), new.as_bytes()).await
            {
                return err_frame(w, &e, "write").await;
            }
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

async fn find_bounded<R, W>(
    reader: &mut R,
    w: &mut W,
    path: String,
    pattern: String,
    glob: Option<String>,
    budget_bytes: usize,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let re = match Regex::new(&pattern) {
        Ok(re) => re,
        Err(e) => return proto::error_frame(w, ErrorKind::BadRequest, e.to_string()).await,
    };
    let name_re = match glob.as_deref().filter(|g| !g.is_empty()).map(glob_regex) {
        None => None,
        Some(Ok(re)) => Some(re),
        Some(Err(e)) => return proto::error_frame(w, ErrorKind::BadRequest, e.to_string()).await,
    };
    // One blocking-pool dispatch for the whole tree, not three per file.
    let (tx, mut rx) = mpsc::channel::<Response>(MATCH_QUEUE);
    let budget = Arc::new(MatchBudget::new(budget_bytes, Handle::current()));
    let walker_budget = budget.clone();
    let walk = tokio::task::spawn_blocking(move || {
        Walk {
            re: &re,
            name_re: name_re.as_ref(),
            tx: &tx,
            budget: &walker_budget,
        }
        .run(PathBuf::from(path))
    });
    let mut batch = Vec::new();
    let mut buf = Vec::new();
    let mut failed = None;
    let mut gone = false;
    loop {
        tokio::select! {
            biased;
            // The client sends nothing during a find, so any readable state ends the walk.
            _ = reader.fill_buf() => {
                gone = true;
                break;
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
        Ok(()) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "read_dir").await,
    }
}

async fn oversized(file: &str) -> bool {
    fs::metadata(file)
        .await
        .is_ok_and(|m| m.len() > FIND_MAX_FILE)
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

/// Compiles a `*`/`?` glob into an anchored regex over the whole file name;
/// every other character matches literally. escape() turns the wildcards
/// into exactly `\*`/`\?`, which the replaces then rewrite.
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
