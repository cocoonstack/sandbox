//! Search verbs: `fs.find` walks a tree and streams regex matches; `fs.replace`
//! rewrites matches in named files. Regex handling lives here so agents pass a
//! pattern as data rather than shell-quoting it through exec.

use std::io::Read;
use std::path::{Path, PathBuf};

use regex::Regex;
use tokio::fs;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, Response, err_frame};

/// The number of bytes a file may have before find skips it as binary/huge —
/// grep-scale line scanning is for source trees, not blobs.
const FIND_MAX_FILE: u64 = 8 * 1024 * 1024;

/// Match frames in flight between the walking thread and the writer.
const MATCH_QUEUE: usize = 256;

/// Streams `match` frames for every line under `path` matching `pattern`,
/// terminated by `done`. `glob` narrows the walk to file names matching it
/// (`*` and `?` wildcards); an invalid pattern is a bad-request error.
pub async fn find<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    pattern: String,
    glob: Option<String>,
) -> std::io::Result<()> {
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
    let walk =
        tokio::task::spawn_blocking(move || walk(PathBuf::from(path), &re, name_re.as_ref(), &tx));
    let mut batch = Vec::new();
    let mut buf = Vec::new();
    let mut failed = None;
    while rx.recv_many(&mut batch, MATCH_QUEUE).await > 0 {
        let sent = proto::write_frames(w, &mut buf, &batch).await;
        batch.clear();
        if let Err(e) = sent {
            failed = Some(e);
            break;
        }
    }
    drop(rx);
    let walked = walk.await.map_err(std::io::Error::other)?;
    match failed {
        Some(e) => Err(e),
        None => match walked {
            Ok(()) => proto::write_frame(w, &Response::Done).await,
            Err(e) => err_frame(w, &e, "read_dir").await,
        },
    }
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

async fn oversized(file: &str) -> bool {
    fs::metadata(file)
        .await
        .is_ok_and(|m| m.len() > FIND_MAX_FILE)
}

/// Walks `root` depth-first, sending a `match` frame per matching line. A
/// failure on the root propagates; deeper ones skip only that directory.
fn walk(
    root: PathBuf,
    re: &Regex,
    name_re: Option<&Regex>,
    tx: &mpsc::Sender<Response>,
) -> std::io::Result<()> {
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
            } else if ft.is_file() && name_matches(&p, name_re) && !scan_file(&p, re, tx) {
                return Ok(());
            }
        }
        root = false;
    }
    Ok(())
}

/// Scans one file, reporting whether the receiver is still listening. The size
/// bound comes off the open handle, so the check and the read see one file.
fn scan_file(path: &Path, re: &Regex, tx: &mpsc::Sender<Response>) -> bool {
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
    let name = path.to_string_lossy();
    for (i, line) in body.lines().enumerate() {
        if re.is_match(line) {
            let frame = Response::Match {
                file: name.to_string(),
                line: i as u64 + 1,
                content: line.to_string(),
            };
            if tx.blocking_send(frame).is_err() {
                return false;
            }
        }
    }
    true
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
