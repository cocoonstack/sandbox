//! Search verbs: `fs.find` walks a tree and streams regex matches; `fs.replace`
//! rewrites matches in named files. Regex handling lives here so agents pass a
//! pattern as data rather than shell-quoting it through exec.

use std::path::Path;

use regex::Regex;
use tokio::fs;
use tokio::io::AsyncWrite;

use crate::proto::{self, ErrorKind, Response, err_frame};

/// The number of bytes a file may have before find skips it as binary/huge —
/// grep-scale line scanning is for source trees, not blobs.
const FIND_MAX_FILE: u64 = 8 * 1024 * 1024;

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
    let mut stack = vec![path];
    while let Some(dir) = stack.pop() {
        let mut rd = match fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) => return err_frame(w, &e, "read_dir").await,
        };
        loop {
            let ent = match rd.next_entry().await {
                Ok(Some(ent)) => ent,
                Ok(None) => break,
                Err(e) => return err_frame(w, &e, "read_dir").await,
            };
            let Ok(ft) = ent.file_type().await else {
                continue;
            };
            let p = ent.path();
            if ft.is_dir() {
                stack.push(p.to_string_lossy().into_owned());
            } else if ft.is_file() && name_matches(&p, name_re.as_ref()) {
                scan_file(w, &re, &p).await?;
            }
        }
    }
    proto::write_frame(w, &Response::Done).await
}

/// Rewrites every `pattern` match to `replacement` in each of `files`,
/// streaming one `replaced` frame per file (with its match count) and a
/// terminal `done`. A read/write failure on one file ends the stream with an
/// error — files whose `replaced` frame already went out are committed (each
/// file is atomic; the list is not). An invalid pattern is rejected before any
/// file is touched.
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
        let body = match fs::read_to_string(&file).await {
            Ok(body) => body,
            Err(e) => return err_frame(w, &e, "read").await,
        };
        // Closure replacer: counts while replacing in one pass; expand keeps
        // the &str replacer's $n capture-group semantics.
        let mut count: u64 = 0;
        let new = re.replace_all(&body, |caps: &regex::Captures| {
            count += 1;
            let mut expanded = String::new();
            caps.expand(&replacement, &mut expanded);
            expanded
        });
        if count > 0 {
            if let Err(e) = crate::fs::write_atomic(Path::new(&file), new.as_bytes()).await {
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

async fn scan_file<W: AsyncWrite + Unpin>(
    w: &mut W,
    re: &Regex,
    path: &Path,
) -> std::io::Result<()> {
    if fs::metadata(path)
        .await
        .is_ok_and(|m| m.len() > FIND_MAX_FILE)
    {
        return Ok(());
    }
    let Ok(body) = fs::read_to_string(path).await else {
        return Ok(()); // non-UTF8 / unreadable: skip, not an error
    };
    let file = path.to_string_lossy().into_owned();
    for (i, line) in body.lines().enumerate() {
        if re.is_match(line) {
            proto::write_frame(
                w,
                &Response::Match {
                    file: file.clone(),
                    line: i as u64 + 1,
                    content: line.to_string(),
                },
            )
            .await?;
        }
    }
    Ok(())
}

fn name_matches(path: &Path, name_re: Option<&Regex>) -> bool {
    let Some(re) = name_re else { return true };
    path.file_name()
        .map(|n| re.is_match(&n.to_string_lossy()))
        .unwrap_or(false)
}

/// Compiles a `*`/`?` glob into an anchored regex over the whole file name;
/// every other character matches literally. escape() turns the wildcards
/// into exactly `\*`/`\?`, which the replaces then rewrite.
fn glob_regex(glob: &str) -> Result<Regex, regex::Error> {
    let pat = regex::escape(glob).replace(r"\*", ".*").replace(r"\?", ".");
    Regex::new(&format!("^{pat}$"))
}
