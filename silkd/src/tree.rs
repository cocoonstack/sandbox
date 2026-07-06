//! Whole-tree transfer: `fs.push` extracts a client tar stream into a
//! directory, `fs.pull` streams a path back as a tar. The archive format is
//! the contract and tar(1) is its authoritative implementation, so these
//! stream through the guest tar binary rather than buffering a whole project
//! in memory — the only project-ingestion path on the no-network lane.
//!
//! Push is network-failure atomic: the stream extracts into a staging dir
//! inside `dest` (same filesystem) and merges into place only after the tar
//! terminates cleanly, so a dropped connection or truncated stream leaves
//! `dest` untouched. The merge itself is local renames — a residual crash
//! window of microseconds instead of the whole transfer.

use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite};
use tokio::process::Command;

use crate::proto::{self, err_frame, ErrorKind, Response};
use crate::sysutil;

/// Extracts a client tar stream (`data` frames until `data_end`) into `dest`,
/// creating it if needed. A non-zero tar exit, a stray frame, or a truncated
/// stream is reported as an error with `dest` unchanged.
pub async fn push<R, W>(mut reader: R, w: &mut W, dest: String) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if let Err(e) = tokio::fs::create_dir_all(&dest).await {
        return err_frame(w, &e, "mkdir dest").await;
    }
    let staging = match stage_dir(&dest) {
        Ok(s) => s,
        Err(e) => return err_frame(w, &e, "stage push").await,
    };
    let res = push_staged(&mut reader, w, &staging, dest).await;
    // One cleanup point for every exit path. A failed push can leave a large
    // partially-extracted tree; tokio walks it off the async runtime. After a
    // successful merge the renames have all but drained it.
    let _ = tokio::fs::remove_dir_all(&staging).await;
    res
}

/// Streams `path` back as a tar (`data` frames then `done`). The archive
/// carries the entry under its own name, so the client extracts it back to
/// the same basename.
pub async fn pull<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> io::Result<()> {
    let p = Path::new(&path);
    let (parent, name) = match (p.parent(), p.file_name()) {
        (Some(par), Some(n)) if !n.is_empty() => (par.to_path_buf(), n.to_os_string()),
        _ => return proto::error_frame(w, ErrorKind::BadRequest, "invalid path").await,
    };
    let cwd = if parent.as_os_str().is_empty() {
        Path::new(".").to_path_buf()
    } else {
        parent
    };
    let mut child = match Command::new("tar")
        .arg("-c")
        .arg("-C")
        .arg(&cwd)
        // `--` so a basename starting with `-` is a path, not a tar option.
        .arg("--")
        .arg(&name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return err_frame(w, &e, "spawn tar").await,
    };
    let mut out = child.stdout.take().expect("stdout piped");
    // Drain stderr concurrently so a noisy tar can't block on a full stderr
    // pipe while we are reading its stdout.
    let err_task = tokio::spawn(drain(child.stderr.take().expect("stderr piped")));

    if let Err(e) = proto::stream_data_frames(&mut out, w).await? {
        let _ = child.wait().await;
        return err_frame(w, &e, "read tar").await;
    }
    let status = child.wait().await?;
    let msg = err_task.await.unwrap_or_default();
    tar_result(w, status, &msg, "tar create").await
}

/// Runs the tar extraction into `staging` and merges into `dest`; the caller
/// owns removing `staging` on every path.
async fn push_staged<R, W>(reader: &mut R, w: &mut W, staging: &str, dest: String) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut child = match Command::new("tar")
        .arg("-x")
        .arg("-C")
        .arg(staging)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return err_frame(w, &e, "spawn tar").await,
    };
    let mut sink = child.stdin.take().expect("stdin piped");
    // Drain stderr concurrently: a noisy tar can fill the stderr pipe and
    // block while we are still writing its stdin, deadlocking both.
    let err_task = tokio::spawn(drain(child.stderr.take().expect("stderr piped")));

    let feed = proto::feed_data_frames(reader, &mut sink).await;
    drop(sink); // EOF to tar regardless of outcome
    let status = child.wait().await?;
    let msg = err_task.await.unwrap_or_default();

    if let Err(fail) = feed {
        return proto::write_feed_error(w, fail).await;
    }
    if !status.success() {
        return tar_result(w, status, &msg, "tar extract").await;
    }
    let src = staging.to_owned();
    // The merge walks and renames on the blocking pool: pure local fs
    // metadata work, but a big tree is thousands of syscalls.
    let merged = tokio::task::spawn_blocking(move || merge_tree(Path::new(&src), Path::new(&dest)))
        .await
        .map_err(io::Error::other)
        .and_then(|r| r);
    match merged {
        Ok(()) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "merge push").await,
    }
}

/// Terminal frame for a finished tar: `done` on success, else the captured
/// stderr under `label`.
async fn tar_result<W: AsyncWrite + Unpin>(
    w: &mut W,
    status: ExitStatus,
    msg: &str,
    label: &str,
) -> io::Result<()> {
    if status.success() {
        proto::write_frame(w, &Response::Done).await
    } else {
        proto::error_frame(w, ErrorKind::Internal, format!("{label}: {}", msg.trim())).await
    }
}

/// Creates a unique staging dir inside `dest` — the same filesystem, so the
/// post-extract merge is pure renames.
fn stage_dir(dest: &str) -> io::Result<String> {
    let staging = format!("{dest}/.silkd-push-{}", sysutil::tmp_suffix());
    std::fs::create_dir(&staging)?;
    Ok(staging)
}

/// Overlays `src` into `dst` with tar's semantics: directories merge
/// recursively, files and symlinks rename atomically over an existing file.
/// A subtree new to `dst` moves in one rename; a file landing on a directory
/// fails like tar does.
fn merge_tree(src: &Path, dst: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let to_meta = std::fs::symlink_metadata(&to).ok();
        match (entry.file_type()?.is_dir(), to_meta) {
            (true, Some(m)) if m.is_dir() => merge_tree(&from, &to)?,
            // A dir replacing a file/symlink drops it first, mirroring tar's
            // unlink-before-extract.
            (true, Some(_)) => {
                std::fs::remove_file(&to)?;
                std::fs::rename(&from, &to)?;
            }
            _ => std::fs::rename(&from, &to)?,
        }
    }
    Ok(())
}

/// Reads a child's stderr to a string, capped so a pathological tar can't
/// balloon memory; the tail is what a human error message needs.
async fn drain(mut stderr: tokio::process::ChildStderr) -> String {
    const CAP: usize = 16 * 1024;
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(n) = stderr.read(&mut buf).await {
        if n == 0 {
            break;
        }
        if out.len() < CAP {
            out.extend_from_slice(&buf[..(n.min(CAP - out.len()))]);
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
