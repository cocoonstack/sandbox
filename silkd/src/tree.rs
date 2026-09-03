//! Whole-tree transfer through the guest tar binary: `fs.push` extracts a client
//! tar stream into a directory, `fs.pull` streams a path back as a tar. Push is
//! network-failure atomic: it stages inside `dest`, then merges by rename.

use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite};
use tokio::process::Command;

use crate::proto::{self, ErrorKind, Response, err_frame};
use crate::sysutil;

/// Extracts a client tar stream into `dest`, creating it; a stream failure leaves `dest` unchanged.
pub async fn push<R, W>(mut reader: R, w: &mut W, dest: String) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if let Err(e) = tokio::fs::create_dir_all(&dest).await {
        return err_frame(w, &e, "mkdir dest").await;
    }
    let staging = match stage_dir(&dest).await {
        Ok(s) => s,
        Err(e) => return err_frame(w, &e, "stage push").await,
    };
    let res = push_staged(&mut reader, w, &staging, dest).await;
    // a failed push can leave a large partial tree, so cleanup runs on every exit path.
    let _ = tokio::fs::remove_dir_all(&staging).await;
    res
}

/// Streams `path` back as a tar carrying the entry under its own basename.
pub async fn pull<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> io::Result<()> {
    let p = Path::new(&path);
    let (parent, name) = match (p.parent(), p.file_name()) {
        (Some(par), Some(n)) if !n.is_empty() => (par, n),
        _ => return proto::error_frame(w, ErrorKind::BadRequest, "invalid path").await,
    };
    // symlink_metadata: a dangling symlink is still a valid tar source.
    if let Err(e) = tokio::fs::symlink_metadata(p).await {
        return err_frame(w, &e, "stat source").await;
    }
    let cwd = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let mut cmd = Command::new("tar");
    // `--` so a basename starting with `-` is a path, not a tar option.
    cmd.arg("-c")
        .arg("-C")
        .arg(cwd)
        .arg("--")
        .arg(name)
        .stdout(Stdio::piped());
    let (mut child, err_task) = match spawn_tar(cmd) {
        Ok(pair) => pair,
        Err(e) => return err_frame(w, &e, "spawn tar").await,
    };
    let Some(mut out) = child.stdout.take() else {
        return err_frame(w, &io::Error::other("tar stdout not piped"), "spawn tar").await;
    };

    if let Err(e) = proto::stream_data_frames(&mut out, w).await? {
        let _ = child.wait().await;
        return err_frame(w, &e, "read tar").await;
    }
    let status = child.wait().await?;
    let msg = err_task.await.unwrap_or_default();
    tar_result(w, status, &msg, "tar create").await
}

/// The caller owns removing `staging` on every path.
async fn push_staged<R, W>(reader: &mut R, w: &mut W, staging: &str, dest: String) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut cmd = Command::new("tar");
    cmd.arg("-x")
        .arg("-C")
        .arg(staging)
        .stdin(Stdio::piped())
        .stdout(Stdio::null());
    let (mut child, err_task) = match spawn_tar(cmd) {
        Ok(pair) => pair,
        Err(e) => return err_frame(w, &e, "spawn tar").await,
    };
    let Some(mut sink) = child.stdin.take() else {
        return err_frame(w, &io::Error::other("tar stdin not piped"), "spawn tar").await;
    };

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
    // spawn_blocking: a big tree is thousands of local fs syscalls.
    let merged = tokio::task::spawn_blocking(move || merge_tree(Path::new(&src), Path::new(&dest)))
        .await
        .map_err(io::Error::other)
        .and_then(|r| r);
    match merged {
        Ok(()) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "merge push").await,
    }
}

async fn tar_result<W: AsyncWrite + Unpin>(
    w: &mut W,
    status: ExitStatus,
    msg: &str,
    label: &str,
) -> io::Result<()> {
    proto::subprocess_result(w, status.success(), label, msg.as_bytes()).await
}

/// Spawns tar draining stderr on its own task, so a full stderr pipe cannot deadlock.
fn spawn_tar(
    mut cmd: Command,
) -> io::Result<(tokio::process::Child, tokio::task::JoinHandle<String>)> {
    let mut child = cmd.stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("tar stderr not piped"))?;
    Ok((child, tokio::spawn(drain(stderr))))
}

/// Creates the staging dir inside `dest` so the merge is same-filesystem renames.
async fn stage_dir(dest: &str) -> io::Result<String> {
    let staging = format!("{dest}/.silkd-push-{}", sysutil::tmp_suffix());
    tokio::fs::create_dir(&staging).await?;
    Ok(staging)
}

/// Overlays `src` into `dst` with tar's semantics: dirs merge, a file over a dir fails.
fn merge_tree(src: &Path, dst: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let to_meta = std::fs::symlink_metadata(&to).ok();
        match (entry.file_type()?.is_dir(), to_meta) {
            (true, Some(m)) if m.is_dir() => merge_tree(&from, &to)?,
            // mirrors tar's unlink-before-extract.
            (true, Some(_)) => {
                std::fs::remove_file(&to)?;
                std::fs::rename(&from, &to)?;
            }
            _ => std::fs::rename(&from, &to)?,
        }
    }
    Ok(())
}

/// Reads a child's stderr, capped at CAP bytes; tar's first error is the useful one.
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
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}
