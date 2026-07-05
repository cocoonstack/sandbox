//! Whole-tree transfer: `fs.push` extracts a client tar stream into a
//! directory, `fs.pull` streams a path back as a tar. The archive format is
//! the contract and tar(1) is its authoritative implementation, so these
//! stream through the guest tar binary rather than buffering a whole project
//! in memory — the only project-ingestion path on the no-network lane.

use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite};
use tokio::process::Command;

use crate::proto::{self, err_frame, ErrorKind, Response, READ_CHUNK};

/// Extracts a client tar stream (`data` frames until `data_end`) into `dest`,
/// creating it if needed. tar runs with the destination as cwd; a non-zero
/// tar exit or a stray frame is reported as an error.
pub async fn push<R, W>(mut reader: R, w: &mut W, dest: String) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if let Err(e) = tokio::fs::create_dir_all(&dest).await {
        return err_frame(w, &e, "mkdir dest").await;
    }
    let mut child = match Command::new("tar")
        .arg("-x")
        .arg("-C")
        .arg(&dest)
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

    let feed = proto::feed_data_frames(&mut reader, &mut sink).await;
    drop(sink); // EOF to tar regardless of outcome
    let status = child.wait().await?;
    let msg = err_task.await.unwrap_or_default();

    match feed {
        Err(fail) => proto::write_feed_error(w, fail).await,
        Ok(()) => tar_result(w, status, &msg, "tar extract").await,
    }
}

/// Streams `path` back as a tar (`data` frames then `done`). The archive
/// carries the entry under its own name, so the client extracts it back to
/// the same basename.
pub async fn pull<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> io::Result<()> {
    let p = Path::new(&path);
    let (parent, name) = match (p.parent(), p.file_name()) {
        (Some(par), Some(n)) if !n.is_empty() => (par.to_path_buf(), n.to_os_string()),
        _ => {
            return proto::write_frame(w, &Response::error(ErrorKind::BadRequest, "invalid path"))
                .await
        }
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

    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        match out.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                proto::write_frame(
                    w,
                    &Response::Data {
                        data: buf[..n].to_vec(),
                    },
                )
                .await?
            }
            Err(e) => {
                let _ = child.wait().await;
                return err_frame(w, &e, "read tar").await;
            }
        }
    }
    let status = child.wait().await?;
    let msg = err_task.await.unwrap_or_default();
    tar_result(w, status, &msg, "tar create").await
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
        proto::write_frame(
            w,
            &Response::error(ErrorKind::Internal, format!("{label}: {}", msg.trim())),
        )
        .await
    }
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
