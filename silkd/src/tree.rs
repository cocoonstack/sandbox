//! Whole-tree transfer: `fs.push` extracts a client tar stream into a
//! directory, `fs.pull` streams a path back as a tar. The archive format is
//! the contract and tar(1) is its authoritative implementation, so these
//! stream through the guest tar binary rather than buffering a whole project
//! in memory — the only project-ingestion path on the no-network lane.

use std::io;
use std::path::Path;
use std::process::Stdio;

use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;

use crate::proto::{self, ErrorKind, Request, Response, READ_CHUNK};

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
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return err_frame(w, &e, "spawn tar").await,
    };
    let mut sink = child.stdin.take().expect("stdin piped");
    // Drain stderr concurrently: a noisy tar can fill the stderr pipe and
    // block while we are still writing its stdin, deadlocking both.
    let stderr = child.stderr.take().expect("stderr piped");
    let err_task = tokio::spawn(drain(stderr));

    let feed = feed_tar(&mut reader, &mut sink).await;
    drop(sink); // EOF to tar regardless of outcome
    let status = child.wait().await?;
    let msg = err_task.await.unwrap_or_default();

    if let Err(fail) = feed {
        return match fail {
            PushFail::Io(e, op) => err_frame(w, &e, op).await,
            PushFail::Protocol => {
                proto::write_frame(
                    w,
                    &Response::error(ErrorKind::BadRequest, "expected data or data_end"),
                )
                .await
            }
            PushFail::Truncated => {
                proto::write_frame(
                    w,
                    &Response::error(ErrorKind::BadRequest, "stream ended before data_end"),
                )
                .await
            }
        };
    }
    if status.success() {
        proto::write_frame(w, &Response::Done).await
    } else {
        proto::write_frame(
            w,
            &Response::error(ErrorKind::Internal, format!("tar extract: {}", msg.trim())),
        )
        .await
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
        .arg(&name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return err_frame(w, &e, "spawn tar").await,
    };
    let mut out = child.stdout.take().expect("stdout piped");
    // Drain stderr concurrently so a noisy tar can't block on a full stderr
    // pipe while we are reading its stdout.
    let stderr = child.stderr.take().expect("stderr piped");
    let err_task = tokio::spawn(drain(stderr));

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
    if status.success() {
        proto::write_frame(w, &Response::Done).await
    } else {
        proto::write_frame(
            w,
            &Response::error(ErrorKind::Internal, format!("tar create: {}", msg.trim())),
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

enum PushFail {
    Io(io::Error, &'static str),
    Protocol,
    Truncated,
}

async fn feed_tar<R>(reader: &mut R, sink: &mut tokio::process::ChildStdin) -> Result<(), PushFail>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let frame = proto::read_frame(reader)
            .await
            .map_err(|e| PushFail::Io(e, "read"))?
            .ok_or(PushFail::Truncated)?;
        match serde_json::from_slice::<Request>(&frame) {
            Ok(Request::Data { data }) => sink
                .write_all(&data)
                .await
                .map_err(|e| PushFail::Io(e, "tar stdin"))?,
            Ok(Request::DataEnd) => return Ok(()),
            _ => return Err(PushFail::Protocol),
        }
    }
}

async fn err_frame<W: AsyncWrite + Unpin>(w: &mut W, e: &io::Error, op: &str) -> io::Result<()> {
    proto::write_frame(
        w,
        &Response::error(ErrorKind::Internal, format!("{op}: {e}")),
    )
    .await
}
