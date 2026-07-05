//! Filesystem verbs: the guest is the sandbox, so paths are taken as-is
//! against the guest root (the vsock boundary is the trust boundary). write
//! consumes a `data` frame stream from the client; read streams `data` frames
//! back; the rest are one-shot.

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::time::UNIX_EPOCH;

use tokio::fs;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::proto::{self, err_frame, DirEntry, FileInfo, FileKind, Response, READ_CHUNK};

/// Entries per `entries` frame. Worst-case entry (255-byte name, fully
/// JSON-escaped) is ~1.6KiB, so a full batch stays under MAX_FRAME.
pub const LIST_BATCH: usize = 4096;

/// Streams `data` frames from the client into `path`, applying `mode` if
/// given, until a `data_end` frame. Writes go to a sibling temp file that is
/// renamed over `path` only on clean completion, so a mid-stream failure or
/// early disconnect never leaves a truncated file at the destination and
/// `done` means "path is exactly what you sent".
pub async fn write<R, W>(
    mut reader: R,
    w: &mut W,
    path: String,
    mode: Option<u32>,
) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let tmp = format!("{path}.silkd-{}.tmp", crate::sysutil::tmp_suffix());
    let mut file = match fs::File::create(&tmp).await {
        Ok(f) => f,
        Err(e) => return err_frame(w, &e, "create").await,
    };

    let outcome = proto::feed_data_frames(&mut reader, &mut file).await.and(
        file.flush()
            .await
            .map_err(|e| proto::FeedError::Io(e, "flush")),
    );
    if let Err(fail) = outcome {
        drop(file);
        let _ = fs::remove_file(&tmp).await;
        return proto::write_feed_error(w, fail).await;
    }

    // Explicit mode wins; otherwise inherit the destination's mode on an
    // overwrite so replacing an executable script doesn't silently strip its
    // exec bit (temp+rename would leave the temp's create-default 0644). A new
    // file with no mode keeps that default.
    let effective_mode = match mode {
        Some(m) => Some(m),
        None => fs::metadata(&path)
            .await
            .ok()
            .map(|meta| meta.permissions().mode() & 0o7777),
    };
    if let Some(m) = effective_mode {
        if let Err(e) = file
            .set_permissions(std::fs::Permissions::from_mode(m))
            .await
        {
            drop(file);
            let _ = fs::remove_file(&tmp).await;
            return err_frame(w, &e, "chmod").await;
        }
    }
    drop(file);
    if let Err(e) = fs::rename(&tmp, &path).await {
        let _ = fs::remove_file(&tmp).await;
        return err_frame(w, &e, "commit").await;
    }
    proto::write_frame(w, &Response::Done).await
}

/// Streams the file at `path` back as `data` frames, then `done`.
pub async fn read<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> io::Result<()> {
    let mut file = match fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) => return err_frame(w, &e, "open").await,
    };
    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        match file.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                proto::write_frame(
                    w,
                    &Response::Data {
                        data: buf[..n].to_vec(),
                    },
                )
                .await?;
            }
            Err(e) => return err_frame(w, &e, "read").await,
        }
    }
    proto::write_frame(w, &Response::Done).await
}

/// Lists a directory as a stream of `entries` frames terminated by `done`,
/// batched so an arbitrarily large directory can never exceed the frame cap.
pub async fn list<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> io::Result<()> {
    let mut rd = match fs::read_dir(&path).await {
        Ok(r) => r,
        Err(e) => return err_frame(w, &e, "read_dir").await,
    };
    let mut entries = Vec::new();
    loop {
        match rd.next_entry().await {
            Ok(None) => break,
            Ok(Some(ent)) => {
                let (kind, size) = match ent.metadata().await {
                    Ok(m) => (file_kind(&m), m.len()),
                    Err(_) => (FileKind::Other, 0),
                };
                entries.push(DirEntry {
                    name: ent.file_name().to_string_lossy().into_owned(),
                    kind,
                    size,
                });
                if entries.len() == LIST_BATCH {
                    let batch = std::mem::take(&mut entries);
                    proto::write_frame(w, &Response::Entries { entries: batch }).await?;
                }
            }
            Err(e) => return err_frame(w, &e, "read_dir").await,
        }
    }
    if !entries.is_empty() {
        proto::write_frame(w, &Response::Entries { entries }).await?;
    }
    proto::write_frame(w, &Response::Done).await
}

/// Reports metadata for `path` (following symlinks).
pub async fn stat<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> io::Result<()> {
    let meta = match fs::metadata(&path).await {
        Ok(m) => m,
        Err(e) => return err_frame(w, &e, "stat").await,
    };
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let info = FileInfo {
        kind: file_kind(&meta),
        size: meta.len(),
        mode: meta.permissions().mode() & 0o7777, // strip S_IFMT; kind carries the type
        mtime_epoch_secs: mtime,
    };
    proto::write_frame(w, &Response::Stat { info }).await
}

/// Creates a directory, with parents when `parents` is set.
pub async fn mkdir<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    parents: bool,
) -> io::Result<()> {
    let res = if parents {
        fs::create_dir_all(&path).await
    } else {
        fs::create_dir(&path).await
    };
    match res {
        Ok(()) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "mkdir").await,
    }
}

/// Removes a file, or a directory (recursively when `recursive` is set).
pub async fn rm<W: AsyncWrite + Unpin>(w: &mut W, path: String, recursive: bool) -> io::Result<()> {
    let meta = match fs::symlink_metadata(&path).await {
        Ok(m) => m,
        Err(e) => return err_frame(w, &e, "rm").await,
    };
    let res = if meta.is_dir() && recursive {
        fs::remove_dir_all(&path).await
    } else if meta.is_dir() {
        fs::remove_dir(&path).await
    } else {
        fs::remove_file(&path).await
    };
    match res {
        Ok(()) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "rm").await,
    }
}

/// Renames `from` to `to` within the guest filesystem.
pub async fn rename<W: AsyncWrite + Unpin>(w: &mut W, from: String, to: String) -> io::Result<()> {
    match fs::rename(&from, &to).await {
        Ok(()) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "rename").await,
    }
}

fn file_kind(meta: &std::fs::Metadata) -> FileKind {
    let t = meta.file_type();
    if t.is_dir() {
        FileKind::Dir
    } else if t.is_symlink() {
        FileKind::Symlink
    } else if t.is_file() {
        FileKind::File
    } else {
        FileKind::Other
    }
}
