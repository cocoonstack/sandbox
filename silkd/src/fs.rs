//! Filesystem verbs: the vsock boundary is the trust boundary, so paths are taken as-is.

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use tokio::fs;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::proto::{self, DirEntry, FileInfo, FileKind, Response, err_frame};

/// Entries per `entries` frame; a worst-case 1.6KiB entry keeps a full batch under MAX_FRAME.
pub const LIST_BATCH: usize = 4096;

/// Streams client `data` frames into `path` via a temp file renamed only on clean completion.
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
    let path = Path::new(&path);
    let tmp = tmp_name(path);
    let mut file = match fs::File::create(&tmp).await {
        Ok(f) => f,
        Err(e) => return err_frame(w, &e, "create").await,
    };

    let outcome = proto::feed_data_frames(&mut reader, &mut file).await.and(
        file.flush()
            .await
            .map_err(|e| proto::FeedError::Io(e, "flush")),
    );
    drop(file);
    if let Err(fail) = outcome {
        let _ = fs::remove_file(&tmp).await;
        return proto::write_feed_error(w, fail).await;
    }
    if let Err(e) = commit_tmp(&tmp, path, mode).await {
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
    if let Err(e) = proto::stream_data_frames(&mut file, w).await? {
        return err_frame(w, &e, "read").await;
    }
    proto::write_frame(w, &Response::Done).await
}

/// Lists a directory as `entries` frames terminated by `done`, batched under the frame cap.
pub async fn list<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> io::Result<()> {
    // One blocking-pool dispatch for the whole directory, not one per entry.
    let (tx, mut rx) = mpsc::channel::<Vec<DirEntry>>(2);
    let scan = tokio::task::spawn_blocking(move || scan_dir(&path, &tx));
    let mut failed = None;
    while let Some(entries) = rx.recv().await {
        if let Err(e) = proto::write_frame(w, &Response::Entries { entries }).await {
            failed = Some(e);
            break;
        }
    }
    drop(rx);
    let scanned = scan.await.map_err(io::Error::other)?;
    match failed {
        Some(e) => Err(e),
        None => match scanned {
            Ok(()) => proto::write_frame(w, &Response::Done).await,
            Err(e) => err_frame(w, &e, "read_dir").await,
        },
    }
}

/// Writes `bytes` to `path` via a sibling temp file, so a crash never leaves a truncated file.
pub async fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = tmp_name(path);
    if let Err(e) = fs::write(&tmp, bytes).await {
        let _ = fs::remove_file(&tmp).await;
        return Err(e);
    }
    commit_tmp(&tmp, path, None).await
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

/// Reads `path` into `LIST_BATCH`-sized batches, stopping when the writer is gone.
fn scan_dir(path: &str, tx: &mpsc::Sender<Vec<DirEntry>>) -> io::Result<()> {
    let mut entries = Vec::new();
    for ent in std::fs::read_dir(path)? {
        let ent = ent?;
        let (kind, size) = match ent.metadata() {
            Ok(m) => (file_kind(&m), m.len()),
            Err(_) => (FileKind::Other, 0),
        };
        entries.push(DirEntry {
            name: ent
                .file_name()
                .into_string()
                .unwrap_or_else(|os| os.to_string_lossy().into_owned()),
            kind,
            size,
        });
        if entries.len() == LIST_BATCH && tx.blocking_send(std::mem::take(&mut entries)).is_err() {
            return Ok(());
        }
    }
    if !entries.is_empty() {
        let _ = tx.blocking_send(entries);
    }
    Ok(())
}

fn tmp_name(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".silkd-");
    tmp.push(crate::sysutil::tmp_suffix());
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

/// Commits a temp file over `path`; without an explicit mode an overwrite inherits the destination's bits.
async fn commit_tmp(tmp: &Path, path: &Path, mode: Option<u32>) -> io::Result<()> {
    let outcome = async {
        let effective = match mode {
            Some(m) => Some(m),
            None => fs::metadata(path)
                .await
                .ok()
                .map(|meta| meta.permissions().mode() & 0o7777),
        };
        if let Some(m) = effective {
            fs::set_permissions(tmp, std::fs::Permissions::from_mode(m)).await?;
        }
        fs::rename(tmp, path).await
    }
    .await;
    if outcome.is_err() {
        let _ = fs::remove_file(tmp).await;
    }
    outcome
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
