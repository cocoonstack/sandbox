//! Whole-tree transfer through the guest tar binary: `fs.push` extracts a client
//! tar stream into a directory, `fs.pull` streams a path back as a tar. Push is
//! network-failure atomic: it stages inside `dest`, then merges by rename.

use std::io;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::process::Stdio;

use nix::dir::Dir;
use nix::fcntl::{self, AtFlags, OFlag};
use nix::sys::stat::{self, Mode, SFlag};
use nix::unistd::{self, UnlinkatFlags};
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite};
use tokio::process::Command;

use crate::proto::{self, ErrorKind, Response, err_frame};
use crate::sysutil;

const STDERR_CAP: usize = 16 * 1024;
const DIR_FLAGS: OFlag = OFlag::O_DIRECTORY
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

/// Extracts a client tar stream into `dest`, creating it; a stream failure leaves `dest` unchanged.
pub async fn push<R, W>(mut reader: R, w: &mut W, dest: String) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if let Err(e) = tokio::fs::create_dir_all(&dest).await {
        return err_frame(w, &e, "mkdir dest").await;
    }
    let name = format!(".silkd-push-{}", sysutil::tmp_suffix());
    let (dest_fd, staging) = match stage_dir(&dest, &name) {
        Ok(fds) => fds,
        Err(e) => return err_frame(w, &e, "stage push").await,
    };
    let res = push_staged(&mut reader, w, staging, dest_fd).await;
    // a failed push can leave a large partial tree, so cleanup runs on every exit path.
    let _ = tokio::fs::remove_dir_all(Path::new(&dest).join(&name)).await;
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
    proto::subprocess_result(w, status.success(), "tar create", msg.as_bytes()).await
}

/// The caller owns removing `staging` on every path.
async fn push_staged<R, W>(
    reader: &mut R,
    w: &mut W,
    staging: OwnedFd,
    dest: OwnedFd,
) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut cmd = Command::new("tar");
    cmd.arg("-x").stdin(Stdio::piped()).stdout(Stdio::null());
    sysutil::start_in(&mut cmd, &staging);
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

    if let Err(fail) = feed
        && (status.success() || !matches!(fail, proto::FeedError::Io(_, "write")))
    {
        return proto::write_feed_error(w, fail).await;
    }
    if !status.success() {
        return proto::subprocess_result(w, status.success(), "tar extract", msg.as_bytes()).await;
    }
    // spawn_blocking: a big tree is thousands of local fs syscalls.
    let merged = tokio::task::spawn_blocking(move || merge_tree(&staging, &dest))
        .await
        .map_err(io::Error::other)
        .and_then(|r| r);
    match merged {
        Ok(()) => proto::write_frame(w, &Response::Done).await,
        Err(e) => err_frame(w, &e, "merge push").await,
    }
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

/// Creates the staging dir inside `dest` (so the merge is same-filesystem renames) and holds both by fd, so renaming either path cannot redirect the push.
fn stage_dir(dest: &str, name: &str) -> io::Result<(OwnedFd, OwnedFd)> {
    let dest = fcntl::open(dest, OFlag::O_DIRECTORY | OFlag::O_CLOEXEC, Mode::empty())?;
    stat::mkdirat(&dest, name, Mode::S_IRWXU)?;
    let staging = fcntl::openat(&dest, name, DIR_FLAGS, Mode::empty())?;
    if stat::fstat(&staging)?.st_uid != unistd::geteuid().as_raw() {
        return Err(io::Error::other("push staging dir was replaced"));
    }
    Ok((dest, staging))
}

/// Overlays `src` into `dst` with tar's semantics through fds that never follow a link; `src` is listed through a fresh open, as overlayfs lists stale through a fd opened before the extract.
fn merge_tree(src: &OwnedFd, dst: &OwnedFd) -> io::Result<()> {
    let mut dir = Dir::openat(src, ".", DIR_FLAGS, Mode::empty())?;
    let mut names = Vec::new();
    for entry in dir.iter() {
        let name = entry?.file_name().to_owned();
        if name.as_bytes() != b"." && name.as_bytes() != b".." {
            names.push(name);
        }
    }
    for name in &names {
        let from_dir =
            is_dir(stat::fstatat(&dir, name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW)?.st_mode);
        match stat::fstatat(dst, name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(to) if from_dir && is_dir(to.st_mode) => {
                let from = fcntl::openat(&dir, name.as_c_str(), DIR_FLAGS, Mode::empty())?;
                let to = fcntl::openat(dst, name.as_c_str(), DIR_FLAGS, Mode::empty())?;
                merge_tree(&from, &to)?;
            }
            // mirrors tar's unlink-before-extract.
            Ok(_) if from_dir => {
                unistd::unlinkat(dst, name.as_c_str(), UnlinkatFlags::NoRemoveDir)?;
                fcntl::renameat(&dir, name.as_c_str(), dst, name.as_c_str())?;
            }
            _ => fcntl::renameat(&dir, name.as_c_str(), dst, name.as_c_str())?,
        }
    }
    Ok(())
}

fn is_dir(mode: stat::mode_t) -> bool {
    SFlag::from_bits_truncate(mode) & SFlag::S_IFMT == SFlag::S_IFDIR
}

/// Reads a child's stderr up to STDERR_CAP; tar's first error is the useful one.
async fn drain(mut stderr: tokio::process::ChildStderr) -> String {
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(n) = stderr.read(&mut buf).await {
        if n == 0 {
            break;
        }
        if out.len() < STDERR_CAP {
            out.extend_from_slice(&buf[..(n.min(STDERR_CAP - out.len()))]);
        }
    }
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn push_holds_its_dirs_by_fd_when_the_staging_path_is_swapped() {
        let root = tempfile::tempdir().unwrap();
        let (dest, victim, src) = (
            root.path().join("dest"),
            root.path().join("victim"),
            root.path().join("src"),
        );
        for dir in [&dest, &victim, &src] {
            std::fs::create_dir_all(dir).unwrap();
        }
        std::fs::write(victim.join("keep"), "victim").unwrap();
        std::fs::write(src.join("pushed"), "client").unwrap();
        let archive = std::process::Command::new("tar")
            .arg("-c")
            .arg("-C")
            .arg(&src)
            .arg(".")
            .output()
            .unwrap()
            .stdout;

        let name = ".silkd-push-swap";
        let (dest_fd, staging) = stage_dir(dest.to_str().unwrap(), name).unwrap();
        std::fs::rename(dest.join(name), root.path().join("moved")).unwrap();
        std::os::unix::fs::symlink(&victim, dest.join(name)).unwrap();

        let data = base64::engine::general_purpose::STANDARD.encode(&archive);
        let frames = format!(
            "{}\n{}\n",
            json!({"op":"data","data":data}),
            json!({"op":"data_end"})
        );
        let mut reader = tokio::io::BufReader::new(frames.as_bytes());
        let mut out = Vec::new();
        push_staged(&mut reader, &mut out, staging, dest_fd)
            .await
            .unwrap();

        let reply = String::from_utf8_lossy(&out);
        assert_eq!(
            std::fs::read_to_string(dest.join("pushed")).ok().as_deref(),
            Some("client"),
            "{reply}"
        );
        assert!(!victim.join("pushed").exists());
        assert!(victim.join("keep").exists() && !dest.join("keep").exists());
    }
}
