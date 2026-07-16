//! `pty.open` / `pty.resize`: run a shell under a pseudo-terminal. A PTY is a
//! process, so it registers in the proc table and `ps`/`kill`/`attach`/`logs`
//! apply unchanged — output rides the same `stdout` chunk stream, input arrives
//! as `stdin` frames, and the child's exit is the terminal frame.

use std::os::fd::{AsRawFd, OwnedFd};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::io::unix::AsyncFd;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::proc::{Chunk, Proc, Table, synth_pid};
use crate::proto::{ErrorKind, PtyReq, READ_CHUNK, Request, Response};
use crate::sysutil;

/// After the shell exits, drain the master's buffered tail for at most this
/// long so the last screenful isn't lost, without wedging on a stuck fd.
const POST_EXIT_DRAIN: Duration = Duration::from_secs(1);

type Master = Arc<AsyncFd<OwnedFd>>;

/// Opens a pty running the guest login shell, streams its output as `stdout`
/// frames and the exit as `exit`, and consumes client `stdin` frames as pty
/// input until the shell exits or the client disconnects.
pub async fn open<W: AsyncWrite + Unpin>(
    table: &Table,
    now: u64,
    req: PtyReq,
    client: mpsc::Receiver<Request>,
    out: &mut W,
) -> std::io::Result<()> {
    let (master, slave) = match sysutil::openpty(req.cols, req.rows) {
        Ok(pair) => pair,
        Err(e) => return crate::proto::err_frame(out, &e, "openpty").await,
    };
    let resize_fd = match master.try_clone() {
        Ok(fd) => fd,
        Err(e) => return crate::proto::err_frame(out, &e, "dup pty master").await,
    };

    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/bash".to_string());
    let mut cmd = Command::new(&shell);
    cmd.arg("-i");
    if let Some(dir) = &req.cwd {
        cmd.current_dir(dir);
    }
    cmd.env_clear().envs(sysutil::base_env()).envs(&req.env);
    if let Some(user) = &req.user {
        if let Err(e) = sysutil::apply_user(&mut cmd, user) {
            return crate::proto::error_frame(out, ErrorKind::BadRequest, e).await;
        }
    }
    match slave.try_clone().and_then(|i| Ok((i, slave.try_clone()?))) {
        Ok((in_fd, out_fd)) => {
            cmd.stdin(Stdio::from(in_fd))
                .stdout(Stdio::from(out_fd))
                .stderr(Stdio::from(slave));
        }
        Err(e) => return crate::proto::err_frame(out, &e, "dup pty slave").await,
    }
    // SAFETY: make_controlling_tty runs only async-signal-safe syscalls, which
    // is the contract for a post-fork pre_exec hook.
    unsafe {
        cmd.pre_exec(|| sysutil::make_controlling_tty());
    }
    cmd.kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return crate::proto::err_frame(out, &e, "spawn shell").await,
    };
    // Drop cmd so its Stdio dups of the slave close: otherwise silkd keeps the
    // slave open and the master never sees the shell's exit (no EOF/EIO), so
    // the pump would hang forever.
    drop(cmd);

    let pid = child.id().unwrap_or_else(synth_pid);
    let proc = table.register(pid, vec![shell, "-i".to_string()], false, now);
    proc.set_pty_master(resize_fd);

    let master: Master = match AsyncFd::new(master) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            child.start_kill().ok();
            table.remove_if(pid, &proc);
            return crate::proto::err_frame(out, &e, "watch pty master").await;
        }
    };
    if crate::proto::write_frame(out, &Response::Started { pid })
        .await
        .is_err()
    {
        // Client vanished before we streamed anything — tear down cleanly.
        child.start_kill().ok();
        finish(&proc, -1);
        table.remove_if(pid, &proc);
        return Ok(());
    }

    let code = pump(&master, &proc, client, out, &mut child).await;
    finish(&proc, code);
    table.remove_if(pid, &proc);
    crate::proto::write_frame(out, &Response::Exit { code }).await
}

/// Applies a resize to a live pty by pid.
pub async fn resize<W: AsyncWrite + Unpin>(
    table: &Table,
    w: &mut W,
    pid: u32,
    cols: u16,
    rows: u16,
) -> std::io::Result<()> {
    let Some(proc) = table.get_or_not_found(w, pid).await? else {
        return Ok(());
    };
    match proc.resize(cols, rows) {
        Ok(()) => crate::proto::write_frame(w, &Response::Done).await,
        Err(e) => crate::proto::error_frame(w, ErrorKind::BadRequest, e.to_string()).await,
    }
}

/// Streams master output to the client and client stdin to the master until
/// the shell exits or the client disconnects, returning the exit code (-1 on
/// a kill or a client-gone teardown). The exit frame is written by the caller.
async fn pump<W: AsyncWrite + Unpin>(
    master: &Master,
    proc: &Arc<Proc>,
    client: mpsc::Receiver<Request>,
    out: &mut W,
    child: &mut tokio::process::Child,
) -> i32 {
    // Stdin rides its own task so a slow write can't stall output or reaping;
    // a client disconnect (recv None) signals teardown via disc.
    let (disc_tx, mut disc_rx) = oneshot::channel::<()>();
    tokio::spawn(pump_stdin(Arc::clone(master), client, disc_tx));

    let mut buf = [0u8; READ_CHUNK];
    let mut frame = Vec::new();
    let mut eof = false;
    let mut disc = false;
    loop {
        tokio::select! {
            status = child.wait() => {
                let code = status.map_or(-1, sysutil::exit_code);
                drain(master, proc, out, &mut buf, &mut frame).await;
                return code;
            }
            // Client closed the terminal: kill the shell and let child.wait
            // publish its real (signalled) code on the next iteration.
            _ = &mut disc_rx, if !disc => {
                disc = true;
                let _ = child.start_kill();
            }
            readable = master.readable(), if !eof => {
                let mut guard = match readable {
                    Ok(g) => g,
                    Err(_) => { eof = true; continue }
                };
                match guard.try_io(|fd| sysutil::read_fd(fd.get_ref().as_raw_fd(), &mut buf)) {
                    // 0 (BSD) and EIO (Linux) both mean the slave is fully closed.
                    Ok(Ok(0)) | Ok(Err(_)) => eof = true,
                    Ok(Ok(n)) => {
                        proc.emit_bytes(false, &buf[..n]);
                        if crate::proto::write_chunk_frame(out, &mut frame, "stdout", &buf[..n]).await.is_err() {
                            // Client gone mid-output: kill and reap for the real code.
                            let _ = child.start_kill();
                            return sysutil::wait_code(child).await;
                        }
                    }
                    Err(_would_block) => {}
                }
            }
        }
    }
}

/// Writes client stdin frames into the master until the client disconnects,
/// then signals teardown. StdinClose does not close the master — a pty stays
/// alive after EOF on its input.
async fn pump_stdin(
    master: Master,
    mut client: mpsc::Receiver<Request>,
    disc: oneshot::Sender<()>,
) {
    loop {
        match client.recv().await {
            Some(Request::Stdin { data }) => {
                if write_all(&master, &data).await.is_err() {
                    return;
                }
            }
            Some(_) => {}
            None => {
                let _ = disc.send(());
                return;
            }
        }
    }
}

/// Reads whatever the master buffered after the shell exits, bounded so a
/// wedged fd can't hang teardown.
async fn drain<W: AsyncWrite + Unpin>(
    master: &Master,
    proc: &Arc<Proc>,
    out: &mut W,
    buf: &mut [u8],
    frame: &mut Vec<u8>,
) {
    let _ = timeout(POST_EXIT_DRAIN, async {
        loop {
            let Ok(mut guard) = master.readable().await else {
                return;
            };
            match guard.try_io(|fd| sysutil::read_fd(fd.get_ref().as_raw_fd(), buf)) {
                Ok(Ok(0)) | Ok(Err(_)) => return,
                Ok(Ok(n)) => {
                    proc.emit_bytes(false, &buf[..n]);
                    if crate::proto::write_chunk_frame(out, frame, "stdout", &buf[..n])
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_would_block) => return,
            }
        }
    })
    .await;
}

/// finish publishes the terminal state once, so attachers on this pid always
/// see an Exit (and never hang waiting for one).
fn finish(proc: &Arc<Proc>, code: i32) {
    if proc.exit_code().is_none() {
        proc.mark_exited(code);
        proc.emit(&Chunk::Exit(code));
    }
}

async fn write_all(master: &Master, mut data: &[u8]) -> std::io::Result<()> {
    while !data.is_empty() {
        let mut guard = master.writable().await?;
        match guard.try_io(|fd| sysutil::write_fd(fd.get_ref().as_raw_fd(), data)) {
            Ok(Ok(n)) => data = &data[n..],
            Ok(Err(e)) => return Err(e),
            Err(_would_block) => continue,
        }
    }
    Ok(())
}
