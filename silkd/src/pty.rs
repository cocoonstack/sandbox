//! `pty.open` / `pty.resize`: run a shell under a pseudo-terminal. A PTY is a
//! process, so it registers in the proc table and `ps`/`kill`/`attach`/`logs`
//! apply unchanged — output rides the same `stdout` chunk stream, input arrives
//! as `stdin` frames, and the child's exit is the terminal frame.

use std::os::fd::AsRawFd;
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::unix::AsyncFd;
use tokio::io::AsyncWrite;
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::proc::{synth_pid, Chunk, Proc, Table};
use crate::proto::{ErrorKind, PtyReq, Request, Response};
use crate::sysutil;

const PTY_READ_CHUNK: usize = 32 * 1024;

/// Opens a pty running the guest login shell, streams its output as `stdout`
/// frames and the exit as `exit`, and consumes client `stdin` frames as pty
/// input until the shell exits or the client disconnects.
pub async fn open<W: AsyncWrite + Unpin>(
    table: &Table,
    now: u64,
    req: PtyReq,
    mut client: mpsc::Receiver<Request>,
    out: &mut W,
) -> std::io::Result<()> {
    let (master, slave) = match sysutil::openpty(req.cols, req.rows) {
        Ok(pair) => pair,
        Err(e) => {
            return crate::proto::write_frame(
                out,
                &Response::error(ErrorKind::Internal, e.to_string()),
            )
            .await
        }
    };
    let resize_fd = match master.try_clone() {
        Ok(fd) => fd,
        Err(e) => {
            return crate::proto::write_frame(
                out,
                &Response::error(ErrorKind::Internal, e.to_string()),
            )
            .await
        }
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
            return crate::proto::write_frame(out, &Response::error(ErrorKind::BadRequest, e))
                .await;
        }
    }
    match (slave.try_clone(), slave.try_clone()) {
        (Ok(in_fd), Ok(out_fd)) => {
            cmd.stdin(Stdio::from(in_fd))
                .stdout(Stdio::from(out_fd))
                .stderr(Stdio::from(slave));
        }
        _ => {
            return crate::proto::write_frame(
                out,
                &Response::error(ErrorKind::Internal, "dup pty slave"),
            )
            .await
        }
    }
    // SAFETY: make_controlling_tty runs only async-signal-safe syscalls, which
    // is the contract for a post-fork pre_exec hook.
    unsafe {
        cmd.pre_exec(|| sysutil::make_controlling_tty());
    }
    cmd.kill_on_drop(true);

    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return crate::proto::write_frame(
                out,
                &Response::error(ErrorKind::Internal, e.to_string()),
            )
            .await
        }
    };
    let pid = child.id().unwrap_or_else(synth_pid);
    let proc = table.register(pid, vec![shell, "-i".to_string()], false, now);
    proc.set_pty_master(resize_fd);

    let master = AsyncFd::new(master)?;
    crate::proto::write_frame(out, &Response::Started { pid }).await?;

    let res = pump(&master, &proc, &mut client, out, child).await;
    table.remove_if(pid, &proc);
    res
}

/// Applies a resize to a live pty by pid.
pub async fn resize<W: AsyncWrite + Unpin>(
    table: &Table,
    w: &mut W,
    pid: u32,
    cols: u16,
    rows: u16,
) -> std::io::Result<()> {
    let Some(proc) = table.get(pid) else {
        return crate::proto::write_frame(w, &Response::error(ErrorKind::NotFound, "no such pid"))
            .await;
    };
    match proc.resize(cols, rows) {
        Ok(()) => crate::proto::write_frame(w, &Response::Done).await,
        Err(e) => {
            crate::proto::write_frame(w, &Response::error(ErrorKind::BadRequest, e.to_string()))
                .await
        }
    }
}

async fn pump<W: AsyncWrite + Unpin>(
    master: &AsyncFd<std::os::fd::OwnedFd>,
    proc: &Arc<Proc>,
    client: &mut mpsc::Receiver<Request>,
    out: &mut W,
    mut child: tokio::process::Child,
) -> std::io::Result<()> {
    let mut buf = [0u8; PTY_READ_CHUNK];
    // Once the master reads EOF, stop selecting on it: EOF stays perpetually
    // "readable", so re-arming the branch would busy-spin until child.wait wins.
    let mut eof = false;
    loop {
        tokio::select! {
            // Reap first: once the shell exits, the master read below would
            // still drain the buffered tail, but the exit is the terminal.
            status = child.wait() => {
                let code = sysutil::exit_code(status?);
                drain_pty(master, proc, out, &mut buf).await?;
                proc.mark_exited(code);
                proc.emit(Chunk::Exit(code));
                return crate::proto::write_frame(out, &Response::Exit { code }).await;
            }
            readable = master.readable(), if !eof => {
                let mut guard = readable?;
                match guard.try_io(|fd| read_fd(fd.get_ref().as_raw_fd(), &mut buf)) {
                    Ok(Ok(0)) => eof = true, // child.wait publishes the terminal
                    Ok(Ok(n)) => {
                        let data = buf[..n].to_vec();
                        proc.emit(Chunk::Stdout(data.clone()));
                        crate::proto::write_frame(out, &Response::Stdout { data }).await?;
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(_would_block) => {}
                }
            }
            frame = client.recv() => match frame {
                Some(Request::Stdin { data }) => write_all(master, &data).await?,
                Some(Request::StdinClose) | None => {} // pty input ends; shell stays alive
                Some(_) => {}
            },
        }
    }
}

/// Reads whatever the master has buffered after the shell exits, so the last
/// screenful isn't lost to the exit race.
async fn drain_pty<W: AsyncWrite + Unpin>(
    master: &AsyncFd<std::os::fd::OwnedFd>,
    proc: &Arc<Proc>,
    out: &mut W,
    buf: &mut [u8],
) -> std::io::Result<()> {
    loop {
        match master.readable().await {
            Ok(mut guard) => match guard.try_io(|fd| read_fd(fd.get_ref().as_raw_fd(), buf)) {
                Ok(Ok(0)) | Ok(Err(_)) => return Ok(()),
                Ok(Ok(n)) => {
                    let data = buf[..n].to_vec();
                    proc.emit(Chunk::Stdout(data.clone()));
                    crate::proto::write_frame(out, &Response::Stdout { data }).await?;
                }
                Err(_would_block) => return Ok(()),
            },
            Err(_) => return Ok(()),
        }
    }
}

async fn write_all(master: &AsyncFd<std::os::fd::OwnedFd>, mut data: &[u8]) -> std::io::Result<()> {
    while !data.is_empty() {
        let mut guard = master.writable().await?;
        match guard.try_io(|fd| write_fd(fd.get_ref().as_raw_fd(), data)) {
            Ok(Ok(n)) => data = &data[n..],
            Ok(Err(e)) => return Err(e),
            Err(_would_block) => continue,
        }
    }
    Ok(())
}

fn read_fd(fd: std::os::fd::RawFd, buf: &mut [u8]) -> std::io::Result<usize> {
    // SAFETY: fd is the live pty master; buf is a valid mutable slice.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(n as usize)
}

fn write_fd(fd: std::os::fd::RawFd, buf: &[u8]) -> std::io::Result<usize> {
    // SAFETY: fd is the live pty master; buf is a valid slice.
    let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(n as usize)
}
