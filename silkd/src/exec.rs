//! exec handler: spawn a child, register it in the process table, stream
//! stdout/stderr as frames, and pump client stdin frames into it. Detached
//! execs return after `started` and keep running for later attach/logs.

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::proc::{synth_pid, Chunk, Proc, Table};
use crate::proto::{ErrorKind, ExecReq, Request, Response};
use crate::sysutil;

/// After the child is reaped, wait at most this long for its pipes to reach
/// EOF before publishing Exit — bounds a daemonizing child that leaves its
/// stdout open in a surviving grandchild.
const POST_EXIT_DRAIN: Duration = Duration::from_secs(2);
const REAP_DELAY: Duration = Duration::from_secs(300);

/// Runs an exec request to completion (or to `started` when detached),
/// writing response frames to `out`. `client` yields further client frames
/// (stdin / stdin_close) for the foreground case.
pub async fn run<W>(
    table: &Table,
    now_secs: u64,
    req: ExecReq,
    client: mpsc::Receiver<Request>,
    out: &mut W,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if req.argv.is_empty() {
        return crate::proto::write_frame(
            out,
            &Response::error(ErrorKind::BadRequest, "argv must not be empty"),
        )
        .await;
    }

    let mut cmd = Command::new(&req.argv[0]);
    cmd.args(&req.argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(!req.detach);
    if let Some(ref cwd) = req.cwd {
        cmd.current_dir(cwd);
    }
    cmd.env_clear();
    cmd.envs(sysutil::base_env());
    cmd.envs(&req.env);
    if let Some(ref user) = req.user {
        if let Err(e) = sysutil::apply_user(&mut cmd, user) {
            return crate::proto::write_frame(out, &Response::error(ErrorKind::BadRequest, e))
                .await;
        }
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return crate::proto::write_frame(
                out,
                &Response::error(ErrorKind::Internal, format!("spawn: {e}")),
            )
            .await;
        }
    };

    let pid = child.id().unwrap_or_else(synth_pid);
    let proc = table.register(pid, req.argv.clone(), req.detach, now_secs);
    if let Err(e) = crate::proto::write_frame(out, &Response::Started { pid }).await {
        // The relay never learned this pid, so nothing will ever supervise or
        // reap it: drop the table entry and kill the just-spawned child rather
        // than leave a permanent Running ghost (and, when detached, an orphan).
        table.remove_if(pid, &proc);
        let _ = child.start_kill();
        return Err(e);
    }

    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // Foreground subscribes before any output is emitted so it cannot miss a
    // chunk; detached attachers replay from the ring buffer instead.
    let rx = (!req.detach).then(|| proc.subscribe());

    let pump = tokio::spawn(pump_out(Arc::clone(&proc), stdout, stderr));
    tokio::spawn(pump_stdin(stdin, client, req.detach));

    // One supervisor per exec: reap the child, drain output within a grace
    // window (so a daemonizer holding the pipe can't wedge us), then publish
    // the terminal Exit chunk that unblocks the foreground streamer.
    let sup_proc = Arc::clone(&proc);
    let supervise = tokio::spawn(async move {
        let code = wait_code(&mut child).await;
        let mut pump = pump;
        if timeout(POST_EXIT_DRAIN, &mut pump).await.is_err() {
            pump.abort();
        }
        sup_proc.mark_exited(code);
        sup_proc.emit(Chunk::Exit(code));
        code
    });

    if req.detach {
        let table = table.clone();
        let proc = Arc::clone(&proc);
        tokio::spawn(async move {
            let _ = supervise.await;
            tokio::time::sleep(REAP_DELAY).await;
            table.remove_if(pid, &proc);
        });
        return Ok(());
    }

    let mut rx = rx.expect("foreground subscribed above");
    if let Err(e) = stream_to_client(&mut rx, out).await {
        // Client vanished mid-stream: abort supervise (drops the child via
        // kill_on_drop) and drop our table entry. Aborting may pre-empt
        // supervise before it published the terminal state, so publish it here
        // too — otherwise a second connection attached to this pid would wait
        // forever for an Exit that never comes.
        supervise.abort();
        let _ = supervise.await;
        if proc.exit_code().is_none() {
            proc.mark_exited(-1);
            proc.emit(Chunk::Exit(-1));
        }
        table.remove_if(pid, &proc);
        return Err(e);
    }
    let code = supervise.await.unwrap_or(-1);
    table.remove_if(pid, &proc);
    crate::proto::write_frame(out, &Response::Exit { code }).await
}

async fn stream_to_client<W>(
    rx: &mut tokio::sync::broadcast::Receiver<Chunk>,
    out: &mut W,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        match rx.recv().await {
            Ok(Chunk::Exit(_)) => return Ok(()),
            Ok(chunk) => crate::proto::write_frame(out, &chunk.into_response()).await?,
            // v1 best-effort: a client that stalls long enough to overflow the
            // fan-out buffer drops the overrun chunks. v2 replaces the
            // foreground path with a backpressured mpsc so output can't be lost.
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

/// Reads stdout and stderr concurrently within this one task, so aborting the
/// pump (POST_EXIT_DRAIN timeout) cancels both. A nested spawn would survive
/// the abort and keep emitting a daemonizer's output after Exit.
async fn pump_out(
    proc: Arc<Proc>,
    stdout: Option<tokio::process::ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
) {
    let out = async {
        if let Some(s) = stdout {
            copy_stream(s, &proc, false).await;
        }
    };
    let err = async {
        if let Some(s) = stderr {
            copy_stream(s, &proc, true).await;
        }
    };
    tokio::join!(out, err);
}

async fn copy_stream<R>(mut r: R, proc: &Proc, stderr: bool)
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 32 * 1024];
    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let d = buf[..n].to_vec();
                proc.emit(if stderr {
                    Chunk::Stderr(d)
                } else {
                    Chunk::Stdout(d)
                });
            }
        }
    }
}

async fn pump_stdin(
    stdin: Option<tokio::process::ChildStdin>,
    mut client: mpsc::Receiver<Request>,
    detach: bool,
) {
    let Some(mut sink) = stdin else { return };
    if detach {
        return; // detached execs take no client stdin
    }
    while let Some(frame) = client.recv().await {
        match frame {
            Request::Stdin { data } => {
                if sink.write_all(&data).await.is_err() {
                    return;
                }
            }
            Request::StdinClose => break,
            _ => {}
        }
    }
    let _ = sink.shutdown().await;
}

async fn wait_code(child: &mut tokio::process::Child) -> i32 {
    match child.wait().await {
        Ok(status) => sysutil::exit_code(status),
        Err(_) => -1,
    }
}
