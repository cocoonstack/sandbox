//! exec handler: spawn a child, register it in the process table, stream
//! stdout/stderr as frames, and pump client stdin frames into it. Detached
//! execs return after `started` and keep running for later attach/logs.

use std::process::Stdio;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::proc::{synth_pid, Chunk, Proc, Table};
use crate::proto::{ErrorKind, ExecReq, Request, Response};
use crate::sysutil;

/// After the child is reaped, wait at most this long for the pump to finish
/// before publishing Exit. It bounds a daemonizing child that leaves stdout
/// open in a surviving grandchild; a foreground client stalled past this
/// window can also lose the not-yet-drained pipe tail (bounded, rare).
const POST_EXIT_DRAIN: Duration = Duration::from_secs(2);
const REAP_DELAY: Duration = Duration::from_secs(300);
/// Foreground output buffer before the child is backpressured. Bounds how far
/// ahead of a slow client the child may run, not a loss threshold.
const FG_CAP: usize = 256;

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
        return crate::proto::error_frame(out, ErrorKind::BadRequest, "argv must not be empty")
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
            return crate::proto::error_frame(out, ErrorKind::BadRequest, e).await;
        }
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return crate::proto::error_frame(out, ErrorKind::Internal, format!("spawn: {e}"))
                .await;
        }
    };

    let pid = child.id().unwrap_or_else(synth_pid);
    let proc = table.register(pid, req.argv, req.detach, now_secs);
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

    // Foreground consumes a backpressured mpsc: when the client falls behind,
    // the sends block, the pipe fills, and the child slows down — nothing
    // drops while the child lives (POST_EXIT_DRAIN bounds only the post-exit
    // tail). Attachers still ride the best-effort broadcast (a secondary
    // observer may drop under extreme lag). Detached has no client to pace
    // it, so it gets no foreground sender.
    let (fg_tx, fg_rx) = mpsc::channel::<Chunk>(FG_CAP);
    let pump_fg = (!req.detach).then(|| fg_tx.clone());
    let sup_fg = (!req.detach).then(|| fg_tx.clone());
    drop(fg_tx); // only the pump/supervise clones keep fg_rx open

    let pump = tokio::spawn(pump_out(Arc::clone(&proc), stdout, stderr, pump_fg));
    let pump_abort = pump.abort_handle();
    tokio::spawn(pump_stdin(stdin, client, req.detach));

    // One supervisor per exec: reap the child, drain output within a grace
    // window (so a daemonizer holding the pipe can't wedge us), then publish
    // the terminal Exit — to the broadcast (attachers) and the foreground mpsc.
    // The reaped code is recorded before the drain: a disconnect can abort the
    // supervisor mid-drain, and its fallback must publish the real code, not
    // fabricate -1 for a child that already exited cleanly.
    let reaped: Arc<OnceLock<i32>> = Arc::new(OnceLock::new());
    let sup_reaped = Arc::clone(&reaped);
    let sup_proc = Arc::clone(&proc);
    let supervise = tokio::spawn(async move {
        let code = sysutil::wait_code(&mut child).await;
        let _ = sup_reaped.set(code);
        let mut pump = pump;
        if timeout(POST_EXIT_DRAIN, &mut pump).await.is_err() {
            pump.abort();
        }
        sup_proc.mark_exited(code);
        sup_proc.emit(&Chunk::Exit(code));
        if let Some(fg) = sup_fg {
            let _ = fg.send(Chunk::Exit(code)).await;
        }
        code
    });

    if req.detach {
        drop(fg_rx);
        let table = table.clone();
        let proc = Arc::clone(&proc);
        tokio::spawn(async move {
            let _ = supervise.await;
            tokio::time::sleep(REAP_DELAY).await;
            table.remove_if(pid, &proc);
        });
        return Ok(());
    }

    let mut rx = fg_rx;
    if let Err(e) = stream_to_client(&mut rx, out).await {
        // Client gone: abort supervise (kills the child via kill_on_drop) and
        // the pump (else its handle only detaches, leaking task/fds behind a
        // silent pipe-holder), then publish the terminal state the abort may
        // have pre-empted — else an attacher on this pid waits forever.
        supervise.abort();
        pump_abort.abort();
        let _ = supervise.await;
        if proc.exit_code().is_none() {
            let code = reaped.get().copied().unwrap_or(-1);
            proc.mark_exited(code);
            proc.emit(&Chunk::Exit(code));
        }
        table.remove_if(pid, &proc);
        return Err(e);
    }
    let code = supervise.await.unwrap_or(-1);
    table.remove_if(pid, &proc);
    crate::proto::write_frame(out, &Response::Exit { code }).await
}

async fn stream_to_client<W>(rx: &mut mpsc::Receiver<Chunk>, out: &mut W) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(chunk) = rx.recv().await {
        match chunk {
            Chunk::Exit(_) => return Ok(()),
            c => crate::proto::write_frame(out, &c.into_response()).await?,
        }
    }
    Ok(())
}

/// Reads stdout and stderr concurrently within this one task, so aborting the
/// pump (POST_EXIT_DRAIN timeout) cancels both. A nested spawn would survive
/// the abort and keep emitting a daemonizer's output after Exit. Each chunk
/// goes to the ring+broadcast (attachers/replay) and, for a foreground exec,
/// to the backpressured `fg` sender.
async fn pump_out(
    proc: Arc<Proc>,
    stdout: Option<tokio::process::ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
    fg: Option<mpsc::Sender<Chunk>>,
) {
    let fg = fg.as_ref();
    let out = async {
        if let Some(s) = stdout {
            copy_stream(s, &proc, false, fg).await;
        }
    };
    let err = async {
        if let Some(s) = stderr {
            copy_stream(s, &proc, true, fg).await;
        }
    };
    tokio::join!(out, err);
}

async fn copy_stream<R>(mut r: R, proc: &Proc, stderr: bool, fg: Option<&mpsc::Sender<Chunk>>)
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let make: fn(Vec<u8>) -> Chunk = if stderr { Chunk::Stderr } else { Chunk::Stdout };
    let mut buf = [0u8; crate::proto::READ_CHUNK];
    loop {
        let n = match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let chunk = make(buf[..n].to_vec());
        proc.emit(&chunk);
        if let Some(fg) = fg {
            // The foreground client backpressures here; emit above is
            // best-effort fan-out to attachers.
            if fg.send(chunk).await.is_err() {
                break;
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
