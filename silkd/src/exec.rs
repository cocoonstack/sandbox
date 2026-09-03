//! exec handler: a detached exec returns after `started` and stays in the table for attach/logs.

use std::process::Stdio;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::proc::{Chunk, Proc, Table, synth_pid};
use crate::proto::{ErrorKind, ExecReq, Request, Response};
use crate::sysutil;

/// Post-exit drain window; it bounds a daemonizing grandchild, and a stalled client loses the undrained tail.
const POST_EXIT_DRAIN: Duration = Duration::from_secs(2);
/// How long an exited detached process stays in the table for a late `logs`/`attach`.
const REAP_DELAY: Duration = Duration::from_secs(300);
/// Foreground output depth; while the child runs it backpressures here instead of dropping output.
const FG_CAP: usize = 256;

/// Runs an exec request, writing response frames to `out`; the dispatcher guarantees argv is non-empty.
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
    if let Some(ref user) = req.user
        && let Err(e) = sysutil::apply_user(&mut cmd, user)
    {
        return crate::proto::error_frame(out, ErrorKind::BadRequest, e).await;
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
        // the client never learned this pid, so nothing else will ever reap the child.
        table.remove_if(pid, &proc);
        let _ = child.start_kill();
        return Err(e);
    }

    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // a backpressured mpsc paces the child to the foreground client; attachers ride the lossy broadcast.
    let (fg_tx, fg_rx) = mpsc::channel::<Chunk>(FG_CAP);
    let pump_fg = (!req.detach).then(|| fg_tx.clone());
    let sup_fg = (!req.detach).then(|| fg_tx.clone());
    drop(fg_tx); // only the pump/supervise clones keep fg_rx open

    let pump = tokio::spawn(pump_out(Arc::clone(&proc), stdout, stderr, pump_fg));
    let pump_abort = pump.abort_handle();
    tokio::spawn(pump_stdin(stdin, client, req.detach));

    // record the reaped code before the drain: an abort mid-drain must publish the real code, not -1.
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
        // client gone: publish the terminal state the abort pre-empts, else an attacher waits forever.
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
    let mut frame = Vec::new();
    while let Some(chunk) = rx.recv().await {
        match chunk {
            Chunk::Exit(_) => return Ok(()),
            Chunk::Stdout(data) => {
                crate::proto::write_chunk_frame(out, &mut frame, "stdout", &data).await?
            }
            Chunk::Stderr(data) => {
                crate::proto::write_chunk_frame(out, &mut frame, "stderr", &data).await?
            }
        }
    }
    Ok(())
}

/// Reads both streams in this one task, so a nested spawn cannot outlive the pump abort.
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
        let Some(fg) = fg else {
            proc.emit_bytes(stderr, &buf[..n]);
            continue;
        };
        let chunk = make(buf[..n].to_vec());
        proc.emit(&chunk);
        if fg.send(chunk).await.is_err() {
            break;
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
        return;
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
