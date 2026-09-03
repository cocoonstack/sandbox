//! Connection handling: read the leading request frame, dispatch to a
//! handler, and for exec forward subsequent client frames over a channel.

use std::borrow::Cow;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::proc::{Chunk, Table};
use crate::proto::{self, ErrorKind, ProcInfo, Request, Response};
use crate::{exec, find, forward, fs, git, lsp, pty, session, tree, watch};

/// Shared daemon state handed to every connection.
pub struct State {
    pub table: Table,
    pub sessions: session::Table,
    pub lsp: lsp::Broker,
    pub started: Instant,
}

impl State {
    pub fn new() -> Self {
        Self {
            table: Table::new(),
            sessions: session::Table::new(),
            lsp: lsp::Broker::new(),
            started: Instant::now(),
        }
    }

    pub async fn serve<R, W>(&self, mut reader: R, mut writer: W) -> std::io::Result<()>
    where
        R: AsyncBufRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        let Some(first) = proto::read_frame(&mut reader).await? else {
            return Ok(());
        };
        let req: Request = match serde_json::from_slice(&first) {
            Ok(r) => r,
            Err(e) => {
                return proto::error_frame(
                    &mut writer,
                    ErrorKind::BadRequest,
                    format!("parse: {e}"),
                )
                .await;
            }
        };

        match req {
            Request::Info => self.info(&mut writer).await,
            Request::Ps => self.ps(&mut writer).await,
            Request::Kill { pid, signal } => self.kill(&mut writer, pid, signal).await,
            Request::Logs { pid } => self.logs(&mut writer, pid).await,
            Request::Attach { pid } => self.attach(&mut writer, pid).await,
            Request::Exec(e) => {
                // Validated here, ahead of the session/process split, so an
                // empty argv answers the same error on both paths (the session
                // shell would otherwise run it as a successful no-op).
                if e.argv.is_empty() {
                    return proto::error_frame(
                        &mut writer,
                        ErrorKind::BadRequest,
                        "argv must not be empty",
                    )
                    .await;
                }
                if e.detach && e.session.is_some() {
                    return proto::error_frame(
                        &mut writer,
                        ErrorKind::BadRequest,
                        "detach is not supported with session",
                    )
                    .await;
                }
                if let Some(sid) = e.session.as_deref() {
                    let Some(sess) = self.sessions.get(sid) else {
                        return proto::error_frame(
                            &mut writer,
                            ErrorKind::NotFound,
                            "no such session",
                        )
                        .await;
                    };
                    if !sess.run(&e.argv, &mut writer).await {
                        self.sessions.remove(sid);
                    }
                    Ok(())
                } else {
                    let (rx, _feeder) = spawn_feeder(reader);
                    exec::run(&self.table, now_secs(), e, rx, &mut writer).await
                }
            }
            Request::SessionCreate { id, cwd, env } => {
                match self.sessions.create(id, cwd, &env).await {
                    Ok(id) => {
                        proto::write_frame(&mut writer, &Response::SessionCreated { id }).await
                    }
                    Err(e) => {
                        proto::error_frame(
                            &mut writer,
                            ErrorKind::Internal,
                            format!("session create: {e}"),
                        )
                        .await
                    }
                }
            }
            Request::SessionList => {
                proto::write_frame(
                    &mut writer,
                    &Response::Sessions {
                        sessions: self.sessions.list(),
                    },
                )
                .await
            }
            Request::SessionRm { id } => {
                if self.sessions.remove(&id) {
                    proto::write_frame(&mut writer, &Response::Done).await
                } else {
                    proto::error_frame(&mut writer, ErrorKind::NotFound, "no such session").await
                }
            }
            Request::FsWrite { path, mode } => fs::write(reader, &mut writer, path, mode).await,
            Request::FsRead { path } => fs::read(&mut writer, path).await,
            Request::FsList { path } => fs::list(&mut writer, path).await,
            Request::FsStat { path } => fs::stat(&mut writer, path).await,
            Request::FsMkdir { path, parents } => fs::mkdir(&mut writer, path, parents).await,
            Request::FsRm { path, recursive } => fs::rm(&mut writer, path, recursive).await,
            Request::FsRename { from, to } => fs::rename(&mut writer, from, to).await,
            Request::FsPush { dest } => tree::push(reader, &mut writer, dest).await,
            Request::FsPull { path } => tree::pull(&mut writer, path).await,
            Request::FsFind {
                path,
                pattern,
                glob,
            } => find::find(&mut reader, &mut writer, path, pattern, glob).await,
            Request::FsReplace {
                files,
                pattern,
                replacement,
            } => find::replace(&mut writer, files, pattern, replacement).await,
            Request::FsWatch { path, recursive } => {
                watch::watch(&mut reader, &mut writer, path, recursive).await
            }
            Request::PtyOpen(req) => {
                let (rx, _feeder) = spawn_feeder(reader);
                pty::open(&self.table, now_secs(), req, rx, &mut writer).await
            }
            Request::PtyResize { pid, cols, rows } => {
                pty::resize(&self.table, &mut writer, pid, cols, rows).await
            }
            Request::LspStart { language, root } => {
                self.lsp
                    .start(&mut writer, &language, root.as_deref())
                    .await
            }
            Request::LspRequest { server_id } => {
                let (rx, _feeder) = spawn_feeder(reader);
                self.lsp.request(rx, &mut writer, &server_id).await
            }
            Request::LspStop { server_id } => self.lsp.stop(&mut writer, &server_id).await,
            Request::PortForward { port } => {
                let (rx, _feeder) = spawn_feeder(reader);
                forward::run(port, rx, &mut writer).await
            }
            Request::GitClone {
                url,
                path,
                branch,
                depth,
                auth,
            } => git::clone(&mut writer, url, path, branch, depth, auth).await,
            Request::GitStatus { path } => git::status(&mut writer, path).await,
            Request::GitAdd { path, files } => git::add(&mut writer, path, files).await,
            Request::GitCommit {
                path,
                message,
                author,
            } => git::commit(&mut writer, path, message, author).await,
            Request::GitPush { path, auth } => git::push(&mut writer, path, auth).await,
            Request::GitPull { path, auth } => git::pull(&mut writer, path, auth).await,
            Request::GitBranch { path, action, name } => {
                git::branch(&mut writer, path, action, name).await
            }
            Request::Stdin { .. }
            | Request::StdinClose
            | Request::Data { .. }
            | Request::DataEnd => {
                proto::error_frame(&mut writer, ErrorKind::BadRequest, "stray data/stdin frame")
                    .await
            }
        }
    }

    async fn info<W: AsyncWrite + Unpin>(&self, w: &mut W) -> std::io::Result<()> {
        proto::write_frame(
            w,
            &Response::Info {
                version: Cow::Borrowed(env!("CARGO_PKG_VERSION")),
                proto: proto::PROTO_VERSION,
                uptime_secs: self.started.elapsed().as_secs(),
                procs: self.table.len(),
                sessions: self.sessions.len(),
            },
        )
        .await
    }

    async fn ps<W: AsyncWrite + Unpin>(&self, w: &mut W) -> std::io::Result<()> {
        let mut procs: Vec<ProcInfo> = self.table.list();
        procs.sort_by_key(|p| p.pid);
        proto::write_frame(w, &Response::Procs { procs }).await
    }

    async fn kill<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        pid: u32,
        signal: Option<i32>,
    ) -> std::io::Result<()> {
        let Some(proc) = self.table.get_or_not_found(w, pid).await? else {
            return Ok(());
        };
        // An exited process's pid may already be recycled by the guest OS —
        // signalling it would hit an unrelated process, so treat kill as a
        // no-op success once the child is reaped.
        if proc.exit_code().is_none() {
            crate::sysutil::signal_pid(proc.pid, signal.unwrap_or(libc::SIGKILL));
        }
        proto::write_frame(w, &Response::Done).await
    }

    async fn logs<W: AsyncWrite + Unpin>(&self, w: &mut W, pid: u32) -> std::io::Result<()> {
        let Some(proc) = self.table.get_or_not_found(w, pid).await? else {
            return Ok(());
        };
        let mut frame = Vec::new();
        for chunk in proc.replay() {
            write_chunk(w, &mut frame, chunk).await?;
        }
        if let Some(code) = proc.exit_code() {
            proto::write_frame(w, &Response::Exit { code }).await?;
        }
        proto::write_frame(w, &Response::Done).await
    }

    async fn attach<W: AsyncWrite + Unpin>(&self, w: &mut W, pid: u32) -> std::io::Result<()> {
        let Some(proc) = self.table.get_or_not_found(w, pid).await? else {
            return Ok(());
        };
        // Snapshot replay and subscribe atomically so a chunk emitted right
        // now lands in one or the other, never both; if already exited, report
        // the code instead of waiting on an Exit that fired before we listened.
        let (replay, mut rx) = proc.attach_stream();
        let mut frame = Vec::new();
        for chunk in replay {
            write_chunk(w, &mut frame, chunk).await?;
        }
        if let Some(code) = proc.exit_code() {
            return proto::write_frame(w, &Response::Exit { code }).await;
        }
        loop {
            match rx.recv().await {
                Ok(Chunk::Exit(code)) => {
                    return proto::write_frame(w, &Response::Exit { code }).await;
                }
                Ok(chunk) => write_chunk(w, &mut frame, chunk).await?,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return proto::write_frame(w, &Response::Done).await;
                }
            }
        }
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

/// Aborts the spawned feeder on drop, so no streaming arm can leak the task
/// (and the reader half it owns) on any return path.
struct Feeder(tokio::task::JoinHandle<()>);

impl Drop for Feeder {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn spawn_feeder<R>(reader: R) -> (mpsc::Receiver<Request>, Feeder)
where
    R: AsyncBufRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(16);
    (rx, Feeder(tokio::spawn(feed_client(reader, tx))))
}

/// Stdout/Stderr ride the reused-buffer bulk path (serde's per-chunk Vec +
/// base64 String otherwise dominate replay/attach); Exit stays a one-off frame.
async fn write_chunk<W: AsyncWrite + Unpin>(
    w: &mut W,
    buf: &mut Vec<u8>,
    chunk: Chunk,
) -> std::io::Result<()> {
    match chunk {
        Chunk::Stdout(data) => proto::write_chunk_frame(w, buf, "stdout", &data).await,
        Chunk::Stderr(data) => proto::write_chunk_frame(w, buf, "stderr", &data).await,
        other => proto::write_frame(w, &other.into_response()).await,
    }
}

/// Forwards post-request client frames (stdin/stdin_close) to the exec
/// handler until the connection half-closes.
async fn feed_client<R>(mut reader: R, tx: mpsc::Sender<Request>)
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    while let Ok(true) = proto::read_frame_into(&mut reader, &mut line).await {
        match serde_json::from_slice::<Request>(&line) {
            Ok(req) => {
                if tx.send(req).await.is_err() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
