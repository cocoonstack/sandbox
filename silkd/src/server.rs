//! Connection handling: dispatch request frames back to back, forwarding each RPC's client input over a channel, until the peer closes.

use std::borrow::Cow;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::proc::{Chunk, Table, write_chunk};
use crate::proto::{self, ErrorKind, ExecReq, ProcInfo, Request, Response};
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
        let mut line = Vec::new();
        let mut next: Option<Request> = None;
        let mut served = false;
        loop {
            let req = match next.take() {
                Some(req) => req,
                None => {
                    if !proto::read_frame_into(&mut reader, &mut line).await? {
                        return Ok(());
                    }
                    match serde_json::from_slice(&line) {
                        Ok(req) => req,
                        Err(e) => {
                            return proto::error_frame(
                                &mut writer,
                                ErrorKind::BadRequest,
                                format!("parse: {e}"),
                            )
                            .await;
                        }
                    }
                }
            };
            // a finished RPC's input frames can still be in flight
            if served && req.is_continuation() {
                continue;
            }
            served = true;
            (reader, next) = self.dispatch(req, reader, &mut writer).await?;
        }
    }

    /// Runs one RPC and hands the reader back with the request the feeder read past it, if any.
    async fn dispatch<R, W>(
        &self,
        req: Request,
        mut reader: R,
        writer: &mut W,
    ) -> std::io::Result<(R, Option<Request>)>
    where
        R: AsyncBufRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        let res = match req {
            Request::Info => self.info(writer).await,
            Request::Ps => self.ps(writer).await,
            Request::Kill { pid, signal } => self.kill(writer, pid, signal).await,
            Request::Logs { pid } => self.logs(writer, pid).await,
            Request::Attach { pid } => self.attach(writer, pid).await,
            Request::Exec(e) => return self.exec(e, reader, writer).await,
            Request::SessionCreate { id, cwd, env } => {
                match self.sessions.create(id, cwd, &env).await {
                    Ok(id) => proto::write_frame(writer, &Response::SessionCreated { id }).await,
                    Err(e) => {
                        let kind = if e.kind() == std::io::ErrorKind::InvalidInput {
                            ErrorKind::BadRequest
                        } else {
                            ErrorKind::Internal
                        };
                        proto::error_frame(writer, kind, format!("session create: {e}")).await
                    }
                }
            }
            Request::SessionList => {
                proto::write_frame(
                    writer,
                    &Response::Sessions {
                        sessions: self.sessions.list(),
                    },
                )
                .await
            }
            Request::SessionRm { id } => {
                if self.sessions.remove(&id) {
                    proto::write_frame(writer, &Response::Done).await
                } else {
                    proto::error_frame(writer, ErrorKind::NotFound, "no such session").await
                }
            }
            Request::FsWrite { path, mode } => fs::write(&mut reader, writer, path, mode).await,
            Request::FsRead { path } => fs::read(writer, path).await,
            Request::FsList { path } => fs::list(writer, path).await,
            Request::FsStat { path } => fs::stat(writer, path).await,
            Request::FsMkdir { path, parents } => fs::mkdir(writer, path, parents).await,
            Request::FsRm { path, recursive } => fs::rm(writer, path, recursive).await,
            Request::FsRename { from, to } => fs::rename(writer, from, to).await,
            Request::FsPush { dest } => tree::push(&mut reader, writer, dest).await,
            Request::FsPull { path } => tree::pull(writer, path).await,
            Request::FsFind {
                path,
                pattern,
                glob,
            } => find::find(&mut reader, writer, path, pattern, glob).await,
            Request::FsReplace {
                files,
                pattern,
                replacement,
            } => find::replace(writer, files, pattern, replacement).await,
            Request::FsWatch { path, recursive } => {
                watch::watch(&mut reader, writer, path, recursive).await
            }
            Request::PtyOpen(req) => {
                let (rx, feeder) = spawn_feeder(reader);
                let res = pty::open(&self.table, now_secs(), req, rx, writer).await;
                return finish_feeder(feeder, res).await;
            }
            Request::PtyResize { pid, cols, rows } => {
                pty::resize(&self.table, writer, pid, cols, rows).await
            }
            Request::LspStart { language, root } => {
                self.lsp.start(writer, &language, root.as_deref()).await
            }
            Request::LspRequest { server_id } => {
                let (rx, feeder) = spawn_feeder(reader);
                let res = self.lsp.request(rx, writer, &server_id).await;
                return finish_feeder(feeder, res).await;
            }
            Request::LspStop { server_id } => self.lsp.stop(writer, &server_id).await,
            Request::PortForward { port } => {
                let (rx, feeder) = spawn_feeder(reader);
                let res = forward::run(port, rx, writer).await;
                return finish_feeder(feeder, res).await;
            }
            Request::GitClone {
                url,
                path,
                branch,
                depth,
                auth,
            } => git::clone(writer, url, path, branch, depth, auth).await,
            Request::GitStatus { path } => git::status(writer, path).await,
            Request::GitAdd { path, files } => git::add(writer, path, files).await,
            Request::GitCommit {
                path,
                message,
                author,
            } => git::commit(writer, path, message, author).await,
            Request::GitPush { path, auth } => git::push(writer, path, auth).await,
            Request::GitPull { path, auth } => git::pull(writer, path, auth).await,
            Request::GitBranch { path, action, name } => {
                git::branch(writer, path, action, name).await
            }
            Request::Stdin { .. }
            | Request::StdinClose
            | Request::Data { .. }
            | Request::DataEnd => {
                proto::error_frame(writer, ErrorKind::BadRequest, "stray data/stdin frame").await
            }
        };
        res.map(|()| (reader, None))
    }

    async fn exec<R, W>(
        &self,
        e: ExecReq,
        reader: R,
        writer: &mut W,
    ) -> std::io::Result<(R, Option<Request>)>
    where
        R: AsyncBufRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        // validated ahead of the session/process split so an empty argv errors on both paths.
        if e.argv.is_empty() {
            proto::error_frame(writer, ErrorKind::BadRequest, "argv must not be empty").await?;
            return Ok((reader, None));
        }
        if e.detach && e.session.is_some() {
            proto::error_frame(
                writer,
                ErrorKind::BadRequest,
                "detach is not supported with session",
            )
            .await?;
            return Ok((reader, None));
        }
        if let Some(sid) = e.session.as_deref() {
            match self.sessions.get(sid) {
                Some(sess) => {
                    if !sess.run(&e.argv, writer).await {
                        self.sessions.remove(sid);
                    }
                }
                None => proto::error_frame(writer, ErrorKind::NotFound, "no such session").await?,
            }
            return Ok((reader, None));
        }
        let (rx, feeder) = spawn_feeder(reader);
        let res = exec::run(&self.table, now_secs(), e, rx, writer).await;
        finish_feeder(feeder, res).await
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
        // an exited pid may already be recycled, so signalling it would hit an unrelated process.
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
        // snapshot replay and subscribe atomically so a chunk lands in exactly one of them.
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

type Feeder<R> = JoinHandle<(R, Option<Request>)>;

fn spawn_feeder<R>(reader: R) -> (mpsc::Receiver<Request>, Feeder<R>)
where
    R: AsyncBufRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(16);
    (rx, tokio::spawn(feed_client(reader, tx)))
}

/// Joins the feeder once its RPC is done; a failed RPC aborts it instead, the connection being gone.
async fn finish_feeder<R>(
    feeder: Feeder<R>,
    res: std::io::Result<()>,
) -> std::io::Result<(R, Option<Request>)> {
    if let Err(e) = res {
        feeder.abort();
        return Err(e);
    }
    feeder.await.map_err(std::io::Error::other)
}

/// Reads the client's frames during a streaming RPC: input frames go to the handler, the next request ends the feed.
async fn feed_client<R>(mut reader: R, tx: mpsc::Sender<Request>) -> (R, Option<Request>)
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    while let Ok(true) = proto::read_frame_into(&mut reader, &mut line).await {
        let Ok(req) = serde_json::from_slice::<Request>(&line) else {
            break;
        };
        if !req.is_continuation() {
            return (reader, Some(req));
        }
        // a closed channel is a finished handler, whose late input is dropped
        let _ = tx.send(req).await;
    }
    (reader, None)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
