//! Connection handling: read the leading request frame, dispatch to a
//! handler, and for exec forward subsequent client frames over a channel.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufRead, AsyncWrite, BufReader};
use tokio::sync::mpsc;

use crate::proc::{Chunk, Proc, Table};
use crate::proto::{self, ErrorKind, ProcInfo, Request, Response};
use crate::{exec, fs, session, tree};

/// Shared daemon state handed to every connection.
pub struct State {
    pub table: Table,
    pub sessions: session::Table,
    pub started: Instant,
}

impl State {
    pub fn new() -> Self {
        Self {
            table: Table::new(),
            sessions: session::Table::new(),
            started: Instant::now(),
        }
    }

    pub async fn serve<R, W>(self: &Arc<Self>, mut reader: R, mut writer: W) -> std::io::Result<()>
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
                return proto::write_frame(
                    &mut writer,
                    &Response::error(ErrorKind::BadRequest, format!("parse: {e}")),
                )
                .await
            }
        };

        match req {
            Request::Info => self.info(&mut writer).await,
            Request::Ps => self.ps(&mut writer).await,
            Request::Kill { pid, signal } => self.kill(&mut writer, pid, signal).await,
            Request::Logs { pid } => self.logs(&mut writer, pid).await,
            Request::Attach { pid } => self.attach(&mut writer, pid).await,
            Request::Exec(e) => {
                if let Some(sid) = e.session.as_deref() {
                    let Some(sess) = self.sessions.get(sid) else {
                        return proto::write_frame(
                            &mut writer,
                            &Response::error(ErrorKind::NotFound, "no such session"),
                        )
                        .await;
                    };
                    if !sess.run(&e.argv, &mut writer).await {
                        self.sessions.remove(sid);
                    }
                    Ok(())
                } else {
                    let (tx, rx) = mpsc::channel(16);
                    let feeder = tokio::spawn(feed_client(reader, tx));
                    let res = exec::run(&self.table, now_secs(), e, rx, &mut writer).await;
                    feeder.abort();
                    res
                }
            }
            Request::SessionCreate { id, cwd, env } => {
                match self.sessions.create(id, cwd, &env).await {
                    Ok(id) => {
                        proto::write_frame(&mut writer, &Response::SessionCreated { id }).await
                    }
                    Err(e) => {
                        proto::write_frame(
                            &mut writer,
                            &Response::error(ErrorKind::Internal, format!("session create: {e}")),
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
                    proto::write_frame(
                        &mut writer,
                        &Response::error(ErrorKind::NotFound, "no such session"),
                    )
                    .await
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
            Request::Stdin { .. }
            | Request::StdinClose
            | Request::Data { .. }
            | Request::DataEnd => {
                proto::write_frame(
                    &mut writer,
                    &Response::error(ErrorKind::BadRequest, "stray data/stdin frame"),
                )
                .await
            }
        }
    }

    async fn info<W: AsyncWrite + Unpin>(&self, w: &mut W) -> std::io::Result<()> {
        proto::write_frame(
            w,
            &Response::Info {
                version: env!("CARGO_PKG_VERSION").to_string(),
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
        let Some(proc) = self.get_or_not_found(w, pid).await? else {
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
        let Some(proc) = self.get_or_not_found(w, pid).await? else {
            return Ok(());
        };
        for chunk in proc.replay() {
            write_chunk(w, chunk).await?;
        }
        if let Some(code) = proc.exit_code() {
            proto::write_frame(w, &Response::Exit { code }).await?;
        }
        proto::write_frame(w, &Response::Done).await
    }

    async fn attach<W: AsyncWrite + Unpin>(&self, w: &mut W, pid: u32) -> std::io::Result<()> {
        let Some(proc) = self.get_or_not_found(w, pid).await? else {
            return Ok(());
        };
        // Snapshot replay and subscribe atomically so a chunk emitted right
        // now lands in one or the other, never both; if already exited, report
        // the code instead of waiting on an Exit that fired before we listened.
        let (replay, mut rx) = proc.attach_stream();
        for chunk in replay {
            write_chunk(w, chunk).await?;
        }
        if let Some(code) = proc.exit_code() {
            return proto::write_frame(w, &Response::Exit { code }).await;
        }
        loop {
            match rx.recv().await {
                Ok(Chunk::Exit(code)) => {
                    return proto::write_frame(w, &Response::Exit { code }).await
                }
                Ok(chunk) => write_chunk(w, chunk).await?,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return proto::write_frame(w, &Response::Done).await
                }
            }
        }
    }

    /// Looks up a pid, writing a NotFound frame and returning None on a miss —
    /// the shared prelude of kill/logs/attach.
    async fn get_or_not_found<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        pid: u32,
    ) -> std::io::Result<Option<Arc<Proc>>> {
        match self.table.get(pid) {
            Some(proc) => Ok(Some(proc)),
            None => {
                proto::write_frame(w, &Response::error(ErrorKind::NotFound, "no such pid")).await?;
                Ok(None)
            }
        }
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

async fn write_chunk<W: AsyncWrite + Unpin>(w: &mut W, chunk: Chunk) -> std::io::Result<()> {
    proto::write_frame(w, &chunk.into_response()).await
}

/// Forwards post-request client frames (stdin/stdin_close) to the exec
/// handler until the connection half-closes.
async fn feed_client<R>(mut reader: R, tx: mpsc::Sender<Request>)
where
    R: AsyncBufRead + Unpin,
{
    while let Ok(Some(bytes)) = proto::read_frame(&mut reader).await {
        match serde_json::from_slice::<Request>(&bytes) {
            Ok(req) => {
                if tx.send(req).await.is_err() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

/// Wrap any reader in the buffering the frame reader needs.
pub fn buffer<R: tokio::io::AsyncRead + Unpin>(r: R) -> BufReader<R> {
    BufReader::new(r)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
