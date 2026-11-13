//! LSP broker: spawns the server `/etc/silkd/lsp.d/<language>` names and pipes JSON-RPC bytes; the stream ending reaps it.

use std::borrow::Cow;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::proto::{self, BULK_CHUNK, ErrorKind, Request, Response};
use crate::sysutil;

const MANIFEST_DIR: &str = "/etc/silkd/lsp.d";

/// Reap TTL for an unattached server: a dead client must not pin a 1-2 GB server.
const ATTACH_TTL: Duration = Duration::from_secs(300);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Table of running language servers, addressed by server id.
#[derive(Clone, Default)]
pub struct Broker {
    inner: Arc<Mutex<HashMap<String, Server>>>,
}

impl Broker {
    /// Spawns the manifested language server for `language` and returns its id.
    pub async fn start<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        language: &str,
        root: Option<&str>,
    ) -> std::io::Result<()> {
        let raw = read_manifest(language).await.unwrap_or_default();
        let mut argv = raw.split_whitespace();
        let Some(program) = argv.next() else {
            return proto::error_frame(
                w,
                ErrorKind::NotFound,
                format!(
                    "no language server for {language:?}; a flavor image provides {}/{language}",
                    manifest_dir()
                ),
            )
            .await;
        };
        let mut cmd = Command::new(program);
        sysutil::align_proxy_env(&mut cmd);
        cmd.args(argv)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);
        if let Some(dir) = root {
            cmd.current_dir(dir);
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => return proto::err_frame(w, &e, "spawn language server").await,
        };
        sysutil::reset_nice(&child);
        let stdio = sysutil::take_pipe(child.stdout.take(), "stdout")
            .and_then(|out| Ok((out, sysutil::take_pipe(child.stdin.take(), "stdin")?)));
        let stdio = match stdio {
            Ok(stdio) => stdio,
            Err(e) => {
                let _ = child.start_kill();
                return proto::err_frame(w, &e, "spawn language server").await;
            }
        };
        let server = Server {
            child,
            stdio: Some(stdio),
        };

        let id = format!("lsp-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
        sysutil::lock(&self.inner).insert(id.clone(), server);

        // if the client is already gone, do not leak the server just spawned.
        if let Err(e) = proto::write_frame(
            w,
            &Response::LspStarted {
                server_id: id.clone(),
            },
        )
        .await
        {
            self.reap(&id).await;
            return Err(e);
        }
        let broker = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(ATTACH_TTL).await;
            broker.reap_unattached(&id).await;
        });
        Ok(())
    }

    /// Attaches this connection to the server's stdio and reaps the server on close.
    pub async fn request<W: AsyncWrite + Unpin>(
        &self,
        client: mpsc::Receiver<Request>,
        w: &mut W,
        server_id: &str,
    ) -> std::io::Result<()> {
        let taken = sysutil::lock(&self.inner)
            .get_mut(server_id)
            .map(|s| s.stdio.take());
        let (stdout, stdin) = match taken {
            None => return proto::error_frame(w, ErrorKind::NotFound, "no such lsp server").await,
            Some(None) => {
                return proto::error_frame(w, ErrorKind::BadRequest, "lsp server already attached")
                    .await;
            }
            Some(Some(stdio)) => stdio,
        };
        // the attach TTL no longer covers a server whose stdio halves are taken.
        if let Err(e) = proto::write_frame(w, &Response::Ready).await {
            self.reap(server_id).await;
            return Err(e);
        }

        let mut feed = tokio::spawn(feed_stdin(client, stdin));
        let res = pump_stdout(stdout, w, &mut feed).await;
        // aborting mid-write is safe: the child is killed next.
        feed.abort();
        self.reap(server_id).await;
        res
    }

    /// Kills a server and reaps it.
    pub async fn stop<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        server_id: &str,
    ) -> std::io::Result<()> {
        if self.reap(server_id).await {
            proto::write_frame(w, &Response::Done).await
        } else {
            proto::error_frame(w, ErrorKind::NotFound, "no such lsp server").await
        }
    }

    async fn reap(&self, server_id: &str) -> bool {
        let removed = sysutil::lock(&self.inner).remove(server_id);
        match removed {
            Some(server) => {
                kill(server).await;
                true
            }
            None => false,
        }
    }

    /// Reaps a server no `lsp_request` ever attached.
    async fn reap_unattached(&self, server_id: &str) {
        let removed = {
            let mut table = sysutil::lock(&self.inner);
            match table.get(server_id) {
                Some(server) if server.stdio.is_some() => table.remove(server_id),
                _ => None,
            }
        };
        if let Some(server) = removed {
            kill(server).await;
        }
    }
}

/// One running language server; a present `stdio` means no connection has attached.
struct Server {
    child: Child,
    stdio: Option<(ChildStdout, ChildStdin)>,
}

async fn kill(mut server: Server) {
    if let Some(pid) = server.child.id() {
        sysutil::signal_group(pid, libc::SIGKILL);
    }
    let _ = server.child.start_kill();
    let _ = server.child.wait().await;
}

async fn feed_stdin(mut client: mpsc::Receiver<Request>, mut stdin: ChildStdin) {
    // a stray frame closes rather than errors: this task has no writer.
    while let Some(Request::Data { data }) = client.recv().await {
        if stdin.write_all(&data).await.is_err() || stdin.flush().await.is_err() {
            break;
        }
    }
    drop(stdin);
    while client.recv().await.is_some() {}
}

async fn pump_stdout<W: AsyncWrite + Unpin>(
    mut stdout: ChildStdout,
    w: &mut W,
    client_gone: &mut JoinHandle<()>,
) -> std::io::Result<()> {
    let mut buf = vec![0u8; BULK_CHUNK];
    let mut frame = Vec::new();
    loop {
        tokio::select! {
            r = stdout.read(&mut buf) => match r {
                Ok(0) => break,
                Ok(n) => proto::write_chunk_frame(w, &mut frame, "data", &buf[..n]).await?,
                Err(e) => return proto::err_frame(w, &e, "read lsp stdout").await,
            },
            _ = &mut *client_gone => break,
        }
    }
    proto::write_frame(w, &Response::Done).await
}

/// SILKD_LSP_DIR relocates the manifest dir for tests and operators.
fn manifest_dir() -> Cow<'static, str> {
    std::env::var("SILKD_LSP_DIR").map_or(Cow::Borrowed(MANIFEST_DIR), Cow::Owned)
}

async fn read_manifest(language: &str) -> Option<String> {
    if language.contains('/') {
        return None;
    }
    tokio::fs::read_to_string(format!("{}/{language}", manifest_dir()))
        .await
        .ok()
}
