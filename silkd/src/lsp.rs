//! LSP broker: silkd spawns the language server named by
//! `/etc/silkd/lsp.d/<language>` and pipes JSON-RPC bytes, never parsing LSP
//! semantics. A session is single-shot: the stream ending reaps the server.

use std::borrow::Cow;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, Request, Response};
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
    pub fn new() -> Self {
        Self::default()
    }

    /// start spawns the manifested language server for `language` and returns its id.
    pub async fn start<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        language: &str,
        root: Option<&str>,
    ) -> std::io::Result<()> {
        let argv = match read_manifest(language).await {
            Some(argv) if !argv.is_empty() => argv,
            _ => {
                return proto::error_frame(
                    w,
                    ErrorKind::NotFound,
                    format!("no language server for {language:?}; a flavor image provides {}/{language}", manifest_dir()),
                )
                .await;
            }
        };
        let mut cmd = Command::new(&argv[0]);
        sysutil::align_proxy_env(&mut cmd);
        cmd.args(&argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(dir) = root {
            cmd.current_dir(dir);
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => return proto::err_frame(w, &e, "spawn language server").await,
        };
        let Some((stdin, stdout)) = child.stdin.take().zip(child.stdout.take()) else {
            let _ = child.start_kill();
            return proto::error_frame(w, ErrorKind::Internal, "language server stdio not piped")
                .await;
        };
        let server = Server {
            child,
            stdin: Some(stdin),
            stdout: Some(stdout),
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

    /// request attaches this connection to the server's stdio and reaps the server on close.
    pub async fn request<W: AsyncWrite + Unpin>(
        &self,
        client: mpsc::Receiver<Request>,
        w: &mut W,
        server_id: &str,
    ) -> std::io::Result<()> {
        let taken = sysutil::lock(&self.inner)
            .get_mut(server_id)
            .map(|s| s.stdout.take().zip(s.stdin.take()));
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

        let feed = tokio::spawn(feed_stdin(client, stdin));
        let res = pump_stdout(stdout, w).await;
        // aborting mid-write is safe: the child is killed next.
        feed.abort();
        let _ = self.reap(server_id).await;
        res
    }

    /// stop kills a server and reaps it.
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

    /// reap for a server no `lsp_request` ever attached.
    async fn reap_unattached(&self, server_id: &str) {
        let removed = {
            let mut table = sysutil::lock(&self.inner);
            match table.get(server_id) {
                Some(server) if server.stdout.is_some() => table.remove(server_id),
                _ => None,
            }
        };
        if let Some(server) = removed {
            kill(server).await;
        }
    }
}

/// One running language server; a present `stdout` means no connection has attached.
struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
}

async fn kill(mut server: Server) {
    let _ = server.child.start_kill();
    let _ = server.child.wait().await;
}

async fn feed_stdin(mut client: mpsc::Receiver<Request>, mut stdin: ChildStdin) {
    while let Some(req) = client.recv().await {
        // a stray frame closes rather than errors: this task has no writer.
        let Request::Data { data } = req else { return };
        if stdin.write_all(&data).await.is_err() || stdin.flush().await.is_err() {
            return;
        }
    }
}

async fn pump_stdout<W: AsyncWrite + Unpin>(
    mut stdout: ChildStdout,
    w: &mut W,
) -> std::io::Result<()> {
    if let Err(e) = proto::stream_data_frames(&mut stdout, w).await? {
        return proto::err_frame(w, &e, "read lsp stdout").await;
    }
    proto::write_frame(w, &Response::Done).await
}

/// SILKD_LSP_DIR relocates the manifest dir for tests and operators.
fn manifest_dir() -> Cow<'static, str> {
    std::env::var("SILKD_LSP_DIR").map_or(Cow::Borrowed(MANIFEST_DIR), Cow::Owned)
}

async fn read_manifest(language: &str) -> Option<Vec<String>> {
    if language.is_empty() || language.contains(['/', '\\', '\0']) || language.contains("..") {
        return None; // never let a language name escape the manifest dir
    }
    // O_NOFOLLOW: a symlink in lsp.d must not read a target outside it.
    let mut f = tokio::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(format!("{}/{language}", manifest_dir()))
        .await
        .ok()?;
    let mut raw = String::new();
    f.read_to_string(&mut raw).await.ok()?;
    let argv: Vec<String> = raw.split_whitespace().map(str::to_string).collect();
    (!argv.is_empty()).then_some(argv)
}
