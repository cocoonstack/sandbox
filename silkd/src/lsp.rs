//! LSP broker: silkd spawns the language server a flavor image ships for a
//! language (its argv named in `/etc/silkd/lsp.d/<language>`), keeps it
//! running addressed by a server id, and relays JSON-RPC bytes between the
//! client and the server's stdio. silkd is a broker, not a gateway: it never
//! parses LSP method semantics — the server multiplexes, silkd just pipes.
//!
//! `lsp_start` spawns and returns an id; `lsp_request` attaches the current
//! connection's byte stream to that server (one client at a time, the LSP
//! model) and pumps until either side closes; `lsp_stop` kills it. The base
//! image ships no manifests, so `lsp_start` for any language there answers a
//! typed `not_found` naming the flavor that provides one.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, Request, Response, READ_CHUNK};

const MANIFEST_DIR: &str = "/etc/silkd/lsp.d";

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Table of running language servers, addressed by server id across
/// connections so an LSP session survives a dropped `lsp_request`.
#[derive(Clone, Default)]
pub struct Broker {
    inner: Arc<Mutex<HashMap<String, Server>>>,
}

impl Broker {
    pub fn new() -> Self {
        Self::default()
    }

    /// start spawns the manifested language server for `language`, rooted at
    /// `root`, and returns its id. Absent manifest → typed not_found.
    pub async fn start<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        language: &str,
        root: Option<&str>,
    ) -> std::io::Result<()> {
        let argv = match read_manifest(language) {
            Some(argv) if !argv.is_empty() => argv,
            _ => {
                return proto::error_frame(
                    w,
                    ErrorKind::NotFound,
                    format!("no language server for {language:?}; a flavor image provides {MANIFEST_DIR}/{language}"),
                )
                .await;
            }
        };
        let mut cmd = Command::new(&argv[0]);
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
        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");
        let id = format!("lsp-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
        self.inner.lock().unwrap().insert(
            id.clone(),
            Server {
                child: Arc::new(Mutex::new(child)),
                stdin: Arc::new(tokio::sync::Mutex::new(stdin)),
                stdout: Arc::new(tokio::sync::Mutex::new(Some(stdout))),
            },
        );
        proto::write_frame(w, &Response::LspStarted { server_id: id }).await
    }

    /// request attaches this connection to the server's stdio: client `data`
    /// frames feed its stdin, its stdout streams back as `data` frames, until
    /// either side closes. The server keeps running for a later reattach.
    pub async fn request<W: AsyncWrite + Unpin>(
        &self,
        client: mpsc::Receiver<Request>,
        w: &mut W,
        server_id: &str,
    ) -> std::io::Result<()> {
        let found = self.inner.lock().unwrap().get(server_id).cloned();
        let server = match found {
            Some(s) => s,
            None => return proto::error_frame(w, ErrorKind::NotFound, "no such lsp server").await,
        };
        let stdout = match server.stdout.lock().await.take() {
            Some(out) => out,
            None => {
                return proto::error_frame(w, ErrorKind::BadRequest, "lsp server already attached")
                    .await;
            }
        };
        proto::write_frame(w, &Response::Ready).await?;

        let feed = tokio::spawn(feed_stdin(client, server.stdin.clone()));
        let (res, stdout) = pump_stdout(stdout, w).await;
        feed.abort();
        // Hand the stdout back so a later lsp_request can reattach.
        *server.stdout.lock().await = Some(stdout);
        res
    }

    /// stop kills a server and drops it from the table.
    pub async fn stop<W: AsyncWrite + Unpin>(
        &self,
        w: &mut W,
        server_id: &str,
    ) -> std::io::Result<()> {
        let removed = self.inner.lock().unwrap().remove(server_id);
        match removed {
            Some(server) => {
                let _ = server.child.lock().unwrap().start_kill();
                proto::write_frame(w, &Response::Done).await
            }
            None => proto::error_frame(w, ErrorKind::NotFound, "no such lsp server").await,
        }
    }
}

/// One running language server: its child handle plus stdio, shared so
/// `lsp_request` can reattach and `lsp_stop` can kill.
#[derive(Clone)]
struct Server {
    child: Arc<Mutex<Child>>,
    stdin: Arc<tokio::sync::Mutex<ChildStdin>>,
    stdout: Arc<tokio::sync::Mutex<Option<ChildStdout>>>,
}

async fn feed_stdin(
    mut client: mpsc::Receiver<Request>,
    stdin: Arc<tokio::sync::Mutex<ChildStdin>>,
) {
    while let Some(req) = client.recv().await {
        match req {
            Request::Data { data } => {
                let mut guard = stdin.lock().await;
                if guard.write_all(&data).await.is_err() || guard.flush().await.is_err() {
                    return;
                }
            }
            Request::DataEnd => return,
            _ => return, // a stray non-data frame ends the LSP stream
        }
    }
}

async fn pump_stdout<W: AsyncWrite + Unpin>(
    mut stdout: ChildStdout,
    w: &mut W,
) -> (std::io::Result<()>, ChildStdout) {
    let mut buf = vec![0u8; READ_CHUNK];
    let res = loop {
        match stdout.read(&mut buf).await {
            Ok(0) => break proto::write_frame(w, &Response::Done).await,
            Ok(n) => {
                if let Err(e) = proto::write_frame(
                    w,
                    &Response::Data {
                        data: buf[..n].to_vec(),
                    },
                )
                .await
                {
                    break Err(e);
                }
            }
            Err(e) => break proto::err_frame(w, &e, "read lsp stdout").await,
        }
    };
    (res, stdout)
}

fn read_manifest(language: &str) -> Option<Vec<String>> {
    if language.contains('/') || language.contains("..") {
        return None; // never let a language name escape the manifest dir
    }
    let raw = std::fs::read_to_string(format!("{MANIFEST_DIR}/{language}")).ok()?;
    let argv: Vec<String> = raw.split_whitespace().map(str::to_string).collect();
    (!argv.is_empty()).then_some(argv)
}
