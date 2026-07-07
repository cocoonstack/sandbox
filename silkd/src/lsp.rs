//! LSP broker: silkd spawns the language server a flavor image ships for a
//! language (its argv named in `/etc/silkd/lsp.d/<language>`), keeps it
//! running addressed by a server id, and relays JSON-RPC bytes between the
//! client and the server's stdio. silkd is a broker, not a gateway: it never
//! parses LSP method semantics — the server multiplexes, silkd just pipes.
//!
//! `lsp_start` spawns and returns an id; `lsp_request` attaches the byte
//! stream and pumps until either side closes; `lsp_stop` kills it. v1 is
//! single-shot per server: the stream ending — clean or dropped — reaps the
//! server, since an LSP stream loses frame sync on any mid-request cut and a
//! resynced reattach is not worth the failure surface. The id and `lsp_stop`
//! still earn their place: `lsp_start` for several languages yields several
//! ids attached concurrently, and `lsp_stop` tears one down early. The base
//! image ships no manifests, so `lsp_start` for any language there answers a
//! typed `not_found` naming the flavor that provides one.

use std::collections::HashMap;
use std::os::unix::fs::OpenOptionsExt;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, Request, Response, READ_CHUNK};

const MANIFEST_DIR: &str = "/etc/silkd/lsp.d";

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
                    format!("no language server for {language:?}; a flavor image provides {}/{language}", manifest_dir()),
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
        let server = Server {
            child: Arc::new(tokio::sync::Mutex::new(child)),
            stdin: Arc::new(tokio::sync::Mutex::new(stdin)),
            stdout: Arc::new(tokio::sync::Mutex::new(Some(stdout))),
        };

        let id = format!("lsp-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
        self.inner.lock().unwrap().insert(id.clone(), server);

        // If the client is already gone, don't leak the server we just spawned.
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
        Ok(())
    }

    /// request attaches this connection to the server's stdio: client `data`
    /// frames feed its stdin, its stdout streams back as `data` frames. When
    /// either side closes the session is over — the server is reaped, so a
    /// dropped connection never leaves a half-written stdin for a next caller.
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
        let res = pump_stdout(stdout, w).await;
        // The session ends with this connection: stop feeding and reap the
        // server. Aborting is safe here — we are killing the child, so a
        // partially-written stdin frame goes nowhere.
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

    /// reap removes a server from the table and kills + waits its child (no
    /// zombie survives), reporting whether it was there.
    async fn reap(&self, server_id: &str) -> bool {
        let removed = self.inner.lock().unwrap().remove(server_id);
        if let Some(server) = removed {
            let mut child = server.child.lock().await;
            let _ = child.start_kill();
            let _ = child.wait().await;
            true
        } else {
            false
        }
    }
}

/// One running language server: child + stdio, shared so `lsp_request` reads
/// stdout, `feed_stdin` writes stdin, and `reap` kills the child.
#[derive(Clone)]
struct Server {
    child: Arc<tokio::sync::Mutex<Child>>,
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
) -> std::io::Result<()> {
    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        match stdout.read(&mut buf).await {
            Ok(0) => return proto::write_frame(w, &Response::Done).await,
            Ok(n) => {
                proto::write_frame(
                    w,
                    &Response::Data {
                        data: buf[..n].to_vec(),
                    },
                )
                .await?
            }
            Err(e) => return proto::err_frame(w, &e, "read lsp stdout").await,
        }
    }
}

// manifest_dir allows tests (and an operator) to relocate the manifest dir
// via SILKD_LSP_DIR, mirroring silkd's other env overrides.
fn manifest_dir() -> String {
    std::env::var("SILKD_LSP_DIR").unwrap_or_else(|_| MANIFEST_DIR.to_string())
}

fn read_manifest(language: &str) -> Option<Vec<String>> {
    if language.is_empty() || language.contains(['/', '\\', '\0']) || language.contains("..") {
        return None; // never let a language name escape the manifest dir
    }
    // O_NOFOLLOW: a symlink planted inside lsp.d must not read a target
    // outside it.
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(format!("{}/{language}", manifest_dir()))
        .ok()?;
    let mut raw = String::new();
    std::io::Read::read_to_string(&mut f, &mut raw).ok()?;
    let argv: Vec<String> = raw.split_whitespace().map(str::to_string).collect();
    (!argv.is_empty()).then_some(argv)
}
