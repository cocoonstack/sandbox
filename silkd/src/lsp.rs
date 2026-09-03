//! LSP broker: silkd spawns the language server a flavor image ships for a
//! language (its argv named in `/etc/silkd/lsp.d/<language>`), keeps it
//! running addressed by a server id, and relays JSON-RPC bytes between the
//! client and the server's stdio. silkd is a broker, not a gateway: it never
//! parses LSP method semantics — the server multiplexes, silkd just pipes.
//!
//! `lsp_start` spawns and returns an id; `lsp_request` attaches the byte
//! stream and pumps until either side closes (`data_end` half-closes the
//! server's stdin, as in port_forward); `lsp_stop` kills it. v1 is
//! single-shot per server: the stream ending — clean or dropped — reaps the
//! server, since an LSP stream loses frame sync on any mid-request cut and a
//! resynced reattach is not worth the failure surface. A server no
//! `lsp_request` ever attaches is reaped on a TTL. The id and `lsp_stop`
//! still earn their place: `lsp_start` for several languages yields several
//! ids attached concurrently, and `lsp_stop` tears one down early. The base
//! image ships no manifests, so `lsp_start` for any language there answers a
//! typed `not_found` naming the flavor that provides one.

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

/// A started server nothing attaches is reaped after this: a client that dies
/// between the calls must not pin a 1-2 GB language server for the VM's life.
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

    /// start spawns the manifested language server for `language`, rooted at
    /// `root`, and returns its id. Absent manifest → typed not_found.
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
        let broker = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(ATTACH_TTL).await;
            broker.reap_unattached(&id).await;
        });
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
        // The stdio halves are out of the table, so the attach TTL no longer
        // covers this server: a client already gone must reap it here.
        if let Err(e) = proto::write_frame(w, &Response::Ready).await {
            self.reap(server_id).await;
            return Err(e);
        }

        let feed = tokio::spawn(feed_stdin(client, stdin));
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
        let removed = sysutil::lock(&self.inner).remove(server_id);
        match removed {
            Some(server) => {
                kill(server).await;
                true
            }
            None => false,
        }
    }

    /// reap for a server no `lsp_request` ever attached; one that is attached
    /// (its stdout taken) belongs to that connection and is left alone.
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

/// One running language server. `lsp_request` takes both stdio halves, so a
/// present stdout is also the flag that no connection has attached yet.
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
        // Anything but data half-closes by dropping stdin, aligned with
        // port_forward: the server sees EOF, flushes, and exits (stdout then
        // EOFs -> Done -> reap). A stray frame gets the same close, since the
        // writer belongs to the stdout pump.
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
    // O_NOFOLLOW: a symlink planted inside lsp.d must not read a target
    // outside it.
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
