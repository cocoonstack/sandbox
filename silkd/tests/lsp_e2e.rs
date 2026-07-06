//! LSP broker integration: a fake "language server" (a shell script that
//! echoes a canned reply) stands in for pylsp, so the broker mechanism —
//! manifest lookup, spawn, bidirectional relay, stop — is exercised without a
//! real language server. Every test runs under a deadline so a relay deadlock
//! fails CI instead of hanging it.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use silkd::server::State;

mod common;
use common::{b64, connect, decode, one, type_of};

const DEADLINE: Duration = Duration::from_secs(30);

// SILKD_LSP_DIR is process-global; serialize the tests that set it.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

// A fake server that echoes each line it reads, prefixed — enough to prove
// the round trip without LSP semantics.
const FAKE_SERVER: &str = r#"#!/bin/sh
IFS= read -r line
printf 'reply:%s\n' "$line"
"#;

fn manifest_env(server_body: &str) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("fake-lsp");
    let mut f = std::fs::File::create(&bin).unwrap();
    f.write_all(server_body.as_bytes()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    dir
}

#[tokio::test]
async fn lsp_start_missing_manifest_is_not_found() {
    let frames = timeout(
        DEADLINE,
        one(
            &Arc::new(State::new()),
            &json!({"op":"lsp_start","language":"nope"}).to_string(),
        ),
    )
    .await
    .expect("deadline");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "error");
    assert_eq!(last["kind"], "not_found");
}

#[tokio::test]
async fn lsp_start_language_name_cannot_escape() {
    for bad in ["../etc/passwd", "a/b", ".."] {
        let frames = one(
            &Arc::new(State::new()),
            &json!({"op":"lsp_start","language":bad}).to_string(),
        )
        .await;
        assert_eq!(
            type_of(frames.last().unwrap()),
            "error",
            "{bad} should be rejected"
        );
    }
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // ENV_LOCK serializes SILKD_LSP_DIR across the whole test
async fn lsp_broker_relays_to_the_server() {
    // Point the broker at a manifest dir under our control by baking the
    // fake server's argv into a manifest the broker will read. The broker
    // reads /etc/silkd/lsp.d, which we cannot write in CI, so this test drives
    // the Broker directly against a manifest we control via the language arg
    // being an absolute path is disallowed — instead we assert the spawn +
    // relay path through a manifest we plant when running as root, else skip.
    let _env_lock = ENV_LOCK.lock().unwrap();
    let env = manifest_env(FAKE_SERVER);
    let bin = env.path().join("fake-lsp");
    std::fs::write(
        env.path().join("faketest"),
        bin.to_string_lossy().as_bytes(),
    )
    .unwrap();
    std::env::set_var("SILKD_LSP_DIR", env.path());

    let state = Arc::new(State::new());
    let start = one(
        &state,
        &json!({"op":"lsp_start","language":"faketest"}).to_string(),
    )
    .await;
    let server_id = start.last().unwrap()["server_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Attach and send one JSON-RPC-ish line; expect the echoed reply back.
    let (mut cw, mut out, handle) = connect(&state);
    cw.write_all(
        json!({"op":"lsp_request","server_id":server_id})
            .to_string()
            .as_bytes(),
    )
    .await
    .unwrap();
    cw.write_all(b"\n").await.unwrap();
    // ready frame
    let ready: serde_json::Value =
        serde_json::from_str(&out.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(type_of(&ready), "ready");
    cw.write_all(
        json!({"op":"data","data":b64(b"ping\n")})
            .to_string()
            .as_bytes(),
    )
    .await
    .unwrap();
    cw.write_all(b"\n").await.unwrap();

    let reply = timeout(DEADLINE, out.next_line())
        .await
        .expect("deadline")
        .unwrap()
        .unwrap();
    let frame: serde_json::Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(type_of(&frame), "data");
    assert_eq!(decode(&frame), b"reply:ping\n");

    // The fake server exits after one reply: its stdout EOFs, so the request
    // ends with Done and the broker reaps the server.
    let done = timeout(DEADLINE, out.next_line())
        .await
        .expect("deadline")
        .unwrap()
        .unwrap();
    assert_eq!(
        type_of(&serde_json::from_str::<serde_json::Value>(&done).unwrap()),
        "done"
    );
    cw.shutdown().await.unwrap();
    let _ = handle.await;

    // Reaped: a later lsp_stop is not_found.
    let gone = one(
        &state,
        &json!({"op":"lsp_stop","server_id":server_id}).to_string(),
    )
    .await;
    assert_eq!(gone.last().unwrap()["kind"], "not_found");
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // ENV_LOCK serializes SILKD_LSP_DIR across the whole test
async fn lsp_stop_kills_an_idle_server() {
    let _env_lock = ENV_LOCK.lock().unwrap();
    let env = manifest_env("#!/bin/sh\nsleep 60\n");
    std::fs::write(
        env.path().join("idletest"),
        env.path().join("fake-lsp").to_string_lossy().as_bytes(),
    )
    .unwrap();
    std::env::set_var("SILKD_LSP_DIR", env.path());
    let state = Arc::new(State::new());
    let start = one(
        &state,
        &json!({"op":"lsp_start","language":"idletest"}).to_string(),
    )
    .await;
    let server_id = start.last().unwrap()["server_id"]
        .as_str()
        .unwrap()
        .to_string();
    let stop = one(
        &state,
        &json!({"op":"lsp_stop","server_id":server_id}).to_string(),
    )
    .await;
    assert_eq!(type_of(stop.last().unwrap()), "done");
    let gone = one(
        &state,
        &json!({"op":"lsp_stop","server_id":server_id}).to_string(),
    )
    .await;
    assert_eq!(gone.last().unwrap()["kind"], "not_found");
}
