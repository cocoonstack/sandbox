//! LSP broker integration against a fake language server, each test under a deadline so a relay deadlock fails CI.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use serde_json::json;
use silkd::server::State;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use common::{DEADLINE, b64, connect, decode, next_frame, one, send, type_of};

const FAKE_SERVER: &str = r#"#!/bin/sh
IFS= read -r line
printf 'reply:%s\n' "$line"
"#;

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn manifest_env(server_body: &str) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("fake-lsp");
    let mut f = std::fs::File::create(&bin).unwrap();
    f.write_all(server_body.as_bytes()).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    // SAFETY: ENV_LOCK, held by the caller for its whole test, serializes every
    // writer and reader of SILKD_LSP_DIR in this binary.
    unsafe { std::env::set_var("SILKD_LSP_DIR", dir.path()) };
    dir
}

async fn started_server(lang: &str, server_body: &str) -> (TempDir, Arc<State>, String) {
    let env = manifest_env(server_body);
    std::fs::write(
        env.path().join(lang),
        env.path().join("fake-lsp").to_string_lossy().as_bytes(),
    )
    .unwrap();
    let state = Arc::new(State::new());
    let start = one(
        &state,
        &json!({"op":"lsp_start","language":lang}).to_string(),
    )
    .await;
    let server_id = start.last().unwrap()["server_id"]
        .as_str()
        .unwrap()
        .to_string();
    (env, state, server_id)
}

async fn assert_stopped(state: &Arc<State>, server_id: &str) {
    let gone = one(
        state,
        &json!({"op":"lsp_stop","server_id":server_id}).to_string(),
    )
    .await;
    assert_eq!(gone.last().unwrap()["kind"], "not_found", "{gone:?}");
}

#[tokio::test]
async fn lsp_start_missing_manifest_is_not_found() {
    let _env_lock = ENV_LOCK.lock().await;
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
    let _env_lock = ENV_LOCK.lock().await;
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
async fn lsp_broker_relays_to_the_server() {
    let _env_lock = ENV_LOCK.lock().await;
    let (_env, state, server_id) = started_server("faketest", FAKE_SERVER).await;

    let (mut cw, mut out, handle) = connect(&state);
    send(&mut cw, json!({"op":"lsp_request","server_id":server_id})).await;
    assert_eq!(type_of(&next_frame(&mut out).await), "ready");
    send(&mut cw, json!({"op":"data","data":b64(b"ping\n")})).await;

    let frame = next_frame(&mut out).await;
    assert_eq!(type_of(&frame), "data");
    assert_eq!(decode(&frame), b"reply:ping\n");
    assert_eq!(type_of(&next_frame(&mut out).await), "done");
    cw.shutdown().await.unwrap();
    let _ = handle.await;

    assert_stopped(&state, &server_id).await;
}

#[tokio::test]
async fn lsp_stop_kills_an_idle_server() {
    let _env_lock = ENV_LOCK.lock().await;
    let (_env, state, server_id) = started_server("idletest", "#!/bin/sh\nsleep 60\n").await;
    let stop = one(
        &state,
        &json!({"op":"lsp_stop","server_id":server_id}).to_string(),
    )
    .await;
    assert_eq!(type_of(stop.last().unwrap()), "done");
    assert_stopped(&state, &server_id).await;
}

#[tokio::test]
async fn lsp_request_reaps_when_the_client_vanishes_before_ready() {
    let _env_lock = ENV_LOCK.lock().await;
    let (_env, state, server_id) = started_server("gonetest", "#!/bin/sh\nsleep 60\n").await;

    let (mut client, server) = tokio::io::duplex(1 << 20);
    let request = json!({"op":"lsp_request","server_id":server_id}).to_string();
    client.write_all(request.as_bytes()).await.unwrap();
    client.write_all(b"\n").await.unwrap();
    drop(client);
    let (sr, sw) = tokio::io::split(server);
    let served = timeout(DEADLINE, state.serve(tokio::io::BufReader::new(sr), sw))
        .await
        .expect("deadline");
    assert!(served.is_err(), "the ready write must fail: {served:?}");

    assert_stopped(&state, &server_id).await;
}

#[tokio::test]
async fn lsp_data_end_half_closes_stdin() {
    let _env_lock = ENV_LOCK.lock().await;
    let (_env, state, server_id) =
        started_server("eoftest", "#!/bin/sh\nprintf 'ate:%s\\n' $(cat | wc -c)\n").await;

    let (mut cw, mut out, handle) = connect(&state);
    send(&mut cw, json!({"op":"lsp_request","server_id":server_id})).await;
    assert_eq!(type_of(&next_frame(&mut out).await), "ready");
    send(&mut cw, json!({"op":"data","data":b64(b"12345")})).await;
    send(&mut cw, json!({"op":"data_end"})).await;

    let frame = next_frame(&mut out).await;
    assert_eq!(type_of(&frame), "data");
    assert_eq!(decode(&frame), b"ate:5\n");
    assert_eq!(type_of(&next_frame(&mut out).await), "done");
    cw.shutdown().await.unwrap();
    let _ = handle.await;
}
