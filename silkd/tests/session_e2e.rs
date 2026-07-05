//! Session verb E2E: a persistent shell whose cwd/env/state survive across
//! exec calls. Spawns real bash, so Unix-only in practice; kept ungated so
//! the persistence contract is checked on the dev host too.

mod common;

use std::sync::Arc;

use common::{one, type_of};
use serde_json::{json, Value};
use silkd::server::State;

fn body(frames: &[Value]) -> String {
    let bytes: Vec<u8> = frames
        .iter()
        .filter(|f| type_of(f) == "stdout")
        .flat_map(common::decode)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

async fn create(state: &Arc<State>, extra: Value) -> String {
    let mut req = json!({"op":"session_create"});
    for (k, v) in extra.as_object().unwrap() {
        req[k] = v.clone();
    }
    let f = one(state, &req.to_string()).await;
    assert_eq!(type_of(&f[0]), "session_created", "create failed: {f:?}");
    f[0]["id"].as_str().unwrap().to_string()
}

async fn sh(state: &Arc<State>, id: &str, cmd: &[&str]) -> Vec<Value> {
    let argv: Vec<Value> = cmd.iter().map(|s| json!(s)).collect();
    one(
        state,
        &json!({"op":"exec","argv":argv,"session":id}).to_string(),
    )
    .await
}

#[tokio::test]
async fn session_persists_env_across_calls() {
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;

    let set = sh(&state, &id, &["export", "MARKER=silk123"]).await;
    assert_eq!(type_of(set.last().unwrap()), "exit");

    let got = sh(&state, &id, &["sh", "-c", "echo $MARKER"]).await;
    assert_eq!(body(&got).trim(), "silk123", "env did not persist: {got:?}");
}

#[tokio::test]
async fn session_applies_cwd_and_env_at_create() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(State::new());
    let id = create(
        &state,
        json!({"cwd": dir.path().to_str().unwrap(), "env": {"GREETING": "hello-silk"}}),
    )
    .await;

    let g = sh(&state, &id, &["sh", "-c", "echo $GREETING"]).await;
    assert_eq!(body(&g).trim(), "hello-silk");

    // cwd applied: a relative touch lands in the session's directory.
    let t = sh(&state, &id, &["touch", "marker"]).await;
    assert_eq!(type_of(t.last().unwrap()), "exit");
    assert!(dir.path().join("marker").exists(), "cwd was not applied");
}

#[tokio::test]
async fn session_exit_code_propagates() {
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;

    let ok = sh(&state, &id, &["true"]).await;
    assert_eq!(ok.last().unwrap()["code"], 0);
    let bad = sh(&state, &id, &["false"]).await;
    assert_eq!(bad.last().unwrap()["code"], 1);
}

#[tokio::test]
async fn session_list_and_rm() {
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;

    let listed = one(&state, r#"{"op":"session_list"}"#).await;
    assert!(listed[0]["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s == &json!(id)));

    let rm = one(&state, &json!({"op":"session_rm","id":id}).to_string()).await;
    assert_eq!(type_of(&rm[0]), "done");

    let gone = sh(&state, &id, &["true"]).await;
    assert_eq!(type_of(&gone[0]), "error");
    assert_eq!(gone[0]["kind"], "not_found");
}

#[tokio::test]
async fn stdin_reading_command_does_not_wedge_the_session() {
    // A command that reads stdin (here `cat`, which would block forever on the
    // control pipe) must not hang the session — its stdin is isolated. Bound
    // with a timeout: a regression would hang instead of returning.
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;
    let r = tokio::time::timeout(std::time::Duration::from_secs(8), sh(&state, &id, &["cat"]))
        .await
        .expect("session wedged on a stdin-reading command");
    assert_eq!(type_of(r.last().unwrap()), "exit");
    // The session is still usable afterward.
    let after = sh(&state, &id, &["echo", "alive"]).await;
    assert_eq!(body(&after).trim(), "alive");
}

#[tokio::test]
async fn shell_killing_command_removes_the_session() {
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;
    // `exit` kills the shell; the session must then stop resolving.
    let _ = sh(&state, &id, &["exit"]).await;
    let again = sh(&state, &id, &["echo", "hi"]).await;
    assert_eq!(type_of(&again[0]), "error");
    assert_eq!(again[0]["kind"], "not_found", "dead session should be gone");
}

#[tokio::test]
async fn forged_sentinel_in_output_does_not_desync() {
    // A command printing a plausible sentinel must not be mistaken for the
    // real one (random token) — the real command output is preserved and the
    // next command still frames correctly.
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;
    let r = sh(
        &state,
        &id,
        &["sh", "-c", "printf '__SILK_deadbeef__ 0\\nreal-output\\n'"],
    )
    .await;
    assert!(
        body(&r).contains("real-output"),
        "output truncated by a forged sentinel: {r:?}"
    );
    let next = sh(&state, &id, &["echo", "next-ok"]).await;
    assert_eq!(
        body(&next).trim(),
        "next-ok",
        "stream desynced after forged sentinel"
    );
}

#[tokio::test]
async fn reap_idle_removes_idle_sessions() {
    use std::time::Duration;
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;
    // Everything is "idle" against a zero TTL.
    let reaped = state.sessions.reap_idle(Duration::ZERO);
    assert_eq!(reaped, 1);
    let gone = sh(&state, &id, &["echo", "hi"]).await;
    assert_eq!(type_of(&gone[0]), "error");
    assert_eq!(
        gone[0]["kind"], "not_found",
        "reaped session should be gone"
    );
}

#[tokio::test]
async fn reap_skips_a_session_running_a_command() {
    use std::time::Duration;
    let state = Arc::new(State::new());
    let id = create(&state, json!({})).await;
    // Hold the session busy with a slow command while a zero-TTL reap runs;
    // an actively-running session must not be reaped.
    let busy = {
        let s = Arc::clone(&state);
        let sid = id.clone();
        tokio::spawn(async move { sh(&s, &sid, &["sleep", "1"]).await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    let reaped = state.sessions.reap_idle(Duration::ZERO);
    assert_eq!(reaped, 0, "a running session must not be reaped");
    let done = busy.await.unwrap();
    assert_eq!(type_of(done.last().unwrap()), "exit");
    // After it finishes it becomes reapable again.
    assert_eq!(state.sessions.reap_idle(Duration::ZERO), 1);
}

#[tokio::test]
async fn exec_in_unknown_session_is_not_found() {
    let state = Arc::new(State::new());
    let f = one(&state, r#"{"op":"exec","argv":["true"],"session":"nope"}"#).await;
    assert_eq!(type_of(&f[0]), "error");
    assert_eq!(f[0]["kind"], "not_found");
}
