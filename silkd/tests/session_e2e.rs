//! Session verb E2E: a persistent shell whose cwd/env/state survive across exec calls.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use silkd::server::State;
use tokio::task::JoinHandle;

use common::{assert_error, one, stdout_body, type_of, wait_for_path};

async fn create(state: &Arc<State>) -> String {
    create_with(state, json!({"op":"session_create"})).await
}

async fn create_with(state: &Arc<State>, req: Value) -> String {
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

async fn spawn_busy(state: &Arc<State>, id: &str, seconds: &'static str) -> JoinHandle<Vec<Value>> {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("started");
    let marker_arg = marker.to_str().unwrap().to_string();
    let s = Arc::clone(state);
    let sid = id.to_string();
    let busy = tokio::spawn(async move {
        sh(
            &s,
            &sid,
            &[
                "sh",
                "-c",
                "touch \"$0\"; exec sleep \"$1\"",
                &marker_arg,
                seconds,
            ],
        )
        .await
    });
    wait_for_path(&marker).await;
    busy
}

#[tokio::test]
async fn session_persists_env_across_calls() {
    let state = Arc::new(State::new());
    let id = create(&state).await;

    let set = sh(&state, &id, &["export", "MARKER=silk123"]).await;
    assert_eq!(type_of(set.last().unwrap()), "exit");

    let got = sh(&state, &id, &["sh", "-c", "echo $MARKER"]).await;
    assert_eq!(
        stdout_body(&got).trim(),
        "silk123",
        "env did not persist: {got:?}"
    );
}

#[tokio::test]
async fn session_applies_cwd_and_env_at_create() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(State::new());
    let id = create_with(
        &state,
        json!({"op":"session_create","cwd": dir.path().to_str().unwrap(), "env": {"GREETING": "hello-silk"}}),
    )
    .await;

    let g = sh(&state, &id, &["sh", "-c", "echo $GREETING"]).await;
    assert_eq!(stdout_body(&g).trim(), "hello-silk");

    let t = sh(&state, &id, &["touch", "marker"]).await;
    assert_eq!(type_of(t.last().unwrap()), "exit");
    assert!(dir.path().join("marker").exists(), "cwd was not applied");
}

#[tokio::test]
async fn session_create_rejects_a_missing_cwd_like_exec() {
    let state = Arc::new(State::new());
    let f = one(
        &state,
        &json!({"op":"session_create","cwd":"/nonexistent/silkd-cwd"}).to_string(),
    )
    .await;
    assert_error(&f[0], "bad_request");
    assert!(state.sessions.list().is_empty());
}

#[tokio::test]
async fn session_create_rejects_an_env_name_bash_cannot_export() {
    let state = Arc::new(State::new());
    let f = tokio::time::timeout(
        Duration::from_secs(8),
        one(
            &state,
            &json!({"op":"session_create","env":{"BAD KEY'\n": "v", "GOOD": "ok"}}).to_string(),
        ),
    )
    .await
    .expect("session_create hung on a hostile env key");
    assert_error(&f[0], "bad_request");
    assert!(
        state.sessions.list().is_empty(),
        "{:?}",
        state.sessions.list()
    );
}

#[tokio::test]
async fn session_create_fails_when_bash_refuses_an_export() {
    let state = Arc::new(State::new());
    let f = tokio::time::timeout(
        Duration::from_secs(8),
        one(
            &state,
            &json!({"op":"session_create","env":{"UID":"1"}}).to_string(),
        ),
    )
    .await
    .expect("session_create hung on a readonly variable");
    assert_eq!(type_of(&f[0]), "error", "{f:?}");
    assert!(
        state.sessions.list().is_empty(),
        "{:?}",
        state.sessions.list()
    );
}

#[tokio::test]
async fn session_exit_code_propagates() {
    let state = Arc::new(State::new());
    let id = create(&state).await;

    let ok = sh(&state, &id, &["true"]).await;
    assert_eq!(ok.last().unwrap()["code"], 0);
    let bad = sh(&state, &id, &["false"]).await;
    assert_eq!(bad.last().unwrap()["code"], 1);
}

#[tokio::test]
async fn session_list_and_rm() {
    let state = Arc::new(State::new());
    let id = create(&state).await;

    let listed = one(&state, r#"{"op":"session_list"}"#).await;
    assert!(
        listed[0]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s == &json!(id))
    );

    let rm = one(&state, &json!({"op":"session_rm","id":id}).to_string()).await;
    assert_eq!(type_of(&rm[0]), "done");

    let gone = sh(&state, &id, &["true"]).await;
    assert_error(&gone[0], "not_found");
}

#[tokio::test]
async fn stdin_reading_command_does_not_wedge_the_session() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let r = tokio::time::timeout(Duration::from_secs(8), sh(&state, &id, &["cat"]))
        .await
        .expect("session wedged on a stdin-reading command");
    assert_eq!(type_of(r.last().unwrap()), "exit");
    let after = sh(&state, &id, &["echo", "alive"]).await;
    assert_eq!(stdout_body(&after).trim(), "alive");
}

#[tokio::test]
async fn shell_killing_command_removes_the_session() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let _ = sh(&state, &id, &["exit"]).await;
    let again = sh(&state, &id, &["echo", "hi"]).await;
    assert_error(&again[0], "not_found");
}

#[tokio::test]
async fn forged_sentinel_in_output_does_not_desync() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let r = sh(
        &state,
        &id,
        &["sh", "-c", "printf '__SILK_deadbeef__ 0\\nreal-output\\n'"],
    )
    .await;
    assert!(
        stdout_body(&r).contains("real-output"),
        "output truncated by a forged sentinel: {r:?}"
    );
    let next = sh(&state, &id, &["echo", "next-ok"]).await;
    assert_eq!(
        stdout_body(&next).trim(),
        "next-ok",
        "stream desynced after forged sentinel"
    );
}

#[tokio::test]
async fn reap_idle_removes_idle_sessions() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
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
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let busy = spawn_busy(&state, &id, "1").await;
    let reaped = state.sessions.reap_idle(Duration::ZERO);
    assert_eq!(reaped, 0, "a running session must not be reaped");
    let done = busy.await.unwrap();
    assert_eq!(type_of(done.last().unwrap()), "exit");
    assert_eq!(state.sessions.reap_idle(Duration::ZERO), 1);
}

#[tokio::test]
async fn session_argv_with_metacharacters_is_one_literal_token() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let evil = "a b; echo INJECTED $(id) `whoami` 'q'";
    let r = sh(&state, &id, &["printf", "%s", evil]).await;
    assert_eq!(
        stdout_body(&r),
        evil,
        "argv metacharacters were interpreted instead of passed literally: {r:?}"
    );
}

#[tokio::test]
async fn session_rm_unwedges_a_running_external_command() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let busy = spawn_busy(&state, &id, "3600").await;
    let rm = one(&state, &json!({"op":"session_rm","id":id}).to_string()).await;
    assert_eq!(type_of(&rm[0]), "done");
    let done = tokio::time::timeout(Duration::from_secs(5), busy)
        .await
        .expect("session_rm did not unwedge the external command");
    let frames = done.unwrap();
    assert_eq!(type_of(frames.last().unwrap()), "error");
}

#[tokio::test]
async fn empty_argv_in_session_is_a_bad_request() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let f = one(
        &state,
        &json!({"op":"exec","argv":[],"session":id}).to_string(),
    )
    .await;
    assert_error(&f[0], "bad_request");
}

#[tokio::test]
async fn detach_with_session_is_a_bad_request() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let f = one(
        &state,
        &json!({"op":"exec","argv":["true"],"session":id,"detach":true}).to_string(),
    )
    .await;
    assert_error(&f[0], "bad_request");
}

#[tokio::test]
async fn exec_in_unknown_session_is_not_found() {
    let state = Arc::new(State::new());
    let f = one(&state, r#"{"op":"exec","argv":["true"],"session":"nope"}"#).await;
    assert_error(&f[0], "not_found");
}

#[tokio::test]
async fn session_merges_stderr_into_stdout() {
    let state = Arc::new(State::new());
    let id = create(&state).await;

    let frames = sh(&state, &id, &["sh", "-c", "echo oops >&2"]).await;
    assert_eq!(
        stdout_body(&frames).trim(),
        "oops",
        "stderr not merged: {frames:?}"
    );
}

#[tokio::test]
async fn a_dead_session_command_leaves_a_recreated_session_alone() {
    let state = Arc::new(State::new());
    let id = create_with(&state, json!({"op":"session_create","id":"s"})).await;
    let bg = sh(
        &state,
        &id,
        &["sh", "-c", "perl -MPOSIX -e 'setsid(); sleep 2' &"],
    )
    .await;
    assert_eq!(type_of(bg.last().unwrap()), "exit", "{bg:?}");
    let busy = spawn_busy(&state, &id, "3600").await;
    let rm = one(&state, &json!({"op":"session_rm","id":id}).to_string()).await;
    assert_eq!(type_of(&rm[0]), "done");
    let again = create_with(&state, json!({"op":"session_create","id":"s"})).await;
    let frames = tokio::time::timeout(Duration::from_secs(10), busy)
        .await
        .expect("the removed session's command never saw its shell die")
        .unwrap();
    assert_eq!(type_of(frames.last().unwrap()), "error");
    let got = sh(&state, &again, &["echo", "alive"]).await;
    assert_eq!(
        stdout_body(&got).trim(),
        "alive",
        "the recreated session died: {got:?}"
    );
}

#[tokio::test]
async fn a_shell_that_exits_ends_its_command_while_a_job_holds_its_output() {
    let state = Arc::new(State::new());
    let id = create(&state).await;
    let bg = sh(&state, &id, &["sh", "-c", "sleep 30 &"]).await;
    assert_eq!(type_of(bg.last().unwrap()), "exit", "{bg:?}");
    let frames = tokio::time::timeout(Duration::from_secs(5), sh(&state, &id, &["exit"]))
        .await
        .expect("exit hung while a background job held the session's stdout");
    assert_eq!(type_of(frames.last().unwrap()), "error", "{frames:?}");
}
