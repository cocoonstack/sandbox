//! RPCs back to back on one connection, and the frames that cross an RPC boundary.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use silkd::server::State;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use common::{
    DEADLINE, FrameLines, assert_error, b64, connect_with, expect_frame, frames_until, next_frame,
    send, type_of,
};

async fn types_until(lines: &mut FrameLines, terminal: &str) -> Vec<String> {
    frames_until(lines, |f| type_of(f) == terminal)
        .await
        .iter()
        .map(|f| type_of(f).to_string())
        .collect()
}

#[tokio::test]
async fn rpcs_run_back_to_back_on_one_connection() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = connect_with(&state, json!({"v":1,"op":"info"})).await;
    expect_frame(&mut lines, "info").await;
    send(&mut cw, json!({"v":1,"op":"fs_stat","path":"/"})).await;
    expect_frame(&mut lines, "stat").await;
    send(&mut cw, json!({"v":1,"op":"ps"})).await;
    expect_frame(&mut lines, "procs").await;

    cw.shutdown().await.unwrap();
    timeout(DEADLINE, handle)
        .await
        .expect("server task did not end on close")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn exec_hands_the_connection_back_after_exit() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect_with(
        &state,
        json!({"v":1,"op":"exec","argv":["echo","hi"],"detach":false}),
    )
    .await;
    send(&mut cw, json!({"v":1,"op":"stdin_close"})).await;
    let types = types_until(&mut lines, "exit").await;
    assert_eq!(
        types.first().map(String::as_str),
        Some("started"),
        "got {types:?}"
    );
    assert!(types.iter().any(|t| t == "stdout"), "got {types:?}");

    send(&mut cw, json!({"v":1,"op":"info"})).await;
    expect_frame(&mut lines, "info").await;
}

#[tokio::test]
async fn late_stdin_after_exit_is_dropped() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect_with(
        &state,
        json!({"v":1,"op":"exec","argv":["true"],"detach":false}),
    )
    .await;
    let types = types_until(&mut lines, "exit").await;
    assert_eq!(types, vec!["started", "exit"]);

    send(&mut cw, json!({"v":1,"op":"stdin","data":b64(b"late")})).await;
    send(&mut cw, json!({"v":1,"op":"stdin_close"})).await;
    send(&mut cw, json!({"v":1,"op":"info"})).await;
    expect_frame(&mut lines, "info").await;
}

#[tokio::test]
async fn a_malformed_request_after_exec_is_bad_request() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect_with(
        &state,
        json!({"v":1,"op":"exec","argv":["true"],"detach":false}),
    )
    .await;
    assert_eq!(
        types_until(&mut lines, "exit").await,
        vec!["started", "exit"]
    );

    send(&mut cw, json!({"op":"exec","argv":"ls -la"})).await;
    let err = timeout(Duration::from_secs(3), next_frame(&mut lines))
        .await
        .expect("a malformed request after exec went unanswered");
    assert_error(&err, "bad_request");
    assert!(
        err["message"].as_str().unwrap().starts_with("parse: "),
        "{err}"
    );
}

#[tokio::test]
async fn upload_hands_the_connection_back_after_done() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f").to_string_lossy().into_owned();
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) =
        connect_with(&state, json!({"v":1,"op":"fs_write","path":path})).await;
    send(&mut cw, json!({"v":1,"op":"data","data":b64(b"hello")})).await;
    send(&mut cw, json!({"v":1,"op":"data_end"})).await;
    expect_frame(&mut lines, "done").await;

    send(&mut cw, json!({"v":1,"op":"fs_stat","path":path})).await;
    let stat = expect_frame(&mut lines, "stat").await;
    assert_eq!(stat["info"]["size"], 5, "got {stat}");
}

#[tokio::test]
async fn a_request_during_exec_ends_its_stdin_and_runs_after_it() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect_with(
        &state,
        json!({"v":1,"op":"exec","argv":["cat"],"detach":false}),
    )
    .await;
    expect_frame(&mut lines, "started").await;
    send(&mut cw, json!({"v":1,"op":"info"})).await;
    let exit = expect_frame(&mut lines, "exit").await;
    assert_eq!(exit["code"], 0, "got {exit}");
    expect_frame(&mut lines, "info").await;
}

#[tokio::test]
async fn a_leading_input_frame_is_bad_request_and_the_connection_stays() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect_with(&state, json!({"v":1,"op":"stdin_close"})).await;
    let err = next_frame(&mut lines).await;
    assert_error(&err, "bad_request");

    send(&mut cw, json!({"v":1,"op":"info"})).await;
    expect_frame(&mut lines, "info").await;
}
