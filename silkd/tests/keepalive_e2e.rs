//! RPCs back to back on one connection, and the frames that cross an RPC boundary.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;

use serde_json::json;
use silkd::server::State;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use common::{DEADLINE, b64, connect, next_frame, send, type_of};

async fn types_until(lines: &mut common::FrameLines, terminal: &str) -> Vec<String> {
    let mut types = Vec::new();
    loop {
        let frame = next_frame(lines).await;
        types.push(type_of(&frame).to_string());
        if type_of(&frame) == terminal {
            return types;
        }
    }
}

#[tokio::test]
async fn rpcs_run_back_to_back_on_one_connection() {
    timeout(DEADLINE, async {
        let state = Arc::new(State::new());
        let (mut cw, mut lines, handle) = connect(&state);

        send(&mut cw, json!({"v":1,"op":"info"})).await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "info");
        send(&mut cw, json!({"v":1,"op":"fs_stat","path":"/"})).await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "stat");
        send(&mut cw, json!({"v":1,"op":"ps"})).await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "procs");

        cw.shutdown().await.unwrap();
        handle.await.unwrap().unwrap();
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn exec_hands_the_connection_back_after_exit() {
    timeout(DEADLINE, async {
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = connect(&state);

        send(
            &mut cw,
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
        assert!(types.contains(&"stdout".to_string()), "got {types:?}");

        send(&mut cw, json!({"v":1,"op":"info"})).await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "info");
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn late_stdin_after_exit_is_dropped() {
    timeout(DEADLINE, async {
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = connect(&state);

        send(
            &mut cw,
            json!({"v":1,"op":"exec","argv":["true"],"detach":false}),
        )
        .await;
        let types = types_until(&mut lines, "exit").await;
        assert_eq!(types, vec!["started", "exit"]);

        send(&mut cw, json!({"v":1,"op":"stdin","data":b64(b"late")})).await;
        send(&mut cw, json!({"v":1,"op":"stdin_close"})).await;
        send(&mut cw, json!({"v":1,"op":"info"})).await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "info");
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn upload_hands_the_connection_back_after_done() {
    timeout(DEADLINE, async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f").to_string_lossy().into_owned();
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = connect(&state);

        send(&mut cw, json!({"v":1,"op":"fs_write","path":path})).await;
        send(&mut cw, json!({"v":1,"op":"data","data":b64(b"hello")})).await;
        send(&mut cw, json!({"v":1,"op":"data_end"})).await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "done");

        send(&mut cw, json!({"v":1,"op":"fs_stat","path":path})).await;
        let stat = next_frame(&mut lines).await;
        assert_eq!(type_of(&stat), "stat", "got {stat}");
        assert_eq!(stat["info"]["size"], 5, "got {stat}");
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn a_request_during_exec_ends_its_stdin_and_runs_after_it() {
    timeout(DEADLINE, async {
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = connect(&state);

        send(
            &mut cw,
            json!({"v":1,"op":"exec","argv":["cat"],"detach":false}),
        )
        .await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "started");
        send(&mut cw, json!({"v":1,"op":"info"})).await;
        let exit = next_frame(&mut lines).await;
        assert_eq!(type_of(&exit), "exit", "got {exit}");
        assert_eq!(exit["code"], 0, "got {exit}");
        assert_eq!(type_of(&next_frame(&mut lines).await), "info");
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn a_leading_input_frame_is_bad_request_and_the_connection_stays() {
    timeout(DEADLINE, async {
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = connect(&state);

        send(&mut cw, json!({"v":1,"op":"stdin_close"})).await;
        let err = next_frame(&mut lines).await;
        assert_eq!(type_of(&err), "error", "got {err}");
        assert_eq!(err["kind"], "bad_request", "got {err}");

        send(&mut cw, json!({"v":1,"op":"info"})).await;
        assert_eq!(type_of(&next_frame(&mut lines).await), "info");
    })
    .await
    .expect("test deadline");
}
