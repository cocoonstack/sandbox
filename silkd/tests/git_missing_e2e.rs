//! git verbs on an image with no git binary; the suite is one test, so it may empty its process PATH.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;

use serde_json::json;
use silkd::server::State;

use common::{assert_error, connect, expect_frame, next_frame, send};

#[tokio::test]
async fn every_git_verb_answers_not_found_and_keeps_the_connection() {
    let empty = tempfile::tempdir().unwrap();
    // SAFETY: this binary runs one test, and no other thread touches the environment while it runs.
    unsafe { std::env::set_var("PATH", empty.path()) };
    let repo = empty.path().to_str().unwrap();
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect(&state);
    for req in [
        json!({"op":"git_clone","url":"u","path":repo}),
        json!({"op":"git_status","path":repo}),
        json!({"op":"git_add","path":repo,"files":["a"]}),
        json!({"op":"git_commit","path":repo,"message":"m","author":"A <a@b>"}),
        json!({"op":"git_push","path":repo}),
        json!({"op":"git_pull","path":repo}),
        json!({"op":"git_branch","path":repo,"action":"list"}),
        json!({"op":"git_branch","path":repo,"action":"create","name":"b"}),
    ] {
        send(&mut cw, req.clone()).await;
        let frame = next_frame(&mut lines).await;
        assert_error(&frame, "not_found");
        assert!(
            frame["message"]
                .as_str()
                .is_some_and(|m| m.starts_with("spawn git: ")),
            "{req} answered {frame}"
        );
    }
    send(&mut cw, json!({"op":"info"})).await;
    expect_frame(&mut lines, "info").await;
}
