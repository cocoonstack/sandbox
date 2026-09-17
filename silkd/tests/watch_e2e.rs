//! fs.watch E2E: a connection-bound event stream ended by client disconnect.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use silkd::server::State;

use common::{connect, exchange, next_frame, send, type_of};

#[tokio::test]
async fn watch_streams_events_until_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = connect(&state);
    send(
        &mut cw,
        json!({"op": "fs_watch", "path": dir.path().to_str().unwrap(), "recursive": true}),
    )
    .await;
    assert_eq!(type_of(&next_frame(&mut lines).await), "ready");
    tokio::fs::write(dir.path().join("new.txt"), b"hi")
        .await
        .unwrap();

    loop {
        let frame = next_frame(&mut lines).await;
        assert_eq!(type_of(&frame), "event", "got {frame:?}");
        if frame["path"].as_str().unwrap().ends_with("new.txt") {
            break;
        }
    }

    drop(lines);
    drop(cw);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("watch did not end on disconnect")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn watch_missing_path_is_error() {
    let frames =
        exchange(&[json!({"op": "fs_watch", "path": "/no/such/dir/silkd-watch"}).to_string()])
            .await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found", "{frames:?}");
}
