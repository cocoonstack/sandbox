//! fs.watch E2E: a connection-bound event stream ended by client disconnect.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use silkd::server::State;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn watch_streams_events_until_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = common::connect(&state);
    let req = json!({
        "op": "fs_watch",
        "path": dir.path().to_str().unwrap(),
        "recursive": true
    })
    .to_string();
    cw.write_all(req.as_bytes()).await.unwrap();
    cw.write_all(b"\n").await.unwrap();

    // The ready frame guarantees the watch is armed — no arming sleep needed.
    let ready = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("no ready within 5s")
        .unwrap()
        .expect("stream closed before ready");
    let frame: serde_json::Value = serde_json::from_str(&ready).unwrap();
    assert_eq!(frame["type"], "ready", "got {frame:?}");
    tokio::fs::write(dir.path().join("new.txt"), b"hi")
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("no event within 5s")
        .unwrap()
        .expect("stream closed before an event");
    let frame: serde_json::Value = serde_json::from_str(&event).unwrap();
    assert_eq!(frame["type"], "event", "got {frame:?}");
    assert!(frame["path"].as_str().unwrap().ends_with("new.txt"));

    // Disconnect ends the watch: the server task returns promptly.
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
    let frames = common::exchange(&[json!({
        "op": "fs_watch",
        "path": "/no/such/dir/silkd-watch"
    })
    .to_string()])
    .await;
    assert_eq!(common::type_of(&frames[0]), "error");
}
