//! fs.watch E2E: a connection-bound event stream ended by client disconnect.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use silkd::server::{buffer, State};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn watch_streams_events_until_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(State::new());
    let (client, server) = tokio::io::duplex(1 << 20);
    let st = Arc::clone(&state);
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { st.serve(buffer(sr), sw).await });

    let (cr, mut cw) = tokio::io::split(client);
    let req = json!({
        "op": "fs_watch",
        "path": dir.path().to_str().unwrap(),
        "recursive": true
    })
    .to_string();
    cw.write_all(req.as_bytes()).await.unwrap();
    cw.write_all(b"\n").await.unwrap();

    // The watcher needs a moment to arm before the create fires.
    tokio::time::sleep(Duration::from_millis(150)).await;
    tokio::fs::write(dir.path().join("new.txt"), b"hi")
        .await
        .unwrap();

    let mut lines = BufReader::new(cr).lines();
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
