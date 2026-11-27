//! fs.watch E2E: a connection-bound event stream ended by client disconnect.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use silkd::server::State;

use common::{Connection, FrameLines, assert_error, connect_with, expect_frame, request};

async fn open_watch(dir: &Path) -> Connection {
    let (cw, mut lines, handle) = connect_with(
        &Arc::new(State::new()),
        json!({"op": "fs_watch", "path": dir.to_str().unwrap(), "recursive": true}),
    )
    .await;
    expect_frame(&mut lines, "ready").await;
    (cw, lines, handle)
}

async fn events_until(lines: &mut FrameLines, suffix: &str) {
    loop {
        let frame = expect_frame(lines, "event").await;
        if frame["path"].as_str().unwrap().ends_with(suffix) {
            return;
        }
    }
}

#[tokio::test]
async fn watch_streams_events_until_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let (cw, mut lines, handle) = open_watch(dir.path()).await;
    tokio::fs::write(dir.path().join("new.txt"), b"hi")
        .await
        .unwrap();
    events_until(&mut lines, "new.txt").await;

    drop(lines);
    drop(cw);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("watch did not end on disconnect")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn watch_survives_a_symlink_loop_in_the_tree() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("a")).unwrap();
    std::os::unix::fs::symlink(".", dir.path().join("a/loop")).unwrap();
    let (_cw, mut lines, _handle) = open_watch(dir.path()).await;
    tokio::fs::write(dir.path().join("a/new.txt"), b"hi")
        .await
        .unwrap();
    events_until(&mut lines, "a/new.txt").await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn watch_survives_directories_replaced_by_files() {
    let dir = tempfile::tempdir().unwrap();
    let (_cw, mut lines, _handle) = open_watch(dir.path()).await;

    for round in 0..30 {
        for i in 0..10 {
            let path = dir.path().join(format!("d{round}-{i}"));
            std::fs::create_dir(&path).unwrap();
            std::thread::yield_now();
            std::fs::create_dir(path.join("s")).unwrap();
            std::fs::remove_dir_all(&path).unwrap();
            std::fs::write(&path, b"x").unwrap();
        }
        let mark = format!("mark{round}");
        std::fs::write(dir.path().join(&mark), b"x").unwrap();
        events_until(&mut lines, &mark).await;
    }
}

#[tokio::test]
async fn watch_missing_path_is_error() {
    let frames = request(json!({"op": "fs_watch", "path": "/no/such/dir/silkd-watch"})).await;
    assert_error(&frames[0], "not_found");
}
