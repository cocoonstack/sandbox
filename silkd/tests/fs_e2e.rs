//! Filesystem verb E2E over the in-memory duplex, against real temp paths.

use std::sync::Arc;

use serde_json::{json, Value};
use silkd::server::{buffer, State};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Sends one or more request lines on a single connection (so a write can be
/// followed by its data frames) and returns the response frames.
async fn exchange(lines: &[String]) -> Vec<Value> {
    let (mut client, server) = tokio::io::duplex(1 << 20);
    let state = Arc::new(State::new());
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { state.serve(buffer(sr), sw).await });

    for line in lines {
        client.write_all(line.as_bytes()).await.unwrap();
        client.write_all(b"\n").await.unwrap();
    }
    client.shutdown().await.unwrap();

    let mut frames = Vec::new();
    let mut out = BufReader::new(client).lines();
    while let Some(l) = out.next_line().await.unwrap() {
        frames.push(serde_json::from_str(&l).unwrap());
    }
    handle.await.unwrap().unwrap();
    frames
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn type_of(f: &Value) -> &str {
    f["type"].as_str().unwrap_or("")
}

#[tokio::test]
async fn write_then_read_roundtrips_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hello.txt");
    let path = path.to_str().unwrap();

    let wrote = exchange(&[
        json!({"op":"fs_write","path":path,"mode":0o600}).to_string(),
        json!({"op":"data","data":b64(b"silk file body")}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;
    assert_eq!(
        type_of(&wrote[0]),
        "done",
        "write should succeed: {wrote:?}"
    );

    let read = exchange(&[json!({"op":"fs_read","path":path}).to_string()]).await;
    let body: Vec<u8> = read
        .iter()
        .filter(|f| type_of(f) == "data")
        .flat_map(|f| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(f["data"].as_str().unwrap())
                .unwrap()
        })
        .collect();
    assert_eq!(body, b"silk file body");
    assert_eq!(type_of(read.last().unwrap()), "done");
}

#[tokio::test]
async fn list_reports_written_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.txt");
    exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"abc")}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;

    let listed =
        exchange(&[json!({"op":"fs_list","path":dir.path().to_str().unwrap()}).to_string()]).await;
    let entries = listed[0]["entries"].as_array().unwrap();
    let a = entries
        .iter()
        .find(|e| e["name"] == "a.txt")
        .expect("a.txt listed");
    assert_eq!(a["kind"], "file");
    assert_eq!(a["size"], 3);
}

#[tokio::test]
async fn stat_reports_kind_and_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.bin");
    exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(&[0u8; 10])}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;

    let st = exchange(&[json!({"op":"fs_stat","path":path.to_str().unwrap()}).to_string()]).await;
    assert_eq!(type_of(&st[0]), "stat");
    assert_eq!(st[0]["info"]["kind"], "file");
    assert_eq!(st[0]["info"]["size"], 10);
}

#[tokio::test]
async fn mkdir_rename_rm_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("d1");
    let moved = dir.path().join("d2");

    let mk = exchange(&[json!({"op":"fs_mkdir","path":sub.to_str().unwrap()}).to_string()]).await;
    assert_eq!(type_of(&mk[0]), "done");

    let mv = exchange(&[
        json!({"op":"fs_rename","from":sub.to_str().unwrap(),"to":moved.to_str().unwrap()})
            .to_string(),
    ])
    .await;
    assert_eq!(type_of(&mv[0]), "done");
    assert!(moved.exists() && !sub.exists());

    let rm = exchange(&[
        json!({"op":"fs_rm","path":moved.to_str().unwrap(),"recursive":true}).to_string(),
    ])
    .await;
    assert_eq!(type_of(&rm[0]), "done");
    assert!(!moved.exists());
}

#[tokio::test]
async fn read_missing_path_is_not_found() {
    let frames = exchange(&[json!({"op":"fs_read","path":"/no/such/file/xyz"}).to_string()]).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found");
}

#[tokio::test]
async fn stat_mode_excludes_type_bits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.txt");
    exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap(),"mode":0o640}).to_string(),
        json!({"op":"data","data":b64(b"x")}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;
    let st = exchange(&[json!({"op":"fs_stat","path":path.to_str().unwrap()}).to_string()]).await;
    assert_eq!(
        st[0]["info"]["mode"], 0o640,
        "mode must be masked to permission bits"
    );
}

#[tokio::test]
async fn truncated_write_leaves_no_file_and_reports_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.txt");
    // Send the write request + a data frame, then close without data_end.
    let frames = exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"half")}).to_string(),
    ])
    .await;
    assert_eq!(
        type_of(&frames[0]),
        "error",
        "truncated write must error: {frames:?}"
    );
    assert!(
        !path.exists(),
        "no file should exist at the destination after a truncated write"
    );
    // No leftover temp files either.
    let leftover: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".silkd-"))
        .collect();
    assert!(leftover.is_empty(), "temp file leaked: {leftover:?}");
}

#[tokio::test]
async fn failed_overwrite_preserves_original() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keep.txt");
    // Establish original content via a clean write.
    exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"ORIGINAL")}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;
    // Now a truncated overwrite attempt.
    exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"NEWDATA")}).to_string(),
    ])
    .await;
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"ORIGINAL",
        "a failed overwrite must not clobber the original"
    );
}
