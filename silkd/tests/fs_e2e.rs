//! Filesystem verb E2E over the in-memory duplex, against real temp paths.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code, like #[cfg(test)]
mod common;

use common::{b64, exchange, type_of};
use serde_json::json;

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
    assert_eq!(common::payload(&read, "data"), b"silk file body");
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
    assert_eq!(type_of(listed.last().unwrap()), "done");
    let a = listed
        .iter()
        .filter(|f| type_of(f) == "entries")
        .flat_map(|f| f["entries"].as_array().unwrap())
        .find(|e| e["name"] == "a.txt")
        .expect("a.txt listed");
    assert_eq!(a["kind"], "file");
    assert_eq!(a["size"], 3);
}

#[tokio::test]
async fn list_batches_large_directories() {
    let dir = tempfile::tempdir().unwrap();
    let total = silkd::fs::LIST_BATCH + 1;
    for i in 0..total {
        std::fs::write(dir.path().join(format!("f{i}")), b"").unwrap();
    }

    let listed =
        exchange(&[json!({"op":"fs_list","path":dir.path().to_str().unwrap()}).to_string()]).await;
    let frames: Vec<_> = listed.iter().filter(|f| type_of(f) == "entries").collect();
    assert_eq!(frames.len(), 2, "one full batch + the remainder");
    assert_eq!(
        frames[0]["entries"].as_array().unwrap().len(),
        silkd::fs::LIST_BATCH
    );
    let n: usize = frames
        .iter()
        .map(|f| f["entries"].as_array().unwrap().len())
        .sum();
    assert_eq!(n, total);
    assert_eq!(type_of(listed.last().unwrap()), "done");
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
async fn truncated_write_leaves_no_file_and_reports_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.txt");
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
        "no file at destination after a truncated write"
    );
    let leftover: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".silkd-"))
        .collect();
    assert!(leftover.is_empty(), "temp file leaked: {leftover:?}");
}

#[tokio::test]
async fn overwrite_preserves_destination_mode() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("script.sh");
    // Original file is executable (0755); overwrite without specifying a mode.
    std::fs::write(&path, b"old").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let f = exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"new content")}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;
    assert_eq!(type_of(&f[0]), "done");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o755, "overwrite must preserve the executable bit");
    assert_eq!(std::fs::read(&path).unwrap(), b"new content");
}

#[tokio::test]
async fn rm_plain_file_and_missing_and_nonempty_dir() {
    let dir = tempfile::tempdir().unwrap();
    // plain file
    let f = dir.path().join("f");
    std::fs::write(&f, b"x").unwrap();
    let r = exchange(&[json!({"op":"fs_rm","path":f.to_str().unwrap()}).to_string()]).await;
    assert_eq!(type_of(&r[0]), "done");
    assert!(!f.exists());
    // missing path → not_found
    let m = exchange(&[json!({"op":"fs_rm","path":"/no/such/xyz"}).to_string()]).await;
    assert_eq!(type_of(&m[0]), "error");
    assert_eq!(m[0]["kind"], "not_found");
    // non-recursive rm of a non-empty dir → error (not not_found)
    let sub = dir.path().join("d");
    std::fs::create_dir(&sub).unwrap();
    std::fs::write(sub.join("inner"), b"y").unwrap();
    let ne = exchange(&[json!({"op":"fs_rm","path":sub.to_str().unwrap()}).to_string()]).await;
    assert_eq!(type_of(&ne[0]), "error");
    assert!(
        sub.exists(),
        "non-recursive rm must not remove a non-empty dir"
    );
}

#[tokio::test]
async fn failed_overwrite_preserves_original() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keep.txt");
    exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"ORIGINAL")}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;
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
