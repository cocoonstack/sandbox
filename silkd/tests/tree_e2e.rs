//! Whole-tree transfer E2E: push a tar stream in, pull a tar stream out,
//! using the system tar to build/extract the reference archives.

mod common;

use std::path::Path;
use std::process::Command;

use common::{b64, data_frames, exchange, payload, type_of};
use serde_json::json;

fn sys_tar_create(dir: &Path) -> Vec<u8> {
    let out = Command::new("tar")
        .arg("-c")
        .arg("-C")
        .arg(dir)
        .arg(".")
        .output()
        .expect("run tar -c");
    assert!(out.status.success(), "tar -c failed: {out:?}");
    out.stdout
}

fn sys_tar_extract(archive: &[u8], into: &Path) {
    use std::io::Write;
    let mut child = Command::new("tar")
        .arg("-x")
        .arg("-C")
        .arg(into)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("spawn tar -x");
    child.stdin.take().unwrap().write_all(archive).unwrap();
    assert!(child.wait().unwrap().success(), "tar -x failed");
}

#[tokio::test]
async fn push_extracts_a_tar_stream() {
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("a.txt"), b"AAA").unwrap();
    std::fs::create_dir(src.path().join("sub")).unwrap();
    std::fs::write(src.path().join("sub/b.txt"), b"BBB").unwrap();
    let archive = sys_tar_create(src.path());

    let dest = tempfile::tempdir().unwrap();
    let dest_path = dest.path().join("proj");
    let mut lines = vec![json!({"op":"fs_push","dest":dest_path.to_str().unwrap()}).to_string()];
    lines.extend(data_frames(&archive));
    let frames = exchange(&lines).await;

    assert_eq!(
        type_of(&frames[0]),
        "done",
        "push should succeed: {frames:?}"
    );
    assert_eq!(std::fs::read(dest_path.join("a.txt")).unwrap(), b"AAA");
    assert_eq!(std::fs::read(dest_path.join("sub/b.txt")).unwrap(), b"BBB");
}

#[tokio::test]
async fn pull_streams_a_tar_of_the_path() {
    let src = tempfile::tempdir().unwrap();
    let proj = src.path().join("proj");
    std::fs::create_dir(&proj).unwrap();
    std::fs::write(proj.join("x.txt"), b"hello-pull").unwrap();

    let frames =
        exchange(&[json!({"op":"fs_pull","path":proj.to_str().unwrap()}).to_string()]).await;
    assert_eq!(
        type_of(frames.last().unwrap()),
        "done",
        "pull done: {frames:?}"
    );
    let archive = payload(&frames, "data");

    let out = tempfile::tempdir().unwrap();
    sys_tar_extract(&archive, out.path());
    // the archive carries the entry under its own name (proj/…)
    assert_eq!(
        std::fs::read(out.path().join("proj/x.txt")).unwrap(),
        b"hello-pull"
    );
}

#[tokio::test]
async fn push_then_pull_roundtrips_a_tree() {
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("f1"), b"one").unwrap();
    std::fs::create_dir(src.path().join("d")).unwrap();
    std::fs::write(src.path().join("d/f2"), b"two").unwrap();
    let archive = sys_tar_create(src.path());

    let work = tempfile::tempdir().unwrap();
    let dest = work.path().join("t");
    let mut lines = vec![json!({"op":"fs_push","dest":dest.to_str().unwrap()}).to_string()];
    lines.extend(data_frames(&archive));
    assert_eq!(type_of(&exchange(&lines).await[0]), "done");

    let pulled =
        exchange(&[json!({"op":"fs_pull","path":dest.to_str().unwrap()}).to_string()]).await;
    let out_archive = payload(&pulled, "data");
    let out = tempfile::tempdir().unwrap();
    sys_tar_extract(&out_archive, out.path());
    assert_eq!(std::fs::read(out.path().join("t/f1")).unwrap(), b"one");
    assert_eq!(std::fs::read(out.path().join("t/d/f2")).unwrap(), b"two");
}

#[tokio::test]
async fn push_truncated_stream_errors() {
    // Send fs_push + a data frame but no data_end: tar gets a partial archive
    // and the handler must report an error, not done.
    let dest = tempfile::tempdir().unwrap();
    let frames = exchange(&[
        json!({"op":"fs_push","dest":dest.path().join("p").to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"not-a-real-tar")}).to_string(),
    ])
    .await;
    assert_eq!(
        type_of(frames.last().unwrap()),
        "error",
        "truncated push: {frames:?}"
    );
}

#[tokio::test]
async fn push_bad_archive_reports_tar_failure() {
    // A complete stream that isn't a valid tar → tar exits nonzero → Internal.
    let dest = tempfile::tempdir().unwrap();
    let frames = exchange(&[
        json!({"op":"fs_push","dest":dest.path().join("p").to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"this is not tar data at all\n")}).to_string(),
        json!({"op":"data_end"}).to_string(),
    ])
    .await;
    assert_eq!(type_of(frames.last().unwrap()), "error");
    assert_eq!(frames.last().unwrap()["kind"], "internal");
}

#[tokio::test]
async fn pull_missing_path_errors() {
    let frames = exchange(&[json!({"op":"fs_pull","path":"/no/such/dir/xyz"}).to_string()]).await;
    assert_eq!(type_of(frames.last().unwrap()), "error");
}

#[tokio::test]
async fn push_failure_leaves_dest_untouched() {
    // Atomicity: a truncated stream must not mutate a populated dest at all —
    // no partial files, no staging leftovers.
    let dest = tempfile::tempdir().unwrap();
    std::fs::write(dest.path().join("keep.txt"), b"KEEP").unwrap();
    std::fs::create_dir(dest.path().join("sub")).unwrap();
    std::fs::write(dest.path().join("sub/old.txt"), b"OLD").unwrap();

    let frames = exchange(&[
        json!({"op":"fs_push","dest":dest.path().to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"partial garbage")}).to_string(),
    ])
    .await;
    assert_eq!(type_of(frames.last().unwrap()), "error");

    let names: Vec<String> = std::fs::read_dir(dest.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(sorted, ["keep.txt", "sub"], "dest mutated: {names:?}");
    assert_eq!(
        std::fs::read(dest.path().join("keep.txt")).unwrap(),
        b"KEEP"
    );
    assert_eq!(
        std::fs::read(dest.path().join("sub/old.txt")).unwrap(),
        b"OLD"
    );
}

#[tokio::test]
async fn push_overlays_existing_tree_and_cleans_staging() {
    // tar semantics survive the staged merge: dirs merge recursively, an
    // existing file is replaced, unrelated files survive, staging vanishes.
    let src = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("sub")).unwrap();
    std::fs::write(src.path().join("sub/new.txt"), b"NEW").unwrap();
    std::fs::write(src.path().join("replaced.txt"), b"AFTER").unwrap();
    let archive = sys_tar_create(src.path());

    let dest = tempfile::tempdir().unwrap();
    std::fs::write(dest.path().join("keep.txt"), b"KEEP").unwrap();
    std::fs::write(dest.path().join("replaced.txt"), b"BEFORE").unwrap();
    std::fs::create_dir(dest.path().join("sub")).unwrap();
    std::fs::write(dest.path().join("sub/old.txt"), b"OLD").unwrap();

    let mut lines = vec![json!({"op":"fs_push","dest":dest.path().to_str().unwrap()}).to_string()];
    lines.extend(data_frames(&archive));
    let frames = exchange(&lines).await;
    assert_eq!(type_of(&frames[0]), "done", "push failed: {frames:?}");

    assert_eq!(
        std::fs::read(dest.path().join("keep.txt")).unwrap(),
        b"KEEP"
    );
    assert_eq!(
        std::fs::read(dest.path().join("replaced.txt")).unwrap(),
        b"AFTER"
    );
    assert_eq!(
        std::fs::read(dest.path().join("sub/old.txt")).unwrap(),
        b"OLD"
    );
    assert_eq!(
        std::fs::read(dest.path().join("sub/new.txt")).unwrap(),
        b"NEW"
    );
    let staging: Vec<String> = std::fs::read_dir(dest.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".silkd-push-"))
        .collect();
    assert!(staging.is_empty(), "staging left behind: {staging:?}");
}
