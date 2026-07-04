//! Whole-tree transfer E2E: push a tar stream in, pull a tar stream out,
//! using the system tar to build/extract the reference archives.

mod common;

use std::path::Path;
use std::process::Command;

use common::{b64, decode, exchange, type_of};
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

fn data_frames(bytes: &[u8]) -> Vec<String> {
    let mut lines: Vec<String> = bytes
        .chunks(16 * 1024)
        .map(|c| json!({"op":"data","data":b64(c)}).to_string())
        .collect();
    lines.push(json!({"op":"data_end"}).to_string());
    lines
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
    let archive: Vec<u8> = frames
        .iter()
        .filter(|f| type_of(f) == "data")
        .flat_map(decode)
        .collect();

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
    let out_archive: Vec<u8> = pulled
        .iter()
        .filter(|f| type_of(f) == "data")
        .flat_map(decode)
        .collect();
    let out = tempfile::tempdir().unwrap();
    sys_tar_extract(&out_archive, out.path());
    assert_eq!(std::fs::read(out.path().join("t/f1")).unwrap(), b"one");
    assert_eq!(std::fs::read(out.path().join("t/d/f2")).unwrap(), b"two");
}

#[tokio::test]
async fn pull_missing_path_errors() {
    let frames = exchange(&[json!({"op":"fs_pull","path":"/no/such/dir/xyz"}).to_string()]).await;
    assert_eq!(type_of(frames.last().unwrap()), "error");
}
