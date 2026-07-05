//! Search verb E2E: fs.find streams matches, fs.replace rewrites files.

mod common;

use common::{exchange, type_of};
use serde_json::json;

async fn write_file(dir: &std::path::Path, name: &str, body: &str) {
    tokio::fs::write(dir.join(name), body).await.unwrap();
}

#[tokio::test]
async fn find_streams_line_matches() {
    let dir = tempfile::tempdir().unwrap();
    write_file(dir.path(), "a.rs", "fn main() {\n    // TODO: fix\n}\n").await;
    write_file(dir.path(), "b.txt", "TODO ignore me (wrong glob)\n").await;
    tokio::fs::create_dir(dir.path().join("sub")).await.unwrap();
    write_file(&dir.path().join("sub"), "c.rs", "// TODO nested\n").await;

    let frames = exchange(&[json!({
        "op": "fs_find",
        "path": dir.path().to_str().unwrap(),
        "pattern": "TODO",
        "glob": ".rs"
    })
    .to_string()])
    .await;

    let matches: Vec<_> = frames.iter().filter(|f| type_of(f) == "match").collect();
    assert_eq!(matches.len(), 2, "recursive .rs matches only: {frames:?}");
    assert!(matches
        .iter()
        .all(|m| m["content"].as_str().unwrap().contains("TODO")));
    assert!(matches.iter().any(|m| m["line"] == 2)); // a.rs
    assert!(matches.iter().any(|m| m["line"] == 1)); // sub/c.rs
    assert_eq!(type_of(frames.last().unwrap()), "done");
}

#[tokio::test]
async fn find_rejects_bad_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let frames = exchange(&[json!({
        "op": "fs_find",
        "path": dir.path().to_str().unwrap(),
        "pattern": "("
    })
    .to_string()])
    .await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
}

#[tokio::test]
async fn replace_rewrites_and_counts() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.txt");
    let b = dir.path().join("b.txt");
    tokio::fs::write(&a, "foo foo bar\n").await.unwrap();
    tokio::fs::write(&b, "no match here\n").await.unwrap();

    let frames = exchange(&[json!({
        "op": "fs_replace",
        "files": [a.to_str().unwrap(), b.to_str().unwrap()],
        "pattern": "foo",
        "replacement": "baz"
    })
    .to_string()])
    .await;

    let replaced: Vec<_> = frames.iter().filter(|f| type_of(f) == "replaced").collect();
    assert_eq!(replaced.len(), 2);
    assert_eq!(replaced[0]["replacements"], 2);
    assert_eq!(replaced[1]["replacements"], 0);
    assert_eq!(type_of(frames.last().unwrap()), "done");
    assert_eq!(
        tokio::fs::read_to_string(&a).await.unwrap(),
        "baz baz bar\n"
    );
    assert_eq!(
        tokio::fs::read_to_string(&b).await.unwrap(),
        "no match here\n"
    );
}

#[tokio::test]
async fn replace_preserves_exec_bit() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("run.sh");
    tokio::fs::write(&script, "#!/bin/sh\nfoo\n").await.unwrap();
    tokio::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .await
        .unwrap();

    exchange(&[json!({
        "op": "fs_replace",
        "files": [script.to_str().unwrap()],
        "pattern": "foo",
        "replacement": "bar"
    })
    .to_string()])
    .await;

    let mode = tokio::fs::metadata(&script)
        .await
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o755, "exec bit stripped by replace");
}
