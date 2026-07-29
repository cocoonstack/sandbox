//! git verb E2E against a real git binary in temp repos. Local verbs run on
//! any host; the none-lane guard is exercised via the test lane override.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code, like #[cfg(test)]
mod common;

use common::{exchange, type_of};
use serde_json::{Value, json};

fn git(dir: &std::path::Path, args: &[&str]) {
    let ok = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@e")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@e")
        .output()
        .unwrap()
        .status
        .success();
    assert!(ok, "git {args:?} failed");
}

fn init_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-q", "-b", "main"]);
    dir
}

fn last(frames: &[Value]) -> &Value {
    frames.last().unwrap()
}

#[tokio::test]
async fn add_commit_status_reports_structured_state() {
    let repo = init_repo();
    let p = repo.path().to_str().unwrap();
    std::fs::write(repo.path().join("a.txt"), "hello\n").unwrap();

    // Untracked file shows in status.
    let st = exchange(&[json!({"op":"git_status","path":p}).to_string()]).await;
    assert_eq!(type_of(&st[0]), "git_status_result");
    assert_eq!(st[0]["branch"], "main");
    let files = st[0]["files"].as_array().unwrap();
    assert!(
        files
            .iter()
            .any(|f| f["path"] == "a.txt" && f["staged"] == "?")
    );

    // Stage and commit; a hash comes back.
    let add = exchange(&[json!({"op":"git_add","path":p,"files":["a.txt"]}).to_string()]).await;
    assert_eq!(type_of(last(&add)), "done");
    let commit = exchange(&[json!({
        "op":"git_commit","path":p,"message":"first","author":"Dev <dev@example.com>"
    })
    .to_string()])
    .await;
    assert_eq!(type_of(&commit[0]), "git_commit_result");
    assert_eq!(commit[0]["hash"].as_str().unwrap().len(), 40);

    // Clean tree now.
    let st2 = exchange(&[json!({"op":"git_status","path":p}).to_string()]).await;
    assert!(
        st2[0]["files"].as_array().unwrap().is_empty(),
        "expected clean: {st2:?}"
    );
}

#[tokio::test]
async fn branch_create_list_checkout() {
    let repo = init_repo();
    let p = repo.path().to_str().unwrap();
    std::fs::write(repo.path().join("f"), "x").unwrap();
    git(repo.path(), &["add", "f"]);
    git(repo.path(), &["commit", "-qm", "c"]);

    let created = exchange(&[
        json!({"op":"git_branch","path":p,"action":"create","name":"feature"}).to_string(),
    ])
    .await;
    assert_eq!(type_of(last(&created)), "done");

    let list = exchange(&[json!({"op":"git_branch","path":p,"action":"list"}).to_string()]).await;
    assert_eq!(type_of(&list[0]), "git_branches");
    assert_eq!(list[0]["current"], "main");
    let branches: Vec<&str> = list[0]["branches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_str().unwrap())
        .collect();
    assert!(branches.contains(&"main") && branches.contains(&"feature"));

    let co = exchange(&[
        json!({"op":"git_branch","path":p,"action":"checkout","name":"feature"}).to_string(),
    ])
    .await;
    assert_eq!(type_of(last(&co)), "done");
}

#[tokio::test]
async fn commit_failure_surfaces_git_error() {
    let repo = init_repo(); // nothing staged
    let p = repo.path().to_str().unwrap();
    let frames = exchange(&[json!({
        "op":"git_commit","path":p,"message":"empty","author":"Dev <dev@example.com>"
    })
    .to_string()])
    .await;
    assert_eq!(type_of(&frames[0]), "error");
}

// One test owns the lane override so the two cases cannot fight over it.
#[tokio::test]
async fn clone_lane_behavior() {
    // none lane refuses with a typed error pointing at fs.push.
    silkd::net::override_egress_for_tests(Some(false));
    let none = exchange(&[json!({
        "op":"git_clone","url":"https://example.com/x.git","path":"/tmp/silkd-x"
    })
    .to_string()])
    .await;
    assert_eq!(type_of(&none[0]), "error");
    assert_eq!(none[0]["kind"], "unimplemented");
    assert!(none[0]["message"].as_str().unwrap().contains("fs.push"));

    // egress lane clones (exercised against a local source repo).
    let src = init_repo();
    std::fs::write(src.path().join("r.txt"), "body\n").unwrap();
    git(src.path(), &["add", "r.txt"]);
    git(src.path(), &["commit", "-qm", "init"]);
    let dst = tempfile::tempdir().unwrap();
    let target = dst.path().join("clone");
    silkd::net::override_egress_for_tests(Some(true));
    let ok = exchange(&[json!({
        "op":"git_clone", "url": src.path().to_str().unwrap(), "path": target.to_str().unwrap()
    })
    .to_string()])
    .await;
    silkd::net::override_egress_for_tests(None);
    assert_eq!(type_of(last(&ok)), "done", "clone failed: {ok:?}");
    assert!(target.join("r.txt").exists());
}

#[tokio::test]
async fn status_handles_rename_and_spaces() {
    let repo = init_repo();
    let p = repo.path().to_str().unwrap();
    std::fs::write(repo.path().join("orig name.txt"), "x\n").unwrap();
    git(repo.path(), &["add", "."]);
    git(repo.path(), &["commit", "-qm", "c"]);
    std::fs::rename(
        repo.path().join("orig name.txt"),
        repo.path().join("new name.txt"),
    )
    .unwrap();
    git(repo.path(), &["add", "-A"]);

    let st = exchange(&[json!({"op":"git_status","path":p}).to_string()]).await;
    let files = st[0]["files"].as_array().unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f["path"].as_str().unwrap()).collect();
    assert!(
        paths.contains(&"new name.txt"),
        "rename path with spaces mangled: {paths:?}"
    );
    assert!(
        !paths.iter().any(|p| p.contains("R100")),
        "score leaked: {paths:?}"
    );
}

#[tokio::test]
async fn commit_works_without_preconfigured_identity() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        std::process::Command::new("git")
            .arg("-C")
            .arg(dir.path())
            .args(["-c", "init.defaultBranch=main", "init", "-q"])
            .env_remove("GIT_AUTHOR_NAME")
            .env_remove("GIT_COMMITTER_NAME")
            .status()
            .unwrap()
            .success()
    );
    std::fs::write(dir.path().join("f"), "x").unwrap();
    let p = dir.path().to_str().unwrap();
    let add = exchange(&[json!({"op":"git_add","path":p,"files":["f"]}).to_string()]).await;
    assert_eq!(type_of(last(&add)), "done");
    let commit = exchange(&[json!({
        "op":"git_commit","path":p,"message":"m","author":"Dev <dev@example.com>"
    })
    .to_string()])
    .await;
    assert_eq!(
        type_of(&commit[0]),
        "git_commit_result",
        "commit failed: {commit:?}"
    );
}
