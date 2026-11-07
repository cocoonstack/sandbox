//! git verb E2E against a real git binary in temp repos; the none-lane guard uses the test lane override.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use serde_json::json;

use common::{assert_error, request, type_of};

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@e")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@e")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn init_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-q", "-b", "main"]);
    dir
}

fn chown_tree(dir: &std::path::Path, uid: u32) {
    std::os::unix::fs::lchown(dir, Some(uid), Some(uid)).unwrap();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() && !path.is_symlink() {
            chown_tree(&path, uid);
        } else {
            std::os::unix::fs::lchown(&path, Some(uid), Some(uid)).unwrap();
        }
    }
}

#[tokio::test]
async fn add_commit_status_reports_structured_state() {
    let repo = init_repo();
    let p = repo.path().to_str().unwrap();
    std::fs::write(repo.path().join("a.txt"), "hello\n").unwrap();

    let st = request(json!({"op":"git_status","path":p})).await;
    assert_eq!(type_of(&st[0]), "git_status_result");
    assert_eq!(st[0]["branch"], "main");
    let files = st[0]["files"].as_array().unwrap();
    assert!(
        files
            .iter()
            .any(|f| f["path"] == "a.txt" && f["staged"] == "?")
    );

    let add = request(json!({"op":"git_add","path":p,"files":["a.txt"]})).await;
    assert_eq!(type_of(add.last().unwrap()), "done");
    let commit = request(json!({
        "op":"git_commit","path":p,"message":"first","author":"Dev <dev@example.com>"
    }))
    .await;
    assert_eq!(type_of(&commit[0]), "git_commit_result");
    assert_eq!(commit[0]["hash"].as_str().unwrap().len(), 40);

    let st2 = request(json!({"op":"git_status","path":p})).await;
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

    let created =
        request(json!({"op":"git_branch","path":p,"action":"create","name":"feature"})).await;
    assert_eq!(type_of(created.last().unwrap()), "done");

    let list = request(json!({"op":"git_branch","path":p,"action":"list"})).await;
    assert_eq!(type_of(&list[0]), "git_branches");
    assert_eq!(list[0]["current"], "main");
    let branches: Vec<&str> = list[0]["branches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_str().unwrap())
        .collect();
    assert!(branches.contains(&"main") && branches.contains(&"feature"));

    let co =
        request(json!({"op":"git_branch","path":p,"action":"checkout","name":"feature"})).await;
    assert_eq!(type_of(co.last().unwrap()), "done");
}

#[tokio::test]
async fn branch_list_names_an_unborn_branch_and_a_detached_head() {
    let repo = init_repo();
    let p = repo.path().to_str().unwrap();

    let unborn = request(json!({"op":"git_branch","path":p,"action":"list"})).await;
    assert_eq!(type_of(&unborn[0]), "git_branches", "{unborn:?}");
    assert_eq!(unborn[0]["current"], "main", "{unborn:?}");

    std::fs::write(repo.path().join("f"), "x").unwrap();
    git(repo.path(), &["add", "f"]);
    git(repo.path(), &["commit", "-qm", "c"]);
    git(repo.path(), &["checkout", "-q", "--detach"]);

    let detached = request(json!({"op":"git_branch","path":p,"action":"list"})).await;
    assert_eq!(detached[0]["current"], "HEAD", "{detached:?}");
}

#[tokio::test]
async fn commit_failure_surfaces_git_error() {
    let repo = init_repo();
    let p = repo.path().to_str().unwrap();
    let frames = request(json!({
        "op":"git_commit","path":p,"message":"empty","author":"Dev <dev@example.com>"
    }))
    .await;
    assert_error(&frames[0], "internal");
}

#[tokio::test]
async fn clone_copies_a_repository() {
    let src = init_repo();
    std::fs::write(src.path().join("r.txt"), "body\n").unwrap();
    git(src.path(), &["add", "r.txt"]);
    git(src.path(), &["commit", "-qm", "init"]);
    let dst = tempfile::tempdir().unwrap();
    let target = dst.path().join("clone");
    let ok = request(json!({
        "op":"git_clone", "url": src.path().to_str().unwrap(), "path": target.to_str().unwrap()
    }))
    .await;
    assert_eq!(type_of(ok.last().unwrap()), "done", "clone failed: {ok:?}");
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

    let st = request(json!({"op":"git_status","path":p})).await;
    let files = st[0]["files"].as_array().unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f["path"].as_str().unwrap()).collect();
    assert!(
        paths.contains(&"new name.txt"),
        "rename path with spaces mangled: {paths:?}"
    );
    assert!(
        !paths.iter().any(|path| path.contains("R100")),
        "score leaked: {paths:?}"
    );
}

#[tokio::test]
async fn commit_works_without_preconfigured_identity() {
    let dir = init_repo();
    std::fs::write(dir.path().join("f"), "x").unwrap();
    let p = dir.path().to_str().unwrap();
    let add = request(json!({"op":"git_add","path":p,"files":["f"]})).await;
    assert_eq!(type_of(add.last().unwrap()), "done");
    let commit = request(json!({
        "op":"git_commit","path":p,"message":"m","author":"Dev <dev@example.com>"
    }))
    .await;
    assert_eq!(
        type_of(&commit[0]),
        "git_commit_result",
        "commit failed: {commit:?}"
    );
    assert_eq!(
        git(dir.path(), &["log", "-1", "--format=%cn <%ce>"]).trim(),
        "Dev <dev@example.com>",
        "the committer must come from the author, not the host's git config"
    );
}

#[tokio::test]
#[ignore = "needs root"]
async fn git_runs_as_the_repo_owner_under_root() {
    let repo = init_repo();
    let p = repo.path().to_str().unwrap();
    std::fs::write(repo.path().join("a.txt"), "hello\n").unwrap();
    chown_tree(repo.path(), 65534);

    let st = request(json!({"op":"git_status","path":p})).await;
    assert_eq!(type_of(&st[0]), "git_status_result", "{st:?}");
    let add = request(json!({"op":"git_add","path":p,"files":["a.txt"]})).await;
    assert_eq!(type_of(add.last().unwrap()), "done", "{add:?}");
    let index = std::fs::metadata(repo.path().join(".git/index")).unwrap();
    assert_eq!(std::os::unix::fs::MetadataExt::uid(&index), 65534);
}
