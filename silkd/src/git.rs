//! git verbs: wrap the guest git binary and return structured results
//! (branch, ahead/behind, per-file status, commit hash) rather than stdout to
//! scrape. clone/push/pull need the network, so on the none lane they fail
//! with a typed error pointing at fs.push; the rest are local and always work.
//! An auth token rides in an in-memory `http.extraHeader`, never the guest disk.

use std::process::Stdio;

use tokio::io::AsyncWrite;
use tokio::process::Command;

use crate::proto::{self, ErrorKind, GitBranchOp, GitFileStatus, Response};
use crate::sysutil;

/// Estimated JSON bytes of file entries one status frame carries before it truncates; an eighth of the frame cap leaves room for escaping.
const STATUS_FILES_BYTES: usize = proto::MAX_FRAME / 8;
const STATUS_ENTRY_OVERHEAD: usize = 40;

/// Clones `url` into `path` (optionally a branch, shallow depth), then Done.
pub async fn clone<W: AsyncWrite + Unpin>(
    w: &mut W,
    url: String,
    path: String,
    branch: Option<String>,
    depth: Option<u32>,
    auth: Option<String>,
) -> std::io::Result<()> {
    if !crate::net::has_egress() {
        return proto::write_frame(w, &no_egress()).await;
    }
    let depth_arg;
    let mut args: Vec<&str> = vec!["clone"];
    if let Some(b) = &branch {
        args.extend(["--branch", b]);
    }
    if let Some(d) = depth {
        depth_arg = d.to_string();
        args.extend(["--depth", &depth_arg]);
    }
    args.push(&url);
    args.push(&path);
    // clone has no repo dir yet, so run it from cwd (-C ".").
    let out = git(".", auth.as_deref(), &args).await?;
    terminal(w, "clone", &out).await
}

/// Reports branch, ahead/behind, and per-file status from porcelain v2.
pub async fn status<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> std::io::Result<()> {
    let out = git(&path, None, &["status", "--porcelain=v2", "--branch"]).await?;
    if !out.status.success() {
        return terminal(w, "status", &out).await;
    }
    proto::write_frame(w, &parse_status(&String::from_utf8_lossy(&out.stdout))).await
}

/// Stages `files` under `path`.
pub async fn add<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    files: Vec<String>,
) -> std::io::Result<()> {
    let mut args = vec!["add", "--"];
    args.extend(files.iter().map(String::as_str));
    let out = git(&path, None, &args).await?;
    terminal(w, "add", &out).await
}

/// Commits staged changes with `message` and `author` ("Name <email>"),
/// returning the new commit hash.
pub async fn commit<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    message: String,
    author: String,
) -> std::io::Result<()> {
    // A fresh guest has no committer identity, so git commit would fail to
    // auto-detect one; derive the committer from the author.
    let (name, email) = split_author(&author);
    let mut cmd = git_cmd(&path, None);
    cmd.env("GIT_COMMITTER_NAME", name)
        .env("GIT_COMMITTER_EMAIL", email);
    let out = cmd
        .args(["commit", "--author", &author, "-m", &message])
        .output()
        .await?;
    if !out.status.success() {
        return terminal(w, "commit", &out).await;
    }
    let rev = git(&path, None, &["rev-parse", "HEAD"]).await?;
    if !rev.status.success() {
        return terminal(w, "rev-parse", &rev).await;
    }
    let hash = String::from_utf8_lossy(&rev.stdout).trim().to_string();
    proto::write_frame(w, &Response::GitCommitResult { hash }).await
}

/// Pushes the current branch.
pub async fn push<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    auth: Option<String>,
) -> std::io::Result<()> {
    net_verb(w, path, auth, "push").await
}

/// Pulls the current branch.
pub async fn pull<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    auth: Option<String>,
) -> std::io::Result<()> {
    net_verb(w, path, auth, "pull").await
}

/// Lists, creates, deletes, or checks out a branch.
pub async fn branch<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    op: GitBranchOp,
    name: Option<String>,
) -> std::io::Result<()> {
    let args: Vec<&str> = match (op, &name) {
        (GitBranchOp::List, _) => return list_branches(w, &path).await,
        (_, None) => {
            return proto::write_frame(
                w,
                &Response::error(ErrorKind::BadRequest, "branch op needs a name"),
            )
            .await;
        }
        (GitBranchOp::Create, Some(name)) => vec!["branch", name],
        (GitBranchOp::Delete, Some(name)) => vec!["branch", "-D", name],
        (GitBranchOp::Checkout, Some(name)) => vec!["checkout", name],
    };
    let out = git(&path, None, &args).await?;
    terminal(w, "branch", &out).await
}

/// Reports every local branch and the checked-out one.
async fn list_branches<W: AsyncWrite + Unpin>(w: &mut W, path: &str) -> std::io::Result<()> {
    let out = git(path, None, &["branch", "--format=%(refname:short)"]).await?;
    if !out.status.success() {
        return terminal(w, "branch", &out).await;
    }
    let branches: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_string)
        .collect();
    let cur = git(path, None, &["rev-parse", "--abbrev-ref", "HEAD"]).await?;
    let current = String::from_utf8_lossy(&cur.stdout).trim().to_string();
    proto::write_frame(w, &Response::GitBranches { current, branches }).await
}

/// push/pull shared body: the egress guard, the bare verb, a terminal frame.
async fn net_verb<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    auth: Option<String>,
    verb: &str,
) -> std::io::Result<()> {
    if !crate::net::has_egress() {
        return proto::write_frame(w, &no_egress()).await;
    }
    let out = git(&path, auth.as_deref(), &[verb]).await?;
    terminal(w, verb, &out).await
}

/// Builds a `git -C dir` command with config and stdio policy applied. Config
/// (an auth token, and quotePath=false so paths come back raw) rides in
/// `GIT_CONFIG_*` env vars, not `-c` args: the process environ is root-only,
/// whereas argv is world-readable via /proc/<pid>/cmdline — a de-escalated
/// exec could otherwise scrape the token.
fn git_cmd(dir: &str, auth: Option<&str>) -> Command {
    let mut cmd = Command::new("git");
    sysutil::align_proxy_env(&mut cmd);
    cmd.arg("-C").arg(dir);
    apply_config(&mut cmd, auth);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}

/// Runs `git`, capturing output.
async fn git(
    dir: &str,
    auth: Option<&str>,
    args: &[&str],
) -> std::io::Result<std::process::Output> {
    git_cmd(dir, auth).args(args).output().await
}

/// Injects git config as GIT_CONFIG_COUNT/KEY_n/VALUE_n env pairs.
fn apply_config(cmd: &mut Command, auth: Option<&str>) {
    let mut pairs: Vec<(&str, String)> = vec![("core.quotePath", "false".to_string())];
    if let Some(token) = auth {
        pairs.push(("http.extraHeader", format!("Authorization: Bearer {token}")));
    }
    cmd.env("GIT_CONFIG_COUNT", pairs.len().to_string());
    for (i, (key, value)) in pairs.iter().enumerate() {
        cmd.env(format!("GIT_CONFIG_KEY_{i}"), key);
        cmd.env(format!("GIT_CONFIG_VALUE_{i}"), value);
    }
}

fn no_egress() -> Response {
    Response::error(
        ErrorKind::Unimplemented,
        "no network on this sandbox; use fs.push to move a repository in",
    )
}

/// Writes Done on success, else the git stderr as an error frame.
async fn terminal<W: AsyncWrite + Unpin>(
    w: &mut W,
    verb: &str,
    out: &std::process::Output,
) -> std::io::Result<()> {
    proto::subprocess_result(w, out.status.success(), &format!("git {verb}"), &out.stderr).await
}

fn parse_status(text: &str) -> Response {
    let mut branch = String::new();
    let (mut ahead, mut behind) = (0, 0);
    let mut files = Vec::new();
    let mut bytes = 0;
    let mut truncated = false;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# branch.head ") {
            branch = rest.to_string();
        } else if let Some(rest) = line.strip_prefix("# branch.ab ") {
            (ahead, behind) = parse_ahead_behind(rest);
        } else if let Some(f) = parse_file_line(line) {
            bytes += f.path.len() + STATUS_ENTRY_OVERHEAD;
            if bytes > STATUS_FILES_BYTES {
                truncated = true;
                break;
            }
            files.push(f);
        }
    }
    Response::GitStatusResult {
        branch,
        ahead,
        behind,
        files,
        truncated,
    }
}

fn parse_ahead_behind(rest: &str) -> (u32, u32) {
    let mut ahead = 0;
    let mut behind = 0;
    for tok in rest.split_whitespace() {
        if let Some(n) = tok.strip_prefix('+') {
            ahead = n.parse().unwrap_or(0);
        } else if let Some(n) = tok.strip_prefix('-') {
            behind = n.parse().unwrap_or(0);
        }
    }
    (ahead, behind)
}

/// Parses one porcelain-v2 entry. XY is field 2; the path is the last
/// space-field (kept intact even with spaces — core.quotePath=false keeps it
/// raw). Ordinary changes ("1") have the path at field 9; renames/copies ("2")
/// add an Xscore field, so the path is field 10 and carries "<new>\t<orig>",
/// of which we keep the new path; unmerged ("u") entries carry four modes and
/// three hashes, putting the bare path at field 11. Untracked ("?") is a
/// bare path.
fn parse_file_line(line: &str) -> Option<GitFileStatus> {
    let kind = line.split(' ').next()?;
    match kind {
        "1" | "2" | "u" => {
            let mut fields = line.split(' ');
            let xy = fields.nth(1)?; // field 2
            let mut chars = xy.chars();
            let staged = chars.next()?.to_string();
            let unstaged = chars.next()?.to_string();
            let skip = match kind {
                "2" => 7,
                "u" => 8,
                _ => 6,
            }; // to reach the path field
            let path = fields.nth(skip)?;
            // Rejoin any spaces the split consumed, then drop a rename's \t<orig>.
            let rest: Vec<&str> = fields.collect();
            let full = if rest.is_empty() {
                path.to_string()
            } else {
                format!("{path} {}", rest.join(" "))
            };
            Some(GitFileStatus {
                path: full.split('\t').next()?.to_string(),
                staged,
                unstaged,
            })
        }
        "?" => Some(GitFileStatus {
            path: line.get(2..)?.to_string(),
            staged: "?".to_string(),
            unstaged: "?".to_string(),
        }),
        _ => None,
    }
}

/// Splits an author "Name <email>" into (name, email); a missing angle form
/// leaves the whole string as the name.
fn split_author(author: &str) -> (&str, &str) {
    if let Some(open) = author.find('<')
        && let Some(close) = author[open..].find('>')
    {
        return (author[..open].trim(), &author[open + 1..open + close]);
    }
    (author.trim(), "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_truncates_past_the_frame_budget() {
        let text: String = (0..200_000).map(|i| format!("? f{i}\n")).collect();
        let Response::GitStatusResult {
            files, truncated, ..
        } = parse_status(&text)
        else {
            panic!("status frame")
        };
        assert!(truncated);
        assert!(!files.is_empty() && files.len() < 200_000);
        assert!(serde_json::to_vec(&parse_status(&text)).unwrap().len() < proto::MAX_FRAME);

        let Response::GitStatusResult { truncated, .. } = parse_status("? one\n") else {
            panic!("status frame")
        };
        assert!(!truncated);
    }

    #[test]
    fn parses_ordinary_rename_untracked_and_conflict() {
        let ordinary = parse_file_line("1 .M N... 100644 100644 100644 h1 h2 src/main.rs").unwrap();
        assert_eq!(ordinary.path, "src/main.rs");
        assert_eq!(
            (ordinary.staged.as_str(), ordinary.unstaged.as_str()),
            (".", "M")
        );

        let rename =
            parse_file_line("2 R. N... 100644 100644 100644 h1 h2 R100 new.rs\told.rs").unwrap();
        assert_eq!(rename.path, "new.rs");

        let untracked = parse_file_line("? build/out.o").unwrap();
        assert_eq!(untracked.path, "build/out.o");

        let conflict =
            parse_file_line("u UU N... 100644 100644 100644 100644 h1 h2 h3 conflict.rs").unwrap();
        assert_eq!(conflict.path, "conflict.rs");
        assert_eq!(
            (conflict.staged.as_str(), conflict.unstaged.as_str()),
            ("U", "U")
        );
    }
}
