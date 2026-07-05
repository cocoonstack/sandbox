//! git verbs: wrap the guest git binary and return structured results
//! (branch, ahead/behind, per-file status, commit hash) rather than stdout to
//! scrape. clone/push/pull need the network, so on the none lane they fail
//! with a typed error pointing at fs.push; the rest are local and always work.
//! An auth token rides in an in-memory `http.extraHeader`, never the guest disk.

use std::process::Stdio;

use tokio::io::AsyncWrite;
use tokio::process::Command;

use crate::proto::{self, ErrorKind, GitBranchOp, GitFileStatus, Response};

/// Runs `git`, capturing output. `auth` (a token) is injected as an in-memory
/// Authorization header via `-c`, so it never touches the guest filesystem.
async fn git(
    dir: &str,
    auth: Option<&str>,
    args: &[&str],
) -> std::io::Result<std::process::Output> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir);
    if let Some(token) = auth {
        cmd.arg("-c")
            .arg(format!("http.extraHeader=Authorization: Bearer {token}"));
    }
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await
}

async fn fail<W: AsyncWrite + Unpin>(w: &mut W, out: &std::process::Output) -> std::io::Result<()> {
    let msg = String::from_utf8_lossy(&out.stderr).trim().to_string();
    proto::write_frame(
        w,
        &Response::error(ErrorKind::Internal, format!("git: {msg}")),
    )
    .await
}

fn no_egress() -> Response {
    Response::error(
        ErrorKind::Unimplemented,
        "no network on this sandbox; use fs.push to move a repository in",
    )
}

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
    let mut args = vec!["clone".to_string()];
    if let Some(b) = branch {
        args.push("--branch".to_string());
        args.push(b);
    }
    if let Some(d) = depth {
        args.push("--depth".to_string());
        args.push(d.to_string());
    }
    args.push(url);
    args.push(path);
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    // clone has no repo dir yet, so run it from cwd (-C ".").
    let out = git(".", auth.as_deref(), &refs).await?;
    terminal(w, out).await
}

/// Reports branch, ahead/behind, and per-file status from porcelain v2.
pub async fn status<W: AsyncWrite + Unpin>(w: &mut W, path: String) -> std::io::Result<()> {
    let out = git(&path, None, &["status", "--porcelain=v2", "--branch"]).await?;
    if !out.status.success() {
        return fail(w, &out).await;
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
    terminal(w, out).await
}

/// Commits staged changes with `message` and `author` ("Name <email>"),
/// returning the new commit hash.
pub async fn commit<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    message: String,
    author: String,
) -> std::io::Result<()> {
    let out = git(
        &path,
        None,
        &["commit", "--author", &author, "-m", &message],
    )
    .await?;
    if !out.status.success() {
        return fail(w, &out).await;
    }
    let rev = git(&path, None, &["rev-parse", "HEAD"]).await?;
    if !rev.status.success() {
        return fail(w, &rev).await;
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
    if !crate::net::has_egress() {
        return proto::write_frame(w, &no_egress()).await;
    }
    let out = git(&path, auth.as_deref(), &["push"]).await?;
    terminal(w, out).await
}

/// Pulls the current branch.
pub async fn pull<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    auth: Option<String>,
) -> std::io::Result<()> {
    if !crate::net::has_egress() {
        return proto::write_frame(w, &no_egress()).await;
    }
    let out = git(&path, auth.as_deref(), &["pull"]).await?;
    terminal(w, out).await
}

/// Lists, creates, deletes, or checks out a branch.
pub async fn branch<W: AsyncWrite + Unpin>(
    w: &mut W,
    path: String,
    op: GitBranchOp,
    name: Option<String>,
) -> std::io::Result<()> {
    match op {
        GitBranchOp::List => {
            let out = git(&path, None, &["branch", "--format=%(refname:short)"]).await?;
            if !out.status.success() {
                return fail(w, &out).await;
            }
            let branches: Vec<String> = String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(str::to_string)
                .collect();
            let cur = git(&path, None, &["rev-parse", "--abbrev-ref", "HEAD"]).await?;
            let current = String::from_utf8_lossy(&cur.stdout).trim().to_string();
            proto::write_frame(w, &Response::GitBranches { current, branches }).await
        }
        GitBranchOp::Create | GitBranchOp::Delete | GitBranchOp::Checkout => {
            let Some(name) = name else {
                return proto::write_frame(
                    w,
                    &Response::error(ErrorKind::BadRequest, "branch op needs a name"),
                )
                .await;
            };
            let args: Vec<&str> = match op {
                GitBranchOp::Create => vec!["branch", &name],
                GitBranchOp::Delete => vec!["branch", "-D", &name],
                GitBranchOp::Checkout => vec!["checkout", &name],
                GitBranchOp::List => unreachable!(),
            };
            let out = git(&path, None, &args).await?;
            terminal(w, out).await
        }
    }
}

/// Writes Done on success, else the git stderr as an error frame.
async fn terminal<W: AsyncWrite + Unpin>(
    w: &mut W,
    out: std::process::Output,
) -> std::io::Result<()> {
    if out.status.success() {
        proto::write_frame(w, &Response::Done).await
    } else {
        fail(w, &out).await
    }
}

fn parse_status(text: &str) -> Response {
    let mut branch = String::new();
    let (mut ahead, mut behind) = (0, 0);
    let mut files = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# branch.head ") {
            branch = rest.to_string();
        } else if let Some(rest) = line.strip_prefix("# branch.ab ") {
            (ahead, behind) = parse_ahead_behind(rest);
        } else if let Some(f) = parse_file_line(line) {
            files.push(f);
        }
    }
    Response::GitStatusResult {
        branch,
        ahead,
        behind,
        files,
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

/// Parses one porcelain-v2 entry. Changed entries carry the 2-char XY code at
/// field 2 and the path as the 9th field (kept intact even with spaces; a
/// rename appends "\t<orig>", dropped here). Untracked ("?") is a bare path.
fn parse_file_line(line: &str) -> Option<GitFileStatus> {
    let kind = line.split(' ').next()?;
    match kind {
        "1" | "2" => {
            let mut fields = line.splitn(9, ' ');
            let xy = fields.nth(1)?; // field 2
            let mut chars = xy.chars();
            let staged = chars.next()?.to_string();
            let unstaged = chars.next()?.to_string();
            let path = fields.nth(6)?.split('\t').next()?.to_string(); // field 9
            Some(GitFileStatus {
                path,
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
