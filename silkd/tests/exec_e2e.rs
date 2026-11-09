//! exec/procs verb E2E over the in-memory duplex, spawning real processes so it runs on any Unix.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use silkd::server::State;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use common::{
    alive, assert_error, b64, connect_with, exchange, frames_until, next_frame, one, proc_entry,
    request, require_perl, roundtrip, send, stdout_body, type_of, wait_for_path,
};

async fn detached_pid(state: &Arc<State>, script: &str) -> u64 {
    let argv = json!(["/bin/sh", "-c", script]);
    let started = one(
        state,
        &json!({"op":"exec","argv":argv,"detach":true}).to_string(),
    )
    .await;
    assert_eq!(type_of(&started[0]), "started");
    started[0]["pid"].as_u64().unwrap()
}

async fn attach_frames(state: &Arc<State>, pid: u64) -> Vec<Value> {
    let (_cw, mut lines, _) = connect_with(state, json!({"op":"attach","pid":pid})).await;
    frames_until(&mut lines, |f| {
        matches!(type_of(f), "exit" | "done" | "error")
    })
    .await
}

async fn wait_for_exit(state: &Arc<State>, pid: u64) -> Vec<Value> {
    for _ in 0..250 {
        let logs = one(state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            return logs;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("pid {pid} never exited");
}

async fn wait_listed(state: &Arc<State>, pid: u64) -> Value {
    for _ in 0..250 {
        let ps = one(state, r#"{"op":"ps"}"#).await;
        if let Some(p) = proc_entry(&ps, pid) {
            return p.clone();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("pid {pid} never appeared in ps");
}

#[tokio::test]
async fn exec_streams_stdout_then_exit() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/echo","-n","hello"]}"#).await;
    assert_eq!(type_of(&frames[0]), "started");
    assert_eq!(stdout_body(&frames), "hello");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 0);
}

#[tokio::test]
async fn large_output_is_delivered_without_loss() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","seq 1 20000"]}"#).await;
    let body = stdout_body(&frames);
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(
        lines.len(),
        20000,
        "expected 20000 lines, got {}",
        lines.len()
    );
    assert_eq!(lines.first().copied(), Some("1"));
    assert_eq!(lines.last().copied(), Some("20000"));
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn nonzero_exit_is_reported() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","exit 7"]}"#).await;
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 7);
}

#[tokio::test]
async fn empty_argv_is_a_bad_request() {
    let frames = roundtrip(r#"{"op":"exec","argv":[]}"#).await;
    assert_error(&frames[0], "bad_request");
}

#[tokio::test]
async fn kill_unknown_pid_is_not_found() {
    let frames = roundtrip(r#"{"op":"kill","pid":999999}"#).await;
    assert_error(&frames[0], "not_found");
}

#[tokio::test]
async fn daemonizer_exit_is_the_last_frame() {
    let frames = tokio::time::timeout(
        Duration::from_secs(8),
        roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","(sleep 5; echo late) & exit 0"]}"#),
    )
    .await
    .expect("exec wedged on a daemonizing child");
    assert_eq!(
        type_of(frames.last().unwrap()),
        "exit",
        "exit must be last: {frames:?}"
    );
    assert_eq!(frames.last().unwrap()["code"], 0, "{frames:?}");
    assert!(
        !stdout_body(&frames).contains("late"),
        "grandchild output must not leak after exit"
    );
}

#[tokio::test]
async fn attach_streams_live_output_then_exit() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "sleep 0.3; echo live-chunk").await;
    let frames = tokio::time::timeout(Duration::from_secs(8), attach_frames(&state, pid))
        .await
        .expect("attach hung");
    assert!(
        stdout_body(&frames).contains("live-chunk"),
        "attach missed live output: {frames:?}"
    );
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn attach_to_exited_process_returns_exit_immediately() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "echo done-fast").await;
    wait_for_exit(&state, pid).await;
    let frames = tokio::time::timeout(
        Duration::from_secs(3),
        one(&state, &format!(r#"{{"op":"attach","pid":{pid}}}"#)),
    )
    .await
    .expect("attach to an exited process must not hang");
    assert!(stdout_body(&frames).contains("done-fast"));
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn attach_keeps_a_tail_emitted_during_the_replay() {
    let state = Arc::new(State::new());
    let proc = state.table.register(4242, vec!["x".to_string()], true, 0);
    proc.emit_bytes(false, &[b'r'; 64 * 1024]);
    let (client, server) = tokio::io::duplex(1024);
    let (sr, sw) = tokio::io::split(server);
    let st = Arc::clone(&state);
    let serve = tokio::spawn(async move { st.serve(BufReader::new(sr), sw).await });
    let (mut cr, mut cw) = tokio::io::split(client);
    cw.write_all(b"{\"op\":\"attach\",\"pid\":4242}\n")
        .await
        .unwrap();
    let mut out = vec![0u8];
    cr.read_exact(&mut out).await.unwrap();

    proc.emit_bytes(false, b"tail");
    proc.finish(0);
    cw.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(8), cr.read_to_end(&mut out))
        .await
        .expect("attach hung")
        .unwrap();
    serve.await.unwrap().unwrap();

    let frames: Vec<Value> = out
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).unwrap())
        .collect();
    assert!(
        stdout_body(&frames).ends_with("rtail"),
        "attach dropped the tail emitted during the replay"
    );
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn attach_ends_when_its_client_hangs_up() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "sleep 30").await;
    let (cw, lines, handle) = connect_with(&state, json!({"op":"attach","pid":pid})).await;

    drop(lines);
    drop(cw);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("attach did not end on hang-up")
        .unwrap()
        .unwrap();
    one(&state, &json!({"op":"kill","pid":pid}).to_string()).await;
}

#[tokio::test]
async fn attach_unknown_pid_is_not_found() {
    let frames = roundtrip(r#"{"op":"attach","pid":999999}"#).await;
    assert_error(&frames[0], "not_found");
}

#[tokio::test]
async fn kill_actually_terminates_a_live_process() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "sleep 30").await;
    wait_listed(&state, pid).await;
    let killed = one(&state, &format!(r#"{{"op":"kill","pid":{pid}}}"#)).await;
    assert_eq!(type_of(&killed[0]), "done");
    wait_for_exit(&state, pid).await;
}

#[tokio::test]
async fn kill_ends_the_whole_command_not_only_its_shell() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("child");
    let state = Arc::new(State::new());
    let script = format!("sleep 300 & echo $! > {}; wait", marker.display());
    let pid = detached_pid(&state, &script).await;
    wait_listed(&state, pid).await;
    let child = tokio::time::timeout(common::DEADLINE, async {
        loop {
            if let Ok(s) = std::fs::read_to_string(&marker)
                && let Ok(n) = s.trim().parse::<u32>()
            {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the shell never wrote its child's pid");
    let killed = one(&state, &format!(r#"{{"op":"kill","pid":{pid}}}"#)).await;
    assert_eq!(type_of(&killed[0]), "done");
    wait_for_exit(&state, pid).await;
    tokio::time::timeout(common::DEADLINE, async {
        while alive(child) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the command's child outlived the kill");
}

#[tokio::test]
async fn exec_unknown_user_is_rejected() {
    let frames =
        roundtrip(r#"{"op":"exec","argv":["true"],"user":"definitely-not-a-user-xyz"}"#).await;
    assert_error(&frames[0], "bad_request");
}

#[tokio::test]
#[ignore = "needs root"]
async fn exec_as_a_user_keeps_its_supplementary_groups() {
    let user = group_member().expect("no /etc/group entry lists a member to exec as");
    let want = std::process::Command::new("id")
        .args(["-G", &user])
        .output()
        .unwrap();
    let mut want: Vec<String> = String::from_utf8(want.stdout)
        .unwrap()
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    want.sort();
    let frames = request(json!({"op":"exec","argv":["id","-G"],"user":user})).await;
    let mut got: Vec<String> = stdout_body(&frames)
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    got.sort();
    assert!(
        want.len() > 1,
        "{user} has no supplementary group: {want:?}"
    );
    assert_eq!(got, want, "{frames:?}");
}

#[tokio::test]
#[ignore = "needs root"]
async fn exec_as_a_user_refuses_a_cwd_the_user_cannot_enter() {
    let locked = tempfile::tempdir().unwrap();
    std::fs::set_permissions(locked.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let inner = locked.path().join("inner");
    std::fs::create_dir(&inner).unwrap();
    let frames = request(json!({
        "op":"exec","argv":["true"],"user":"nobody","cwd":inner.to_str().unwrap()
    }))
    .await;
    assert_error(&frames[0], "bad_request");
}

#[tokio::test]
async fn exec_missing_binary_is_a_bad_request() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/no/such/binary-xyz"]}"#).await;
    assert_error(&frames[0], "bad_request");
    assert!(frames[0]["message"].as_str().unwrap().contains("spawn"));
}

#[tokio::test]
async fn exec_missing_cwd_is_a_bad_request() {
    let frames = roundtrip(r#"{"op":"exec","argv":["true"],"cwd":"/no/such/dir-xyz"}"#).await;
    assert_error(&frames[0], "bad_request");
}

#[tokio::test]
async fn exec_non_executable_file_is_a_bad_request() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let state = Arc::new(State::new());
    let frames = one(
        &state,
        &json!({"op":"exec","argv":[file.path()]}).to_string(),
    )
    .await;
    assert_error(&frames[0], "bad_request");
    assert!(frames[0]["message"].as_str().unwrap().contains("spawn"));
}

#[tokio::test]
async fn exec_stdin_is_piped_to_the_child() {
    let frames = exchange(&[
        r#"{"op":"exec","argv":["/bin/cat"]}"#.to_string(),
        json!({"op":"stdin","data":b64(b"piped-in\n")}).to_string(),
        r#"{"op":"stdin_close"}"#.to_string(),
    ])
    .await;
    assert_eq!(stdout_body(&frames), "piped-in\n");
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn kill_of_an_exited_process_is_a_noop_success() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "echo x").await;
    wait_for_exit(&state, pid).await;
    let killed = one(&state, &format!(r#"{{"op":"kill","pid":{pid}}}"#)).await;
    assert_eq!(
        type_of(&killed[0]),
        "done",
        "kill of exited pid should be a no-op success"
    );
}

#[tokio::test]
async fn detached_exec_is_listed_then_logs_replay_output_and_exit() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "echo detached-hi").await;
    let listed = wait_listed(&state, pid).await;
    assert_eq!(listed["detached"], true, "{listed:?}");
    let logs = wait_for_exit(&state, pid).await;
    assert_eq!(stdout_body(&logs), "detached-hi\n");
}

#[tokio::test]
async fn disconnect_during_drain_publishes_real_exit_code() {
    let state = Arc::new(State::new());
    let (cw, mut lines, _) = connect_with(
        &state,
        json!({
            "op": "exec",
            "argv": ["/bin/sh", "-c", "{ sleep 0.5; echo late; sleep 30; } & exit 7"]
        }),
    )
    .await;

    let started = next_frame(&mut lines).await;
    assert_eq!(type_of(&started), "started");
    let pid = started["pid"].as_u64().unwrap();

    let (_aw, mut attached, _) = connect_with(&state, json!({"op":"attach","pid":pid})).await;
    let late = next_frame(&mut attached).await;
    assert_eq!(type_of(&late), "stdout", "{late:?}");
    drop(lines);
    drop(cw);

    let frames = tokio::time::timeout(
        Duration::from_secs(8),
        frames_until(&mut attached, |f| {
            matches!(type_of(f), "exit" | "done" | "error")
        }),
    )
    .await
    .expect("attach wedged");
    let last = frames.last().unwrap();
    assert_eq!(
        type_of(last),
        "exit",
        "attach must end with exit: {frames:?}"
    );
    assert_eq!(last["code"], 7, "real exit code, not a fabricated -1");
}

#[tokio::test]
async fn stdin_held_by_a_forked_child_does_not_wedge_the_next_rpc() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = connect_with(
        &state,
        json!({
            "op": "exec",
            "argv": ["/bin/sh", "-c", "exec 3<&0; sleep 8 <&3 3<&- & exit 0"]
        }),
    )
    .await;
    let chunk = b64(&[b'x'; 32 * 1024]);
    for _ in 0..30 {
        send(&mut cw, json!({"op":"stdin","data":chunk})).await;
    }
    let frames = frames_until(&mut lines, |f| type_of(f) == "exit").await;
    assert_eq!(frames.last().unwrap()["code"], 0, "{frames:?}");

    send(&mut cw, json!({"op":"info"})).await;
    let info = tokio::time::timeout(Duration::from_secs(3), next_frame(&mut lines))
        .await
        .expect("the next RPC is wedged on a stdin nobody reads");
    assert_eq!(type_of(&info), "info");
    cw.shutdown().await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_client_gone_mid_output_releases_the_stdin_a_forked_child_holds() {
    require_perl();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("count");
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect_with(
        &state,
        json!({
            "op": "exec",
            "argv": ["/bin/sh", "-c", "exec 3<&0; perl -e 'setpgrp; exec @ARGV' /bin/sh -c ': >\"$OUT.ready\"; sleep 1; wc -c <&3 >\"$OUT.tmp\"; mv \"$OUT.tmp\" \"$OUT\"' >/dev/null 2>&1 & while :; do echo x; sleep 0.05; done"],
            "env": {"OUT": out.to_str().unwrap()}
        }),
    )
    .await;
    let chunk = b64(&[b'x'; 32 * 1024]);
    for _ in 0..30 {
        send(&mut cw, json!({"op":"stdin","data":chunk})).await;
    }
    frames_until(&mut lines, |f| type_of(f) == "stdout").await;
    wait_for_path(&dir.path().join("count.ready")).await;
    drop(lines);
    drop(cw);

    wait_for_path(&out).await;
    let read: usize = std::fs::read_to_string(&out)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        read < 256 * 1024,
        "the forked child read {read} bytes: stdin kept flowing after its client left"
    );
}

fn group_member() -> Option<String> {
    std::fs::read_to_string("/etc/group")
        .unwrap()
        .lines()
        .filter_map(|line| line.rsplit(':').next())
        .find_map(|members| members.split(',').find(|u| !u.is_empty()))
        .map(str::to_owned)
}
