//! exec/procs verb E2E over the in-memory duplex. Spawns real processes
//! (echo, sh), so it runs on any Unix — keep it that way so exec-path bugs
//! surface on the dev host, not only on Linux CI.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{decode, exchange, one, roundtrip, stdout_body, type_of};
use silkd::server::State;

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
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
}

#[tokio::test]
async fn info_reports_version_and_proto() {
    let frames = roundtrip(r#"{"op":"info"}"#).await;
    assert_eq!(type_of(&frames[0]), "info");
    assert_eq!(frames[0]["proto"], 1);
    assert!(frames[0]["version"].as_str().is_some());
}

#[tokio::test]
async fn kill_unknown_pid_is_not_found() {
    let frames = roundtrip(r#"{"op":"kill","pid":999999}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found");
}

#[tokio::test]
async fn daemonizing_child_does_not_wedge_the_exec() {
    let frames = tokio::time::timeout(
        Duration::from_secs(8),
        roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","sleep 30 & exit 0"]}"#),
    )
    .await
    .expect("exec wedged on a daemonizing child");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 0);
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
    assert!(
        !frames
            .iter()
            .filter(|f| type_of(f) == "stdout")
            .any(|f| String::from_utf8_lossy(&decode(f)).contains("late")),
        "grandchild output must not leak after exit"
    );
}

async fn detached_pid(state: &Arc<State>, script: &str) -> u64 {
    let argv = serde_json::json!(["/bin/sh", "-c", script]);
    let started = one(
        state,
        &serde_json::json!({"op":"exec","argv":argv,"detach":true}).to_string(),
    )
    .await;
    assert_eq!(type_of(&started[0]), "started");
    started[0]["pid"].as_u64().unwrap()
}

#[tokio::test]
async fn attach_streams_live_output_then_exit() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "sleep 0.3; echo live-chunk").await;
    let frames = tokio::time::timeout(
        Duration::from_secs(8),
        one(&state, &format!(r#"{{"op":"attach","pid":{pid}}}"#)),
    )
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
    for _ in 0..100 {
        let logs = one(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
async fn attach_unknown_pid_is_not_found() {
    let frames = roundtrip(r#"{"op":"attach","pid":999999}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found");
}

#[tokio::test]
async fn kill_actually_terminates_a_live_process() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "sleep 30").await;
    for _ in 0..50 {
        let ps = one(&state, r#"{"op":"ps"}"#).await;
        if ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let killed = one(&state, &format!(r#"{{"op":"kill","pid":{pid}}}"#)).await;
    assert_eq!(type_of(&killed[0]), "done");
    let mut exited = false;
    for _ in 0..150 {
        let logs = one(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(exited, "killed process never reached exited state");
}

#[tokio::test]
async fn exec_unknown_user_is_rejected() {
    let frames =
        roundtrip(r#"{"op":"exec","argv":["true"],"user":"definitely-not-a-user-xyz"}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
}

#[tokio::test]
async fn exec_missing_binary_is_an_internal_error() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/no/such/binary-xyz"]}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "internal");
    assert!(frames[0]["message"].as_str().unwrap().contains("spawn"));
}

#[tokio::test]
async fn exec_stdin_is_piped_to_the_child() {
    let frames = exchange(&[
        r#"{"op":"exec","argv":["/bin/cat"]}"#.to_string(),
        serde_json::json!({"op":"stdin","data":common::b64(b"piped-in\n")}).to_string(),
        r#"{"op":"stdin_close"}"#.to_string(),
    ])
    .await;
    assert_eq!(stdout_body(&frames), "piped-in\n");
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn kill_of_an_exited_process_is_a_noop_success() {
    let state = Arc::new(State::new());
    let started = one(
        &state,
        r#"{"op":"exec","argv":["/bin/echo","x"],"detach":true}"#,
    )
    .await;
    let pid = started[0]["pid"].as_u64().unwrap();
    for _ in 0..100 {
        let logs = one(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
    let started = one(
        &state,
        r#"{"op":"exec","argv":["/bin/echo","detached-hi"],"detach":true}"#,
    )
    .await;
    assert_eq!(type_of(&started[0]), "started");
    let pid = started[0]["pid"].as_u64().unwrap();

    let mut listed = false;
    for _ in 0..250 {
        let ps = one(&state, r#"{"op":"ps"}"#).await;
        if ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid) && p["detached"] == true)
        {
            listed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(listed, "detached pid {pid} never appeared in ps");

    for _ in 0..250 {
        let logs = one(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            assert_eq!(stdout_body(&logs), "detached-hi\n");
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("logs never reported an exit for pid {pid}");
}

#[tokio::test]
async fn disconnect_during_drain_publishes_real_exit_code() {
    use tokio::io::AsyncWriteExt;

    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = common::connect(&state);
    let req = serde_json::json!({
        "op": "exec",
        "argv": ["/bin/sh", "-c", "{ sleep 0.5; echo late; sleep 30; } & exit 7"]
    })
    .to_string();
    cw.write_all(req.as_bytes()).await.unwrap();
    cw.write_all(b"\n").await.unwrap();

    let started: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(type_of(&started), "started");
    let pid = started["pid"].as_u64().unwrap();

    let st = Arc::clone(&state);
    let attach = tokio::spawn(async move {
        one(
            &st,
            &serde_json::json!({"op":"attach","pid":pid}).to_string(),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(lines);
    drop(cw);

    let frames = tokio::time::timeout(Duration::from_secs(8), attach)
        .await
        .expect("attach wedged")
        .unwrap();
    let last = frames.last().unwrap();
    assert_eq!(
        type_of(last),
        "exit",
        "attach must end with exit: {frames:?}"
    );
    assert_eq!(last["code"], 7, "real exit code, not a fabricated -1");
}
