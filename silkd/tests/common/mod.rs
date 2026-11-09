//! Shared harness: drives silkd's server over an in-memory duplex as a relayed host connection would; each test binary compiles it separately, hence allow(dead_code).
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use serde_json::{Value, json};
use silkd::server::State;
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub const DEADLINE: Duration = Duration::from_secs(30);

pub type FrameWriter = WriteHalf<DuplexStream>;
pub type FrameLines = Lines<BufReader<ReadHalf<DuplexStream>>>;

pub fn connect(state: &Arc<State>) -> (FrameWriter, FrameLines, JoinHandle<std::io::Result<()>>) {
    let (client, server) = tokio::io::duplex(1 << 20);
    let state = Arc::clone(state);
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { state.serve(BufReader::new(sr), sw).await });
    let (cr, cw) = tokio::io::split(client);
    (cw, BufReader::new(cr).lines(), handle)
}

pub async fn connect_with(
    state: &Arc<State>,
    first: Value,
) -> (FrameWriter, FrameLines, JoinHandle<std::io::Result<()>>) {
    let (mut cw, lines, handle) = connect(state);
    send(&mut cw, first).await;
    (cw, lines, handle)
}

pub async fn under_deadline<T>(fut: impl Future<Output = T>) -> T {
    timeout(DEADLINE, fut).await.expect("deadline")
}

pub async fn send(cw: &mut FrameWriter, frame: Value) {
    let mut line = serde_json::to_vec(&frame).expect("encode frame");
    line.push(b'\n');
    cw.write_all(&line).await.expect("write frame");
}

pub async fn next_frame(lines: &mut FrameLines) -> Value {
    let line = timeout(DEADLINE, lines.next_line())
        .await
        .expect("deadline")
        .expect("read line")
        .expect("stream closed");
    serde_json::from_str(&line).unwrap_or_else(|e| panic!("bad frame {line:?}: {e}"))
}

pub async fn expect_frame(lines: &mut FrameLines, kind: &str) -> Value {
    let frame = next_frame(lines).await;
    assert_type(&frame, kind);
    frame
}

pub async fn frames_until(lines: &mut FrameLines, pred: impl Fn(&Value) -> bool) -> Vec<Value> {
    let mut frames = Vec::new();
    loop {
        let frame = next_frame(lines).await;
        let hit = pred(&frame);
        frames.push(frame);
        if hit {
            return frames;
        }
    }
}

pub async fn roundtrip(request_line: &str) -> Vec<Value> {
    request_on(&Arc::new(State::new()), &[request_line.to_string()]).await
}

pub async fn request(frame: Value) -> Vec<Value> {
    roundtrip(&frame.to_string()).await
}

#[track_caller]
pub fn assert_error(frame: &Value, kind: &str) {
    assert_eq!(type_of(frame), "error", "{frame}");
    assert_eq!(frame["kind"], kind, "{frame}");
}

#[track_caller]
pub fn assert_type(frame: &Value, kind: &str) {
    assert_eq!(type_of(frame), kind, "{frame}");
}

pub async fn one(state: &Arc<State>, request_line: &str) -> Vec<Value> {
    request_on(state, &[request_line.to_string()]).await
}

pub async fn exchange(lines: &[String]) -> Vec<Value> {
    request_on(&Arc::new(State::new()), lines).await
}

pub fn proc_entry(ps: &[Value], pid: u64) -> Option<&Value> {
    ps[0]["procs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["pid"].as_u64() == Some(pid))
}

pub async fn bind_local() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    (listener, port)
}

pub async fn fs_push(dest: &Path, archive: &[u8]) -> Vec<Value> {
    let mut lines = vec![json!({"op":"fs_push","dest":dest.to_str().unwrap()}).to_string()];
    lines.extend(data_frames(archive));
    exchange(&lines).await
}

pub async fn wait_for_path(path: &Path) {
    under_deadline(async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[track_caller]
pub fn require_perl() {
    assert!(
        std::process::Command::new("perl")
            .arg("-e1")
            .status()
            .is_ok_and(|status| status.success()),
        "this test needs perl on PATH"
    );
}

pub fn alive(pid: u32) -> bool {
    // SAFETY: kill(2) with signal 0 only probes whether the pid exists; it takes no pointers.
    if unsafe { libc::kill(pid as libc::pid_t, 0) } != 0 {
        return false;
    }
    !std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit(')')
            .next()
            .is_some_and(|rest| rest.trim_start().starts_with('Z'))
    })
}

pub fn type_of(frame: &Value) -> &str {
    frame["type"].as_str().unwrap_or("")
}

pub fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn decode(frame: &Value) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(frame["data"].as_str().unwrap())
        .unwrap()
}

pub fn payload(frames: &[Value], frame_type: &str) -> Vec<u8> {
    frames
        .iter()
        .filter(|f| type_of(f) == frame_type)
        .flat_map(decode)
        .collect()
}

pub fn stdout_body(frames: &[Value]) -> String {
    String::from_utf8_lossy(&payload(frames, "stdout")).into_owned()
}

pub fn data_frames(bytes: &[u8]) -> Vec<String> {
    let mut lines: Vec<String> = bytes
        .chunks(16 * 1024)
        .map(|c| json!({"op":"data","data":b64(c)}).to_string())
        .collect();
    lines.push(r#"{"op":"data_end"}"#.to_string());
    lines
}

async fn request_on(state: &Arc<State>, lines: &[String]) -> Vec<Value> {
    let (mut cw, mut out, handle) = connect(state);
    for line in lines {
        cw.write_all(line.as_bytes()).await.unwrap();
        cw.write_all(b"\n").await.unwrap();
    }
    cw.shutdown().await.unwrap();

    let mut frames = Vec::new();
    while let Some(l) = out.next_line().await.unwrap() {
        frames.push(serde_json::from_str(&l).unwrap());
    }
    handle.await.unwrap().unwrap();
    frames
}
