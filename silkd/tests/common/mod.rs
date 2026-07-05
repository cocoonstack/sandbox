//! Shared harness for the integration tests: drive silkd's server over an
//! in-memory duplex exactly as a relayed host connection would.
//!
//! Compiled into each test binary separately, so not every helper is used by
//! every binary — allow the resulting dead_code rather than fragment this.
#![allow(dead_code)]

use std::sync::Arc;

use base64::Engine;
use serde_json::Value;
use silkd::server::State;
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf,
};
use tokio::task::JoinHandle;

pub type FrameWriter = WriteHalf<DuplexStream>;
pub type FrameLines = Lines<BufReader<ReadHalf<DuplexStream>>>;

/// Opens a framed connection to a task serving `state`: the write half, a
/// line reader over the response frames, and the serve task handle. The split
/// halves let streaming verbs (pty, watch) interleave writes with reads.
pub fn connect(state: &Arc<State>) -> (FrameWriter, FrameLines, JoinHandle<std::io::Result<()>>) {
    let (client, server) = tokio::io::duplex(1 << 20);
    let state = Arc::clone(state);
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { state.serve(BufReader::new(sr), sw).await });
    let (cr, cw) = tokio::io::split(client);
    (cw, BufReader::new(cr).lines(), handle)
}

/// Runs one request line against a fresh server, returning the response frames.
pub async fn roundtrip(request_line: &str) -> Vec<Value> {
    request_on(&Arc::new(State::new()), &[request_line.to_string()]).await
}

/// Runs one or more request lines on a single connection against `state` (so a
/// write/push can be followed by its data frames, and calls can be chained on
/// a shared state), returning the response frames.
pub async fn request_on(state: &Arc<State>, lines: &[String]) -> Vec<Value> {
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

/// Convenience for the many single-line-on-shared-state calls.
pub async fn one(state: &Arc<State>, request_line: &str) -> Vec<Value> {
    request_on(state, &[request_line.to_string()]).await
}

/// Runs multiple request lines on one connection against a fresh server.
pub async fn exchange(lines: &[String]) -> Vec<Value> {
    request_on(&Arc::new(State::new()), lines).await
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

/// Concatenated `data` of every frame of the given type (e.g. "stdout", "data").
pub fn payload(frames: &[Value], frame_type: &str) -> Vec<u8> {
    frames
        .iter()
        .filter(|f| type_of(f) == frame_type)
        .flat_map(decode)
        .collect()
}

/// UTF-8 text of the concatenated stdout frames.
pub fn stdout_body(frames: &[Value]) -> String {
    String::from_utf8_lossy(&payload(frames, "stdout")).into_owned()
}

/// A `data`-frame stream (16K chunks) terminated by `data_end`, for feeding
/// fs.write / fs.push.
pub fn data_frames(bytes: &[u8]) -> Vec<String> {
    let mut lines: Vec<String> = bytes
        .chunks(16 * 1024)
        .map(|c| serde_json::json!({"op":"data","data":b64(c)}).to_string())
        .collect();
    lines.push(r#"{"op":"data_end"}"#.to_string());
    lines
}
