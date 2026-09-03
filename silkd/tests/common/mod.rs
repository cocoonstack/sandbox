//! Shared harness: drives silkd's server over an in-memory duplex as a relayed host connection would.
//! Each test binary compiles it separately, so #![allow(dead_code)] covers the helpers one binary skips.
#![allow(clippy::unwrap_used, clippy::expect_used)]
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

pub fn connect(state: &Arc<State>) -> (FrameWriter, FrameLines, JoinHandle<std::io::Result<()>>) {
    let (client, server) = tokio::io::duplex(1 << 20);
    let state = Arc::clone(state);
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { state.serve(BufReader::new(sr), sw).await });
    let (cr, cw) = tokio::io::split(client);
    (cw, BufReader::new(cr).lines(), handle)
}

pub async fn roundtrip(request_line: &str) -> Vec<Value> {
    request_on(&Arc::new(State::new()), &[request_line.to_string()]).await
}

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

pub async fn one(state: &Arc<State>, request_line: &str) -> Vec<Value> {
    request_on(state, &[request_line.to_string()]).await
}

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
        .map(|c| serde_json::json!({"op":"data","data":b64(c)}).to_string())
        .collect();
    lines.push(r#"{"op":"data_end"}"#.to_string());
    lines
}
