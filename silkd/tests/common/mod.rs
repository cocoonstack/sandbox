//! Shared harness for the integration tests: drive silkd's server over an
//! in-memory duplex exactly as a relayed host connection would.
//!
//! Compiled into each test binary separately, so not every helper is used by
//! every binary — allow the resulting dead_code rather than fragment this.
#![allow(dead_code)]

use std::sync::Arc;

use base64::Engine;
use serde_json::Value;
use silkd::server::{buffer, State};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Runs one request line against a fresh server, returning the response frames.
pub async fn roundtrip(request_line: &str) -> Vec<Value> {
    request_on(&Arc::new(State::new()), &[request_line.to_string()]).await
}

/// Runs one or more request lines on a single connection against `state` (so a
/// write/push can be followed by its data frames, and calls can be chained on
/// a shared state), returning the response frames.
pub async fn request_on(state: &Arc<State>, lines: &[String]) -> Vec<Value> {
    let (mut client, server) = tokio::io::duplex(1 << 20);
    let state = Arc::clone(state);
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { state.serve(buffer(sr), sw).await });

    for line in lines {
        client.write_all(line.as_bytes()).await.unwrap();
        client.write_all(b"\n").await.unwrap();
    }
    client.shutdown().await.unwrap();

    let mut frames = Vec::new();
    let mut out = BufReader::new(client).lines();
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
