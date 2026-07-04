//! Wire protocol: newline-delimited JSON frames, one connection per RPC.
//! The first frame on a connection is the request; for exec, later frames
//! from the client carry stdin. Server frames stream until a terminal one
//! (exit / done / error). Binary payloads ride as base64 (`data` fields),
//! matching Go's default []byte JSON encoding for the SDK side.

use std::collections::HashMap;
use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

// AsyncBufReadExt brings fill_buf/consume, used by read_frame.

/// Mirrors cocoon-agent's frame cap so a malformed peer can't OOM us.
pub const MAX_FRAME: usize = 8 * 1024 * 1024;
pub const PROTO_VERSION: u32 = 1;

/// Client → server frames. Unknown JSON fields (e.g. a future `v`) are
/// ignored by construction, which is the forward-compatibility story.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Exec(ExecReq),
    Info,
    Ps,
    Kill {
        pid: u32,
        #[serde(default)]
        signal: Option<i32>,
    },
    Attach {
        pid: u32,
    },
    Logs {
        pid: u32,
    },
    Stdin {
        #[serde(with = "b64")]
        data: Vec<u8>,
    },
    StdinClose,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ExecReq {
    pub argv: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub detach: bool,
}

/// Server → client frames.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Started {
        pid: u32,
    },
    Stdout {
        #[serde(with = "b64")]
        data: Vec<u8>,
    },
    Stderr {
        #[serde(with = "b64")]
        data: Vec<u8>,
    },
    Exit {
        code: i32,
    },
    Done,
    Error {
        kind: ErrorKind,
        message: String,
    },
    Info {
        version: String,
        proto: u32,
        uptime_secs: u64,
        procs: usize,
        sessions: usize,
    },
    Procs {
        procs: Vec<ProcInfo>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    BadRequest,
    NotFound,
    Unimplemented,
    Internal,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProcInfo {
    pub pid: u32,
    pub argv: Vec<String>,
    pub detached: bool,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub started_at_epoch_secs: u64,
}

impl Response {
    pub fn error(kind: ErrorKind, message: impl Into<String>) -> Self {
        Response::Error {
            kind,
            message: message.into(),
        }
    }
}

/// Reads one newline-terminated frame (newline stripped); None on clean EOF.
/// Scans the buffered reader chunk by chunk so a peer that never sends a
/// newline is cut off at MAX_FRAME instead of growing the line unbounded.
pub async fn read_frame<R: AsyncBufRead + Unpin>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = r.fill_buf().await?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            line.extend_from_slice(&available[..pos]);
            r.consume(pos + 1);
            return cap_check(line).map(Some);
        }
        let n = available.len();
        line.extend_from_slice(available);
        r.consume(n);
        line = cap_check(line)?;
    }
}

fn cap_check(line: Vec<u8>) -> io::Result<Vec<u8>> {
    if line.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds cap",
        ));
    }
    Ok(line)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, resp: &Response) -> io::Result<()> {
    let mut buf = serde_json::to_vec(resp).map_err(io::Error::other)?;
    buf.push(b'\n');
    w.write_all(&buf).await?;
    w.flush().await
}

mod b64 {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(data))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(d)?;
        STANDARD.decode(text).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip_and_unknown_fields_ignored() {
        let raw = br#"{"v":1,"op":"exec","argv":["echo","hi"],"cwd":"/tmp","future_field":true}"#;
        let req: Request = serde_json::from_slice(raw).expect("parse");
        match req {
            Request::Exec(e) => {
                assert_eq!(e.argv, ["echo", "hi"]);
                assert_eq!(e.cwd.as_deref(), Some("/tmp"));
                assert!(!e.detach);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn data_fields_are_base64() {
        let frame = serde_json::to_string(&Response::Stdout {
            data: b"hi".to_vec(),
        })
        .expect("encode");
        assert_eq!(frame, r#"{"type":"stdout","data":"aGk="}"#);
    }

    #[tokio::test]
    async fn frame_reader_caps_oversized_lines() {
        let big = vec![b'a'; MAX_FRAME + 2];
        let mut r = tokio::io::BufReader::new(big.as_slice());
        let err = read_frame(&mut r).await.expect_err("must reject");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn golden_fixtures_parse() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../protocol/fixtures/v1");
        let mut seen = 0;
        for entry in std::fs::read_dir(dir).expect("fixtures dir") {
            let path = entry.expect("entry").path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            // Only the corpus itself; skip editor/OS cruft (.DS_Store,
            // AppleDouble ._ sidecars) that can land in the dir.
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            let raw = std::fs::read(&path).expect("read fixture");
            if stem.starts_with("req_") {
                serde_json::from_slice::<Request>(&raw).unwrap_or_else(|e| panic!("{name}: {e}"));
            } else if stem.starts_with("resp_") {
                serde_json::from_slice::<Response>(&raw).unwrap_or_else(|e| panic!("{name}: {e}"));
            } else {
                continue;
            }
            seen += 1;
        }
        assert!(seen >= 6, "fixture corpus went missing");
    }
}
