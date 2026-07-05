//! Wire protocol: newline-delimited JSON frames, one connection per RPC.
//! The first frame on a connection is the request; for exec, later frames
//! from the client carry stdin. Server frames stream until a terminal one
//! (exit / done / error). Binary payloads ride as base64 (`data` fields),
//! matching Go's default []byte JSON encoding for the SDK side.

use std::collections::HashMap;
use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

/// Mirrors cocoon-agent's frame cap so a malformed peer can't OOM us.
pub const MAX_FRAME: usize = 8 * 1024 * 1024;
pub const PROTO_VERSION: u32 = 1;

/// Chunk size for streaming a file back over `fs.read`.
pub const READ_CHUNK: usize = 32 * 1024;

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
    SessionCreate {
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    SessionList,
    SessionRm {
        id: String,
    },
    Stdin {
        #[serde(with = "b64")]
        data: Vec<u8>,
    },
    StdinClose,
    FsWrite {
        path: String,
        #[serde(default)]
        mode: Option<u32>,
    },
    FsRead {
        path: String,
    },
    FsList {
        path: String,
    },
    FsStat {
        path: String,
    },
    FsMkdir {
        path: String,
        #[serde(default)]
        parents: bool,
    },
    FsRm {
        path: String,
        #[serde(default)]
        recursive: bool,
    },
    FsRename {
        from: String,
        to: String,
    },
    FsPush {
        dest: String,
    },
    FsPull {
        path: String,
    },
    FsFind {
        path: String,
        pattern: String,
        #[serde(default)]
        glob: Option<String>,
    },
    FsReplace {
        files: Vec<String>,
        pattern: String,
        replacement: String,
    },
    FsWatch {
        path: String,
        #[serde(default)]
        recursive: bool,
    },
    PtyOpen(PtyReq),
    PtyResize {
        pid: u32,
        cols: u16,
        rows: u16,
    },
    Data {
        #[serde(with = "b64")]
        data: Vec<u8>,
    },
    DataEnd,
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
    // When set, run inside the named persistent shell session (cwd/env/state
    // persist across calls) instead of spawning a fresh process.
    #[serde(default)]
    pub session: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PtyReq {
    pub cols: u16,
    pub rows: u16,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub user: Option<String>,
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
    Data {
        #[serde(with = "b64")]
        data: Vec<u8>,
    },
    Entries {
        entries: Vec<DirEntry>,
    },
    Stat {
        info: FileInfo,
    },
    SessionCreated {
        id: String,
    },
    Sessions {
        sessions: Vec<String>,
    },
    Match {
        file: String,
        line: u64,
        content: String,
    },
    Replaced {
        file: String,
        replacements: u64,
    },
    Event {
        kind: EventKind,
        path: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Created,
    Modified,
    Deleted,
    Renamed,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    BadRequest,
    NotFound,
    Unimplemented,
    Internal,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Dir,
    Symlink,
    Other,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DirEntry {
    pub name: String,
    pub kind: FileKind,
    pub size: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FileInfo {
    pub kind: FileKind,
    pub size: u64,
    pub mode: u32,
    pub mtime_epoch_secs: u64,
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

/// Maps an io error to an Error frame, classifying NotFound so a client can
/// tell a missing path from a real failure. Shared by the fs and tree verbs.
pub async fn err_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    e: &io::Error,
    op: &str,
) -> io::Result<()> {
    let kind = if e.kind() == io::ErrorKind::NotFound {
        ErrorKind::NotFound
    } else {
        ErrorKind::Internal
    };
    write_frame(w, &Response::error(kind, format!("{op}: {e}"))).await
}

/// Why a `data`/`data_end` upload stream ended without a clean `data_end`.
pub enum FeedError {
    Io(io::Error, &'static str),
    Protocol,
    Truncated,
}

/// Feeds client `data` frames into `sink` until `data_end`. Shared by fs.write
/// (sink = temp file) and fs.push (sink = tar stdin); the caller flushes/closes
/// the sink and, on error, reports via `write_feed_error`.
pub async fn feed_data_frames<R, S>(reader: &mut R, sink: &mut S) -> Result<(), FeedError>
where
    R: AsyncBufRead + Unpin,
    S: AsyncWrite + Unpin,
{
    loop {
        let frame = read_frame(reader)
            .await
            .map_err(|e| FeedError::Io(e, "read"))?
            .ok_or(FeedError::Truncated)?;
        match serde_json::from_slice::<Request>(&frame) {
            Ok(Request::Data { data }) => sink
                .write_all(&data)
                .await
                .map_err(|e| FeedError::Io(e, "write"))?,
            Ok(Request::DataEnd) => return Ok(()),
            _ => return Err(FeedError::Protocol),
        }
    }
}

/// Writes the terminal error frame for a failed `feed_data_frames`.
pub async fn write_feed_error<W: AsyncWrite + Unpin>(w: &mut W, err: FeedError) -> io::Result<()> {
    match err {
        FeedError::Io(e, op) => err_frame(w, &e, op).await,
        FeedError::Protocol => {
            write_frame(
                w,
                &Response::error(ErrorKind::BadRequest, "expected data or data_end"),
            )
            .await
        }
        FeedError::Truncated => {
            write_frame(
                w,
                &Response::error(ErrorKind::BadRequest, "stream ended before data_end"),
            )
            .await
        }
    }
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
