//! Filesystem verb E2E over the in-memory duplex, against real temp paths.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::Arc;
use std::time::Instant;

use nix::sys::stat::Mode;
use serde_json::{Value, json};
use silkd::server::State;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use common::{assert_error, b64, data_frames, exchange, payload, request, send, type_of};

async fn fs_write(req: Value, body: &[u8]) -> Vec<Value> {
    let mut lines = vec![req.to_string()];
    lines.extend(data_frames(body));
    exchange(&lines).await
}

#[tokio::test]
async fn write_then_read_roundtrips_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hello.txt");
    let path = path.to_str().unwrap();

    let wrote = fs_write(
        json!({"op":"fs_write","path":path,"mode":0o600}),
        b"silk file body",
    )
    .await;
    assert_eq!(
        type_of(&wrote[0]),
        "done",
        "write should succeed: {wrote:?}"
    );

    let read = request(json!({"op":"fs_read","path":path})).await;
    assert_eq!(payload(&read, "data"), b"silk file body");
    assert_eq!(type_of(read.last().unwrap()), "done");
}

#[tokio::test]
async fn list_reports_written_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.txt");
    fs_write(
        json!({"op":"fs_write","path":path.to_str().unwrap()}),
        b"abc",
    )
    .await;

    let listed = request(json!({"op":"fs_list","path":dir.path().to_str().unwrap()})).await;
    assert_eq!(type_of(listed.last().unwrap()), "done");
    let a = listed
        .iter()
        .filter(|f| type_of(f) == "entries")
        .flat_map(|f| f["entries"].as_array().unwrap())
        .find(|e| e["name"] == "a.txt")
        .expect("a.txt listed");
    assert_eq!(a["kind"], "file");
    assert_eq!(a["size"], 3);
}

#[tokio::test]
async fn list_batches_large_directories() {
    let dir = tempfile::tempdir().unwrap();
    let total = silkd::fs::LIST_BATCH + 1;
    for i in 0..total {
        std::fs::write(dir.path().join(format!("f{i}")), b"").unwrap();
    }

    let listed = request(json!({"op":"fs_list","path":dir.path().to_str().unwrap()})).await;
    let frames: Vec<_> = listed.iter().filter(|f| type_of(f) == "entries").collect();
    assert_eq!(frames.len(), 2, "one full batch + the remainder");
    assert_eq!(
        frames[0]["entries"].as_array().unwrap().len(),
        silkd::fs::LIST_BATCH
    );
    let n: usize = frames
        .iter()
        .map(|f| f["entries"].as_array().unwrap().len())
        .sum();
    assert_eq!(n, total);
    assert_eq!(type_of(listed.last().unwrap()), "done");
}

#[tokio::test]
async fn stat_reports_kind_and_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.bin");
    fs_write(
        json!({"op":"fs_write","path":path.to_str().unwrap()}),
        &[0u8; 10],
    )
    .await;

    let st = request(json!({"op":"fs_stat","path":path.to_str().unwrap()})).await;
    assert_eq!(type_of(&st[0]), "stat");
    assert_eq!(st[0]["info"]["kind"], "file");
    assert_eq!(st[0]["info"]["size"], 10);
}

#[tokio::test]
async fn stat_mode_excludes_type_bits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.txt");
    fs_write(
        json!({"op":"fs_write","path":path.to_str().unwrap(),"mode":0o640}),
        b"x",
    )
    .await;
    let st = request(json!({"op":"fs_stat","path":path.to_str().unwrap()})).await;
    assert_eq!(
        st[0]["info"]["mode"], 0o640,
        "mode must be masked to permission bits"
    );
}

#[tokio::test]
async fn mkdir_rename_rm_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("d1");
    let moved = dir.path().join("d2");

    let mk = request(json!({"op":"fs_mkdir","path":sub.to_str().unwrap()})).await;
    assert_eq!(type_of(&mk[0]), "done");

    let mv = request(
        json!({"op":"fs_rename","from":sub.to_str().unwrap(),"to":moved.to_str().unwrap()}),
    )
    .await;
    assert_eq!(type_of(&mv[0]), "done");
    assert!(moved.exists() && !sub.exists());

    let rm = request(json!({"op":"fs_rm","path":moved.to_str().unwrap(),"recursive":true})).await;
    assert_eq!(type_of(&rm[0]), "done");
    assert!(!moved.exists());
}

#[tokio::test]
async fn read_streams_files_around_the_first_chunk_bound() {
    let dir = tempfile::tempdir().unwrap();
    let chunk = silkd::proto::BULK_CHUNK;
    for (len, frames) in [(0, 0), (chunk, 1), (chunk + 1, 2)] {
        let path = dir.path().join(format!("f{len}"));
        let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &body).unwrap();
        let read = request(json!({"op":"fs_read","path":path})).await;
        let data = read.iter().filter(|f| type_of(f) == "data").count();
        assert_eq!(data, frames, "len {len}");
        assert!(payload(&read, "data") == body, "len {len}");
        assert_eq!(type_of(read.last().unwrap()), "done", "len {len}");
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_returns_a_proc_file_that_reports_zero_length() {
    let read = request(json!({"op":"fs_read","path":"/proc/self/status"})).await;
    assert!(
        String::from_utf8(payload(&read, "data"))
            .unwrap()
            .contains("Name:"),
        "{read:?}"
    );
    assert_eq!(type_of(read.last().unwrap()), "done");
}

#[tokio::test]
async fn read_of_a_non_regular_file_is_bad_request() {
    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("pipe");
    nix::unistd::mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
    for path in [
        dir.path().to_str().unwrap(),
        "/dev/null",
        fifo.to_str().unwrap(),
    ] {
        let frames = request(json!({"op":"fs_read","path":path})).await;
        assert_eq!(type_of(&frames[0]), "error", "{path}: {frames:?}");
        assert_eq!(frames[0]["kind"], "bad_request", "{path}");
    }
}

#[tokio::test]
async fn read_missing_path_is_not_found() {
    let frames = request(json!({"op":"fs_read","path":"/no/such/file/xyz"})).await;
    assert_error(&frames[0], "not_found");
}

#[tokio::test]
async fn truncated_write_leaves_no_file_and_reports_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.txt");
    let frames = exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"half")}).to_string(),
    ])
    .await;
    assert_eq!(
        type_of(&frames[0]),
        "error",
        "truncated write must error: {frames:?}"
    );
    assert!(
        !path.exists(),
        "no file at destination after a truncated write"
    );
    let leftover: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".silkd-"))
        .collect();
    assert!(leftover.is_empty(), "temp file leaked: {leftover:?}");
}

#[tokio::test]
async fn overwrite_preserves_destination_mode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("script.sh");
    std::fs::write(&path, b"old").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let f = fs_write(
        json!({"op":"fs_write","path":path.to_str().unwrap()}),
        b"new content",
    )
    .await;
    assert_eq!(type_of(&f[0]), "done");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o755, "overwrite must preserve the executable bit");
    assert_eq!(std::fs::read(&path).unwrap(), b"new content");
}

#[tokio::test]
#[ignore = "needs root"]
async fn overwrite_keeps_the_destination_owner_and_mode_under_root() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owned");
    for want in [0o644, 0o4755] {
        std::fs::write(&path, b"old").unwrap();
        std::os::unix::fs::chown(&path, Some(1000), Some(1000)).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(want)).unwrap();
        let f = fs_write(
            json!({"op":"fs_write","path":path.to_str().unwrap()}),
            b"new",
        )
        .await;
        assert_eq!(type_of(&f[0]), "done", "{f:?}");
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!((meta.uid(), meta.gid()), (1000, 1000));
        assert_eq!(meta.permissions().mode() & 0o7777, want);
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }
}

#[tokio::test]
async fn rm_plain_file_and_missing_and_nonempty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("f");
    std::fs::write(&f, b"x").unwrap();
    let r = request(json!({"op":"fs_rm","path":f.to_str().unwrap()})).await;
    assert_eq!(type_of(&r[0]), "done");
    assert!(!f.exists());
    let m = request(json!({"op":"fs_rm","path":"/no/such/xyz"})).await;
    assert_error(&m[0], "not_found");
    let sub = dir.path().join("d");
    std::fs::create_dir(&sub).unwrap();
    std::fs::write(sub.join("inner"), b"y").unwrap();
    let ne = request(json!({"op":"fs_rm","path":sub.to_str().unwrap()})).await;
    assert_eq!(type_of(&ne[0]), "error");
    assert!(
        sub.exists(),
        "non-recursive rm must not remove a non-empty dir"
    );
}

#[tokio::test]
async fn failed_overwrite_preserves_original() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keep.txt");
    fs_write(
        json!({"op":"fs_write","path":path.to_str().unwrap()}),
        b"ORIGINAL",
    )
    .await;
    exchange(&[
        json!({"op":"fs_write","path":path.to_str().unwrap()}).to_string(),
        json!({"op":"data","data":b64(b"NEWDATA")}).to_string(),
    ])
    .await;
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"ORIGINAL",
        "a failed overwrite must not clobber the original"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_file_round_trips_and_reports_its_throughput() {
    const CHUNK: usize = 256 * 1024;
    const DONE: &[u8] = b"{\"type\":\"done\"}\n";
    let mib: usize = std::env::var("SILKD_BENCH_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let chunks = mib * (1 << 20) / CHUNK;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big");
    let pattern: Vec<u8> = (0..CHUNK).map(|i| (i * 31 + i / 7) as u8).collect();
    let encoded = b64(&pattern);
    let upload = format!("{{\"v\":1,\"op\":\"data\",\"data\":\"{encoded}\"}}\n");
    let download = format!("{{\"type\":\"data\",\"data\":\"{encoded}\"}}\n");

    let (client, server) = tokio::io::duplex(1 << 20);
    let (sr, sw) = tokio::io::split(server);
    let state = Arc::new(State::new());
    tokio::spawn(async move {
        state
            .serve(BufReader::with_capacity(64 * 1024, sr), sw)
            .await
    });
    let (cr, mut cw) = tokio::io::split(client);
    let mut cr = BufReader::with_capacity(64 * 1024, cr);
    let mut line = Vec::new();

    let start = Instant::now();
    send(
        &mut cw,
        json!({"op":"fs_write","path":path.to_str().unwrap()}),
    )
    .await;
    for _ in 0..chunks {
        cw.write_all(upload.as_bytes()).await.unwrap();
    }
    send(&mut cw, json!({"op":"data_end"})).await;
    cr.read_until(b'\n', &mut line).await.unwrap();
    let wrote = start.elapsed();
    assert_eq!(line, DONE, "{}", String::from_utf8_lossy(&line));

    let mut file = std::fs::File::open(&path).unwrap();
    assert_eq!(file.metadata().unwrap().len(), (chunks * CHUNK) as u64);
    let mut buf = vec![0u8; CHUNK];
    for chunk in 0..chunks {
        file.read_exact(&mut buf).unwrap();
        assert!(buf == pattern, "written chunk {chunk} differs");
    }

    let start = Instant::now();
    send(
        &mut cw,
        json!({"op":"fs_read","path":path.to_str().unwrap()}),
    )
    .await;
    let mut frames = 0;
    loop {
        line.clear();
        cr.read_until(b'\n', &mut line).await.unwrap();
        if line == DONE {
            break;
        }
        assert!(line == download.as_bytes(), "read frame {frames} differs");
        frames += 1;
    }
    let read = start.elapsed();
    assert_eq!(frames, chunks);

    println!(
        "fs_write {:.0} MiB/s, fs_read {:.0} MiB/s over {mib} MiB",
        mib as f64 / wrote.as_secs_f64(),
        mib as f64 / read.as_secs_f64()
    );
}
