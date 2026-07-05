//! `fs.watch`: stream filesystem events under a path until the client
//! disconnects. Watch is the one connection-bound verb — an event feed has no
//! meaningful detached state, so it lives only as long as its connection.

use notify::{RecursiveMode, Watcher};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite};
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, EventKind, Response};

/// Watches `path` (recursively when set), streaming `event` frames until the
/// client disconnects or the watcher dies. The blocking notify watcher runs on
/// its own thread and forwards frames into an async channel; the client half is
/// polled concurrently so a disconnect ends the watch even when no event is
/// pending (otherwise an abandoned quiet watch would leak the task and the
/// notify thread). A watcher error (inotify overflow, watcher death) arrives as
/// an `error` frame, which is terminal. Dropping the watcher on return stops it.
pub async fn watch<R, W>(
    reader: &mut R,
    w: &mut W,
    path: String,
    recursive: bool,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (tx, mut rx) = mpsc::channel::<Response>(256);
    let mut watcher = match notify::recommended_watcher(move |res| {
        for frame in to_frames(res) {
            // Best-effort: a full channel means the client is slower than the
            // filesystem; drop rather than block the notify thread.
            let _ = tx.try_send(frame);
        }
    }) {
        Ok(watcher) => watcher,
        Err(e) => {
            return proto::write_frame(w, &Response::error(ErrorKind::Internal, e.to_string()))
                .await
        }
    };
    let mode = if recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    if let Err(e) = watcher.watch(std::path::Path::new(&path), mode) {
        return proto::write_frame(w, &Response::error(map_kind(&e), e.to_string())).await;
    }
    loop {
        tokio::select! {
            frame = rx.recv() => match frame {
                Some(frame) => {
                    let terminal = matches!(frame, Response::Error { .. });
                    proto::write_frame(w, &frame).await?;
                    if terminal {
                        return Ok(());
                    }
                }
                None => return Ok(()),
            },
            // The client sends nothing during a watch, so any readable state —
            // EOF (disconnect), a stray frame, or an error — ends the watch.
            _ = reader.fill_buf() => return Ok(()),
        }
    }
}

/// Maps one notify result to the frames to stream: `event` frames for a good
/// event, a single terminal `error` frame for a watcher error.
fn to_frames(res: notify::Result<notify::Event>) -> Vec<Response> {
    use notify::EventKind as N;
    let event = match res {
        Ok(event) => event,
        Err(e) => return vec![Response::error(ErrorKind::Internal, e.to_string())],
    };
    let kind = match event.kind {
        N::Create(_) => EventKind::Created,
        N::Modify(notify::event::ModifyKind::Name(_)) => EventKind::Renamed,
        N::Modify(_) => EventKind::Modified,
        N::Remove(_) => EventKind::Deleted,
        _ => return Vec::new(),
    };
    event
        .paths
        .into_iter()
        .map(|p| Response::Event {
            kind,
            path: p.to_string_lossy().into_owned(),
        })
        .collect()
}

fn map_kind(e: &notify::Error) -> ErrorKind {
    match &e.kind {
        notify::ErrorKind::PathNotFound => ErrorKind::NotFound,
        _ => ErrorKind::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_error_becomes_terminal_error_frame() {
        let frames = to_frames(Err(notify::Error::generic("inotify overflow")));
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            Response::Error { message, .. } => assert!(message.contains("inotify overflow")),
            other => panic!("expected error frame, got {other:?}"),
        }
    }

    #[test]
    fn create_event_becomes_event_frames() {
        let event = notify::Event::new(notify::EventKind::Create(notify::event::CreateKind::File))
            .add_path("/x/y.txt".into());
        let frames = to_frames(Ok(event));
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            Response::Event { kind, path } => {
                assert_eq!(*kind, EventKind::Created);
                assert!(path.ends_with("y.txt"));
            }
            other => panic!("expected event frame, got {other:?}"),
        }
    }
}
