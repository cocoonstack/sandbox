//! `fs.watch`: stream filesystem events under a path until the client
//! disconnects. Watch is the one connection-bound verb — an event feed has no
//! meaningful detached state, so it lives only as long as its connection.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use notify::{RecursiveMode, Watcher};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite};
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, EventKind, Response};

const OVERFLOW_MESSAGE: &str = "watch event queue overflow";

/// Watches `path`, writing `ready`, ordered events, or a terminal error until disconnect.
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
    let overflowed = Arc::new(AtomicBool::new(false));
    let callback_overflowed = Arc::clone(&overflowed);
    let mut watcher = match notify::recommended_watcher(move |res| {
        enqueue_frames(&tx, &callback_overflowed, to_frames(res));
    }) {
        Ok(watcher) => watcher,
        Err(e) => return proto::error_frame(w, ErrorKind::Internal, e.to_string()).await,
    };
    let mode = if recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    if let Err(e) = watcher.watch(std::path::Path::new(&path), mode) {
        return proto::error_frame(w, map_kind(&e), e.to_string()).await;
    }
    proto::write_frame(w, &Response::Ready).await?;
    loop {
        if overflowed.load(Ordering::Acquire) {
            return proto::error_frame(w, ErrorKind::Internal, OVERFLOW_MESSAGE).await;
        }
        tokio::select! {
            frame = rx.recv() => match frame {
                Some(frame) => {
                    let terminal = matches!(frame, Response::Error { .. });
                    // A failed write is the disconnect the EOF arm can lose the select to.
                    if proto::write_frame(w, &frame).await.is_err() || terminal {
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

fn enqueue_frames(tx: &mpsc::Sender<Response>, overflowed: &AtomicBool, frames: Vec<Response>) {
    if overflowed.load(Ordering::Acquire) {
        return;
    }
    for frame in frames {
        match tx.try_send(frame) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                overflowed.store(true, Ordering::Release);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        }
    }
}

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
                assert_eq!(path, "/x/y.txt");
            }
            other => panic!("expected event frame, got {other:?}"),
        }
    }

    #[test]
    fn full_channel_sets_overflow() {
        let (tx, mut rx) = mpsc::channel(1);
        let overflowed = AtomicBool::new(false);

        enqueue_frames(&tx, &overflowed, vec![Response::Ready, Response::Ready]);

        assert!(overflowed.load(Ordering::Acquire));
        assert!(matches!(rx.try_recv(), Ok(Response::Ready)));
        assert!(rx.try_recv().is_err());
    }
}
