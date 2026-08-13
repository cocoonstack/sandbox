//! `fs.watch`: stream filesystem events under a path until the client
//! disconnects. Watch is the one connection-bound verb — an event feed has no
//! meaningful detached state, so it lives only as long as its connection.

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
    let mut tx = Some(tx);
    let mut watcher = match notify::recommended_watcher(move |res| {
        forward_frames(&mut tx, to_frames(res));
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
        tokio::select! {
            frame = rx.recv() => match frame {
                Some(frame) => {
                    let terminal = matches!(frame, Response::Error { .. });
                    // A failed write is the disconnect the EOF arm can lose the select to.
                    if proto::write_frame(w, &frame).await.is_err() || terminal {
                        return Ok(());
                    }
                }
                // While the watcher lives, only overflow drops the sender: the
                // buffered prefix has all been delivered, then the terminal error.
                None => return proto::error_frame(w, ErrorKind::Internal, OVERFLOW_MESSAGE).await,
            },
            // The client sends nothing during a watch, so any readable state —
            // EOF (disconnect), a stray frame, or an error — ends the watch.
            _ = reader.fill_buf() => return Ok(()),
        }
    }
}

/// Forwards frames into the bounded channel; a full channel drops the sender,
/// so the closed channel itself is the overflow signal — a wakeup the async
/// loop cannot miss even while parked on an empty queue.
fn forward_frames(tx: &mut Option<mpsc::Sender<Response>>, frames: Vec<Response>) {
    let Some(sender) = tx.as_ref() else { return };
    for frame in frames {
        if sender.try_send(frame).is_err() {
            *tx = None;
            return;
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
    fn full_channel_drops_sender_after_the_delivered_prefix() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut tx = Some(tx);

        forward_frames(&mut tx, vec![Response::Ready, Response::Ready]);

        assert!(tx.is_none());
        assert!(matches!(rx.try_recv(), Ok(Response::Ready)));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
