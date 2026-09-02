//! `fs.watch`: stream filesystem events under a path until the client
//! disconnects. Like every connection-bound verb, an event feed has no
//! meaningful detached state, so it lives only as long as its connection.

use notify::{RecursiveMode, Watcher};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite};
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, EventKind, Response};

const OVERFLOW_MESSAGE: &str = "watch event queue overflow";

/// Events written per syscall. A build tool emits them in tight bursts, so
/// draining the queue in batches also keeps it far from its 256-slot cap.
const EVENT_BATCH: usize = 64;

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
    let mut frames = Vec::new();
    let mut watcher = match notify::recommended_watcher(move |res| {
        to_frames(res, &mut frames);
        forward_frames(&mut tx, &mut frames);
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
    let mut batch = Vec::new();
    let mut buf = Vec::new();
    loop {
        tokio::select! {
            n = rx.recv_many(&mut batch, EVENT_BATCH) => {
                // Only overflow drops the sender mid-watch; the buffered prefix is already out.
                if n == 0 {
                    return proto::error_frame(w, ErrorKind::Internal, OVERFLOW_MESSAGE).await;
                }
                let terminal = batch.iter().position(|f| matches!(f, Response::Error { .. }));
                let upto = terminal.map_or(batch.len(), |i| i + 1);
                // A failed write is the disconnect the EOF arm can lose the select to.
                let sent = proto::write_frames(w, &mut buf, &batch[..upto]).await;
                batch.clear();
                if sent.is_err() || terminal.is_some() {
                    return Ok(());
                }
            },
            // The client sends nothing during a watch, so any readable state —
            // EOF (disconnect), a stray frame, or an error — ends the watch.
            _ = reader.fill_buf() => return Ok(()),
        }
    }
}

fn forward_frames(tx: &mut Option<mpsc::Sender<Response>>, frames: &mut Vec<Response>) {
    let Some(sender) = tx.as_ref() else {
        frames.clear();
        return;
    };
    for frame in frames.drain(..) {
        if sender.try_send(frame).is_err() {
            *tx = None;
            return;
        }
    }
}

/// Renders one notify event onto `out`, which the caller reuses — nearly every
/// event carries a single path.
fn to_frames(res: notify::Result<notify::Event>, out: &mut Vec<Response>) {
    use notify::EventKind as N;
    let event = match res {
        Ok(event) => event,
        Err(e) => {
            out.push(Response::error(ErrorKind::Internal, e.to_string()));
            return;
        }
    };
    let kind = match event.kind {
        N::Create(_) => EventKind::Created,
        N::Modify(notify::event::ModifyKind::Name(_)) => EventKind::Renamed,
        N::Modify(_) => EventKind::Modified,
        N::Remove(_) => EventKind::Deleted,
        _ => return,
    };
    out.extend(event.paths.into_iter().map(|p| Response::Event {
        kind,
        path: p.to_string_lossy().into_owned(),
    }));
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
        let mut frames = Vec::new();
        to_frames(Err(notify::Error::generic("inotify overflow")), &mut frames);
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
        let mut frames = Vec::new();
        to_frames(Ok(event), &mut frames);
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

        let mut frames = vec![Response::Ready, Response::Ready];
        forward_frames(&mut tx, &mut frames);

        assert!(tx.is_none());
        assert!(frames.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Response::Ready)));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
