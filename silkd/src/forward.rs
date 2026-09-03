//! `port_forward` relays a guest TCP port: the no-network lane's only host reach.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;

use crate::proto::{self, BULK_CHUNK, ErrorKind, Request, Response};

/// Relays 127.0.0.1:port with one task per direction, against a head-of-line deadlock.
pub async fn run<W: AsyncWrite + Unpin>(
    port: u16,
    client: mpsc::Receiver<Request>,
    w: &mut W,
) -> io::Result<()> {
    let stream = match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(stream) => stream,
        Err(e) => {
            let kind = if e.kind() == io::ErrorKind::ConnectionRefused {
                ErrorKind::NotFound
            } else {
                ErrorKind::Internal
            };
            return proto::error_frame(w, kind, format!("connect {port}: {e}")).await;
        }
    };
    proto::write_frame(w, &Response::Ready).await?;

    let (mut tr, tw) = stream.into_split();
    let mut feed = tokio::spawn(feed_socket(client, tw));
    let mut feed_done = false;

    let mut buf = vec![0u8; BULK_CHUNK];
    let mut frame = Vec::new();
    let res = loop {
        tokio::select! {
            r = tr.read(&mut buf) => match r {
                Ok(0) => break proto::write_frame(w, &Response::Done).await,
                // a read failure must not reach the client as a clean close.
                Err(e) => {
                    break proto::error_frame(
                        w,
                        ErrorKind::Internal,
                        format!("read port {port}: {e}"),
                    )
                    .await
                }
                Ok(n) => {
                    if let Err(e) = proto::write_chunk_frame(w, &mut frame, "data", &buf[..n]).await
                    {
                        break Err(e);
                    }
                }
            },
            // only a protocol violation from the feeder ends the relay.
            f = &mut feed, if !feed_done => {
                feed_done = true;
                if let Ok(Err(resp)) = f {
                    break proto::write_frame(w, &resp).await;
                }
            }
        }
    };
    if !feed_done {
        feed.abort();
    }
    res
}

async fn feed_socket(
    mut client: mpsc::Receiver<Request>,
    mut tw: OwnedWriteHalf,
) -> Result<(), Response> {
    while let Some(req) = client.recv().await {
        match req {
            Request::Data { data } => {
                if tw.write_all(&data).await.is_err() {
                    return Ok(());
                }
            }
            Request::DataEnd => {
                let _ = tw.shutdown().await;
                return Ok(());
            }
            _ => {
                return Err(Response::error(
                    ErrorKind::BadRequest,
                    "unexpected frame during port_forward",
                ));
            }
        }
    }
    Ok(())
}
