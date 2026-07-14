//! `port_forward`: relay a guest TCP port over the RPC connection, so the
//! host reaches in-guest servers (dev servers, sshd) on either lane — the
//! vsock relay is the only transport the no-network lane has. After `ready`,
//! client `data` frames feed the socket (`data_end` half-closes it) and
//! socket bytes stream back as `data` frames; the guest server closing ends
//! the stream with `done`.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;

use crate::proto::{self, BULK_CHUNK, ErrorKind, Request, Response};

/// Connects to 127.0.0.1:port and relays until either side finishes. The two
/// directions run on separate tasks: a blocking `write_all` into the guest
/// socket must never stall draining the guest's output (a single select loop
/// would head-of-line deadlock any bidirectional bulk transfer), mirroring
/// exec's split pump_stdin/pump_out.
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
                // Guest closed its write side: the stream is done.
                Ok(0) => break proto::write_frame(w, &Response::Done).await,
                // A read failure (RST, server crash) must not pass for a
                // clean close — the client would mistake truncation for EOF.
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
            // A clean feeder end (data_end, client gone) keeps draining the
            // socket; a protocol violation terminates the relay with its
            // error instead of masquerading as a clean close.
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

/// Writes client `data` frames into the guest socket until `data_end`
/// (half-close) or the client disconnects; a stray frame is a protocol
/// violation, reported like the fs feeders do. Owns the write half so its
/// `write_all` can block on a slow guest without stalling the output
/// direction.
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
