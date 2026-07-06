//! `port_forward`: relay a guest TCP port over the RPC connection, so the
//! host reaches in-guest servers (dev servers, sshd) on either lane — the
//! vsock relay is the only transport the no-network lane has. After `ready`,
//! client `data` frames feed the socket (`data_end` half-closes it) and
//! socket bytes stream back as `data` frames; the guest server closing ends
//! the stream with `done`.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::proto::{self, ErrorKind, Request, Response, READ_CHUNK};

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
            return proto::write_frame(w, &Response::error(kind, format!("connect {port}: {e}")))
                .await;
        }
    };
    proto::write_frame(w, &Response::Ready).await?;

    let (mut tr, tw) = stream.into_split();
    let feed = tokio::spawn(feed_socket(client, tw));

    let mut buf = vec![0u8; READ_CHUNK];
    let res = loop {
        match tr.read(&mut buf).await {
            // Guest closed its write side: the stream is done.
            Ok(0) | Err(_) => break proto::write_frame(w, &Response::Done).await,
            Ok(n) => {
                if let Err(e) = proto::write_frame(
                    w,
                    &Response::Data {
                        data: buf[..n].to_vec(),
                    },
                )
                .await
                {
                    break Err(e);
                }
            }
        }
    };
    feed.abort();
    res
}

/// Writes client `data` frames into the guest socket until `data_end`
/// (half-close), a stray frame, or the client disconnects. Owns the write
/// half so its `write_all` can block on a slow guest without stalling the
/// output direction.
async fn feed_socket(mut client: mpsc::Receiver<Request>, mut tw: OwnedWriteHalf) {
    while let Some(req) = client.recv().await {
        match req {
            Request::Data { data } => {
                if tw.write_all(&data).await.is_err() {
                    return;
                }
            }
            Request::DataEnd => {
                let _ = tw.shutdown().await;
                return;
            }
            _ => return,
        }
    }
}
