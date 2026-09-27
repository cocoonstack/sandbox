//! The instance-metadata document guest agents read, served in the IMDSv2 shape on 169.254.169.254 where an image aliases it.
//! The token is fixed: the IMDSv2 defense is the PUT plus the custom header, not the token's secrecy.

use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The metadata address; an image opts in by aliasing it on `lo`, and without the alias the responder stays off.
pub const ADDR: &str = "169.254.169.254:80";
/// The document the host writes per sandbox through `fs_write`.
pub const DOC_PATH: &str = "/run/silkd/instance-metadata.json";

const TOKEN: &[u8] = b"silkd-instance-metadata";
const HEAD_MAX: usize = 8 * 1024;
const CONN_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn serve() -> io::Result<()> {
    let listener = match tokio::net::TcpListener::bind(ADDR).await {
        Ok(l) => l,
        Err(e) if e.kind() == io::ErrorKind::AddrNotAvailable => return Ok(()),
        Err(e) => return Err(e),
    };
    if let Some(dir) = Path::new(DOC_PATH).parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    loop {
        let conn = match listener.accept().await {
            Ok((conn, _)) => conn,
            Err(e) => {
                eprintln!("silkd imds: accept: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        tokio::spawn(async move {
            let _ = tokio::time::timeout(CONN_TIMEOUT, handle(conn, Path::new(DOC_PATH))).await;
        });
    }
}

/// Answers one request on `conn` and closes it: `PUT /latest/api/token` mints a token, `GET /` serves `doc` or `{}`.
pub async fn handle<S>(mut conn: S, doc: &Path) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = [0u8; HEAD_MAX];
    let mut n = 0;
    let end = loop {
        if n == buf.len() {
            return respond(&mut conn, "431 Request Header Fields Too Large", b"").await;
        }
        let read = conn.read(&mut buf[n..]).await?;
        if read == 0 {
            return Ok(());
        }
        n += read;
        if let Some(i) = memchr::memmem::find(&buf[..n], b"\r\n\r\n") {
            break i;
        }
    };
    let Ok(head) = std::str::from_utf8(&buf[..end]) else {
        return respond(&mut conn, "400 Bad Request", b"").await;
    };
    let mut lines = head.split("\r\n");
    let mut request = lines.next().unwrap_or_default().split(' ');
    match (request.next(), request.next()) {
        (Some("PUT"), Some("/latest/api/token")) => respond(&mut conn, "200 OK", TOKEN).await,
        (Some("GET"), Some("/")) => {
            let token = lines.find_map(|l| {
                let (name, value) = l.split_once(':')?;
                name.eq_ignore_ascii_case("x-metadata-token")
                    .then(|| value.trim())
            });
            if token.is_none_or(str::is_empty) {
                return respond(&mut conn, "401 Unauthorized", b"").await;
            }
            match tokio::fs::read(doc).await {
                Ok(body) => respond(&mut conn, "200 OK", &body).await,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    respond(&mut conn, "200 OK", b"{}").await
                }
                Err(_) => respond(&mut conn, "500 Internal Server Error", b"").await,
            }
        }
        _ => respond(&mut conn, "404 Not Found", b"").await,
    }
}

async fn respond<W: AsyncWrite + Unpin>(w: &mut W, status: &str, body: &[u8]) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    w.write_all(head.as_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn exchange(request: &[u8], doc: &Path) -> String {
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let doc = doc.to_path_buf();
        let served = tokio::spawn(async move { handle(server, &doc).await });
        client.write_all(request).await.expect("write request");
        let mut out = String::new();
        client
            .read_to_string(&mut out)
            .await
            .expect("read response");
        served.await.expect("join").expect("handle");
        out
    }

    fn body(response: &str) -> &str {
        response.split_once("\r\n\r\n").map_or("", |(_, b)| b)
    }

    #[tokio::test]
    async fn token_then_get_serves_the_written_document() {
        let dir = tempfile::tempdir().expect("tempdir");
        let doc = dir.path().join("instance-metadata.json");
        tokio::fs::write(&doc, br#"{"region":"local","tags":["a","b"]}"#)
            .await
            .expect("write doc");

        let put = exchange(
            b"PUT /latest/api/token HTTP/1.1\r\nHost: 169.254.169.254\r\nX-metadata-token-ttl-seconds: 60\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            &doc,
        )
        .await;
        assert!(put.starts_with("HTTP/1.1 200 OK\r\n"), "{put}");
        let token = body(&put);
        assert!(!token.is_empty(), "{put}");

        let get = exchange(
            format!("GET / HTTP/1.1\r\nHost: 169.254.169.254\r\nX-Metadata-Token: {token}\r\nAccept: application/json\r\n\r\n").as_bytes(),
            &doc,
        )
        .await;
        assert!(get.starts_with("HTTP/1.1 200 OK\r\n"), "{get}");
        assert_eq!(body(&get), r#"{"region":"local","tags":["a","b"]}"#);
    }

    #[tokio::test]
    async fn get_before_any_document_answers_an_empty_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let get = exchange(
            b"GET / HTTP/1.1\r\nX-metadata-token: t\r\n\r\n",
            &dir.path().join("instance-metadata.json"),
        )
        .await;
        assert!(get.starts_with("HTTP/1.1 200 OK\r\n"), "{get}");
        assert_eq!(body(&get), "{}");
    }

    #[tokio::test]
    async fn get_without_a_token_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let doc = dir.path().join("instance-metadata.json");
        for request in [
            &b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"[..],
            b"GET / HTTP/1.1\r\nX-metadata-token:  \r\n\r\n",
        ] {
            let get = exchange(request, &doc).await;
            assert!(get.starts_with("HTTP/1.1 401 Unauthorized\r\n"), "{get}");
        }
    }

    #[tokio::test]
    async fn other_paths_are_not_found() {
        let dir = tempfile::tempdir().expect("tempdir");
        let doc = dir.path().join("instance-metadata.json");
        for request in [
            &b"GET /latest/meta-data HTTP/1.1\r\nX-metadata-token: t\r\n\r\n"[..],
            b"POST / HTTP/1.1\r\n\r\n",
        ] {
            let res = exchange(request, &doc).await;
            assert!(res.starts_with("HTTP/1.1 404 Not Found\r\n"), "{res}");
        }
    }

    #[tokio::test]
    async fn an_oversize_head_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut request = b"GET / HTTP/1.1\r\nX-Pad: ".to_vec();
        request.resize(HEAD_MAX + 16, b'a');
        let res = exchange(&request, &dir.path().join("instance-metadata.json")).await;
        assert!(
            res.starts_with("HTTP/1.1 431 Request Header Fields Too Large\r\n"),
            "{res}"
        );
    }
}
