"""Exercise both SDK planes through the TLS cluster fixture. Usage: tls_client.py ENTRY OWNER CA"""

import argparse
import socket
import ssl
import sys

from cocoonsandbox import APIError, Client, ProtocolError


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("entry", "owner", "ca"):
        parser.add_argument(name)
    args = parser.parse_args()
    entry, owner = args.entry, args.owner
    context = ssl.create_default_context(cafile=args.ca)
    client = Client(entry, api_token="node-token", ssl_context=context)
    with client.new("rt:24.04") as sb:
        assert sb.owner == owner, sb.owner
        assert sb.exec("echo", "tls") == "tls\n"
        assert client.lookup(sb.id, sb.token).owner == owner
        assert client.attach(owner.removeprefix("https://"), sb.id, sb.token).exec("echo", "bare") == "bare\n"
        with sb.dial_port(5000) as port:
            port.send(b"tail")
            port.close_write()
            output = b""
            while chunk := port.recv():
                output += chunk
            assert output == b"tail", output
        with (
            sb.proxy_port("127.0.0.1:0", 5000) as proxy,
            socket.create_connection(proxy.getsockname(), timeout=5) as conn,
        ):
            conn.sendall(b"proxy")
            conn.shutdown(socket.SHUT_WR)
            output = b""
            while chunk := conn.recv(4096):
                output += chunk
            assert output == b"proxy", output
        untrusted = Client(entry, api_token="node-token")
        try:
            untrusted.info()
        except APIError:
            pass
        else:
            raise AssertionError("control request accepted an untrusted certificate")
        try:
            untrusted.attach(owner, sb.id, sb.token).exec("echo", "untrusted")
        except ProtocolError:
            pass
        else:
            raise AssertionError("relay accepted an untrusted certificate")


if __name__ == "__main__":
    sys.exit(main())
