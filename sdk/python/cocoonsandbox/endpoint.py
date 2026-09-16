"""HTTP origins shared by control requests and agent relays."""

from __future__ import annotations

import urllib.parse


def _endpoint_url(addr: str, scheme: str = "http") -> urllib.parse.SplitResult:
    if any(ord(c) <= 32 or ord(c) == 127 for c in addr):
        raise ValueError("sandboxd endpoint contains whitespace or a control character")
    if "://" not in addr:
        addr = f"{scheme}://{addr}"
    endpoint = urllib.parse.urlsplit(addr)
    if (
        endpoint.scheme not in ("http", "https")
        or not endpoint.hostname
        or endpoint.username is not None
        or endpoint.path not in ("", "/")
        or "?" in addr
        or "#" in addr
    ):
        raise ValueError("sandboxd endpoint must be an http or https origin")
    try:
        port = endpoint.port
    except ValueError:
        port = 0
    if port == 0:
        raise ValueError("sandboxd endpoint port must be between 1 and 65535")
    return endpoint._replace(path="")
