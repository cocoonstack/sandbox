"""Silkd newline-delimited JSON frames with base64 binary payloads."""

from __future__ import annotations

import base64
import binascii
import json
import sys
from typing import Any

PROTO_VERSION = 1
# the info proto from which silkd serves RPCs back to back on one connection
KEEP_ALIVE_PROTO = 2
MAX_FRAME = 8 * 1024 * 1024
FS_CHUNK = 256 * 1024
# tar and port streams chunk at 1 MiB: fewer frames per byte, still under MAX_FRAME after base64
BULK_CHUNK = 1 << 20
_FAST_DATA = sys.version_info >= (3, 11)


def encode_request(op: str, **fields: object) -> bytes:
    """Encodes one request, omitting None fields and base64-encoding byte values."""
    if len(fields) == 1 and isinstance(data := fields.get("data"), (bytes, bytearray, memoryview)):
        # base64 needs no JSON escaping, so a bare data frame renders without a str round trip
        return b'{"v":%d,"op":"%s","data":"%s"}\n' % (PROTO_VERSION, op.encode(), base64.b64encode(data))
    frame = {"v": PROTO_VERSION, "op": op}
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = base64.b64encode(value).decode()
        frame[key] = value
    return json.dumps(frame, separators=(",", ":")).encode() + b"\n"


def decode_response(line: bytes) -> dict[str, Any]:
    """Decodes one response with binary payloads under data."""
    # base64 is JSON-escape-free, so an exactly-shaped data frame slices without json.loads.
    if _FAST_DATA and line.startswith(b'{"type":"'):
        te = line.find(b'"', 9)
        if te > 0 and line[9:te] in (b"stdout", b"stderr", b"data") and line.startswith(b'","data":"', te):
            de = line.find(b'"', te + 10)
            if de > 0 and line[de:] in (b'"}', b'"}\n'):
                try:
                    return {"type": line[9:te].decode(), "data": base64.b64decode(line[te + 10 : de], validate=True)}
                except binascii.Error:
                    pass
    frame = json.loads(line)
    if not isinstance(frame, dict) or "type" not in frame:
        raise ValueError(f"frame without a type tag: {line[:80]!r}")
    if isinstance(frame.get("data"), str):
        frame["data"] = base64.b64decode(frame["data"])
    return frame
