"""silkd wire frames: newline-delimited JSON, requests tagged by "op",
responses by "type", binary payloads base64 in "data" fields. Mirrors the Go
and Rust implementations; all three round-trip protocol/wire/fixtures/v1."""

from __future__ import annotations

import base64
import binascii
import json

PROTO_VERSION = 1
MAX_FRAME = 8 * 1024 * 1024
FS_CHUNK = 256 * 1024  # silkd's per-frame chunk size, distinct from BULK_CHUNK below
# bulk streams chunk larger: fewer frames per byte, still under MAX_FRAME after base64.
BULK_CHUNK = 1 << 20


def encode_request(op: str, **fields) -> bytes:
    """Renders {"v":1,"op":...,fields} with a trailing newline; None fields
    are omitted and bytes values ride base64 under their field name."""
    frame = {"v": PROTO_VERSION, "op": op}
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = base64.b64encode(value).decode()
        frame[key] = value
    return json.dumps(frame, separators=(",", ":")).encode() + b"\n"


def decode_response(line: bytes) -> dict:
    """Parses one response frame; the returned dict carries its tag under
    "type" and any binary payload decoded under "data"."""
    # base64 is JSON-escape-free, so an exactly-shaped data frame slices without json.loads.
    if line.startswith(b'{"type":"'):
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
