"""silkd wire frames: newline-delimited JSON, requests tagged by "op",
responses by "type", binary payloads base64 in "data" fields. Mirrors the Go
and Rust implementations; all three round-trip protocol/fixtures/v1."""

from __future__ import annotations

import base64
import json

PROTO_VERSION = 1
MAX_FRAME = 8 * 1024 * 1024
FS_CHUNK = 32 * 1024
# Bulk streams (push tars, port bytes) chunk larger — fewer frames for the
# same bytes, still far under MAX_FRAME after base64; mirrors the Go SDK.
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
    # Bulk frames (stdout/stderr/data) are the throughput path where json.loads
    # of the big base64 string dominates. base64 is JSON-escape-free and these
    # frames carry only type+data, so slice both out and skip json.
    if line.startswith(b'{"type":"'):
        te = line.find(b'"', 9)
        if te > 0 and line[9:te] in (b"stdout", b"stderr", b"data"):
            di = line.find(b'"data":"', te)
            de = line.find(b'"', di + 8) if di > 0 else -1
            if de > 0:
                return {"type": line[9:te].decode(), "data": base64.b64decode(line[di + 8 : de])}
    frame = json.loads(line)
    if not isinstance(frame, dict) or "type" not in frame:
        raise ValueError(f"frame without a type tag: {line[:80]!r}")
    if isinstance(frame.get("data"), str):
        frame["data"] = base64.b64decode(frame["data"])
    return frame
