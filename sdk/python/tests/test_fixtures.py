"""Round-trips the shared protocol fixture corpus, the same drift guard the
Rust and Go implementations run: every request fixture must re-encode
byte-for-byte (up to key order) through frames.encode_request, and every
response fixture must decode."""

import base64
import json
import pathlib

import pytest

from cocoonsandbox import frames

FIXTURES = pathlib.Path(__file__).parents[3] / "protocol" / "wire" / "fixtures" / "v1"


def fixture_files(prefix):
    files = sorted(FIXTURES.glob(prefix + "*.json"))
    assert files, f"no {prefix} fixtures under {FIXTURES}"
    return files


@pytest.mark.parametrize("path", fixture_files("req_"), ids=lambda p: p.stem)
def test_request_round_trip(path):
    want = json.loads(path.read_text())
    fields = {k: base64.b64decode(v) if k == "data" else v for k, v in want.items() if k not in ("v", "op")}
    got = json.loads(frames.encode_request(want["op"], **fields))
    assert got == want


@pytest.mark.parametrize("path", fixture_files("resp_"), ids=lambda p: p.stem)
def test_response_decodes(path):
    raw = path.read_text().encode()
    want = json.loads(raw)
    frame = frames.decode_response(raw)
    assert frame["type"] == want["type"]
    if "data" in want:
        assert frame["data"] == base64.b64decode(want["data"])
