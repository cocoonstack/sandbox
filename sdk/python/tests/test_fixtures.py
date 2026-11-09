"""Round-trips the protocol corpus's request frames and decodes each response frame to its type."""

import base64
import json

import pytest
from cocoonsandbox import frames
from conftest import FIXTURES


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
