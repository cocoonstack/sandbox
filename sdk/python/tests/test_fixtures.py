"""Round-trips the protocol corpus's request frames and decodes each response frame whole."""

import base64
import json

import pytest
from cocoonsandbox import frames
from conftest import FIXTURES


def fixture_files(prefix):
    return sorted(FIXTURES.glob(prefix + "*.json"))


def test_corpus_counts_are_pinned():
    assert (len(fixture_files("req_")), len(fixture_files("resp_"))) == (41, 22)


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
    if "data" in want:
        want["data"] = base64.b64decode(want["data"])
    assert frames.decode_response(raw) == want
