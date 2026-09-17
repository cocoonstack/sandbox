"""Guards the fast bulk slicer in frames.decode_response: a "data" match
inside a nested unknown field or trailing bytes after the frame must never
yield a silently wrong payload — unexpected shapes take the full parse."""

import base64
import json

import pytest

from cocoonsandbox import frames


def test_bulk_fast_path_decodes_canonical_shape():
    frame = frames.decode_response(b'{"type":"stdout","data":"aGk="}\n')
    assert frame == {"type": "stdout", "data": b"hi"}


def test_nested_data_field_not_shadowed():
    line = b'{"type":"stdout","meta":{"data":"WFhY"},"data":"aGk="}'
    frame = frames.decode_response(line)
    assert frame["data"] == b"hi"
    assert frame["meta"] == {"data": "WFhY"}


def test_tag_after_other_keys_takes_the_full_parse():
    frame = frames.decode_response(b'{"data":"aGk=","type":"stdout"}')
    assert frame == {"data": b"hi", "type": "stdout"}


@pytest.mark.parametrize(
    "raw",
    [
        b'{"type":"stdout","data":"aGk="}garbage',
        b'{"type":"data","data":"QUJD\r\nREVG"}',
        b'{"type":"stdout","data":"aGk="',
        b'{"type":"stdout"}garbage"data":"QQ=="}',
        b'{"type":"stdout"garbage,"data":"QQ=="}',
        b'{"type":"stdout":,"data":"QQ=="}',
        b'{"type":"stdout",garbage"data":"QQ=="}',
        b'{"type":"stdout","data":"QQ==" }x',
        b'{"type":"stdout","data":"Q\nQ=="}',
    ],
    ids=[
        "trailing_bytes",
        "control_bytes_in_base64",
        "unterminated_frame",
        "closed_before_data",
        "garbage_after_tag",
        "colon_after_tag",
        "garbage_before_data",
        "space_before_close",
        "newline_in_base64",
    ],
)
def test_malformed_frame_rejected(raw):
    with pytest.raises(json.JSONDecodeError):
        frames.decode_response(raw)


def test_bare_data_frame_fast_path_matches_the_generic_encoding():
    payload = bytes(range(256))
    fast = frames.encode_request("data", data=payload)
    generic = frames.encode_request("stdin", data=payload, extra=None).replace(b'"op":"stdin"', b'"op":"data"')
    assert fast == generic
    assert json.loads(fast)["data"] == base64.b64encode(payload).decode()
