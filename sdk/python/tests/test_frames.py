"""Guards the fast bulk slicer in frames.decode_response: a "data" match
inside a nested unknown field or trailing bytes after the frame must never
yield a silently wrong payload — unexpected shapes take the full parse."""

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


@pytest.mark.parametrize(
    "raw",
    [
        b'{"type":"stdout","data":"aGk="}garbage',
        b'{"type":"data","data":"QUJD\r\nREVG"}',
        b'{"type":"stdout","data":"aGk="',
    ],
    ids=["trailing_bytes", "control_bytes_in_base64", "unterminated_frame"],
)
def test_malformed_frame_rejected(raw):
    with pytest.raises(json.JSONDecodeError):
        frames.decode_response(raw)
