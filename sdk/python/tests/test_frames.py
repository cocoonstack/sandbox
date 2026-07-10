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


def test_trailing_bytes_rejected():
    with pytest.raises(json.JSONDecodeError):
        frames.decode_response(b'{"type":"stdout","data":"aGk="}garbage')


def test_unterminated_frame_rejected():
    with pytest.raises(json.JSONDecodeError):
        frames.decode_response(b'{"type":"stdout","data":"aGk="')
