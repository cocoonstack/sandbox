"""Checkpoint claim redirect, mirroring the Client.new redirect in test_fault_matrix.py."""

import pytest
from cocoonsandbox import APIError, Client


def test_checkpoint_new_follows_redirect(spawn_node):
    seen = []

    def claim_at_b(body, path):
        seen.append(body)
        return 200, {"id": "sb_ck1", "token": "tok"}

    node_b = spawn_node({("POST", "/v1/checkpoints/ck_1/claim"): claim_at_b})

    entry_hits = []

    def redirect(body, path):
        entry_hits.append(body)
        return 200, {"redirect": [node_b]}

    entry = spawn_node({("POST", "/v1/checkpoints/ck_1/claim"): redirect})

    sb = Client(entry).checkpoint("ck_1").new()
    assert sb.id == "sb_ck1"
    assert sb.owner == node_b
    assert len(entry_hits) == 1
    assert seen[0]["no_redirect"] is True


def test_checkpoint_new_redirect_never_yields_empty_id(spawn_node, dead_addr):
    entry = spawn_node({("POST", "/v1/checkpoints/ck_3/claim"): lambda body, path: (200, {"redirect": [dead_addr]})})

    with pytest.raises(APIError):
        Client(entry).checkpoint("ck_3").new()


def test_checkpoint_new_second_level_redirect_fails(spawn_node):
    path = "/v1/checkpoints/ck_4/claim"
    node_b = spawn_node({("POST", path): lambda body, p: (200, {"redirect": ["127.0.0.1:1"]})})
    entry = spawn_node({("POST", path): lambda body, p: (200, {"redirect": [node_b]})})

    with pytest.raises(APIError):
        Client(entry).checkpoint("ck_4").new()
