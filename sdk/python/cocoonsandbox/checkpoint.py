"""Checkpoint: a captured sandbox state bound to the node that holds it.
Branch any number of fresh sandboxes from the captured moment; the source
keeps running and can be checkpointed again, so captures form a tree."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .sandbox import Sandbox


class Checkpoint:
    """A captured sandbox state on its owner node."""

    def __init__(self, client, addr: str, rec: dict):
        self._client = client
        self._addr = addr
        self.id = rec["id"]
        self.name = rec.get("name", "")
        self.sandbox_id = rec.get("sandbox_id", "")
        self.created_at = rec.get("created_at", "")

    def new(self, ttl_seconds: int = 0) -> Sandbox:
        """Claims a fresh sandbox branched from the checkpoint."""
        body = {"ttl_seconds": ttl_seconds} if ttl_seconds else {}
        reply = self._client._post_json(self._addr, f"/v1/checkpoints/{self.id}/claim", body, "claim checkpoint")
        return self._client._handle_from(self._addr, reply)

    def delete(self) -> None:
        """Removes the checkpoint from its node."""
        self._client._request(self._addr, "DELETE", f"/v1/checkpoints/{self.id}", None, "delete checkpoint")
