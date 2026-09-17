"""Checkpoint handles bound to the node that holds their captured state."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .errors import require_field

if TYPE_CHECKING:
    from .client import Client
    from .sandbox import Sandbox


class Checkpoint:
    """A captured sandbox state on its owner node."""

    def __init__(self, client: Client, addr: str, rec: dict[str, Any]) -> None:
        self._client = client
        self._addr = addr
        self.id = require_field(rec, "id", "checkpoint")
        self.name = rec.get("name", "")
        self.sandbox_id = rec.get("sandbox_id", "")
        self.created_at = rec.get("created_at", "")

    def new(self, ttl_seconds: int = 0, *, deadline: float | None = None) -> Sandbox:
        """Claims from the checkpoint, following redirects with one origin fallback."""
        claim = {"ttl_seconds": ttl_seconds} if ttl_seconds else {}
        return self._client._claim_from(
            self._addr, claim, f"/v1/checkpoints/{self.id}/claim", "claim checkpoint", deadline=deadline
        )

    def delete(self) -> None:
        """Deletes the checkpoint with eventual peer cleanup bounded by checkpoint_ttl_hours."""
        self._client._request(self._addr, "DELETE", f"/v1/checkpoints/{self.id}", None, "delete checkpoint")
