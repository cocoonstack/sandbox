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
        """Claims a fresh sandbox branched from the checkpoint, following a
        redirect to the node that actually holds it; if every candidate
        fails transiently, the claim falls back to the origin once so it
        heals (pulls the checkpoint) locally."""
        # Local import: a top-level one would close the client -> sandbox ->
        # checkpoint cycle.
        from .client import _redirect_fallback

        claim = {"ttl_seconds": ttl_seconds} if ttl_seconds else {}
        path = f"/v1/checkpoints/{self.id}/claim"
        reply = self._client._post_json(self._addr, path, claim, "claim checkpoint")
        redirect = reply.get("redirect") or []
        if not redirect:
            return self._client._handle_from(self._addr, reply)
        claim["no_redirect"] = True

        def post(peer):
            return self._client._post_json(peer, path, claim, "claim checkpoint")

        addr, reply = _redirect_fallback(self._addr, redirect, post, "claim checkpoint")
        return self._client._handle_from(addr, reply)

    def delete(self) -> None:
        """Removes the checkpoint from its node and asks every peer that node
        currently sees to drop any replica a heal pulled — best-effort eventual
        cleanup, not a fleet-wide revocation. A peer that misses that broadcast
        (offline, partitioned, or joined later) keeps serving branches from its
        replica until the node's checkpoint_ttl_hours ages it out; with that
        TTL at its default of 0 (keep forever), an unreachable peer's replica
        has no cleanup bound at all."""
        self._client._request(self._addr, "DELETE", f"/v1/checkpoints/{self.id}", None, "delete checkpoint")
