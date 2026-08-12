"""Promoted template handles and owner-aware claims."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client
    from .sandbox import Sandbox


class Template:
    """A promoted template on its owner node."""

    def __init__(self, client: Client, addr: str, name: str, net: str, size: str, content_digest: str = ""):
        self._client = client
        self._addr = addr
        self.name = name
        self.net = net
        self.size = size
        self.content_digest = content_digest

    def new(self, ttl_seconds: int = 0, volumes: list[str | Mapping[str, str]] | None = None) -> Sandbox:
        """Claims the template, following placement when volumes require it."""
        # Local import: a top-level one would close the client → sandbox →
        # template cycle.
        from .client import _claim_body

        claim = _claim_body(self.name, self.net, self.size, ttl_seconds, volumes)
        if volumes:
            return self._client._claim_from(self._addr, claim)
        claim["no_redirect"] = True
        reply = self._client._post_json(self._addr, "/v1/claim", claim, "claim")
        return self._client._handle_from(self._addr, reply)

    def delete(self) -> None:
        """Removes the template from its node."""
        from .client import _template_query

        query = _template_query(self.name, self.net, self.size)
        query["no_redirect"] = "1"
        self._client._request(self._addr, "DELETE", "/v1/templates?" + urllib.parse.urlencode(query),
                              None, "delete template")
