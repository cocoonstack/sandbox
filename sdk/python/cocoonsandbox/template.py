"""Template: a promoted sandbox state, claimable by name. The handle is
bound to the node that holds it, so it works the instant promote returns —
name-based Client calls route via gossip and lag a promote by about a tick."""

from __future__ import annotations

import urllib.parse


class Template:
    """A promoted template on its owner node."""

    def __init__(self, client, addr: str, name: str, net: str, size: str):
        self._client = client
        self._addr = addr
        self.name = name
        self.net = net
        self.size = size

    def new(self, ttl_seconds: int = 0):
        """Claims a sandbox cloned from the template, on the template's
        node; the key axes are the template's own."""
        # Local import: a top-level one would close the client → sandbox →
        # template cycle.
        from .client import _claim_body

        claim = _claim_body(self.name, self.net, self.size, ttl_seconds)
        claim["no_redirect"] = True
        reply = self._client._post_json(self._addr, "/v1/claim", claim, "claim")
        return self._client._handle_from(self._addr, reply)

    def delete(self) -> None:
        """Removes the template from its node."""
        query = {"template": self.name, "no_redirect": "1"}
        if self.net:
            query["net"] = self.net
        if self.size:
            query["size"] = self.size
        self._client._request(self._addr, "DELETE", "/v1/templates?" + urllib.parse.urlencode(query),
                              None, "delete template")
