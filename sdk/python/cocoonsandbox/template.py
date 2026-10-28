"""Promoted template handles and owner-aware claims."""

from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING, Any

from .volumes import Volume, _volume_body

if TYPE_CHECKING:
    from .client import Client
    from .sandbox import Sandbox


class Template:
    """A promoted template on its owner node."""

    def __init__(self, client: Client, addr: str, name: str, net: str, size: str, content_digest: str = "") -> None:
        self._client = client
        self._addr = addr
        self.name = name
        self.net = net
        self.size = size
        self.content_digest = content_digest

    def new(
        self,
        ttl_seconds: int = 0,
        volumes: list[str | Volume] | None = None,
        mount: bool = True,
        *,
        deadline: float | None = None,
    ) -> Sandbox:
        """Claims the template, following placement when volumes require it.
        mount=False attaches the volumes without mounting them."""
        claim = _claim_body(self.name, self.net, self.size, ttl_seconds, volumes, mount)
        claim["require_promoted"] = True
        if volumes:
            return self._client._claim_from(self._addr, claim, deadline=deadline)
        claim["no_redirect"] = True
        reply = self._client._post_json(self._addr, "/v1/claim", claim, "claim", deadline=deadline)
        return self._client._handle_from(self._addr, reply)

    def delete(self) -> None:
        query = _template_query(self.name, self.net, self.size)
        query["no_redirect"] = "1"
        self._client._request(
            self._addr, "DELETE", "/v1/templates?" + urllib.parse.urlencode(query), None, "delete template"
        )


def _claim_body(
    template: str,
    net: str,
    size: str,
    ttl_seconds: int,
    volumes: list[str | Volume] | None = None,
    mount: bool = True,
    claim_ref: str = "",
) -> dict[str, Any]:
    claim: dict[str, Any] = {"template": template}
    if net:
        claim["net"] = net
    if size:
        claim["size"] = size
    if ttl_seconds:
        claim["ttl_seconds"] = ttl_seconds
    if volumes is not None and not isinstance(volumes, list):
        raise TypeError("volumes must be a list")
    if volumes:
        claim["volumes"] = [_volume_body(volume) for volume in volumes]
    if not mount:
        claim["volumes_attach_only"] = True
    if claim_ref:
        claim["claim_ref"] = claim_ref
    return claim


def _template_query(template: str, net: str, size: str) -> dict[str, str]:
    query = {"template": template}
    if net:
        query["net"] = net
    if size:
        query["size"] = size
    return query
