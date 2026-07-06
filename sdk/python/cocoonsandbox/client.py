"""Client: the control-plane entry point. Dial any node of a cluster; a
claim either lands locally or follows one MOVED-style redirect to the node
that has capacity, and the returned Sandbox is bound to its owner."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request

from .checkpoint import Checkpoint
from .errors import APIError
from .sandbox import Sandbox


class Client:
    """Talks to one sandboxd node (and, transparently, its cluster)."""

    def __init__(self, addr: str, api_token: str = "", timeout: float = 120.0):
        self.addr = addr.split(",")[0].strip()
        self.api_token = api_token
        self.timeout = timeout

    def new(self, template: str, net: str = "", size: str = "", ttl_seconds: int = 0) -> Sandbox:
        """Claims a sandbox; a warm hit is milliseconds. On a cluster a warm
        miss may redirect to a peer, followed transparently."""
        claim = _claim_body(template, net, size, ttl_seconds)
        reply = self._post_json(self.addr, "/v1/claim", claim, "claim")
        redirect = reply.get("redirect") or []
        if redirect:
            claim["no_redirect"] = True
            last_error = None
            for peer in redirect:
                try:
                    return self._handle_from(peer, self._post_json(peer, "/v1/claim", claim, "claim"))
                except APIError as exc:
                    last_error = exc
            raise last_error
        return self._handle_from(self.addr, reply)

    def delete_template(self, template: str, net: str = "", size: str = "") -> None:
        """Removes a promoted template by name; on a cluster the delete
        follows gossip to the owner node (one hop)."""
        query = {"template": template}
        if net:
            query["net"] = net
        if size:
            query["size"] = size
        path = "/v1/templates?" + urllib.parse.urlencode(query)
        reply = self._request(self.addr, "DELETE", path, None, "delete template")
        for peer in (reply or {}).get("redirect") or []:
            query["no_redirect"] = "1"
            path = "/v1/templates?" + urllib.parse.urlencode(query)
            self._request(peer, "DELETE", path, None, "delete template")
            return

    def lookup(self, id: str, token: str) -> Sandbox:
        """Relocates a handle from id + token: asks the entry node, then
        each mesh peer, and binds to whichever confirms ownership."""
        for addr in [self.addr, *(self.info().get("peers") or [])]:
            try:
                reply = self._request(addr, "GET", f"/v1/sandboxes/{id}/owner", None, "owner", bearer=token)
            except APIError:
                continue
            return Sandbox(client=self, id=id, token=token,
                           owner=reply.get("owner_addr") or addr)
        raise APIError("lookup", 404, f"no owner found for {id}")

    def checkpoints(self) -> list[Checkpoint]:
        """Lists the connected node's checkpoints, newest first."""
        reply = self._request(self.addr, "GET", "/v1/checkpoints", None, "list checkpoints")
        return [Checkpoint(self, self.addr, rec) for rec in reply.get("checkpoints") or []]

    def info(self) -> dict:
        """The node's pool/claim counters, as served by GET /v1/info."""
        return self._request(self.addr, "GET", "/v1/info", None, "info")

    def _handle_from(self, dialed: str, reply: dict) -> Sandbox:
        return Sandbox(
            client=self,
            id=reply["id"],
            token=reply["token"],
            owner=reply.get("owner_addr") or dialed,
            deadline=reply.get("deadline", ""),
            from_checkpoint=reply.get("from_checkpoint", ""),
        )

    def _post_json(self, addr: str, path: str, body: dict, verb: str) -> dict:
        return self._request(addr, "POST", path, body, verb)

    def _request(self, addr: str, method: str, path: str, body, verb: str, bearer: str = "") -> dict:
        """Issues one control-plane request. bearer overrides the api token —
        sandbox-scoped verbs (release, hibernate) authenticate with the
        per-sandbox token instead."""
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(f"http://{addr}{path}", data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        token = bearer or self.api_token
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            raise APIError(verb, exc.code, _error_message(exc.read())) from None
        except urllib.error.URLError as exc:
            raise APIError(verb, 0, str(exc.reason)) from None
        return json.loads(raw) if raw else {}


def _claim_body(template: str, net: str, size: str, ttl_seconds: int) -> dict:
    claim = {"template": template}
    if net:
        claim["net"] = net
    if size:
        claim["size"] = size
    if ttl_seconds:
        claim["ttl_seconds"] = ttl_seconds
    return claim


def _error_message(raw: bytes) -> str:
    try:
        return json.loads(raw)["error"]
    except (ValueError, KeyError, TypeError):
        return raw.decode(errors="replace").strip()

