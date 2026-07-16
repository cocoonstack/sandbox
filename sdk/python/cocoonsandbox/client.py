"""Client: the control-plane entry point. Dial any node of a cluster; a
claim either lands locally or follows one MOVED-style redirect to the node
that has capacity, and the returned Sandbox is bound to its owner."""

from __future__ import annotations

import http.client
import json
import queue
import threading
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
            # Retry at a peer with no_redirect set: it warm-or-provisions and
            # cannot bounce us again.
            claim["no_redirect"] = True
            # A candidate that is full (429) or erroring must not abort the
            # follow — try every one until a node answers, matching the Go SDK.
            return _try_each(redirect, lambda peer: self._handle_from(
                peer, self._post_json(peer, "/v1/claim", claim, "claim")),
                retry=lambda _: True)
        return self._handle_from(self.addr, reply)

    def delete_template(self, template: str, net: str = "", size: str = "") -> None:
        """Removes a promoted template by name; on a cluster the delete
        follows gossip to the owner node (one hop)."""
        query = _template_query(template, net, size)
        path = "/v1/templates?" + urllib.parse.urlencode(query)
        reply = self._request(self.addr, "DELETE", path, None, "delete template")
        candidates = (reply or {}).get("redirect") or []
        if not candidates:
            return
        # The retry carries no_redirect, mirroring the claim protocol: the
        # owner answers for itself, never a second hop.
        query["no_redirect"] = "1"
        path = "/v1/templates?" + urllib.parse.urlencode(query)
        _try_each(candidates, lambda peer: self._request(peer, "DELETE", path, None, "delete template"))

    def lookup(self, id: str, token: str) -> Sandbox:
        """Relocates a handle from id + token: asks the entry node and every
        mesh peer concurrently, binding to whichever confirms ownership
        first — one dead peer must not cost its full timeout."""
        def probe(addr: str) -> Sandbox:
            reply = self._request(addr, "GET", f"/v1/sandboxes/{id}/owner", None, "owner", bearer=token)
            return Sandbox(client=self, id=id, token=token,
                           owner=reply.get("owner_addr") or addr)

        addrs = [self.addr, *self._peers()]
        try:
            return _scatter(addrs, probe)
        except APIError:
            raise APIError("lookup", 404, f"no owner found for {id}") from None

    def checkpoint(self, id: str) -> Checkpoint:
        """A handle for a known checkpoint id, bound to the entry node — no
        listing round-trip; an unknown id surfaces as 404 at claim time."""
        return Checkpoint(self, self.addr, {"id": id})

    def checkpoints(self) -> list[Checkpoint]:
        """Lists the connected node's checkpoints, newest first."""
        reply = self._request(self.addr, "GET", "/v1/checkpoints", None, "list checkpoints")
        return [Checkpoint(self, self.addr, rec) for rec in reply.get("checkpoints") or []]

    def info(self) -> dict:
        """The node's pool/claim counters, as served by GET /v1/info."""
        return self._request(self.addr, "GET", "/v1/info", None, "info")

    def _peers(self) -> list:
        # /v1/peers is tenant-accessible (cluster topology); /v1/info is
        # operator-only, so a tenant lookup cannot read peers from it. A
        # single node has none — degrade to just the entry node.
        try:
            return self._request(self.addr, "GET", "/v1/peers", None, "peers").get("peers") or []
        except APIError:
            return []

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
            try:
                detail = _error_message(exc.read())
            except (OSError, http.client.HTTPException) as read_exc:
                detail = str(read_exc)
            raise APIError(verb, exc.code, detail) from None
        except urllib.error.URLError as exc:
            raise APIError(verb, 0, str(exc.reason)) from None
        except (OSError, http.client.HTTPException) as exc:
            # A reset or truncated body read is neither HTTPError nor URLError.
            raise APIError(verb, 0, str(exc)) from None
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise APIError(verb, 0, "malformed JSON in response") from exc


def _claim_body(template: str, net: str, size: str, ttl_seconds: int) -> dict:
    claim = {"template": template}
    if net:
        claim["net"] = net
    if size:
        claim["size"] = size
    if ttl_seconds:
        claim["ttl_seconds"] = ttl_seconds
    return claim


def _template_query(template: str, net: str, size: str) -> dict:
    query = {"template": template}
    if net:
        query["net"] = net
    if size:
        query["size"] = size
    return query


def _error_message(raw: bytes) -> str:
    try:
        return json.loads(raw)["error"]
    except (ValueError, KeyError, TypeError):
        return raw.decode(errors="replace").strip()


def _try_each(candidates, call, retry=lambda exc: exc.status in (404, 0)):
    """Calls call against each candidate in turn, returning the first
    success. An APIError for which retry(exc) is true moves on to the next
    candidate and the last such error propagates; any other raises at once.
    The default retries only a miss (404 or a dead peer, status 0);
    candidates must be non-empty."""
    last_error = None
    for addr in candidates:
        try:
            return call(addr)
        except APIError as exc:
            if not retry(exc):
                raise
            last_error = exc
    raise last_error


def _scatter(addrs, probe):
    """Probes every addr concurrently and returns the first success; when
    all probes fail the last error propagates. Loser threads are daemons
    whose requests die with _request's own timeout; the queue is bounded by
    len(addrs) so they never block. addrs must be non-empty."""
    results = queue.Queue(maxsize=len(addrs))

    def run(addr):
        try:
            results.put((probe(addr), None))
        except Exception as exc:  # any escape would hang the drain below
            results.put((None, exc))

    for addr in addrs:
        threading.Thread(target=run, args=(addr,), daemon=True).start()
    last_error = None
    for _ in addrs:
        value, error = results.get()
        if error is None:
            return value
        last_error = error
    raise last_error
