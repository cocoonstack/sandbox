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
from collections.abc import Mapping

from .checkpoint import Checkpoint
from .errors import APIError
from .sandbox import Sandbox

_PEERS_TIMEOUT = 5.0


class Client:
    """Talks to one sandboxd node (and, transparently, its cluster)."""

    def __init__(self, addr: str, api_token: str = "", timeout: float = 120.0):
        self.addr = addr.split(",")[0].strip()
        self.api_token = api_token
        self.timeout = timeout

    def new(self, template: str, net: str = "", size: str = "", ttl_seconds: int = 0, claim_ref: str = "",
            volumes: list[str | Mapping[str, str]] | None = None, mount: bool = True) -> Sandbox:
        """Claims a sandbox; a warm hit is milliseconds. On a cluster a warm
        miss may redirect to a peer, followed transparently; if every
        candidate fails transiently, the claim falls back to the origin
        once so it provisions or heals locally. mount=False attaches the
        volumes without mounting them, leaving that — and the flush — to
        the workload: releasing without a clean self-umount discards
        unsynced guest pages, since the sandbox performs no sync."""
        claim = _claim_body(template, net, size, ttl_seconds, volumes, mount, claim_ref)
        return self._claim_from(self.addr, claim)

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

    def attach(self, owner_addr: str, id: str, token: str) -> Sandbox:
        """Binds a handle to an already-claimed sandbox whose owner address is
        known (an apiserver annotation, say), with no lookup round-trip."""
        return Sandbox(client=self, id=id, token=token, owner=owner_addr)

    def sandboxes(self) -> list[dict]:
        """Lists the claims this token may see: id, key, deadline, claim_ref —
        never tokens or host paths."""
        reply = self._request(self.addr, "GET", "/v1/sandboxes", None, "list sandboxes")
        return [dict(sb) for sb in reply.get("sandboxes") or []]

    def drain(self) -> dict:
        """Cordons the node (root token): new claims are refused, live ones run
        to their leases."""
        return self._request(self.addr, "POST", "/v1/drain", None, "drain")

    def uncordon(self) -> dict:
        """Lifts a drain on the node (root token)."""
        return self._request(self.addr, "DELETE", "/v1/drain", None, "uncordon")

    def checkpoint(self, id: str) -> Checkpoint:
        """A handle for a known checkpoint id, bound to the entry node — no
        listing round-trip; an unknown id surfaces as 404 at claim time."""
        return Checkpoint(self, self.addr, {"id": id})

    def checkpoints(self) -> list[Checkpoint]:
        """Lists the connected node's checkpoints, newest first."""
        reply = self._request(self.addr, "GET", "/v1/checkpoints", None, "list checkpoints")
        return [Checkpoint(self, self.addr, rec) for rec in reply.get("checkpoints") or []]

    def volumes(self) -> list[dict]:
        """Lists the caller-visible fleet catalog; availability is local."""
        reply = self._request(self.addr, "GET", "/v1/volumes", None, "list volumes")
        return [dict(volume) for volume in reply.get("volumes") or []]

    def info(self) -> dict:
        """The node's pool/claim counters, as served by GET /v1/info."""
        return self._request(self.addr, "GET", "/v1/info", None, "info")

    def _claim_from(self, addr: str, claim: dict) -> Sandbox:
        reply = self._post_json(addr, "/v1/claim", claim, "claim")
        redirect = reply.get("redirect") or []
        if not redirect:
            return self._handle_from(addr, reply)
        claim["no_redirect"] = True
        if reply.get("require_promoted"):
            claim["require_promoted"] = True

        def post(peer):
            return self._post_json(peer, "/v1/claim", claim, "claim")

        owner, reply = _redirect_fallback(addr, redirect, post, "claim")
        return self._handle_from(owner, reply)

    def _peers(self) -> list:
        # /v1/peers is tenant-accessible (cluster topology); /v1/info is
        # operator-only, so a tenant lookup cannot read peers from it. A
        # single node has none — degrade to just the entry node. Bounded
        # tighter than the client default (mirrors the Go SDK's peersTimeout)
        # so one slow entry node cannot stall the scatter it feeds.
        try:
            return self._request(self.addr, "GET", "/v1/peers", None, "peers",
                                 timeout=min(_PEERS_TIMEOUT, self.timeout)).get("peers") or []
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
            template_digest=reply.get("template_digest", ""),
            volumes=reply.get("volumes") or [],
        )

    def _post_json(self, addr: str, path: str, body: dict, verb: str) -> dict:
        return self._request(addr, "POST", path, body, verb)

    def _request(self, addr: str, method: str, path: str, body, verb: str, bearer: str = "",
                 timeout: float = 0.0) -> dict:
        """Issues one control-plane request. bearer overrides the api token —
        sandbox-scoped verbs (release, hibernate) authenticate with the
        per-sandbox token instead; timeout overrides the client default (0 keeps it)."""
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(f"http://{addr}{path}", data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        token = bearer or self.api_token
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
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


def _claim_body(template: str, net: str, size: str, ttl_seconds: int,
                volumes: list[str | Mapping[str, str]] | None = None, mount: bool = True,
                claim_ref: str = "") -> dict:
    claim = {"template": template}
    if net:
        claim["net"] = net
    if size:
        claim["size"] = size
    if ttl_seconds:
        claim["ttl_seconds"] = ttl_seconds
    if volumes:
        claim["volumes"] = [_volume_body(volume, mount) for volume in volumes]
    if not mount:
        claim["volumes_attach_only"] = True
    if claim_ref:
        claim["claim_ref"] = claim_ref
    return claim


def _volume_body(volume: str | Mapping[str, str], mount: bool) -> dict:
    if isinstance(volume, str):
        return {"name": volume}
    if not isinstance(volume, Mapping):
        raise TypeError("volume must be a name string or mapping")
    unknown = sorted(set(volume) - {"name", "mount", "mode"})
    if unknown:
        raise TypeError(
            f"volume mapping accepts only name, mount, and mode, got unexpected key(s): {', '.join(unknown)}")
    if not mount and "mount" in volume:
        raise TypeError("volume mount is meaningless with mount=False, which leaves mounting to the caller")
    body = dict(volume)
    mode = body.get("mode")
    if mode in (None, "", "ro"):
        body.pop("mode", None)
    elif mode != "rw":
        name = volume.get("name")
        prefix = f"volume {name!r}: " if name is not None else ""
        raise TypeError(f"{prefix}mode must be 'rw' or 'ro', got {mode!r}")
    return body


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


def _retry_transient(exc: APIError) -> bool:
    """Origin-fallback policy: worth the round-trip for a transport failure
    (status 0), a miss (404), full (429), mid-heal (503), an engine/proxy
    failure (500/502/504), or a mid-rotation 401 (the origin proved the
    token valid by issuing the redirect). A served 4xx like a bad request,
    a forbidden token, or an egress conflict is definitive: the origin
    would fail the same way."""
    return exc.status in (0, 401, 404, 429, 503, 500, 502, 504)


def _redirect_fallback(origin: str, candidates: list, post, verb: str):
    """Walks candidates via post(addr) -> raw reply dict, retrying broadly
    (any candidate failure moves to the next) so one wrong candidate doesn't
    cost a candidate that would still succeed. If every candidate is
    exhausted and the last failure was transient (_retry_transient), gives
    the origin one more no_redirect attempt -- the node that issued the
    redirect provisions or heals locally instead of leaving the claim stuck
    on stale gossip. A definitive last failure skips the fallback: the origin
    would fail the same way. A second-level redirect (a compliant server
    never sends one once no_redirect is set) fails the candidate rather than
    being followed. Returns (addr, reply)."""
    def attempt(addr):
        reply = post(addr)
        if reply.get("redirect"):
            raise APIError(verb, 0, f"{addr} redirected again despite no_redirect")
        return addr, reply

    try:
        return _try_each(candidates, attempt, retry=lambda _: True)
    except APIError as exc:
        if not _retry_transient(exc):
            raise
        try:
            return attempt(origin)
        except APIError as origin_exc:
            # Both halves matter to whoever reads this: the peers' failure says
            # why the claim left the origin, the origin's why returning did not help.
            origin_exc.message = f"{origin_exc.message} (after redirect targets failed: {exc.message})"
            origin_exc.args = (f"{verb}: {origin_exc.message} (HTTP {origin_exc.status})",)
            raise


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
