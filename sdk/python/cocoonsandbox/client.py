"""Client: the control-plane entry point."""

from __future__ import annotations

import http.client
import json
import queue
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, TypeVar, cast

from .checkpoint import Checkpoint
from .conn import Conn, dial_agent
from .endpoint import _endpoint_url
from .errors import APIError
from .sandbox import Sandbox

T = TypeVar("T")

_PEERS_TIMEOUT = 5.0


class Client:
    """Talks to one sandboxd node (and, transparently, its cluster)."""

    def __init__(
        self,
        addr: str,
        api_token: str = "",
        timeout: float = 120.0,
        *,
        ssl_context: ssl.SSLContext | None = None,
        keep_alive: float = 30.0,
    ) -> None:
        """keep_alive keeps a handle's idle relay connection, which holds the sandbox's idle clock; 0 dials per call."""
        endpoint = _endpoint_url(addr.split(",")[0].strip())
        self.addr = endpoint.geturl().removeprefix("http://")
        self._scheme = endpoint.scheme
        self._ssl_context = ssl_context
        if ssl_context is not None:
            ssl_context.set_alpn_protocols(["http/1.1"])
        self._opener: urllib.request.OpenerDirector | None = None
        self.api_token = api_token
        self.timeout = timeout
        self.keep_alive = keep_alive

    def new(
        self,
        template: str,
        net: str = "",
        size: str = "",
        ttl_seconds: int = 0,
        claim_ref: str = "",
        volumes: list[str | Mapping[str, str]] | None = None,
        mount: bool = True,
    ) -> Sandbox:
        """Claims a sandbox; a warm hit is milliseconds."""
        claim = _claim_body(template, net, size, ttl_seconds, volumes, mount, claim_ref)
        return self._claim_from(self.addr, claim)

    def delete_template(self, template: str, net: str = "", size: str = "") -> None:
        """Removes a promoted template by name; on a cluster the delete follows gossip to the owner node (one hop)."""
        query = _template_query(template, net, size)
        path = "/v1/templates?" + urllib.parse.urlencode(query)
        reply = self._request(self.addr, "DELETE", path, None, "delete template")
        candidates = (reply or {}).get("redirect") or []
        if not candidates:
            return
        query["no_redirect"] = "1"
        path = "/v1/templates?" + urllib.parse.urlencode(query)
        _try_each(candidates, lambda peer: self._request(peer, "DELETE", path, None, "delete template"))

    def lookup(self, id: str, token: str) -> Sandbox:
        """Finds the owner by probing the entry and peers concurrently."""

        def probe(addr: str) -> Sandbox:
            reply = self._request(
                addr,
                "GET",
                f"/v1/sandboxes/{id}/owner",
                None,
                "owner",
                bearer=token,
                timeout=min(_PEERS_TIMEOUT, self.timeout),
            )
            return Sandbox(client=self, id=id, token=token, owner=reply.get("owner_addr") or addr)

        addrs = [self.addr, *self._peers()]
        try:
            return _scatter(addrs, probe)
        except APIError:
            raise APIError("lookup", 404, f"no owner found for {id}") from None

    def attach(self, owner_addr: str, id: str, token: str) -> Sandbox:
        """Binds a handle to a known owner without a lookup."""
        return Sandbox(client=self, id=id, token=token, owner=owner_addr)

    def sandboxes(self) -> list[dict[str, Any]]:
        """Lists the claims this token may see: id, key, deadline, claim_ref — never tokens or host paths."""
        reply = self._request(self.addr, "GET", "/v1/sandboxes", None, "list sandboxes")
        return [dict(sb) for sb in reply.get("sandboxes") or []]

    def drain(self) -> dict[str, Any]:
        """Cordons the node (root token): new claims are refused, live ones run to their leases."""
        return self._request(self.addr, "POST", "/v1/drain", None, "drain")

    def uncordon(self) -> dict[str, Any]:
        """Lifts a drain on the node (root token)."""
        return self._request(self.addr, "DELETE", "/v1/drain", None, "uncordon")

    def checkpoint(self, id: str) -> Checkpoint:
        """Binds a checkpoint handle to the entry node without a lookup."""
        return Checkpoint(self, self.addr, {"id": id})

    def checkpoints(self) -> list[Checkpoint]:
        """Lists the connected node's checkpoints, newest first."""
        reply = self._request(self.addr, "GET", "/v1/checkpoints", None, "list checkpoints")
        return [Checkpoint(self, self.addr, rec) for rec in reply.get("checkpoints") or []]

    def volumes(self) -> list[dict[str, Any]]:
        """Lists the caller-visible fleet catalog; availability is local."""
        reply = self._request(self.addr, "GET", "/v1/volumes", None, "list volumes")
        return [dict(volume) for volume in reply.get("volumes") or []]

    def info(self) -> dict[str, Any]:
        """The node's pool/claim counters, as served by GET /v1/info."""
        return self._request(self.addr, "GET", "/v1/info", None, "info")

    def _claim_from(self, addr: str, claim: dict[str, Any], path: str = "/v1/claim", verb: str = "claim") -> Sandbox:
        reply = self._post_json(addr, path, claim, verb)
        redirect = reply.get("redirect") or []
        if not redirect:
            return self._handle_from(addr, reply)
        claim["no_redirect"] = True
        if reply.get("require_promoted"):
            claim["require_promoted"] = True

        def post(peer: str) -> dict[str, Any]:
            return self._post_json(peer, path, claim, verb)

        owner, reply = _redirect_fallback(addr, redirect, post, verb)
        return self._handle_from(owner, reply)

    def _peers(self) -> list[str]:
        try:
            return (
                self._request(
                    self.addr, "GET", "/v1/peers", None, "peers", timeout=min(_PEERS_TIMEOUT, self.timeout)
                ).get("peers")
                or []
            )
        except APIError:
            return []

    def _handle_from(self, dialed: str, reply: dict[str, Any]) -> Sandbox:
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

    def _post_json(self, addr: str, path: str, body: dict[str, Any], verb: str) -> dict[str, Any]:
        return self._request(addr, "POST", path, body, verb)

    def _request(
        self,
        addr: str,
        method: str,
        path: str,
        body: dict[str, Any] | None,
        verb: str,
        bearer: str = "",
        timeout: float = 0.0,
    ) -> dict[str, Any]:
        data = json.dumps(body).encode() if body is not None else None
        url = addr + path if "://" in addr else f"{self._scheme}://{addr}{path}"
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        token = bearer or self.api_token
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with self._open(req, timeout or self.timeout) as resp:
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
            raise APIError(verb, 0, str(exc)) from None
        if not raw:
            return {}
        try:
            return cast(dict[str, Any], json.loads(raw))
        except json.JSONDecodeError as exc:
            raise APIError(verb, 0, "malformed JSON in response") from exc

    def _dial(self, addr: str, sandbox_id: str, token: str, deadline: float | None = None) -> Conn:
        origin = addr if "://" in addr else f"{self._scheme}://{addr}"
        context = self._tls() if origin.startswith("https://") else None
        return dial_agent(origin, sandbox_id, token, self.timeout, deadline, ssl_context=context)

    def _open(self, req: urllib.request.Request, timeout: float) -> Any:
        if req.type != "https":
            return urllib.request.urlopen(req, timeout=timeout)
        if self._opener is None:
            self._opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=self._tls()))
        return self._opener.open(req, timeout=timeout)

    def _tls(self) -> ssl.SSLContext:
        if self._ssl_context is None:
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.set_alpn_protocols(["http/1.1"])
        return self._ssl_context


def _claim_body(
    template: str,
    net: str,
    size: str,
    ttl_seconds: int,
    volumes: list[str | Mapping[str, str]] | None = None,
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
    if volumes:
        claim["volumes"] = [_volume_body(volume, mount) for volume in volumes]
    if not mount:
        claim["volumes_attach_only"] = True
    if claim_ref:
        claim["claim_ref"] = claim_ref
    return claim


def _volume_body(volume: str | Mapping[str, str], mount: bool) -> dict[str, str]:
    if isinstance(volume, str):
        return {"name": volume}
    if not isinstance(volume, Mapping):
        raise TypeError("volume must be a name string or mapping")
    unknown = sorted(set(volume) - {"name", "mount", "mode"})
    if unknown:
        raise TypeError(
            f"volume mapping accepts only name, mount, and mode, got unexpected key(s): {', '.join(unknown)}"
        )
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


def _template_query(template: str, net: str, size: str) -> dict[str, str]:
    query = {"template": template}
    if net:
        query["net"] = net
    if size:
        query["size"] = size
    return query


def _error_message(raw: bytes) -> str:
    try:
        return cast(str, json.loads(raw)["error"])
    except (ValueError, KeyError, TypeError):
        return raw.decode(errors="replace").strip()


def _retry_miss(exc: APIError) -> bool:
    return exc.status in (404, 0)


def _try_each(
    candidates: Iterable[str], call: Callable[[str], T], retry: Callable[[APIError], bool] = _retry_miss
) -> T:
    last_error = None
    for addr in candidates:
        try:
            return call(addr)
        except APIError as exc:
            if not retry(exc):
                raise
            last_error = exc
    raise cast(APIError, last_error)


def _retry_transient(exc: APIError) -> bool:
    return exc.status in (0, 401, 404, 429, 503, 500, 502, 504)


def _redirect_fallback(
    origin: str, candidates: Sequence[str], post: Callable[[str], dict[str, Any]], verb: str
) -> tuple[str, dict[str, Any]]:
    def attempt(addr: str) -> tuple[str, dict[str, Any]]:
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
            combined = f"{origin_exc.message} (after redirect targets failed: {exc.message})"
            raise APIError(verb, origin_exc.status, combined) from origin_exc


def _scatter(addrs: Sequence[str], probe: Callable[[str], T]) -> T:
    results: queue.Queue[tuple[T | None, Exception | None]] = queue.Queue(maxsize=len(addrs))

    def run(addr: str) -> None:
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
            return cast(T, value)
        last_error = error
    raise cast(Exception, last_error)
