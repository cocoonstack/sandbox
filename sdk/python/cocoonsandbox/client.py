"""Client: the control-plane entry point."""

from __future__ import annotations

import http.client
import json
import queue
import ssl
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterable, Sequence
from typing import Any, TypeVar, cast

from .checkpoint import Checkpoint
from .conn import Conn, _Closeable, dial_agent, is_quiet, remaining_timeout
from .endpoint import _endpoint_url
from .errors import APIError, SandboxTimeout, _DialError, _error_message, _require_field
from .sandbox import Sandbox
from .template import _claim_body, _template_query
from .volumes import Volume

_T = TypeVar("_T")

_PEERS_TIMEOUT = 5.0
_IDLE_CONNS = 8


class Client(_Closeable):
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
        """keep_alive parks idle connections, 0 dials per call; a parked relay one holds the sandbox's idle clock.

        ssl_context is set to offer ALPN http/1.1, which the relay upgrade needs; pass a dedicated context.
        """
        endpoint = _endpoint_url(addr.strip())
        self.addr = endpoint.geturl().removeprefix("http://")
        self._scheme = endpoint.scheme
        self._ssl_context = ssl_context
        if ssl_context is not None:
            ssl_context.set_alpn_protocols(["http/1.1"])
        self._idle: dict[tuple[str, str], list[tuple[http.client.HTTPConnection, float]]] = {}
        self._idle_lock = threading.Lock()
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
        volumes: list[str | Volume] | None = None,
        *,
        deadline: float | None = None,
    ) -> Sandbox:
        """Claims a sandbox; a warm hit is milliseconds. deadline is a time.monotonic() bound over the whole claim."""
        claim = _claim_body(template, net, size, ttl_seconds, volumes, claim_ref)
        return self._claim_from(self.addr, claim, deadline=deadline)

    def delete_template(self, template: str, net: str = "", size: str = "") -> None:
        """Removes a promoted template by name; on a cluster the delete follows gossip to the owner node (one hop)."""
        query = _template_query(template, net, size)
        path = "/v1/templates?" + urllib.parse.urlencode(query)
        reply = self._request(self.addr, "DELETE", path, None, "delete template")
        candidates = reply.get("redirect") or []
        if not candidates:
            return
        query["no_redirect"] = "1"
        path = "/v1/templates?" + urllib.parse.urlencode(query)
        _try_each(candidates, lambda peer: self._request(peer, "DELETE", path, None, "delete template"))

    def lookup(self, id: str, token: str) -> Sandbox:
        """Finds the owner by listing the peers, then probing the entry and every peer concurrently."""

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

        peers, peers_err = self._peers()
        try:
            return _scatter([self.addr, *peers], probe)
        except APIError:
            reason = f"no owner found for {id}" if peers_err is None else f"no owner found for {id}; {peers_err}"
            raise APIError("lookup", 404, reason) from None

    def attach(self, owner_addr: str, id: str, token: str) -> Sandbox:
        """Binds a handle to a known owner without a lookup."""
        return Sandbox(client=self, id=id, token=token, owner=owner_addr)

    def sandboxes(self) -> list[dict[str, Any]]:
        """Lists the claims this token may see: id, key, deadline, claim_ref — never tokens or host paths."""
        reply = self._request(self.addr, "GET", "/v1/sandboxes", None, "list sandboxes")
        return [dict(sb) for sb in reply.get("sandboxes") or []]

    def drain(self) -> dict[str, Any]:
        """Drains the node (root token): new claims are refused, live ones run to their leases."""
        return self._request(self.addr, "POST", "/v1/drain", None, "drain")

    def undrain(self) -> dict[str, Any]:
        """Lifts a drain on the node (root token)."""
        return self._request(self.addr, "DELETE", "/v1/drain", None, "undrain")

    def cordon(self) -> dict[str, Any]:
        """Refuses new claims across restarts (root token); live claims and moves in stay admitted."""
        return self._request(self.addr, "POST", "/v1/cordon", None, "cordon")

    def uncordon(self) -> dict[str, Any]:
        """Lifts a cordon on the node (root token); a drain stays."""
        return self._request(self.addr, "DELETE", "/v1/cordon", None, "uncordon")

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

    def close(self) -> None:
        """Closes the parked control connections; a later call dials afresh."""
        with self._idle_lock:
            while self._idle:
                _, idle = self._idle.popitem()
                for conn, _ in idle:
                    conn.close()

    def _claim_from(
        self,
        addr: str,
        claim: dict[str, Any],
        path: str = "/v1/claim",
        verb: str = "claim",
        *,
        deadline: float | None = None,
    ) -> Sandbox:
        reply = self._request(addr, "POST", path, claim, verb, deadline=deadline)
        redirect = reply.get("redirect") or []
        if not redirect:
            return self._handle_from(addr, reply, verb)
        claim["no_redirect"] = True
        if reply.get("require_promoted"):
            claim["require_promoted"] = True

        def post(peer: str) -> dict[str, Any]:
            return self._request(peer, "POST", path, claim, verb, deadline=deadline)

        owner, reply = _redirect_fallback(addr, redirect, post, verb)
        return self._handle_from(owner, reply, verb)

    def _peers(self) -> tuple[list[str], APIError | None]:
        try:
            reply = self._request(
                self.addr, "GET", "/v1/peers", None, "peers", timeout=min(_PEERS_TIMEOUT, self.timeout)
            )
        except APIError as exc:
            return [], exc
        return reply.get("peers") or [], None

    def _handle_from(self, dialed: str, reply: dict[str, Any], verb: str = "claim") -> Sandbox:
        return Sandbox(
            client=self,
            id=_require_field(reply, "id", verb),
            token=_require_field(reply, "token", verb),
            owner=reply.get("owner_addr") or dialed,
            deadline=reply.get("deadline", ""),
            from_checkpoint=reply.get("from_checkpoint", ""),
            template_digest=reply.get("template_digest", ""),
            volumes=reply.get("volumes") or [],
        )

    def _request(
        self,
        addr: str,
        method: str,
        path: str,
        body: dict[str, Any] | None,
        verb: str,
        bearer: str = "",
        timeout: float = 0.0,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        timeout = remaining_timeout(timeout or self.timeout, deadline, verb)
        data = json.dumps(body).encode() if body is not None else None
        url = urllib.parse.urlsplit(addr + path if "://" in addr else f"{self._scheme}://{addr}{path}")
        headers = {"Content-Type": "application/json"} if data is not None else {}
        token = bearer or self.api_token
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            status, raw = self._roundtrip(url, method, data, headers, timeout)
        except (OSError, http.client.HTTPException) as exc:
            if deadline is not None and time.monotonic() >= deadline:
                raise SandboxTimeout(f"{verb} timed out") from None
            raise APIError(verb, 0, str(exc)) from exc
        if status >= 400:
            try:
                reply = json.loads(raw)
            except (ValueError, TypeError):
                reply = None
            raise APIError(verb, status, _error_message(raw), reply=reply if isinstance(reply, dict) else None)
        if not raw:
            return {}
        try:
            return cast(dict[str, Any], json.loads(raw))
        except json.JSONDecodeError as exc:
            raise APIError(verb, 0, "malformed JSON in response") from exc

    def _dial(self, addr: str, sandbox_id: str, token: str, deadline: float | None = None) -> Conn:
        origin = addr if "://" in addr else f"{self._scheme}://{addr}"
        context = self._tls() if origin.startswith("https://") else None
        return dial_agent(
            origin, sandbox_id, token, self.timeout, deadline, ssl_context=context, tag=(sandbox_id, addr)
        )

    def _roundtrip(
        self, url: urllib.parse.SplitResult, method: str, data: bytes | None, headers: dict[str, str], timeout: float
    ) -> tuple[int, bytes]:
        key = (url.scheme, url.netloc)
        conn = self._take(key, timeout)
        if conn.sock is None:
            try:
                conn.connect()
            except OSError as exc:
                conn.close()
                raise _DialError(str(exc)) from exc
        status = 0
        try:
            conn.request(method, url.path + (f"?{url.query}" if url.query else ""), body=data, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            raw = resp.read()
        except (OSError, http.client.HTTPException) as exc:
            conn.close()
            if status < 400:
                raise
            return status, str(exc).encode()
        except BaseException:
            conn.close()
            raise
        with self._idle_lock:
            idle = self._idle.setdefault(key, [])
            if resp.will_close or not self.keep_alive or len(idle) >= _IDLE_CONNS:
                conn.close()
            else:
                idle.append((conn, time.monotonic()))
        return resp.status, raw

    def _take(self, key: tuple[str, str], timeout: float) -> http.client.HTTPConnection:
        with self._idle_lock:
            idle = self._idle.get(key, [])
            while idle:
                conn, parked = idle.pop()
                if time.monotonic() - parked < self.keep_alive and is_quiet(conn.sock):
                    conn.sock.settimeout(timeout)
                    return conn
                conn.close()
        scheme, netloc = key
        if scheme == "https":
            return http.client.HTTPSConnection(netloc, timeout=timeout, context=self._tls())
        return http.client.HTTPConnection(netloc, timeout=timeout)

    def _tls(self) -> ssl.SSLContext:
        if self._ssl_context is None:
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.set_alpn_protocols(["http/1.1"])
        return self._ssl_context


def _retry_miss(exc: APIError) -> bool:
    return exc.status in (404, 0)


def _try_each(
    candidates: Iterable[str], call: Callable[[str], _T], retry: Callable[[APIError], bool] = _retry_miss
) -> _T:
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
    return exc.status in (0, 401, 404, 429, 500, 503)


def _claim_refused(exc: APIError) -> bool:
    if exc.status:
        return exc.status not in (502, 504)
    return exc.__cause__ is None or isinstance(exc.__cause__, _DialError)


def _redirect_fallback(
    origin: str, candidates: Sequence[str], post: Callable[[str], dict[str, Any]], verb: str
) -> tuple[str, dict[str, Any]]:
    def attempt(addr: str) -> tuple[str, dict[str, Any]]:
        reply = post(addr)
        if reply.get("redirect"):
            raise APIError(verb, 0, f"{addr} redirected again despite no_redirect")
        return addr, reply

    try:
        return _try_each(candidates, attempt, retry=_claim_refused)
    except APIError as exc:
        if not (_claim_refused(exc) and _retry_transient(exc)):
            raise
        try:
            return attempt(origin)
        except APIError as origin_exc:
            combined = f"{origin_exc.message} (after redirect targets failed: {exc.message})"
            raise APIError(verb, origin_exc.status, combined) from origin_exc


def _scatter(addrs: Sequence[str], probe: Callable[[str], _T]) -> _T:
    results: queue.Queue[tuple[_T | None, Exception | None]] = queue.Queue(maxsize=len(addrs))

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
            return cast(_T, value)
        last_error = error
    raise cast(Exception, last_error)
