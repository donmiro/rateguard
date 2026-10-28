"""A client for rateguard-sidecar: one global rate limit across a fleet,
from Python.

    from rateguard_client import Client

    limiter = Client()  # http://127.0.0.1:7947, or $RATEGUARD_SIDECAR_URL
    decision = limiter.check("tenant:42", cls="pro")
    if not decision.allowed:
        return too_many_requests(retry_after=decision.retry_after)

Standard library only. One keep-alive connection per thread. If the
sidecar cannot be reached in time, or answers with anything but a decision,
the request is allowed (``on_error="allow"``, the default): a broken limiter
should not take the service down with it. ``on_error="deny"`` denies
instead.

The rest of the sidecar's API is here too, off the request path: ``stats``
and ``key_stats`` read, and ``set_classes``, ``set_limit``,
``set_key_limits``, ``set_shadows`` and ``set_dry_run`` change limits on a
sidecar started with ``--admin``. They raise :class:`SidecarError` when the
sidecar does not carry them out.
"""

from __future__ import annotations

import http.client
import json
import os
import threading
import urllib.parse
import weakref
from dataclasses import dataclass, field

__all__ = ["Client", "Decision", "SidecarError"]

DEFAULT_URL = "http://127.0.0.1:7947"


@dataclass(frozen=True)
class Decision:
    """Whether to serve the request, and if not, how many seconds until a
    retry may be admitted.

    ``error`` is set when the sidecar could not give a decision: ``allowed``
    then follows ``on_error``, or is false for a key the sidecar refuses as
    too big. It is left out of comparisons, so decisions compare by what
    they decide."""

    allowed: bool
    retry_after: float = 0.0
    error: str | None = field(default=None, compare=False)


class SidecarError(Exception):
    """A call other than ``check`` that the sidecar did not carry out.

    ``status`` is the HTTP status it answered, or ``None`` when it did not
    answer at all; the message has the sidecar's own reason, as for a limit
    it refused with a 400."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class Client:
    """Checks keys through a rateguard-sidecar on this machine.

    :param url: the sidecar, ``$RATEGUARD_SIDECAR_URL`` or
        ``http://127.0.0.1:7947`` by default.
    :param timeout: seconds to wait for an answer; 50 ms by default, the
        sidecar answering in a fraction of a millisecond on loopback.
    :param on_error: ``"allow"`` or ``"deny"`` the request when the sidecar
        cannot be reached or answers badly.
    :param admin_timeout: seconds to wait for every call but ``check``: one
        second by default. They are not on the request path, and raise
        :class:`SidecarError` rather than follow ``on_error``.
    """

    def __init__(
        self,
        url: str | None = None,
        timeout: float = 0.05,
        on_error: str = "allow",
        admin_timeout: float = 1.0,
    ):
        if on_error not in ("allow", "deny"):
            raise ValueError(f'on_error is "allow" or "deny", not {on_error!r}')
        # An empty string is unset, as a blank variable in a deployment file
        # usually means "not configured".
        url = url or os.environ.get("RATEGUARD_SIDECAR_URL") or DEFAULT_URL
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"not an http:// or https:// URL: {url}")
        self._connect = (
            http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        )
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        # A path in the URL is kept, as a proxy in front of the sidecar may need.
        self._base = parsed.path.rstrip("/")
        self._path = self._base + "/v1/check"
        self._timeout = timeout
        self._admin_timeout = admin_timeout
        self._allow_on_error = on_error == "allow"
        self._local = threading.local()
        # Every live thread's connection, for close(). Weak: a thread that
        # is gone takes its connection with it.
        self._connections: weakref.WeakSet[_Held] = weakref.WeakSet()
        self._lock = threading.Lock()

    def check(self, key: str, cls: str | None = None) -> Decision:
        """Admits or denies one request for ``key``, in the class ``cls`` or
        the default one."""
        body = {"key": key} if cls is None else {"key": key, "class": cls}
        payload = json.dumps(body).encode()
        # Once more on a fresh connection if the kept one turns out closed.
        for attempt in range(2):
            connection = self._connection()
            try:
                connection.request(
                    "POST",
                    self._path,
                    body=payload,
                    headers={"Content-Type": "application/json"},
                )
                response = connection.getresponse()
                data = response.read()
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError):
                self._drop()
                if attempt == 0:
                    continue
                return self._failed("the sidecar closed the connection")
            except (OSError, http.client.HTTPException) as error:
                self._drop()
                return self._failed(f"the sidecar did not answer: {error!r}")
            if response.status == 413:
                # The sidecar refuses a body this large for every attempt, so
                # allowing it would let the key past the limit for good.
                return Decision(
                    allowed=False, error="the key is over the sidecar's 64 KiB limit"
                )
            if response.status != 200:
                return self._failed(f"the sidecar answered {response.status}")
            try:
                answer = json.loads(data)
                if answer["allowed"]:
                    return Decision(allowed=True)
                return Decision(allowed=False, retry_after=answer["retry_after_ms"] / 1000)
            except (ValueError, KeyError, TypeError):
                return self._failed("not a decision from the sidecar")
        return self._failed("the sidecar closed the connection")

    def _failed(self, error: str) -> Decision:
        return Decision(allowed=self._allow_on_error, error=error)

    def stats(self) -> dict:
        """What the sidecar admitted and denied, and why: ``/v1/stats``, as
        the sidecar's README describes it, with the keys denied most by
        name."""
        return self._call("GET", "/v1/stats")

    def key_stats(self, key: str, cls: str | None = None) -> dict | None:
        """One key's rates, share, limit and shadow, in the class ``cls`` or
        the default one; ``None`` while the sidecar keeps no state for the
        key, as it does not for a key that is not busy there."""
        query = {"key": key} if cls is None else {"key": key, "class": cls}
        path = "/v1/keys?" + urllib.parse.urlencode(query)
        return self._call("GET", path, missing_is_none=True)

    def set_classes(self, classes: dict[str, dict]) -> None:
        """Replaces every named class: ``{"pro": {"per_minute": 60000,
        "burst": 50}}``. A limit has one of ``per_second``, ``per_minute``
        and ``per_hour``, and an optional ``burst``."""
        self._call("PUT", "/v1/classes", classes)

    def set_limit(self, limit: dict) -> None:
        """Sets the default class's limit: ``{"per_second": 100}``."""
        self._call("PUT", "/v1/limit", limit)

    def set_key_limits(self, key_limits: list[dict]) -> None:
        """Replaces every key's own limit. An entry has a ``key``, the
        ``class`` it is checked in (the default one if left out), and either
        its own ``limit`` or ``as_class``, the class whose limit it
        follows."""
        self._call("PUT", "/v1/key_limits", key_limits)

    def set_shadows(self, shadows: dict[str, dict | None]) -> None:
        """Replaces every shadow limit, by class, ``""`` for the default
        one: a limit that denies nothing, and counts what it would deny."""
        self._call("PUT", "/v1/shadows", shadows)

    def set_dry_run(self, on: bool) -> None:
        """Admits everything while on, and counts what would be denied."""
        self._call("PUT", "/v1/dry_run", {"on": on})

    def _call(self, method: str, path: str, body=None, missing_is_none: bool = False):
        # A connection of its own: these calls are rare, and should not
        # disturb the one a thread keeps for its checks.
        connection = self._connect(self._host, self._port, timeout=self._admin_timeout)
        try:
            if body is None:
                connection.request(method, self._base + path)
            else:
                connection.request(
                    method,
                    self._base + path,
                    body=json.dumps(body).encode(),
                    headers={"Content-Type": "application/json"},
                )
            response = connection.getresponse()
            data = response.read()
        except (OSError, http.client.HTTPException) as error:
            raise SidecarError(f"the sidecar did not answer: {error!r}") from error
        finally:
            connection.close()
        if response.status == 404 and missing_is_none:
            return None
        if response.status == 404 and method == "PUT":
            raise SidecarError("the sidecar has no admin routes: start it with --admin", 404)
        if response.status not in (200, 204):
            raise SidecarError(
                f"the sidecar answered {response.status}: {_reason(data)}", response.status
            )
        if response.status == 204:
            return None
        try:
            return json.loads(data)
        except ValueError as error:
            raise SidecarError("not JSON from the sidecar", response.status) from error

    def close(self) -> None:
        """Closes every thread's connection to the sidecar; the next check
        opens another."""
        with self._lock:
            held, self._connections = list(self._connections), weakref.WeakSet()
        for one in held:
            one.connection.close()
        self._local = threading.local()

    def _open(self) -> int:
        """How many connections are open, for tests."""
        with self._lock:
            return len(self._connections)

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _connection(self) -> http.client.HTTPConnection:
        held = getattr(self._local, "held", None)
        if held is None:
            held = _Held(self._connect(self._host, self._port, timeout=self._timeout))
            self._local.held = held
            with self._lock:
                self._connections.add(held)
        return held.connection

    def _drop(self) -> None:
        held = getattr(self._local, "held", None)
        if held is not None:
            held.connection.close()
            self._local.held = None
            with self._lock:
                self._connections.discard(held)


def _reason(data: bytes) -> str:
    """The sidecar's ``{"error": ...}``, or the body as it is."""
    try:
        return str(json.loads(data)["error"])
    except (ValueError, KeyError, TypeError):
        return data.decode(errors="replace") or "no reason given"


class _Held:
    """A thread's connection, in that thread's local storage alone: closed
    once the thread is gone, rather than left open for good."""

    def __init__(self, connection: http.client.HTTPConnection):
        self.connection = connection

    def __del__(self) -> None:
        self.connection.close()
