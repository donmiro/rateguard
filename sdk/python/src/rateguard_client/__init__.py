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
"""

from __future__ import annotations

import http.client
import json
import os
import threading
import urllib.parse
from dataclasses import dataclass

__all__ = ["Client", "Decision"]

DEFAULT_URL = "http://127.0.0.1:7947"


@dataclass(frozen=True)
class Decision:
    """Whether to serve the request, and if not, how many seconds until a
    retry may be admitted."""

    allowed: bool
    retry_after: float = 0.0


class Client:
    """Checks keys through a rateguard-sidecar on this machine.

    :param url: the sidecar, ``$RATEGUARD_SIDECAR_URL`` or
        ``http://127.0.0.1:7947`` by default.
    :param timeout: seconds to wait for an answer; 50 ms by default, the
        sidecar answering in a fraction of a millisecond on loopback.
    :param on_error: ``"allow"`` or ``"deny"`` the request when the sidecar
        cannot be reached or answers badly.
    """

    def __init__(self, url: str | None = None, timeout: float = 0.05, on_error: str = "allow"):
        if on_error not in ("allow", "deny"):
            raise ValueError(f'on_error is "allow" or "deny", not {on_error!r}')
        url = url or os.environ.get("RATEGUARD_SIDECAR_URL", DEFAULT_URL)
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"not an http:// or https:// URL: {url}")
        self._connect = (
            http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        )
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        # A path in the URL is kept, as a proxy in front of the sidecar may need.
        self._path = parsed.path.rstrip("/") + "/v1/check"
        self._timeout = timeout
        self._on_error = Decision(allowed=on_error == "allow")
        self._local = threading.local()
        # Every thread's connection, for close().
        self._connections: set[http.client.HTTPConnection] = set()
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
                return self._on_error
            except (OSError, http.client.HTTPException):
                self._drop()
                return self._on_error
            if response.status != 200:
                return self._on_error
            try:
                answer = json.loads(data)
                if answer["allowed"]:
                    return Decision(allowed=True)
                return Decision(allowed=False, retry_after=answer["retry_after_ms"] / 1000)
            except (ValueError, KeyError, TypeError):
                return self._on_error
        return self._on_error

    def close(self) -> None:
        """Closes every thread's connection to the sidecar; the next check
        opens another."""
        with self._lock:
            connections, self._connections = self._connections, set()
        for connection in connections:
            connection.close()
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
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = self._connect(self._host, self._port, timeout=self._timeout)
            self._local.connection = connection
            with self._lock:
                self._connections.add(connection)
        return connection

    def _drop(self) -> None:
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            self._local.connection = None
            with self._lock:
                self._connections.discard(connection)
