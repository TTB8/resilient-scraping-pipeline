"""A local HTTP server for tests: the saved practice-site pages plus programmable failure scenarios.

It binds ``127.0.0.1`` on an ephemeral port and runs in a background thread
(``ThreadingHTTPServer``, so slow responses do not block other requests). Nothing here reaches the
network.

What it serves, in order of precedence:

1. ``/robots.txt``: the configured ``robots_txt`` body, or 404 (RFC 9309: "allow all") when unset.
   Robots requests never consume scenario steps, so a scenario counts page requests only.
2. ``routes``: exact-path overrides set with :meth:`FixtureServer.route` (redirects, big bodies...).
3. The active **scenario**: a list of :class:`Phase` values, each answering ``times`` requests
   (``None`` = forever) before the next takes over. ``Phase.serve()`` means "answer normally".
   Example, "after 2 good pages, 429 once, then a block page forever"::

       server.scenario(Phase.serve(2), Phase.rate_limited("1", times=1), Phase.block())

4. Static files: ``tests/fixtures/sites/<site>/`` at their real paths (``/page/2/`` ->
   ``page/2/index.html``), and ``tests/fixtures/js/`` under ``/_js/``. Unknown paths are 404.
   Like the real sites, a saved directory asked for without its trailing slash
   (``/author/Albert-Einstein``, the form the quotes pages link to) answers ``301`` to the slashed
   path. ``/static/<file>`` is served from ``tests/fixtures/js/static/`` when the saved site has no
   such file: the captured JavaScript page loads ``/static/jquery.js``, which was not captured, and a
   tiny stand-in there provides the one helper the page's script calls.

Every request is logged (path, User-Agent, status, time) under a lock; :meth:`hits` counts them.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar, Self
from urllib.parse import urlsplit

FIXTURES = Path(__file__).parent / "fixtures"
SITES = FIXTURES / "sites"
JS_PAGES = FIXTURES / "js"

BLOCK_HTML = (
    b"<html><head><title>Access denied</title></head>"
    b"<body><h1>Access denied</h1><p>You have been blocked.</p></body></html>"
)
CHALLENGE_HTML = (
    b"<html><head><title>One moment</title></head>"
    b"<body><h1>Checking your browser</h1><p>Verify you are human.</p></body></html>"
)

_TYPES = {".html": "text/html; charset=utf-8", ".txt": "text/plain; charset=utf-8"}


@dataclass(frozen=True, slots=True)
class Phase:
    """One step of a scenario. ``status is None`` means "serve the request normally"."""

    status: int | None = None
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)
    delay_s: float = 0.0
    times: int | None = None
    drip_s: float = 0.0  # > 0: send the body one byte at a time, this many seconds apart

    @classmethod
    def serve(cls, times: int | None = None, *, delay_s: float = 0.0) -> Self:
        return cls(times=times, delay_s=delay_s)

    @classmethod
    def slow(cls, delay_s: float, times: int | None = None) -> Self:
        return cls(times=times, delay_s=delay_s)

    @classmethod
    def drip(cls, every_s: float, size: int = 40, times: int | None = None) -> Self:
        """200 whose body trickles in a byte at a time: each read is quick, the whole is not."""
        return cls(200, b"x" * size, {"Content-Type": "text/plain"}, times=times, drip_s=every_s)

    @classmethod
    def error(cls, status: int = 500, times: int | None = None) -> Self:
        return cls(status, b"<html><body>Server error</body></html>", times=times)

    @classmethod
    def rate_limited(cls, retry_after: str | None = "1", times: int | None = None) -> Self:
        headers = {} if retry_after is None else {"Retry-After": retry_after}
        return cls(429, b"<html><body>Too many requests</body></html>", headers, times=times)

    @classmethod
    def block(cls, times: int | None = None) -> Self:
        return cls(403, BLOCK_HTML, {"Content-Type": "text/html"}, times=times)

    @classmethod
    def challenge(cls, times: int | None = None) -> Self:
        return cls(200, CHALLENGE_HTML, {"Content-Type": "text/html"}, times=times)


@dataclass(frozen=True, slots=True)
class Hit:
    path: str
    user_agent: str | None
    status: int
    at: float


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    owner: FixtureServer


class _Handler(BaseHTTPRequestHandler):
    server: _Server
    protocol_version = "HTTP/1.0"
    error_content_type = "text/html; charset=utf-8"
    disable_nagle_algorithm: ClassVar[bool] = True

    def do_GET(self) -> None:
        self.server.owner.handle(self)

    def log_message(self, format: str, *args: object) -> None:
        """Silence the per-request stderr line; the server keeps its own log."""


class FixtureServer:
    """See the module docstring. Use :func:`start` or the ``fixture_server`` pytest fixture."""

    def __init__(self, site: str = "quotes", *, robots_txt: str | None = None) -> None:
        self.root = SITES / site
        if not self.root.is_dir():
            raise ValueError(f"no saved site {site!r} under {SITES}")
        self.robots_txt = robots_txt
        self._lock = threading.Lock()
        self._log: list[Hit] = []
        self._routes: dict[str, Phase] = {}
        self._phases: list[Phase] = []
        self._used = 0  # requests answered by the current phase
        self._httpd = _Server(("127.0.0.1", 0), _Handler)
        self._httpd.owner = self
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )

    # --- lifecycle ---------------------------------------------------------------------------

    def start(self) -> Self:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    @property
    def port(self) -> int:
        return int(self._httpd.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, path: str) -> str:
        return self.base_url + path

    # --- programming -------------------------------------------------------------------------

    def scenario(self, *phases: Phase) -> None:
        """Replace the active scenario. No phases: serve everything normally."""
        with self._lock:
            self._phases = list(phases)
            self._used = 0

    def route(self, path: str, phase: Phase) -> None:
        """Answer ``path`` with ``phase`` every time (``Phase.serve`` is not allowed here)."""
        if phase.status is None:
            raise ValueError("a route needs an explicit status")
        with self._lock:
            self._routes[path] = phase

    def redirect(self, path: str, to: str, status: int = 302) -> None:
        self.route(path, Phase(status, b"", {"Location": to}))

    # --- inspection --------------------------------------------------------------------------

    def log(self) -> list[Hit]:
        with self._lock:
            return list(self._log)

    def hits(self, path: str | None = None) -> int:
        with self._lock:
            return sum(1 for hit in self._log if path is None or hit.path == path)

    def hit_counts(self) -> Counter[str]:
        with self._lock:
            return Counter(hit.path for hit in self._log)

    # --- request handling (server threads) ---------------------------------------------------

    def _next_phase(self) -> Phase:
        with self._lock:
            while self._phases:
                phase = self._phases[0]
                if phase.times is None or self._used < phase.times:
                    self._used += 1
                    return phase
                self._phases.pop(0)
                self._used = 0
            return Phase.serve()

    def handle(self, request: _Handler) -> None:
        path = urlsplit(request.path).path
        if path == "/robots.txt":
            if self.robots_txt is None:
                self._send(request, path, Phase(404, b"not found"))
            else:
                body = self.robots_txt.encode("utf-8")
                self._send(request, path, Phase(200, body, {"Content-Type": _TYPES[".txt"]}))
            return
        with self._lock:
            routed = self._routes.get(path)
        phase = routed if routed is not None else self._next_phase()
        if phase.delay_s > 0:
            time.sleep(phase.delay_s)
        if phase.status is None:
            self._send(request, path, self._static(path))
        else:
            self._send(request, path, phase)

    def _static(self, path: str) -> Phase:
        if path.startswith("/_js/"):
            base, rel = JS_PAGES, path.removeprefix("/_js/")
        else:
            base, rel = self.root, path.lstrip("/")
            if path.startswith("/static/") and not (base / rel).is_file():
                base = JS_PAGES
        if rel and not rel.endswith("/"):
            folder = (base / rel).resolve()
            if folder.is_relative_to(base.resolve()) and (folder / "index.html").is_file():
                return Phase(301, b"", {"Location": path + "/"})
        if rel == "" or rel.endswith("/"):
            rel += "index.html"
        target = (base / rel).resolve()
        if not target.is_relative_to(base.resolve()) or not target.is_file():
            return Phase(404, b"<html><body>Not found</body></html>")
        content_type = _TYPES.get(target.suffix, "application/octet-stream")
        return Phase(200, target.read_bytes(), {"Content-Type": content_type})

    def _send(self, request: _Handler, path: str, phase: Phase) -> None:
        status = phase.status if phase.status is not None else 200
        with self._lock:
            self._log.append(Hit(path, request.headers.get("User-Agent"), status, time.monotonic()))
        try:
            request.send_response(status)
            for name, value in phase.headers.items():
                request.send_header(name, value)
            request.send_header("Content-Length", str(len(phase.body)))
            request.end_headers()
            if phase.drip_s > 0:
                for index in range(len(phase.body)):
                    request.wfile.write(phase.body[index : index + 1])
                    request.wfile.flush()
                    time.sleep(phase.drip_s)
            else:
                request.wfile.write(phase.body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the client gave up (a timeout test); nothing to answer


def start(site: str = "quotes", *, robots_txt: str | None = None) -> FixtureServer:
    return FixtureServer(site, robots_txt=robots_txt).start()
