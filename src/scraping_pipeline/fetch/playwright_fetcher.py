"""A fetcher for pages that need JavaScript: plain headless Chromium through Playwright.

Detection, not evasion (ADR 0005). This browser does not pretend to be anything else: it sends the
project's honest User-Agent, keeps Playwright's defaults (``navigator.webdriver`` stays ``true``)
and loads no plugin that patches fingerprints. If a site decides it does not want an automated
browser, that is its call; ``core.classify`` sees the block or challenge and the scheduler stops
the domain.

No proxy, enforced. Playwright adds ``--proxy-server`` only when a ``proxy`` option is given (read
from the bundled driver, ``chromium._innerDefaultArgs``) and otherwise lets Chromium resolve proxies
itself, from the system configuration or, on Linux, the ``*_proxy`` environment variables, which
Playwright passes to the browser by default. So the browser is launched with ``--no-proxy-server``
*and* an environment with every proxy variable removed (:func:`launch_options`), the same guarantee
``trust_env=False`` gives the httpx fetcher.

Confinement: every request the page makes goes through a route handler (:func:`route_verdict`):

* the **top-level document** is allowed once, for the URL asked for. Redirects are *not* followed:
  a redirected navigation is aborted and the fetcher returns the 3xx (status and ``Location``), so
  the scheduler applies the same rules as for httpx (same origin only, robots.txt, a token per hop);
* **same-origin scripts** are allowed: the page needs them to render;
* everything else is aborted: images, stylesheets, fonts, media, XHR/fetch, frames, any request to
  another origin, and any later navigation the page's script attempts.

Budget: the document is the request that took a token. The same-origin scripts it pulls in are not
throttled separately; they are bounded by the page itself (a handful, cached by the browser for the
run's single context), which is the documented exception to "one token per request".

Lifecycle: the browser is launched once, lazily on the first ``fetch`` (or on ``__aenter__``), with
one browser context for the run; each fetch opens and closes its own page. Failing to *launch* the
browser raises: it means the environment is broken (Chromium not installed), which is not a fact
about the site and must not be recorded as one.

Waiting for content: after navigation, when ``wait_for_selector`` is set and the response is 2xx,
the fetcher waits for that selector (for instance ``.quote``). If it never appears, the page did not
render what we came for. That is reported as a ``timeout`` transport error, *unless* the rendered
page is a block or challenge page: then the result is returned without an error, so classification
sees the refusal and the domain is stopped instead of being retried. That check reuses
``core.classify`` with the same markers as the scheduler (pass ``markers``). Navigation and the
selector wait share one overall deadline of ``request_timeout_s``.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import TracebackType
from typing import Self
from urllib.parse import urlsplit

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    Request,
    Route,
    async_playwright,
)
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scraping_pipeline.config import Settings
from scraping_pipeline.core.classify import DEFAULT_MARKERS, BlockMarkers, TransportError, classify
from scraping_pipeline.core.types import ResponseClass
from scraping_pipeline.fetch.protocol import DEFAULT_MAX_BODY_BYTES, FetchResult, check_http_url

# Chromium's net error names, from the message of a failed navigation.
_NET_ERRORS: tuple[tuple[str, TransportError], ...] = (
    ("err_name_not_resolved", TransportError.DNS),
    ("err_name_resolution_failed", TransportError.DNS),
    ("err_timed_out", TransportError.TIMEOUT),
    ("err_connection_timed_out", TransportError.TIMEOUT),
    ("err_connection_refused", TransportError.CONNECT),
    ("err_address_unreachable", TransportError.CONNECT),
    ("err_internet_disconnected", TransportError.CONNECT),
    ("err_connection_reset", TransportError.RESET),
    ("err_connection_closed", TransportError.RESET),
    ("err_connection_aborted", TransportError.RESET),
    ("err_empty_response", TransportError.RESET),
    ("err_cert_", TransportError.TLS),
    ("err_ssl_", TransportError.TLS),
)

_REFUSALS = frozenset({ResponseClass.BLOCKED, ResponseClass.CHALLENGE})


def map_playwright_error(message: str) -> TransportError:
    """The :class:`TransportError` for a Playwright navigation error message.

    Unknown messages map to ``protocol``: the browser got *something* it could not use.
    """
    lowered = message.lower()
    for needle, error in _NET_ERRORS:
        if needle in lowered:
            return error
    if "timeout" in lowered:
        return TransportError.TIMEOUT
    return TransportError.PROTOCOL


def is_refusal_page(
    status: int, headers: Mapping[str, str], html: str, markers: BlockMarkers = DEFAULT_MARKERS
) -> bool:
    """True when ``core.classify`` (with ``markers``) reads the rendered page as a refusal."""
    verdict = classify(status, headers, html, None, markers=markers)
    return verdict.response_class in _REFUSALS


_PROXY_VARIABLES = frozenset({"http_proxy", "https_proxy", "all_proxy", "no_proxy", "ftp_proxy"})


def launch_options(environ: Mapping[str, str] | None = None) -> dict[str, object]:
    """Keyword arguments for ``chromium.launch``: headless, no proxy, no proxy variables."""
    source = os.environ if environ is None else environ
    env = {k: v for k, v in source.items() if k.lower() not in _PROXY_VARIABLES}
    return {"headless": True, "args": ["--no-proxy-server"], "env": env}


class RouteVerdict(StrEnum):
    """What the route handler does with one browser request."""

    CONTINUE = "continue"
    ABORT = "abort"
    REPORT_REDIRECT = "report_redirect"  # abort, and hand the 3xx to the scheduler


def _origin(url: str) -> tuple[str, str, int | None]:
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    try:
        port = parts.port
    except ValueError:
        port = -1
    default = {"http": 80, "https": 443}.get(scheme)
    return scheme, (parts.hostname or "").lower(), port if port is not None else default


def route_verdict(
    *,
    target: str,
    request_url: str,
    resource_type: str,
    is_main_navigation: bool,
    is_redirect: bool,
    document_seen: bool,
) -> RouteVerdict:
    """The confinement policy for one request made while rendering ``target`` (pure, testable)."""
    if is_main_navigation:
        if is_redirect:
            return RouteVerdict.REPORT_REDIRECT
        if document_seen or _origin(request_url) != _origin(target):
            return RouteVerdict.ABORT  # a script trying to navigate away, or another origin
        return RouteVerdict.CONTINUE
    if resource_type == "script" and _origin(request_url) == _origin(target):
        return RouteVerdict.CONTINUE
    return RouteVerdict.ABORT


@dataclass(slots=True)
class _PageGuard:
    """Per-fetch state of the route handler."""

    target: str
    document_seen: bool = False
    redirect: tuple[int, dict[str, str]] | None = None
    blocked: list[str] = field(default_factory=list)

    async def handle(self, route: Route, request: Request) -> None:
        main = request.is_navigation_request() and request.frame.parent_frame is None
        verdict = route_verdict(
            target=self.target,
            request_url=request.url,
            resource_type=request.resource_type,
            is_main_navigation=main,
            is_redirect=request.redirected_from is not None,
            document_seen=self.document_seen,
        )
        if verdict is RouteVerdict.CONTINUE:
            if main:
                self.document_seen = True
            await route.continue_()
            return
        if verdict is RouteVerdict.REPORT_REDIRECT and request.redirected_from is not None:
            previous = await request.redirected_from.response()
            if previous is not None:
                self.redirect = (previous.status, await previous.all_headers())
        self.blocked.append(request.url)
        await route.abort("blockedbyclient")


class PlaywrightFetcher:
    """Render pages in headless Chromium. Use as ``async with PlaywrightFetcher(settings) as f:``."""

    def __init__(
        self,
        settings: Settings,
        wait_for_selector: str | None = None,
        *,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
        clock: Callable[[], float] = time.perf_counter,
        markers: BlockMarkers = DEFAULT_MARKERS,
    ) -> None:
        if max_body_bytes < 1:
            raise ValueError(f"max_body_bytes must be >= 1, got {max_body_bytes!r}")
        self._user_agent = settings.user_agent
        self._timeout_ms = settings.request_timeout_s * 1000
        self._deadline_s = settings.request_timeout_s
        self._markers = markers
        self._selector = wait_for_selector
        self._max_body = max_body_bytes
        self._clock = clock
        self._lock = asyncio.Lock()
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._closed = False

    async def __aenter__(self) -> Self:
        await self._ensure_context()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        context, browser, playwright = self._context, self._browser, self._playwright
        self._context = self._browser = self._playwright = None
        if context is not None:
            await context.close()
        if browser is not None:
            await browser.close()
        if playwright is not None:
            await playwright.stop()

    async def _ensure_context(self) -> BrowserContext:
        async with self._lock:
            if self._closed:
                raise RuntimeError("PlaywrightFetcher used after aclose()")
            if self._context is None:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(**launch_options())  # type: ignore[arg-type]
                self._context = await self._browser.new_context(user_agent=self._user_agent)
            return self._context

    async def fetch(self, url: str) -> FetchResult:
        check_http_url(url)
        context = await self._ensure_context()
        started = self._clock()
        page = await context.new_page()
        guard = _PageGuard(target=url)
        await page.route("**/*", guard.handle)
        try:
            try:
                async with asyncio.timeout(self._deadline_s):
                    outcome = await self._navigate(page, url, guard)
            except TimeoutError:
                detail = f"page not rendered within {self._deadline_s:g}s"
                outcome = _Outcome.failed(TransportError.TIMEOUT, detail)
        finally:
            await page.close()
        return FetchResult(
            requested_url=url,
            final_url=url,  # redirects are never followed here (see the module docstring)
            status=outcome.status,
            headers=outcome.headers,
            body=outcome.body if outcome.error is None or outcome.keep_body else b"",
            error=outcome.error,
            elapsed_s=max(0.0, self._clock() - started),
            fetcher="playwright",
            error_detail=outcome.detail,
        )

    async def _navigate(self, page: Page, url: str, guard: _PageGuard) -> _Outcome:
        try:
            response = await page.goto(url, timeout=self._timeout_ms, wait_until="load")
        except PlaywrightError as exc:
            if guard.redirect is not None:  # the site redirected: report it, do not follow
                status, headers = guard.redirect
                return _Outcome(status, headers, b"", None, None)
            return _Outcome.failed(map_playwright_error(exc.message), _first_line(exc.message))
        if response is None:
            return _Outcome.failed(TransportError.PROTOCOL, "navigation produced no response")
        status = response.status
        headers = await response.all_headers()
        waited_out: PlaywrightError | None = None
        if self._selector is not None and 200 <= status < 300:  # noqa: PLR2004
            try:
                await page.wait_for_selector(self._selector, timeout=self._timeout_ms)
            except PlaywrightTimeoutError as exc:
                waited_out = exc
        html = await page.content()
        body = html.encode("utf-8")
        # The DOM is re-serialised by the browser, always as UTF-8.
        headers["content-type"] = "text/html; charset=utf-8"
        if len(body) > self._max_body:
            detail = f"rendered page larger than {self._max_body} bytes"
            return _Outcome(status, headers, b"", TransportError.PROTOCOL, detail)
        if waited_out is not None and not is_refusal_page(status, headers, html, self._markers):
            detail = (
                f"selector {self._selector!r} never appeared: {_first_line(waited_out.message)}"
            )
            return _Outcome(status, headers, body, TransportError.TIMEOUT, detail, keep_body=True)
        return _Outcome(status, headers, body, None, None)


@dataclass(frozen=True, slots=True)
class _Outcome:
    status: int | None
    headers: dict[str, str]
    body: bytes
    error: TransportError | None
    detail: str | None
    keep_body: bool = False  # a page that rendered without the selector is kept for debugging

    @classmethod
    def failed(cls, error: TransportError, detail: str) -> _Outcome:
        return cls(None, {}, b"", error, detail)


def _first_line(message: str) -> str:
    lines = message.strip().splitlines()
    return lines[0][:200] if lines else "playwright error"
