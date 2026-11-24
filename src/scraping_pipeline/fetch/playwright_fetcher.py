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

* the **top-level document** is allowed once, for the URL asked for;
* **same-origin scripts** are allowed: the page needs them to render;
* everything else is aborted: images, stylesheets, fonts, media, XHR/fetch, frames, any request to
  another origin, and any later navigation the page's script attempts.

Redirects are never followed, by the fetcher or by Chromium. Chromium follows a 3xx inside its own
network stack and never calls a route handler for the redirected request (Playwright continues it
automatically), so ``route.continue_()`` cannot confine a redirect. Instead, an allowed request is
sent by the handler itself, as exactly one request: ``route.fetch(max_redirects=0)``. A 2xx/4xx/5xx
answer is handed to the page with ``route.fulfill(response=...)``. A 3xx is not: the handler records
it and aborts the request, and the fetcher returns the 3xx (status and ``Location``) like
:class:`~scraping_pipeline.fetch.httpx_fetcher.HttpxFetcher` does, so the scheduler applies the same
rules to the hop (same origin only, robots.txt, a token per hop). A same-origin script that
redirects is aborted. ``final_url`` is the URL of the document the browser actually rendered
(``response.url``), never assumed, so the scheduler's ``final_url`` check still guards the result
if a browser ever got past the handler.

Size: the document fetched by ``route.fetch`` is capped at ``max_body_bytes``, the same cap the
httpx fetcher applies and with the same result (a ``protocol`` error, ``body larger than N bytes``,
no body kept): a declared ``Content-Length`` over the cap is refused without reading the body, and
otherwise the decoded body's length is checked before it is handed to Chromium. Playwright has no
streaming route body, so the driver has already downloaded the whole response by then; a chunked
answer with no length is only refused after it is in the driver's memory. The rendered DOM is
capped again after rendering.

Budget: the document is the request that took a token. The same-origin scripts it pulls in are not
throttled separately; they are bounded by the page itself (a handful per page), which is the
documented exception to "one token per request". They are *not* cached: Playwright disables the
HTTP cache while routing is enabled, so every page downloads its scripts again.

Lifecycle: the browser is launched once, lazily on the first ``fetch`` (or on ``__aenter__``), with
one browser context for the run; each fetch opens and closes its own page. Failing to start the
driver, launch the browser or open its context raises :class:`BrowserUnavailableError`: it means
the environment is broken (Chromium not installed, or a system library it or the driver needs is
missing), which is not a fact about the site and must not be recorded as one.

Waiting for content: navigation and the selector wait share one deadline of ``request_timeout_s``,
measured from the start of the fetch. After navigation, when ``wait_for_selector`` is set and the
response is 2xx, the fetcher waits for that selector (for instance ``.quote``) for what is left of
the deadline, minus a small margin to read the page. If the selector never appears (or no time is
left to wait), the page did not render what we came for. That is reported as a ``timeout`` transport
error with the 200 status and the rendered body kept, *unless* the rendered page is a block or
challenge page: then the result is returned without an error, so classification sees the refusal
and the domain is stopped instead of being retried. That check reuses ``core.classify`` with the
same markers as the scheduler (pass ``markers``). An outer ``asyncio.timeout`` one second past the
deadline is only a safety net for a driver that stops answering.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import TracebackType
from typing import Self
from urllib.parse import urlsplit

from playwright.async_api import (
    APIResponse,
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

# Chromium's net error names, from the message of a failed navigation or subresource.
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
    # The document is sent by the driver (``route.fetch``), so its failures carry Node's names.
    ("enotfound", TransportError.DNS),
    ("eai_again", TransportError.DNS),
    ("etimedout", TransportError.TIMEOUT),
    ("econnrefused", TransportError.CONNECT),
    ("ehostunreach", TransportError.CONNECT),
    ("enetunreach", TransportError.CONNECT),
    ("econnreset", TransportError.RESET),
    ("socket hang up", TransportError.RESET),
    ("eproto", TransportError.TLS),
    ("certificate", TransportError.TLS),
)

_REFUSALS = frozenset({ResponseClass.BLOCKED, ResponseClass.CHALLENGE})
_URL = re.compile(r"\S+://\S*")


class BrowserUnavailableError(RuntimeError):
    """The browser cannot be used here: the driver did not start, or Chromium did not launch.

    An environment fact (not installed, a missing system library), never a fact about a site. The
    CLI maps it to exit 2 with the fix.
    """


def map_playwright_error(message: str) -> TransportError:
    """The :class:`TransportError` for a Playwright navigation error message.

    Only the error itself is read: its first line, with any URL removed. Playwright appends a
    ``Call log:`` with the request line and headers, and an error names the URL it failed at, so a
    URL such as ``/tag/timeout/`` must not make an unknown error look like a timeout.
    Unknown messages map to ``protocol``: the browser got *something* it could not use.
    """
    lines = message.strip().splitlines()
    lowered = _URL.sub(" ", lines[0] if lines else "").lower()
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

    ALLOW = "allow"  # sent by the handler as exactly one request, redirects not followed
    ABORT = "abort"


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
    document_seen: bool,
) -> RouteVerdict:
    """The confinement policy for one request made while rendering ``target`` (pure, testable).

    There is no redirect case: an allowed request is sent with ``max_redirects=0``, so Chromium never
    sees a 3xx and never makes a redirected request (see the module docstring).
    """
    if is_main_navigation:
        if document_seen or _origin(request_url) != _origin(target):
            return RouteVerdict.ABORT  # a script trying to navigate away, or another origin
        return RouteVerdict.ALLOW
    if resource_type == "script" and _origin(request_url) == _origin(target):
        return RouteVerdict.ALLOW
    return RouteVerdict.ABORT


# Response headers that describe the bytes on the wire, not the body: the fetched body is already
# decoded and complete, so passing these on to Chromium would make it decode or frame it again.
_WIRE_HEADERS = frozenset({"content-encoding", "content-length", "transfer-encoding", "connection"})
_MIN_TIMEOUT_MS = 1.0  # Playwright reads a timeout of 0 as "no timeout"


@dataclass  # no slots: Playwright caches its wrapper on the handler's object with setattr
class _PageGuard:
    """Per-fetch state of the route handler."""

    target: str
    deadline: float  # in ``clock`` seconds
    clock: Callable[[], float]
    max_body: int = DEFAULT_MAX_BODY_BYTES
    document_seen: bool = False
    redirect: tuple[int, dict[str, str]] | None = None  # the document's 3xx, not followed
    oversize: tuple[int, dict[str, str]] | None = None  # the document's answer, over the cap
    document_error: str | None = None  # why the document request itself failed
    blocked: list[str] = field(default_factory=list)

    async def handle(self, route: Route, request: Request) -> None:
        # The page may close under the handler (deadline or cancel): then nothing is left to answer.
        with contextlib.suppress(PlaywrightError):
            await self._handle(route, request)

    async def _handle(self, route: Route, request: Request) -> None:
        main = request.is_navigation_request() and request.frame.parent_frame is None
        verdict = route_verdict(
            target=self.target,
            request_url=request.url,
            resource_type=request.resource_type,
            is_main_navigation=main,
            document_seen=self.document_seen,
        )
        if verdict is RouteVerdict.ABORT:
            self.blocked.append(request.url)
            await route.abort("blockedbyclient")
            return
        if main:
            self.document_seen = True
        timeout_ms = max(_MIN_TIMEOUT_MS, (self.deadline - self.clock()) * 1000)
        try:
            response = await route.fetch(max_redirects=0, timeout=timeout_ms)
        except PlaywrightError as exc:
            if main:
                self.document_error = exc.message
            await route.abort("failed")
            return
        try:
            if 300 <= response.status < 400:  # noqa: PLR2004
                if main:  # reported to the scheduler, which decides about the hop
                    self.redirect = (response.status, dict(response.headers))
                self.blocked.append(request.url)
                await route.abort("blockedbyclient")
                return
            if await self._too_big(response):
                if main:  # reported like the httpx fetcher's oversized body
                    self.oversize = (response.status, dict(response.headers))
                self.blocked.append(request.url)
                await route.abort("failed")
                return
            headers = {k: v for k, v in response.headers.items() if k.lower() not in _WIRE_HEADERS}
            await route.fulfill(response=response, headers=headers)
        finally:
            await response.dispose()  # the driver keeps every fetched body until disposed

    async def _too_big(self, response: APIResponse) -> bool:
        """The httpx fetcher's rule: a declared length over the cap, or a decoded body over it."""
        declared = next(
            (v.strip() for k, v in response.headers.items() if k.lower() == "content-length"), ""
        )
        if declared.isdigit() and int(declared) > self.max_body:
            return True
        return len(await response.body()) > self.max_body


_SELECTOR_MARGIN_S = 0.25  # left of the deadline after the selector wait, to read the page
_SAFETY_NET_S = 1.0  # the outer asyncio deadline, past the real one, for a driver that hangs


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
                try:
                    self._playwright = await async_playwright().start()
                    self._browser = await self._playwright.chromium.launch(**launch_options())  # type: ignore[arg-type]
                    self._context = await self._browser.new_context(user_agent=self._user_agent)
                except Exception as exc:
                    # The driver failing to start raises a bare Exception ("Connection closed while
                    # reading from the driver"), a missing Chromium a PlaywrightError: both are the
                    # environment. Close what did start, then report it as one typed error.
                    with contextlib.suppress(Exception):
                        await self.aclose()
                    self._closed = False  # not closed by the caller: a later call may try again
                    text = str(exc).strip() or type(exc).__name__
                    raise BrowserUnavailableError(f"{type(exc).__name__}: {text}") from exc
            return self._context

    async def fetch(self, url: str) -> FetchResult:
        check_http_url(url)
        context = await self._ensure_context()
        started = self._clock()
        deadline = started + self._deadline_s
        page = await context.new_page()
        guard = _PageGuard(
            target=url, deadline=deadline, clock=self._clock, max_body=self._max_body
        )
        await page.route("**/*", guard.handle)
        try:
            try:
                async with asyncio.timeout(self._deadline_s + _SAFETY_NET_S):
                    outcome = await self._navigate(page, url, guard, deadline)
            except TimeoutError:
                detail = f"page not rendered within {self._deadline_s:g}s"
                outcome = _Outcome.failed(TransportError.TIMEOUT, detail)
        finally:
            await page.close()
        return FetchResult(
            requested_url=url,
            final_url=outcome.final_url or url,
            status=outcome.status,
            headers=outcome.headers,
            body=outcome.body if outcome.error is None or outcome.keep_body else b"",
            error=outcome.error,
            elapsed_s=max(0.0, self._clock() - started),
            fetcher="playwright",
            error_detail=outcome.detail,
        )

    def _left_ms(self, deadline: float) -> float:
        return max(_MIN_TIMEOUT_MS, (deadline - self._clock()) * 1000)

    def _not_navigated(self, guard: _PageGuard, exc: PlaywrightError) -> _Outcome:
        """Why ``goto`` raised: the handler aborted the document on purpose, or it failed."""
        if guard.redirect is not None:  # the site redirected: report it, do not follow
            status, headers = guard.redirect
            return _Outcome(status, headers, b"", None, None)
        if guard.oversize is not None:  # what the httpx fetcher reports for the same answer
            status, headers = guard.oversize
            detail = f"body larger than {self._max_body} bytes"
            return _Outcome(status, headers, b"", TransportError.PROTOCOL, detail)
        message = guard.document_error if guard.document_error is not None else exc.message
        return _Outcome.failed(map_playwright_error(message), _first_line(message))

    async def _navigate(self, page: Page, url: str, guard: _PageGuard, deadline: float) -> _Outcome:
        try:
            response = await page.goto(url, timeout=self._left_ms(deadline), wait_until="load")
        except PlaywrightError as exc:
            return self._not_navigated(guard, exc)
        if response is None:
            return _Outcome.failed(TransportError.PROTOCOL, "navigation produced no response")
        final_url = response.url  # the document the browser rendered, whatever was asked for
        status = response.status
        headers = await response.all_headers()
        not_seen: str | None = None
        if self._selector is not None and 200 <= status < 300:  # noqa: PLR2004
            budget_s = deadline - self._clock() - _SELECTOR_MARGIN_S
            if budget_s <= 0:
                not_seen = "no time left in the request deadline to wait for it"
            else:
                try:
                    await page.wait_for_selector(self._selector, timeout=budget_s * 1000)
                except PlaywrightTimeoutError as exc:
                    not_seen = _first_line(exc.message)
        html = await page.content()
        body = html.encode("utf-8")
        # The DOM is re-serialised by the browser, always as UTF-8.
        headers["content-type"] = "text/html; charset=utf-8"
        if len(body) > self._max_body:
            detail = f"rendered page larger than {self._max_body} bytes"
            return _Outcome(status, headers, b"", TransportError.PROTOCOL, detail, final_url)
        if not_seen is not None and not is_refusal_page(status, headers, html, self._markers):
            detail = f"selector {self._selector!r} never appeared: {not_seen}"
            return _Outcome(
                status, headers, body, TransportError.TIMEOUT, detail, final_url, keep_body=True
            )
        return _Outcome(status, headers, body, None, None, final_url)


@dataclass(frozen=True, slots=True)
class _Outcome:
    status: int | None
    headers: dict[str, str]
    body: bytes
    error: TransportError | None
    detail: str | None
    final_url: str | None = None  # the rendered document's URL; None means the requested one
    keep_body: bool = False  # a page that rendered without the selector is kept for debugging

    @classmethod
    def failed(cls, error: TransportError, detail: str) -> _Outcome:
        return cls(None, {}, b"", error, detail)


def _first_line(message: str) -> str:
    lines = message.strip().splitlines()
    return lines[0][:200] if lines else "playwright error"
