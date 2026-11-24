"""A fetcher for pages that need JavaScript: plain headless Chromium through Playwright.

Detection, not evasion (ADR 0005). This browser does not pretend to be anything else: it sends the
project's honest User-Agent, keeps Playwright's defaults (``navigator.webdriver`` stays ``true``),
loads no plugin that patches fingerprints, and goes through no proxy. If a site decides it does not
want an automated browser, that is its call; ``core.classify`` sees the block or challenge and the
scheduler stops the domain.

Lifecycle: the browser is launched once, lazily on the first ``fetch`` (or on ``__aenter__``), with
one browser context for the run; each fetch opens and closes its own page. Failing to *launch* the
browser raises: it means the environment is broken (Chromium not installed), which is not a fact
about the site and must not be recorded as one.

Waiting for content: after navigation, when ``wait_for_selector`` is set and the response is 2xx,
the fetcher waits for that selector (for instance ``.quote``). If it never appears, the page did not
render what we came for. That is reported as a ``timeout`` transport error, *unless* the rendered
page is a block or challenge page: then the result is returned without an error, so classification
sees the refusal and the domain is stopped instead of being retried. That check reuses
``core.classify``; this module holds no marker list of its own.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Self

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from scraping_pipeline.config import Settings
from scraping_pipeline.core.classify import TransportError, classify
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


def is_refusal_page(status: int, headers: dict[str, str], html: str) -> bool:
    """True when ``core.classify`` reads the rendered page as a block or a challenge."""
    verdict = classify(status, headers, html, None)
    return verdict.response_class in _REFUSALS


class PlaywrightFetcher:
    """Render pages in headless Chromium. Use as ``async with PlaywrightFetcher(settings) as f:``."""

    def __init__(
        self,
        settings: Settings,
        wait_for_selector: str | None = None,
        *,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        if max_body_bytes < 1:
            raise ValueError(f"max_body_bytes must be >= 1, got {max_body_bytes!r}")
        self._user_agent = settings.user_agent
        self._timeout_ms = settings.request_timeout_s * 1000
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
                self._browser = await self._playwright.chromium.launch(headless=True)
                self._context = await self._browser.new_context(user_agent=self._user_agent)
            return self._context

    async def fetch(self, url: str) -> FetchResult:
        check_http_url(url)
        context = await self._ensure_context()
        started = self._clock()
        page = await context.new_page()
        try:
            outcome = await self._navigate(page, url)
            final_url = page.url or url
        finally:
            await page.close()
        return FetchResult(
            requested_url=url,
            final_url=final_url,
            status=outcome.status,
            headers=outcome.headers,
            body=outcome.body if outcome.error is None or outcome.keep_body else b"",
            error=outcome.error,
            elapsed_s=max(0.0, self._clock() - started),
            fetcher="playwright",
            error_detail=outcome.detail,
        )

    async def _navigate(self, page: Page, url: str) -> _Outcome:
        try:
            response = await page.goto(url, timeout=self._timeout_ms, wait_until="load")
        except PlaywrightError as exc:
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
        if waited_out is not None and not is_refusal_page(status, headers, html):
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
