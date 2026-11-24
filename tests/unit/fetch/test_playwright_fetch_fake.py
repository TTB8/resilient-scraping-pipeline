"""PlaywrightFetcher.fetch / _navigate driven through a fake Page and fake Routes (no Chromium).

The fakes model the two browser facts the fetcher depends on (both read from the installed driver,
Playwright 1.65):

* Chromium follows a 3xx itself and never calls the route handler for the redirected request. So
  :meth:`FakeRoute.continue_` follows redirects on the fake network without asking the handler,
  and only ``route.fetch(max_redirects=0)`` hands a 3xx back (``FakeRoute.fetch`` follows up to
  20 hops by default, as Playwright does);
* ``wait_for_selector`` gives up only after its own ``timeout``, so a selector budget longer than
  what is left of the deadline loses the race against the fetcher's outer deadline (R2-02).

Real-browser behaviour is covered by ``tests/integration/test_playwright_browser.py`` in CI.
"""

# The fakes keep Playwright's method signatures (timeout=..., unused selector/pattern arguments).
# ruff: noqa: ASYNC109, ARG002

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin

import pytest
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from fetch_support import make_settings
from scraping_pipeline.core.classify import TransportError, classify
from scraping_pipeline.core.types import ResponseClass
from scraping_pipeline.fetch.fake import CHALLENGE_PAGE
from scraping_pipeline.fetch.playwright_fetcher import PlaywrightFetcher

TARGET = "https://quotes.example.test/js/"
QUOTES = b"<html><head><title>Quotes</title></head><body><div class=quote>q</div></body></html>"
PLAIN = b"<html><head><title>Quotes</title></head><body><p>nothing rendered yet</p></body></html>"
HTML = {"content-type": "text/html"}

Answer = tuple[int, dict[str, str], bytes] | PlaywrightError


@dataclass
class FakeNet:
    """What the driver's HTTP client (``route.fetch``) and Chromium's network stack can reach."""

    pages: dict[str, Answer] = field(default_factory=dict)
    requested: list[str] = field(default_factory=list)

    def get(self, url: str, max_redirects: int) -> FakeAPIResponse:
        hops = max_redirects
        while True:
            self.requested.append(url)
            answer = self.pages.get(url, (404, {}, b"not found"))
            if isinstance(answer, PlaywrightError):
                raise answer
            status, headers, body = answer
            if 300 <= status < 400 and hops > 0 and "location" in headers:
                url = urljoin(url, headers["location"])
                hops -= 1
                continue
            return FakeAPIResponse(url, status, dict(headers), body)


@dataclass
class FakeAPIResponse:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    disposed: bool = False

    async def dispose(self) -> None:
        self.disposed = True


@dataclass
class FakeFrame:
    parent_frame: None = None


@dataclass
class FakeRequest:
    url: str
    resource_type: str
    navigation: bool
    frame: FakeFrame = field(default_factory=FakeFrame)
    redirected_from: None = None

    def is_navigation_request(self) -> bool:
        return self.navigation


@dataclass
class FakeRoute:
    request: FakeRequest
    net: FakeNet
    action: str | None = None
    response: FakeAPIResponse | None = None
    fulfil_headers: dict[str, str] | None = None
    fetch_timeouts: list[float] = field(default_factory=list)

    async def fetch(self, *, max_redirects: int | None = None, timeout: float | None = None) -> Any:
        self.fetch_timeouts.append(30_000.0 if timeout is None else timeout)
        return self.net.get(self.request.url, 20 if max_redirects is None else max_redirects)

    async def continue_(self) -> None:  # Chromium: redirects are followed, the handler not asked
        self.action = "continue"
        self.response = self.net.get(self.request.url, 20)

    async def fulfill(self, *, response: Any, headers: dict[str, str] | None = None) -> None:
        self.action = "fulfill"
        self.response = response
        self.fulfil_headers = dict(response.headers if headers is None else headers)

    async def abort(self, error_code: str = "failed") -> None:
        self.action = f"abort:{error_code}"


@dataclass
class FakeResponse:
    url: str
    status: int
    headers: dict[str, str]

    async def all_headers(self) -> dict[str, str]:
        return dict(self.headers)


@dataclass
class FakeClock:
    now: float = 100.0

    def __call__(self) -> float:
        return self.now


Handler = Callable[[Any, Any], Awaitable[None]]


@dataclass
class FakePage:
    """One tab. ``goto`` runs the document (and its ``scripts``) through the route handler."""

    net: FakeNet
    clock: FakeClock | None = None
    goto_s: float = 0.0  # time the navigation takes (virtual if ``clock`` is set, else real)
    scripts: tuple[str, ...] = ()
    selector_present: bool = False
    selector_waits_for_real: bool = False
    rendered_url: str | None = None  # the document URL Chromium reports, when not the request's
    handler: Handler | None = None
    routes: list[FakeRoute] = field(default_factory=list)
    goto_timeouts: list[float] = field(default_factory=list)
    selector_timeouts: list[float] = field(default_factory=list)
    html: str = ""
    closed: bool = False

    async def route(self, pattern: str, handler: Handler) -> None:
        self.handler = handler

    async def _send(self, url: str, kind: str, navigation: bool) -> FakeRoute:
        assert self.handler is not None
        route = FakeRoute(FakeRequest(url, kind, navigation), self.net)
        self.routes.append(route)
        await self.handler(route, route.request)
        return route

    async def goto(self, url: str, *, timeout: float, wait_until: str) -> FakeResponse:
        self.goto_timeouts.append(timeout)
        if self.clock is not None:
            self.clock.now += self.goto_s
        elif self.goto_s:
            await asyncio.sleep(self.goto_s)
        route = await self._send(url, "document", navigation=True)
        if route.action is None or route.action.startswith("abort"):
            code = "failed" if route.action is None else route.action.removeprefix("abort:")
            raise PlaywrightError(f"page.goto: net::ERR_{code.upper()} at {url}")
        assert route.response is not None
        self.html = route.response.body.decode("utf-8")
        for script in self.scripts:
            await self._send(script, "script", navigation=False)
        headers = route.fulfil_headers if route.fulfil_headers is not None else {}
        final = self.rendered_url or (route.response.url if route.action == "continue" else url)
        return FakeResponse(final, route.response.status, headers)

    async def wait_for_selector(self, selector: str, *, timeout: float) -> None:
        self.selector_timeouts.append(timeout)
        if self.selector_present:
            return
        if self.selector_waits_for_real:
            await asyncio.sleep(timeout / 1000)
        raise PlaywrightTimeoutError(f"Timeout {timeout:.0f}ms exceeded.")

    async def content(self) -> str:
        return self.html

    async def close(self) -> None:
        self.closed = True


class FakeContext:
    def __init__(self, page: FakePage) -> None:
        self.page = page

    async def new_page(self) -> FakePage:
        return self.page


def fetcher_for(
    page: FakePage,
    *,
    timeout_s: float = 2.0,
    selector: str | None = ".quote",
    monkeypatch: pytest.MonkeyPatch,
) -> PlaywrightFetcher:
    settings = make_settings(request_timeout_s=timeout_s)
    fetcher = (
        PlaywrightFetcher(settings, selector, clock=page.clock)
        if page.clock is not None
        else PlaywrightFetcher(settings, selector)
    )
    context = FakeContext(page)

    async def ensure_context() -> Any:
        return context

    monkeypatch.setattr(fetcher, "_ensure_context", ensure_context)
    return fetcher


# --- R2-02: the selector wait fits inside the deadline ---------------------------------------


async def test_challenge_page_without_the_selector_is_returned_for_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Real time, as in the review's simulation: navigation takes 50 ms and the selector wait gives up
    # only after its own timeout. Before the fix the outer deadline always won: status None, TIMEOUT.
    net = FakeNet({TARGET: (200, HTML, CHALLENGE_PAGE.encode())})
    page = FakePage(net, goto_s=0.05, selector_waits_for_real=True)
    result = await fetcher_for(page, timeout_s=1.0, monkeypatch=monkeypatch).fetch(TARGET)
    assert result.status == 200
    assert result.error is None
    assert result.body == CHALLENGE_PAGE.encode()
    verdict = classify(result.status, result.headers, result.text(), result.error)
    assert verdict.response_class is ResponseClass.CHALLENGE
    assert page.closed


async def test_selector_wait_gets_what_is_left_of_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    net = FakeNet({TARGET: (200, HTML, PLAIN)})
    page = FakePage(net, clock=clock, goto_s=0.5)
    result = await fetcher_for(page, timeout_s=2.0, monkeypatch=monkeypatch).fetch(TARGET)
    assert page.goto_timeouts == [pytest.approx(2000.0)]
    # 2.0 s deadline - 0.5 s navigation - 0.25 s margin to read the page.
    assert page.selector_timeouts == [pytest.approx(1250.0)]
    # A page that rendered without the selector: a timeout, with the 200 and the body kept.
    assert result.error is TransportError.TIMEOUT
    assert result.status == 200
    assert result.body == PLAIN
    assert result.error_detail is not None
    assert ".quote" in result.error_detail


@pytest.mark.parametrize(
    ("body", "error"),
    [(CHALLENGE_PAGE.encode(), None), (PLAIN, TransportError.TIMEOUT)],
)
async def test_no_time_left_means_selector_not_seen(
    body: bytes, error: TransportError | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock()
    page = FakePage(FakeNet({TARGET: (200, HTML, body)}), clock=clock, goto_s=1.9)
    result = await fetcher_for(page, timeout_s=2.0, monkeypatch=monkeypatch).fetch(TARGET)
    assert page.selector_timeouts == []  # no wait at all, but the refusal check still runs
    assert result.status == 200
    assert result.error is error
    assert result.body == body


async def test_selector_present_is_a_clean_result(monkeypatch: pytest.MonkeyPatch) -> None:
    page = FakePage(
        FakeNet({TARGET: (200, HTML, QUOTES)}), clock=FakeClock(), selector_present=True
    )
    result = await fetcher_for(page, monkeypatch=monkeypatch).fetch(TARGET)
    assert (result.status, result.error, result.body) == (200, None, QUOTES)
    assert result.final_url == TARGET


async def test_error_status_skips_the_selector_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    page = FakePage(FakeNet({TARGET: (403, HTML, b"<html>no</html>")}), clock=FakeClock())
    result = await fetcher_for(page, monkeypatch=monkeypatch).fetch(TARGET)
    assert (result.status, result.error) == (403, None)
    assert page.selector_timeouts == []


# --- R2-01: redirects are reported, never followed --------------------------------------------


@pytest.mark.parametrize(
    "location",
    ["http://169.254.169.254/latest/", "https://other.example.test/x", "/_js/quotes.html"],
)
async def test_redirected_document_is_reported_and_its_target_never_requested(
    location: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    hop = urljoin(TARGET, location)
    net = FakeNet(
        {
            TARGET: (302, {"location": location, "content-length": "0"}, b""),
            hop: (200, HTML, QUOTES),
        }
    )
    page = FakePage(net, clock=FakeClock())
    result = await fetcher_for(page, monkeypatch=monkeypatch).fetch(TARGET)
    assert result.status == 302
    assert result.headers["location"] == location
    assert result.error is None
    assert result.body == b""
    assert result.final_url == TARGET  # the scheduler follows (or refuses) the hop itself
    assert net.requested == [TARGET]  # one request; the target was never asked for
    assert page.routes[0].action == "abort:blockedbyclient"
    assert page.routes[0].fetch_timeouts == [pytest.approx(2000.0)]
    assert page.selector_timeouts == []


async def test_rendered_document_url_is_the_final_url(monkeypatch: pytest.MonkeyPatch) -> None:
    # Backstop: whatever the browser ended up rendering is reported, never assumed to be the request,
    # so the scheduler's final_url check can refuse an off-site document.
    elsewhere = "https://other.example.test/landing"
    net = FakeNet({TARGET: (200, HTML, QUOTES)})
    page = FakePage(net, clock=FakeClock(), selector_present=True, rendered_url=elsewhere)
    result = await fetcher_for(page, monkeypatch=monkeypatch).fetch(TARGET)
    assert result.final_url == elsewhere
    assert result.requested_url == TARGET


async def test_same_origin_script_that_redirects_off_site_is_aborted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = "https://quotes.example.test/static/app.js"
    cdn = "https://cdn.other.test/app.js"
    net = FakeNet(
        {
            TARGET: (200, {**HTML, "content-encoding": "gzip", "content-length": "5"}, QUOTES),
            script: (302, {"location": cdn}, b""),
            cdn: (200, {}, b"void 0;"),
        }
    )
    page = FakePage(
        net,
        clock=FakeClock(),
        selector_present=True,
        scripts=(script, "https://cdn.other.test/direct.js"),
    )
    result = await fetcher_for(page, monkeypatch=monkeypatch).fetch(TARGET)
    assert result.status == 200
    assert net.requested == [TARGET, script]  # neither the hop nor the off-origin script
    document, redirected, off_origin = page.routes
    assert document.action == "fulfill"
    # The fetched body is already decoded and whole: no wire headers handed to Chromium.
    assert document.fulfil_headers == HTML
    assert document.response is not None
    assert document.response.disposed
    assert redirected.action == "abort:blockedbyclient"
    assert off_origin.action == "abort:blockedbyclient"


async def test_document_request_failure_is_mapped(monkeypatch: pytest.MonkeyPatch) -> None:
    refused = PlaywrightError("route.fetch: connect ECONNREFUSED 127.0.0.1:9")
    page = FakePage(FakeNet({TARGET: refused}), clock=FakeClock())
    result = await fetcher_for(page, monkeypatch=monkeypatch).fetch(TARGET)
    assert result.error is TransportError.CONNECT
    assert result.status is None
    assert result.error_detail is not None
    assert "ECONNREFUSED" in result.error_detail
    assert page.routes[0].action == "abort:failed"
