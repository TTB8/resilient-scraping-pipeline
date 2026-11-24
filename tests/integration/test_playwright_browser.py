"""PlaywrightFetcher in a real headless Chromium against the fixture server.

Marked ``browser``: the fast check deselects these, CI runs them. They skip (with the launch
error as the reason) where Chromium cannot start, unless ``REQUIRE_BROWSER_TESTS=1``.
"""

from collections.abc import Callable

import pytest

from fetch_support import make_settings
from fixture_server import FixtureServer, Phase
from scraping_pipeline.core.classify import TransportError, classify
from scraping_pipeline.core.types import ResponseClass
from scraping_pipeline.fetch.playwright_fetcher import PlaywrightFetcher

pytestmark = [pytest.mark.browser, pytest.mark.usefixtures("chromium_available")]

UA = "browser-check/1.0 (+https://example.org/contact)"


async def test_js_rendered_quotes_appear(fixture_server: FixtureServer) -> None:
    settings = make_settings(user_agent=UA, request_timeout_s=10.0)
    async with PlaywrightFetcher(settings, wait_for_selector=".quote") as fetcher:
        result = await fetcher.fetch(fixture_server.url("/_js/quotes.html"))
    assert result.error is None
    assert result.status == 200
    assert result.fetcher == "playwright"
    html = result.text()
    assert html.count('class="quote"') == 3
    assert 'class="author">Fixture Author<' in html
    assert 'class="next"' in html
    assert {hit.user_agent for hit in fixture_server.log()} == {UA}


async def test_missing_selector_is_a_timeout_with_the_200_and_body_kept(
    fixture_server: FixtureServer,
) -> None:
    # The selector wait gets what is left of the deadline (R2-02), so the navigation's 200 and the
    # rendered page survive; only the error says the content never appeared.
    settings = make_settings(user_agent=UA, request_timeout_s=3.0)
    async with PlaywrightFetcher(settings, wait_for_selector=".never-there") as fetcher:
        result = await fetcher.fetch(fixture_server.url("/_js/quotes.html"))
    assert result.error is TransportError.TIMEOUT
    assert result.status == 200
    assert b'class="quote"' in result.body  # rendered by the page's script, kept for debugging
    assert result.final_url == fixture_server.url("/_js/quotes.html")


async def test_challenge_page_is_returned_for_classification(fixture_server: FixtureServer) -> None:
    fixture_server.scenario(Phase.challenge())
    settings = make_settings(user_agent=UA, request_timeout_s=2.0)
    async with PlaywrightFetcher(settings, wait_for_selector=".quote") as fetcher:
        result = await fetcher.fetch(fixture_server.url("/"))
    assert result.error is None  # a refusal, not a timeout to retry
    assert result.status == 200
    assert result.body
    verdict = classify(result.status, result.headers, result.text(), result.error)
    assert verdict.response_class is ResponseClass.CHALLENGE


async def test_block_status_skips_the_selector_wait(fixture_server: FixtureServer) -> None:
    fixture_server.scenario(Phase.block())
    settings = make_settings(user_agent=UA, request_timeout_s=5.0)
    async with PlaywrightFetcher(settings, wait_for_selector=".quote") as fetcher:
        result = await fetcher.fetch(fixture_server.url("/"))
    assert result.status == 403
    assert result.error is None


async def test_connection_refused_is_mapped() -> None:
    import socket  # noqa: PLC0415

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    async with PlaywrightFetcher(make_settings(user_agent=UA, request_timeout_s=5.0)) as fetcher:
        result = await fetcher.fetch(f"http://127.0.0.1:{port}/")
    assert result.error is TransportError.CONNECT
    assert result.status is None


async def test_redirect_is_reported_not_followed(fixture_server: FixtureServer) -> None:
    fixture_server.redirect("/old", "/_js/quotes.html", status=301)
    async with PlaywrightFetcher(make_settings(user_agent=UA, request_timeout_s=5.0)) as fetcher:
        result = await fetcher.fetch(fixture_server.url("/old"))
    assert result.status == 301
    assert result.headers["location"] == "/_js/quotes.html"
    assert result.error is None
    assert result.final_url == fixture_server.url("/old")
    assert fixture_server.hits("/old") == 1
    assert fixture_server.hits("/_js/quotes.html") == 0  # the scheduler decides about the hop


async def test_redirect_to_another_origin_never_reaches_it(
    fixture_server: FixtureServer, make_fixture_server: Callable[..., FixtureServer]
) -> None:
    other = make_fixture_server("quotes")  # another port on 127.0.0.1: another origin
    target = other.url("/_js/quotes.html")
    fixture_server.redirect("/old", target)
    async with PlaywrightFetcher(make_settings(user_agent=UA, request_timeout_s=5.0)) as fetcher:
        result = await fetcher.fetch(fixture_server.url("/old"))
    assert result.status == 302
    assert result.headers["location"] == target
    assert result.error is None
    assert result.final_url == fixture_server.url("/old")
    assert other.hits() == 0  # Chromium did not follow it on its own


async def test_only_the_document_and_same_origin_scripts_are_requested(
    fixture_server: FixtureServer,
) -> None:
    page = (
        b"<html><head><link rel=stylesheet href=/style.css>"
        b"<script src=/app.js></script></head><body><img src=/pic.png>"
        b"<img src=http://localhost:9/off.png><div class=quote>q</div></body></html>"
    )
    fixture_server.route("/page", Phase(200, page, {"Content-Type": "text/html"}))
    fixture_server.route("/app.js", Phase(200, b"void 0;", {"Content-Type": "text/javascript"}))
    async with PlaywrightFetcher(make_settings(user_agent=UA), ".quote") as fetcher:
        result = await fetcher.fetch(fixture_server.url("/page"))
    assert result.status == 200
    assert set(fixture_server.hit_counts()) == {"/page", "/app.js"}  # no css, no images
