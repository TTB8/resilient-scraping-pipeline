"""The browser-free parts of PlaywrightFetcher: error mapping and refusal detection."""

import pytest

from fetch_support import make_settings
from scraping_pipeline.core.classify import BlockMarkers, TransportError
from scraping_pipeline.fetch.fake import BLOCK_PAGE, CHALLENGE_PAGE
from scraping_pipeline.fetch.playwright_fetcher import (
    PlaywrightFetcher,
    RouteVerdict,
    is_refusal_page,
    launch_options,
    map_playwright_error,
    route_verdict,
)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("net::ERR_NAME_NOT_RESOLVED at https://x.test/", TransportError.DNS),
        ("net::ERR_CONNECTION_REFUSED at http://127.0.0.1:1/", TransportError.CONNECT),
        ("net::ERR_CONNECTION_RESET", TransportError.RESET),
        ("net::ERR_EMPTY_RESPONSE", TransportError.RESET),
        ("net::ERR_CERT_AUTHORITY_INVALID", TransportError.TLS),
        ("net::ERR_TIMED_OUT", TransportError.TIMEOUT),
        ("Timeout 20000ms exceeded.", TransportError.TIMEOUT),
        ("net::ERR_ABORTED; maybe frame was detached?", TransportError.PROTOCOL),
    ],
)
def test_navigation_errors_map(message: str, expected: TransportError) -> None:
    assert map_playwright_error(message) is expected


def test_refusal_pages_are_recognised_by_core_classify() -> None:
    assert is_refusal_page(200, {}, CHALLENGE_PAGE)
    assert is_refusal_page(403, {}, BLOCK_PAGE)
    assert not is_refusal_page(200, {}, "<html><body><p>just slow to render</p></body></html>")


def test_constructor_validates_without_launching() -> None:
    with pytest.raises(ValueError, match="max_body_bytes"):
        PlaywrightFetcher(make_settings(), max_body_bytes=0)


async def test_use_after_close_is_a_bug() -> None:
    fetcher = PlaywrightFetcher(make_settings(), ".quote")
    await fetcher.aclose()  # nothing launched: closing is still fine
    with pytest.raises(RuntimeError, match="after aclose"):
        await fetcher.fetch("https://x.test/")


# --- confinement policy (R1-07) -------------------------------------------------------------

TARGET = "https://quotes.example.test/js/"


def verdict(url: str, kind: str = "document", **flags: bool) -> RouteVerdict:
    return route_verdict(
        target=TARGET,
        request_url=url,
        resource_type=kind,
        is_main_navigation=flags.get("main", False),
        is_redirect=flags.get("redirect", False),
        document_seen=flags.get("seen", False),
    )


def test_the_page_itself_loads_once() -> None:
    assert verdict(TARGET, main=True) is RouteVerdict.CONTINUE
    assert verdict(TARGET, main=True, seen=True) is RouteVerdict.ABORT  # script navigating again


def test_redirects_are_reported_not_followed() -> None:
    same = "https://quotes.example.test/elsewhere/"
    assert verdict(same, main=True, redirect=True) is RouteVerdict.REPORT_REDIRECT
    assert verdict("http://169.254.169.254/", main=True, redirect=True) is (
        RouteVerdict.REPORT_REDIRECT
    )


def test_navigation_to_another_origin_is_aborted() -> None:
    assert verdict("https://other.test/", main=True) is RouteVerdict.ABORT
    assert verdict("https://quotes.example.test:8443/js/", main=True) is RouteVerdict.ABORT


@pytest.mark.parametrize(
    ("url", "kind", "expected"),
    [
        ("https://quotes.example.test/static/jquery.js", "script", RouteVerdict.CONTINUE),
        ("https://cdn.other.test/lib.js", "script", RouteVerdict.ABORT),
        ("https://quotes.example.test/static/main.css", "stylesheet", RouteVerdict.ABORT),
        ("https://quotes.example.test/logo.png", "image", RouteVerdict.ABORT),
        ("https://quotes.example.test/font.woff2", "font", RouteVerdict.ABORT),
        ("https://quotes.example.test/api/quotes", "fetch", RouteVerdict.ABORT),
        ("https://quotes.example.test/api/quotes", "xhr", RouteVerdict.ABORT),
        ("https://quotes.example.test/frame.html", "document", RouteVerdict.ABORT),  # iframe
        ("https://quotes.example.test/v.mp4", "media", RouteVerdict.ABORT),
    ],
)
def test_subresources(url: str, kind: str, expected: RouteVerdict) -> None:
    assert verdict(url, kind) is expected


def test_launch_options_force_no_proxy() -> None:
    environ = {
        "PATH": "/usr/bin",
        "HTTPS_PROXY": "http://10.0.0.1:3128",
        "http_proxy": "http://10.0.0.1:3128",
        "ALL_PROXY": "socks5://10.0.0.1:1080",
        "NO_PROXY": "localhost",
    }
    options = launch_options(environ)
    assert options["headless"] is True
    assert options["args"] == ["--no-proxy-server"]
    assert options["env"] == {"PATH": "/usr/bin"}


def test_refusal_check_uses_the_given_markers() -> None:
    custom = BlockMarkers(challenge=("please hold the line",), blocked=("go away now",))
    # On a 2xx, markers count only in the <title> / first <h1> of a short page (core.classify, R1-05).
    page = "<html><head><title>Please hold the line</title></head><body><p>...</p></body></html>"
    assert is_refusal_page(200, {}, page, custom)
    assert not is_refusal_page(200, {}, page)  # the defaults do not know this phrase
