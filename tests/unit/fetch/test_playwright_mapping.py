"""The browser-free parts of PlaywrightFetcher: error mapping and refusal detection."""

import pytest

from fetch_support import make_settings
from scraping_pipeline.core.classify import TransportError
from scraping_pipeline.fetch.fake import BLOCK_PAGE, CHALLENGE_PAGE
from scraping_pipeline.fetch.playwright_fetcher import (
    PlaywrightFetcher,
    is_refusal_page,
    map_playwright_error,
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
