import pytest

from fetch_support import VirtualTime
from scraping_pipeline.core.classify import TransportError
from scraping_pipeline.fetch.fake import (
    FakeFetcher,
    Respond,
    block_page,
    challenge_page,
    connect_error,
    ok,
    rate_limited,
    slow,
    timeout,
)
from scraping_pipeline.fetch.protocol import FetchResult, check_http_url


def result(**overrides: object) -> FetchResult:
    fields: dict[str, object] = {
        "requested_url": "https://a.test/",
        "final_url": "https://a.test/",
        "status": 200,
        "headers": {},
        "body": b"",
        "error": None,
        "elapsed_s": 0.0,
        "fetcher": "fake",
    }
    fields.update(overrides)
    return FetchResult(**fields)  # type: ignore[arg-type]


def test_result_needs_status_or_error() -> None:
    with pytest.raises(ValueError, match="status, a transport error"):
        result(status=None)


def test_headers_are_lowercased_joined_and_read_only() -> None:
    r = result(headers={"Content-Type": "text/html", "X-A": "1", "x-a": "2"})
    assert dict(r.headers) == {"content-type": "text/html", "x-a": "1, 2"}
    with pytest.raises(TypeError):
        r.headers["new"] = "x"  # type: ignore[index]


@pytest.mark.parametrize(
    ("content_type", "charset"),
    [
        ("text/html; charset=ISO-8859-1", "ISO-8859-1"),
        ('text/html; charset="utf-8"', "utf-8"),
        ("text/html; charset=no-such-codec", "utf-8"),
        ("text/html", "utf-8"),
    ],
)
def test_charset_from_content_type(content_type: str, charset: str) -> None:
    assert result(headers={"content-type": content_type}).charset == charset


def test_text_never_raises_on_bad_bytes() -> None:
    assert result(body=b"caf\xe9").text() == "caf�"
    latin = result(body=b"caf\xe9", headers={"content-type": "text/html; charset=latin-1"})
    assert latin.text() == "café"


@pytest.mark.parametrize("url", ["ftp://a.test/", "a.test/x", "https://", "https:///path", "/x"])
def test_check_http_url_rejects_non_http(url: str) -> None:
    with pytest.raises(ValueError, match="absolute http"):
        check_http_url(url)


async def test_fake_sequences_repeat_last_step_and_record_calls() -> None:
    fake = FakeFetcher({"https://a.test/x": [rate_limited("2"), ok("hi")]})
    first = await fake.fetch("https://a.test/x")
    second = await fake.fetch("https://a.test/x")
    third = await fake.fetch("https://a.test/x")
    unscripted = await fake.fetch("https://a.test/robots.txt")
    assert (first.status, first.headers["retry-after"]) == (429, "2")
    assert second.body == third.body == b"hi"
    assert unscripted.status == 404
    assert fake.call_counts()["https://a.test/x"] == 3
    assert [c.order for c in fake.calls] == [0, 1, 2, 3]
    assert fake.urls_called()[-1] == "https://a.test/robots.txt"


async def test_fake_failures_and_pages() -> None:
    fake = FakeFetcher(
        {
            "https://a.test/t": timeout(),
            "https://a.test/c": connect_error(),
            "https://a.test/b": block_page(),
            "https://a.test/ch": challenge_page(),
            "https://a.test/r": Respond(200, b"x", final_url="https://a.test/final"),
        }
    )
    assert (await fake.fetch("https://a.test/t")).error is TransportError.TIMEOUT
    assert (await fake.fetch("https://a.test/c")).error is TransportError.CONNECT
    assert (await fake.fetch("https://a.test/b")).status == 403
    assert b"Checking your browser" in (await fake.fetch("https://a.test/ch")).body
    assert (await fake.fetch("https://a.test/r")).final_url == "https://a.test/final"


async def test_fake_slow_uses_injected_sleep_and_tracks_in_flight() -> None:
    vt = VirtualTime()
    fake = FakeFetcher({"https://a.test/s": slow(ok(), 3.0)}, sleep=vt.sleep, clock=vt.clock)

    async def two() -> None:
        import asyncio  # noqa: PLC0415

        await asyncio.gather(fake.fetch("https://a.test/s"), fake.fetch("https://a.test/s"))

    await vt.run(two())
    assert vt.now == 3.0
    assert fake.max_in_flight == 2
    assert fake.in_flight == 0
    assert all(c.ended_at == 3.0 for c in fake.calls)


async def test_fake_lifecycle_and_programming_errors() -> None:
    async with FakeFetcher() as fake:
        with pytest.raises(ValueError, match="at least one step"):
            fake.script("https://a.test/")
    assert fake.closed
    with pytest.raises(RuntimeError, match="after aclose"):
        await fake.fetch("https://a.test/")


async def test_fake_default_sleep_costs_no_real_time() -> None:
    fake = FakeFetcher({"https://a.test/s": slow(ok(), 3600.0)})
    result = await fake.fetch("https://a.test/s")
    assert result.status == 200
    assert result.elapsed_s == 0.0
