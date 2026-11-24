"""HttpxFetcher with httpx.MockTransport: the exception mapping and the body cap, no sockets."""

import asyncio
import socket
import ssl
import time
from collections.abc import AsyncIterator, Callable

import httpx
import pytest

from fetch_support import make_settings
from scraping_pipeline.core.classify import TransportError
from scraping_pipeline.fetch.httpx_fetcher import HttpxFetcher, map_httpx_error

URL = "https://site.test/page"


def fetcher(
    handler: Callable[[httpx.Request], httpx.Response], max_body_bytes: int = 5 * 1024 * 1024
) -> HttpxFetcher:
    transport = httpx.MockTransport(handler)
    return HttpxFetcher(make_settings(), transport=transport, max_body_bytes=max_body_bytes)


def raising(exc: Exception) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc

    return handler


def _chained(exc: httpx.HTTPError, cause: BaseException) -> httpx.HTTPError:
    exc.__cause__ = cause
    return exc


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (httpx.ReadTimeout("slow"), TransportError.TIMEOUT),
        (httpx.ConnectTimeout("slow"), TransportError.TIMEOUT),
        (httpx.PoolTimeout("pool"), TransportError.TIMEOUT),
        (httpx.ConnectError("[Errno 111] Connection refused"), TransportError.CONNECT),
        (
            _chained(httpx.ConnectError("boom"), socket.gaierror(-2, "Name or service not known")),
            TransportError.DNS,
        ),
        (httpx.ConnectError("[Errno -3] Temporary failure in name resolution"), TransportError.DNS),
        (_chained(httpx.ConnectError("hs"), ssl.SSLError(1, "bad cert")), TransportError.TLS),
        (httpx.ReadError("reset by peer"), TransportError.RESET),
        (httpx.WriteError("broken pipe"), TransportError.RESET),
        (httpx.RemoteProtocolError("bad framing"), TransportError.PROTOCOL),
        (httpx.UnsupportedProtocol("ftp"), TransportError.PROTOCOL),
        (httpx.DecodingError("bad gzip"), TransportError.PROTOCOL),
    ],
)
async def test_exceptions_map_to_transport_errors(
    exc: httpx.HTTPError, expected: TransportError
) -> None:
    assert map_httpx_error(exc) is expected
    async with fetcher(raising(exc)) as f:
        result = await f.fetch(URL)
    assert result.error is expected
    assert result.status is None
    assert result.error_detail
    assert result.fetcher == "httpx"


def test_status_error_is_not_a_transport_error() -> None:
    request = httpx.Request("GET", URL)
    exc = httpx.HTTPStatusError("x", request=request, response=httpx.Response(500))
    assert map_httpx_error(exc) is None


async def test_unmapped_httpx_error_is_raised_as_a_bug() -> None:
    request = httpx.Request("GET", URL)
    exc = httpx.HTTPStatusError("x", request=request, response=httpx.Response(500))
    async with fetcher(raising(exc)) as f:
        with pytest.raises(httpx.HTTPStatusError):
            await f.fetch(URL)


async def test_redirects_are_returned_not_followed() -> None:
    hits: list[str] = []

    def redirecting(request: httpx.Request) -> httpx.Response:
        hits.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/"})

    async with fetcher(redirecting) as f:
        result = await f.fetch(URL)
    assert hits == [URL]  # one request: the scheduler decides about the hop
    assert result.status == 302
    assert result.error is None
    assert result.headers["location"] == "http://169.254.169.254/latest/"
    assert result.final_url == URL


async def test_overall_deadline_bounds_a_dripping_body() -> None:
    async def drip() -> AsyncIterator[bytes]:
        for _ in range(100):
            await asyncio.sleep(0.05)  # each chunk well inside any per-read timeout
            yield b"x"

    def slow(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=drip())

    f = HttpxFetcher(make_settings(request_timeout_s=0.3), transport=httpx.MockTransport(slow))
    async with f:
        started = time.monotonic()
        result = await f.fetch(URL)
    assert time.monotonic() - started < 1.0  # 100 chunks would take 5 s
    assert result.error is TransportError.TIMEOUT
    assert result.status == 200  # the headers had arrived
    assert result.body == b""
    assert "within 0.3s" in (result.error_detail or "")


async def test_body_over_cap_is_reported_with_status_kept() -> None:
    def big(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 2048)

    async with fetcher(big, max_body_bytes=1024) as f:
        result = await f.fetch(URL)
    assert result.status == 200
    assert result.error is TransportError.PROTOCOL
    assert result.body == b""
    assert "larger than 1024" in (result.error_detail or "")


async def test_streamed_body_over_cap_without_content_length() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"x" * 600
        yield b"y" * 600

    def stream(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=chunks())  # chunked: no Content-Length to check first

    async with fetcher(stream, max_body_bytes=1000) as f:
        result = await f.fetch(URL)
    assert result.error is TransportError.PROTOCOL


async def test_body_at_cap_is_fine() -> None:
    def exact(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 1000, headers={"X-Thing": "a"})

    async with fetcher(exact, max_body_bytes=1000) as f:
        result = await f.fetch(URL)
    assert result.error is None
    assert len(result.body) == 1000
    assert result.headers["x-thing"] == "a"


async def test_programming_errors_raise() -> None:
    with pytest.raises(ValueError, match="max_body_bytes"):
        HttpxFetcher(make_settings(), max_body_bytes=0)
    f = fetcher(lambda _: httpx.Response(200))
    with pytest.raises(ValueError, match="absolute http"):
        await f.fetch("file:///etc/hosts")
    await f.aclose()
    with pytest.raises(RuntimeError, match="after aclose"):
        await f.fetch(URL)


def test_client_ignores_environment_proxies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:9")
    f = HttpxFetcher(make_settings())
    assert f._client.trust_env is False
    assert f._client.follow_redirects is False
    assert f._client.headers["user-agent"] == make_settings().user_agent
