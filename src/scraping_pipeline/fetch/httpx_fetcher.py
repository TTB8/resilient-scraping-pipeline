"""The default fetcher: one ``httpx.AsyncClient``, plain HTTP/1.1, an honest User-Agent.

Choices, and why:

* **One client for the whole run.** Connection pooling means fewer TCP handshakes against the
  site, which is both faster and politer. Cookies live in that client's jar and die with it: nothing
  is persisted between runs, and nothing is imported from a browser profile.
* **``trust_env=False``.** By default httpx reads ``HTTP(S)_PROXY``, ``ALL_PROXY``, ``NO_PROXY``,
  ``SSL_CERT_FILE`` and ``.netrc`` from the environment. A pipeline whose network path silently
  changes with the shell it runs in is neither reproducible nor honest about where requests come
  from, and a proxy picked up from the environment is exactly the kind of "rotation" this project
  refuses to do (ADR 0005). Requests go straight out, always.
* **Redirects are followed here, with a small cap** (5, the number RFC 9309 asks robots.txt fetchers
  to follow). ``core.classify`` treats a raw 3xx as a bug on our side, so the fetcher must resolve
  them; the cap stops a redirect loop from becoming an unbounded request stream.
* **The body is capped** (5 MiB by default) and read as a stream, so an oversized or endless
  response cannot exhaust memory. The cap applies to the *decoded* body, which also bounds
  compression bombs. An oversize body is reported as a ``protocol`` transport error with the status
  kept, so ``classify`` treats it as a transient network fault.
* **HTTP/1.1 only.** It is what the practice sites speak, it needs no extra dependency, and it keeps
  the request shape boring and predictable.

Every httpx exception that means "the network or the peer failed" becomes a
:class:`~scraping_pipeline.core.classify.TransportError`; see :func:`map_httpx_error`.
"""

from __future__ import annotations

import socket
import ssl
import time
from collections.abc import Callable
from types import TracebackType
from typing import Self

import httpx

from scraping_pipeline.config import Settings
from scraping_pipeline.core.classify import TransportError
from scraping_pipeline.fetch.protocol import DEFAULT_MAX_BODY_BYTES, FetchResult, check_http_url

MAX_REDIRECTS = 5

_DNS_HINTS = (
    "name or service not known",
    "nodename nor servname",
    "temporary failure in name resolution",
    "getaddrinfo failed",
    "no address associated with hostname",
)


def _causes(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def _connect_error_kind(exc: httpx.ConnectError) -> TransportError:
    chain = _causes(exc)
    if any(isinstance(c, ssl.SSLError) for c in chain):
        return TransportError.TLS
    if any(isinstance(c, socket.gaierror) for c in chain) or any(
        hint in str(c).lower() for c in chain for hint in _DNS_HINTS
    ):
        return TransportError.DNS
    return TransportError.CONNECT


def map_httpx_error(exc: httpx.HTTPError) -> TransportError | None:
    """The :class:`TransportError` for an httpx exception, or ``None`` if it is not a network fault.

    DNS and TLS failures surface in httpx as a plain ``ConnectError``; they are told apart by the
    exception chain (``socket.gaierror``, ``ssl.SSLError``) and, for DNS, by the resolver's message.
    The order of the checks matters: ``ConnectTimeout`` is a timeout, and ``ConnectError`` is a
    ``NetworkError``.
    """
    if isinstance(exc, httpx.TimeoutException):
        return TransportError.TIMEOUT
    if isinstance(exc, httpx.ConnectError):
        return _connect_error_kind(exc)
    if isinstance(exc, httpx.NetworkError):  # ReadError, WriteError, CloseError
        return TransportError.RESET
    if isinstance(exc, httpx.TransportError | httpx.TooManyRedirects | httpx.DecodingError):
        # ProtocolError (remote and local), UnsupportedProtocol after a redirect, ProxyError (cannot
        # happen with trust_env=False), too many redirects, a corrupt compressed body.
        return TransportError.PROTOCOL
    return None


class HttpxFetcher:
    """Fetch pages with httpx. Use as ``async with HttpxFetcher(settings) as fetcher: ...``.

    ``transport`` exists for tests (``httpx.MockTransport``); production code leaves it ``None``.
    ``clock`` measures ``elapsed_s`` and defaults to ``time.perf_counter``.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
        max_redirects: int = MAX_REDIRECTS,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        if max_body_bytes < 1:
            raise ValueError(f"max_body_bytes must be >= 1, got {max_body_bytes!r}")
        self._max_body = max_body_bytes
        self._clock = clock
        self._client = httpx.AsyncClient(
            headers={"User-Agent": settings.user_agent},
            timeout=httpx.Timeout(settings.request_timeout_s),
            follow_redirects=True,
            max_redirects=max_redirects,
            http1=True,
            http2=False,
            trust_env=False,
            transport=transport,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch(self, url: str) -> FetchResult:
        check_http_url(url)
        if self._client.is_closed:
            raise RuntimeError("HttpxFetcher used after aclose()")
        started = self._clock()
        status: int | None = None
        headers: dict[str, str] = {}
        final_url = url
        body = b""
        error: TransportError | None = None
        detail: str | None = None
        try:
            async with self._client.stream("GET", url) as response:
                status = response.status_code
                headers = dict(response.headers.items())
                final_url = str(response.url)
                body, oversize = await self._read_capped(response)
        except httpx.HTTPError as exc:
            error = map_httpx_error(exc)
            if error is None:  # e.g. HTTPStatusError: only raise_for_status() makes one, a bug here
                raise
            detail = _detail(exc)
        else:
            if oversize:
                error, detail = TransportError.PROTOCOL, f"body larger than {self._max_body} bytes"
        return FetchResult(
            requested_url=url,
            final_url=final_url,
            status=status,
            headers=headers,
            body=body if error is None else b"",
            error=error,
            elapsed_s=max(0.0, self._clock() - started),
            fetcher="httpx",
            error_detail=detail,
        )

    async def _read_capped(self, response: httpx.Response) -> tuple[bytes, bool]:
        declared = response.headers.get("content-length", "")
        if declared.isdigit() and int(declared) > self._max_body:
            return b"", True
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > self._max_body:
                return b"", True
            chunks.append(chunk)
        return b"".join(chunks), False


def _detail(exc: BaseException) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__
