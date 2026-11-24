"""What every fetcher promises: one URL in, one :class:`FetchResult` out, and no exceptions for failures.

Why fetchers never raise for HTTP or transport failures
--------------------------------------------------------

A crawl is mostly failure handling. If a timeout, a refused connection or a 503 surfaced as an
exception, every caller would need its own ``try``/``except`` ladder, and each ladder would be a
chance to swallow the wrong thing or to treat one failure differently from another. Instead, a
failure is *data*: the fetcher records what happened (``status`` for an HTTP answer, ``error`` for
no usable answer) and hands it to :func:`scraping_pipeline.core.classify.classify`, the single place
where outcomes become policy. Exceptions are kept for what they are good at: programming errors
(a non-http(s) URL, a fetcher used after ``aclose()``), which should stop the run loudly.

Both ``status`` and ``error`` may be set at once, for instance when a server answered 200 but the
body was larger than the fetcher is willing to hold. The transport error wins in ``classify``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType, TracebackType
from typing import Literal, Protocol, Self

from scraping_pipeline.core.classify import TransportError

FetcherName = Literal["httpx", "playwright", "fake"]

#: Default ceiling on a response body. Practice-site pages are tens of KiB; 5 MiB leaves room for
#: any real page while keeping a misbehaving server from filling memory.
DEFAULT_MAX_BODY_BYTES = 5 * 1024 * 1024

_CHARSET = re.compile(r"charset\s*=\s*[\"']?([\w.:-]+)", re.I)


def _freeze_headers(headers: Mapping[str, str]) -> Mapping[str, str]:
    lowered: dict[str, str] = {}
    for key, value in headers.items():
        name = key.lower()
        lowered[name] = f"{lowered[name]}, {value}" if name in lowered else value
    return MappingProxyType(lowered)


@dataclass(frozen=True, slots=True)
class FetchResult:
    """The outcome of one fetch attempt, as plain data.

    ``headers`` keys are lower-cased (repeated headers are joined with ``", "``) and the mapping is
    read-only. Fetchers never follow redirects, so a fetcher returns ``final_url == requested_url``
    and a 3xx with its ``Location``; the scheduler follows same-origin hops itself and reports the
    attempt with the entry's URL as ``requested_url`` and the last hop as ``final_url``.
    ``error_detail`` is a short human description of a transport error, for logs and the run
    report; it is never used for decisions.
    """

    requested_url: str
    final_url: str
    status: int | None
    headers: Mapping[str, str]
    body: bytes
    error: TransportError | None
    elapsed_s: float
    fetcher: FetcherName
    error_detail: str | None = field(default=None)

    def __post_init__(self) -> None:
        if self.status is None and self.error is None:
            raise ValueError("a FetchResult needs a status, a transport error, or both")
        object.__setattr__(self, "headers", _freeze_headers(self.headers))

    @property
    def charset(self) -> str:
        """The body's charset from ``Content-Type``, or ``utf-8`` when absent or unknown."""
        match = _CHARSET.search(self.headers.get("content-type", ""))
        if match:
            name = match.group(1)
            try:
                "".encode(name)
            except LookupError:
                return "utf-8"
            return name
        return "utf-8"

    def text(self) -> str:
        """The body decoded with :attr:`charset`; undecodable bytes become U+FFFD, never an error."""
        return self.body.decode(self.charset, errors="replace")


class Fetcher(Protocol):
    """Fetch one URL. Implementations are async context managers that own their connections.

    ``fetch`` must return a :class:`FetchResult` for every HTTP answer and every transport failure,
    and may raise only for programming errors. It must be safe to call concurrently. ``aclose``
    releases connections or the browser and is idempotent.
    """

    async def fetch(self, url: str) -> FetchResult: ...

    async def aclose(self) -> None: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


def check_http_url(url: str) -> None:
    """Raise ``ValueError`` unless ``url`` is an absolute http(s) URL (a caller bug, not a failure)."""
    scheme, sep, rest = url.partition("://")
    if not sep or scheme.lower() not in ("http", "https") or not rest or rest.startswith("/"):
        raise ValueError(f"fetchers take absolute http(s) URLs, got {url!r}")
