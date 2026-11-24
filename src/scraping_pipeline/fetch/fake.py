"""A scripted fetcher for tests: every URL answers from a script, nothing touches a socket.

Each URL gets a sequence of :data:`Step` values. Every fetch consumes the next one, and the last one
repeats forever, so ``fake.script(url, rate_limited("1"), ok("<html>"))`` reads as "429 once, then
fine". URLs without a script answer with ``default`` (a 404 unless set otherwise, which makes an
unscripted ``/robots.txt`` mean "everything allowed", as RFC 9309 says).

Delays go through the injected ``sleep`` and timestamps through the injected ``clock``, so in a
virtual-time test a "slow" response costs no real time and every call carries a deterministic
timestamp. Every call is recorded (:attr:`FakeFetcher.calls`) with its order, start and end time,
and the fetcher tracks how many calls were in flight at once, so tests can assert politeness
properties directly: "no URL was fetched twice", "never more than two in flight", "never faster than
the rate".

Ready-made steps cover the failure modes the scheduler must handle: :func:`ok`, :func:`not_found`,
:func:`server_error`, :func:`rate_limited` (Retry-After as seconds or an HTTP-date),
:func:`block_page`, :func:`challenge_page`, :func:`timeout`, :func:`connect_error`, and
:func:`slow` to delay any of them.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import Self

from scraping_pipeline.core.classify import TransportError
from scraping_pipeline.fetch.protocol import FetchResult, check_http_url

BLOCK_PAGE = (
    "<html><head><title>Access denied</title></head>"
    "<body><h1>Access denied</h1><p>You have been blocked.</p></body></html>"
)
CHALLENGE_PAGE = (
    "<html><head><title>Just a moment</title></head>"
    "<body><h1>Checking your browser</h1><p>Verify you are human to continue.</p></body></html>"
)


@dataclass(frozen=True, slots=True)
class Respond:
    """An HTTP answer. ``final_url`` simulates a followed redirect."""

    status: int
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)
    delay_s: float = 0.0
    final_url: str | None = None


@dataclass(frozen=True, slots=True)
class Fail:
    """No usable answer: a transport error after ``delay_s``."""

    error: TransportError
    delay_s: float = 0.0
    detail: str = "scripted failure"


Step = Respond | Fail


def _bytes(body: bytes | str) -> bytes:
    return body.encode("utf-8") if isinstance(body, str) else body


def ok(body: bytes | str = "<html><body>ok</body></html>", **headers: str) -> Respond:
    """200 with ``body`` (``text/html; charset=utf-8`` unless a ``content_type`` is given)."""
    merged = {"content-type": "text/html; charset=utf-8"}
    merged.update({k.replace("_", "-"): v for k, v in headers.items()})
    return Respond(200, _bytes(body), merged)


def not_found() -> Respond:
    return Respond(404, b"<html><body>Not found</body></html>")


def server_error(status: int = 500) -> Respond:
    return Respond(status, b"<html><body>Server error</body></html>")


def rate_limited(retry_after: str | None = None, status: int = 429) -> Respond:
    """429 (or 503) with an optional ``Retry-After``: delta-seconds (``"2"``) or an HTTP-date."""
    headers = {} if retry_after is None else {"retry-after": retry_after}
    return Respond(status, b"<html><body>Too many requests</body></html>", headers)


def block_page(status: int = 403) -> Respond:
    return Respond(status, _bytes(BLOCK_PAGE), {"content-type": "text/html"})


def challenge_page(status: int = 200) -> Respond:
    return Respond(status, _bytes(CHALLENGE_PAGE), {"content-type": "text/html"})


def timeout(delay_s: float = 0.0) -> Fail:
    return Fail(TransportError.TIMEOUT, delay_s, "scripted timeout")


def connect_error() -> Fail:
    return Fail(TransportError.CONNECT, 0.0, "scripted connection refused")


def slow(step: Step, delay_s: float) -> Step:
    """``step``, answered after ``delay_s`` seconds of (injected) sleep."""
    return replace(step, delay_s=delay_s)


@dataclass(slots=True)
class FakeCall:
    """One recorded fetch. ``ended_at`` stays ``None`` while in flight (or if it was cancelled)."""

    order: int
    url: str
    started_at: float
    ended_at: float | None = None


async def _no_sleep(_: float) -> None:
    await asyncio.sleep(0)


def _zero() -> float:
    return 0.0


class FakeFetcher:
    """A :class:`~.protocol.Fetcher` that answers from scripts. See the module docstring."""

    def __init__(
        self,
        scripts: Mapping[str, Step | list[Step]] | None = None,
        *,
        default: Step | None = None,
        sleep: Callable[[float], Awaitable[None]] = _no_sleep,
        clock: Callable[[], float] = _zero,
    ) -> None:
        self._scripts: dict[str, list[Step]] = {}
        self._cursor: Counter[str] = Counter()
        self._default: Step = default if default is not None else not_found()
        self._sleep = sleep
        self._clock = clock
        self.calls: list[FakeCall] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.closed = False
        for url, steps in (scripts or {}).items():
            self.script(url, *(steps if isinstance(steps, list) else [steps]))

    def script(self, url: str, *steps: Step) -> None:
        """Answer ``url`` with ``steps`` in order; the last one repeats. Replaces any earlier script."""
        if not steps:
            raise ValueError("a script needs at least one step")
        self._scripts[url] = list(steps)
        self._cursor[url] = 0

    def urls_called(self) -> list[str]:
        return [call.url for call in self.calls]

    def call_counts(self) -> Counter[str]:
        return Counter(call.url for call in self.calls)

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
        self.closed = True

    def _next_step(self, url: str) -> Step:
        steps = self._scripts.get(url)
        if steps is None:
            return self._default
        index = min(self._cursor[url], len(steps) - 1)
        self._cursor[url] += 1
        return steps[index]

    async def fetch(self, url: str) -> FetchResult:
        check_http_url(url)
        if self.closed:
            raise RuntimeError("FakeFetcher used after aclose()")
        call = FakeCall(order=len(self.calls), url=url, started_at=self._clock())
        self.calls.append(call)
        step = self._next_step(url)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if step.delay_s > 0:
                await self._sleep(step.delay_s)
        finally:
            self.in_flight -= 1
        call.ended_at = self._clock()
        elapsed = call.ended_at - call.started_at
        if isinstance(step, Fail):
            return FetchResult(
                requested_url=url,
                final_url=url,
                status=None,
                headers={},
                body=b"",
                error=step.error,
                elapsed_s=elapsed,
                fetcher="fake",
                error_detail=step.detail,
            )
        return FetchResult(
            requested_url=url,
            final_url=step.final_url or url,
            status=step.status,
            headers=step.headers,
            body=step.body,
            error=None,
            elapsed_s=elapsed,
            fetcher="fake",
        )
