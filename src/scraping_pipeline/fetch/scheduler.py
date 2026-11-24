"""The polite crawl loop: drive a :class:`~.protocol.Fetcher` over due frontier entries.

The scheduler is the glue between I/O and the pure rules in ``core/``. It holds per-domain state and
calls the rules; it never re-decides anything they already decide:

* ``core.robots``: robots.txt is fetched **once per domain per run**, through the same fetcher and
  the same politeness budget as every other request, *before* any other request to that domain.
  Once, because the rules must not change halfway through a run (a URL allowed at minute one and
  refused at minute ten makes the output depend on timing), and because re-reading robots.txt for
  every URL would double the load we put on the site. A resumed run fetches it again.
* ``core.ratelimit``: one token bucket per domain, capped by robots ``Crawl-delay``, slowed down
  after every ``rate_limited`` answer and gently sped up after successes (AIMD).
* ``core.circuit``: one breaker per domain. While it is open, nothing is sent to the domain; when it
  trips, the domain is stopped.
* ``core.classify`` / ``core.retry`` / ``core.frontier``: every finished attempt is classified
  (with ``now_epoch``, so HTTP-date ``Retry-After`` values count), decided, and recorded with
  :func:`~scraping_pipeline.core.frontier.apply_decision`.

Why a blocked domain is stopped, not retried
--------------------------------------------

A 403 block page, a challenge interstitial, a ``Retry-After`` longer than we are willing to wait, or
a breaker that keeps tripping all mean the same thing: the site does not want this traffic now.
Retrying is how a polite crawler becomes an impolite one, and getting past the refusal is evasion,
which this project does not do (ADR 0005). So the scheduler stops the domain: no further request is
sent to it, each remaining entry is marked ``stopped`` with the reason, and the sink hears
``on_domain_stopped`` exactly once. Other domains carry on.

Time
----

Two clocks are injected, plus ``sleep`` and ``rand``:

* ``clock`` is monotonic seconds, for the bucket and the breaker (state that lives for one run);
* ``wall_clock`` is Unix epoch seconds, for frontier times (``next_attempt_at`` is persisted and
  compared again after a restart) and for HTTP-date ``Retry-After``;
* ``sleep`` is how the scheduler waits (retry delays, bucket waits, breaker cooldowns);
* ``rand`` returns floats in ``[0, 1)`` for backoff jitter.

Production passes the defaults. Tests pass a virtual clock whose ``sleep`` advances time instantly,
so a crawl with 30-second cooldowns and minute-long backoffs runs in milliseconds, deterministically,
and the tests can assert exact request timestamps against the rate limit. Real-time tests of these
properties would be slow, flaky, and unable to tell "polite" from "lucky".

Retries are waited out within the same run (the task sleeps until ``next_attempt_at``), but a stop
wakes every sleeper at once, so a stopped domain's entries are reported immediately rather than
after their backoff.

Cancellation
------------

``run`` is cancellation-safe. When it is cancelled (``asyncio.run`` turns Ctrl-C into a
cancellation of the main task), in-flight requests are abandoned and *not* reported, everything
already emitted stays emitted, and ``CancelledError`` propagates. Each sink call is one complete
event; the sink must make each call atomic (commit or roll back), including when the call itself is
cancelled. Because the frontier is updated only through emitted events, an abandoned attempt leaves
its entry as it was: ``pending``, to be picked up by the next run.

Programming errors (a fetcher or sink that raises) propagate after the other tasks are cancelled.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol
from urllib.parse import urlsplit

from scraping_pipeline.config import Settings
from scraping_pipeline.core.circuit import (
    BreakerConfig,
    BreakerPhase,
    BreakerState,
    allow,
    describe,
    is_tripped,
    new_breaker,
    record,
)
from scraping_pipeline.core.classify import DEFAULT_MARKERS, BlockMarkers, Classification, classify
from scraping_pipeline.core.frontier import (
    FrontierEntry,
    apply_decision,
    mark_skipped_robots,
    mark_stopped,
)
from scraping_pipeline.core.ratelimit import (
    TokenBucket,
    acquire,
    apply_crawl_delay,
    new_bucket,
    slow_down,
    speed_up,
)
from scraping_pipeline.core.retry import BackoffPolicy, RetryAction, decide
from scraping_pipeline.core.robots import RobotsPolicy
from scraping_pipeline.core.types import FrontierStatus, ResponseClass
from scraping_pipeline.core.urls import domain as domain_of
from scraping_pipeline.fetch.protocol import Fetcher, FetchResult

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]
Rand = Callable[[], float]


class FetchSink(Protocol):
    """Where the scheduler reports what happened. The fetch stage implements it to persist state.

    Calls are awaited one at a time per event, in causal order for each URL. Each call must be
    atomic (see "Cancellation" in the module docstring). An exception raised here stops the run.
    """

    async def on_robots(self, domain: str, policy: RobotsPolicy, result: FetchResult) -> None:
        """robots.txt for ``domain`` was fetched (once per run) and parsed into ``policy``."""

    async def on_result(
        self,
        entry: FrontierEntry,
        result: FetchResult,
        classification: Classification,
        new_entry: FrontierEntry,
    ) -> None:
        """One fetch attempt finished. ``new_entry`` is ``entry`` after the attempt: ``done``,
        ``pending`` with ``next_attempt_at`` for a retry, or another final state."""

    async def on_marked(self, entry: FrontierEntry, new_entry: FrontierEntry) -> None:
        """``entry`` became final without a fetch: ``skipped_robots``, or ``stopped`` because its
        domain was stopped."""

    async def on_domain_stopped(self, domain: str, reason: str) -> None:
        """``domain`` was stopped for the rest of the run (emitted once, before its entries are
        marked ``stopped``)."""


@dataclass(frozen=True, slots=True)
class CrawlSummary:
    """What a run did. ``final_statuses`` counts every entry the run settled, by final status."""

    attempts: int
    by_class: Mapping[ResponseClass, int]
    final_statuses: Mapping[FrontierStatus, int]
    domains_stopped: Mapping[str, str]
    robots: Mapping[str, str]
    duplicates_ignored: int


@dataclass(slots=True)
class _Domain:
    """Mutable per-run state of one domain. Only the event loop thread touches it."""

    name: str
    robots_url: str
    bucket: TokenBucket
    breaker: BreakerState
    semaphore: asyncio.Semaphore
    robots_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    policy: RobotsPolicy | None = None
    stopped: str | None = None
    stop_reported: asyncio.Event = field(default_factory=asyncio.Event)
    wake: asyncio.Event = field(default_factory=asyncio.Event)

    def poke(self) -> None:
        """Wake every task waiting on this domain (state changed: re-check it)."""
        self.wake.set()
        self.wake = asyncio.Event()


def robots_url_for(url: str) -> str:
    """``scheme://host[:port]/robots.txt`` for the origin of ``url``."""
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}/robots.txt"


class PoliteScheduler:
    """Fetch due frontier entries politely. Build one per run; call :meth:`run` once.

    State is keyed by :func:`~scraping_pipeline.core.urls.domain` (the host). The robots.txt URL is
    taken from the first entry seen for a host, so one run should not mix origins (scheme or port)
    on the same host; the pipeline's sites each have a single origin.
    """

    def __init__(
        self,
        fetcher: Fetcher,
        settings: Settings,
        sink: FetchSink,
        *,
        clock: Clock = time.monotonic,
        wall_clock: Clock = time.time,
        sleep: Sleep = asyncio.sleep,
        rand: Rand = random.random,
        markers: BlockMarkers = DEFAULT_MARKERS,
    ) -> None:
        self._fetcher = fetcher
        self._settings = settings
        self._sink = sink
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleep = sleep
        self._rand = rand
        self._markers = markers
        self._policy = BackoffPolicy(
            base_s=settings.backoff_base_s,
            cap_s=settings.backoff_cap_s,
            max_attempts=settings.max_attempts,
            max_retry_after_s=settings.max_retry_after_s,
        )
        self._breaker_config = BreakerConfig(
            failure_threshold=settings.breaker_failure_threshold,
            cooldown_s=settings.breaker_cooldown_s,
            max_cooldown_s=max(BreakerConfig().max_cooldown_s, settings.breaker_cooldown_s),
            max_open_cycles=settings.breaker_max_open_cycles,
        )
        self._domains: dict[str, _Domain] = {}
        self._seen: set[str] = set()
        self._attempts = 0
        self._by_class: Counter[ResponseClass] = Counter()
        self._final: Counter[FrontierStatus] = Counter()
        self._duplicates = 0
        self._started = False

    # --- public ------------------------------------------------------------------------------

    async def run(
        self, entries: Iterable[FrontierEntry] | AsyncIterable[FrontierEntry]
    ) -> CrawlSummary:
        """Fetch every entry until it is final, and return what happened.

        ``entries`` must be ``pending`` (``ValueError`` otherwise); an entry whose
        ``next_attempt_at`` is in the future is waited for. A URL seen twice is fetched once.
        """
        if self._started:
            raise RuntimeError("a PoliteScheduler runs once; build a new one per run")
        self._started = True
        try:
            async with asyncio.TaskGroup() as group:
                if isinstance(entries, AsyncIterable):
                    async for entry in entries:
                        self._spawn(group, entry)
                else:
                    for entry in entries:
                        self._spawn(group, entry)
        except BaseExceptionGroup as failure:
            if len(failure.exceptions) == 1:
                raise failure.exceptions[0] from None
            raise
        return CrawlSummary(
            attempts=self._attempts,
            by_class=MappingProxyType(dict(self._by_class)),
            final_statuses=MappingProxyType(dict(self._final)),
            domains_stopped=MappingProxyType(
                {d.name: d.stopped for d in self._domains.values() if d.stopped is not None}
            ),
            robots=MappingProxyType(
                {d.name: d.policy.reason for d in self._domains.values() if d.policy is not None}
            ),
            duplicates_ignored=self._duplicates,
        )

    # --- intake ------------------------------------------------------------------------------

    def _spawn(self, group: asyncio.TaskGroup, entry: FrontierEntry) -> None:
        if entry.status is not FrontierStatus.PENDING:
            raise ValueError(
                f"{entry.url}: only pending entries can be fetched, got {entry.status}"
            )
        if entry.url in self._seen:
            self._duplicates += 1
            return
        self._seen.add(entry.url)
        group.create_task(self._drive(entry, self._domain_for(entry.url)))

    def _domain_for(self, url: str) -> _Domain:
        name = domain_of(url)
        state = self._domains.get(name)
        if state is None:
            state = _Domain(
                name=name,
                robots_url=robots_url_for(url),
                bucket=new_bucket(self._settings.requests_per_second, self._settings.burst),
                breaker=new_breaker(self._breaker_config),
                semaphore=asyncio.Semaphore(self._settings.max_concurrency_per_domain),
            )
            self._domains[name] = state
        return state

    # --- one URL, start to finish ------------------------------------------------------------

    async def _drive(self, entry: FrontierEntry, d: _Domain) -> None:
        policy = await self._ensure_robots(d)
        if d.stopped is not None:
            await self._mark_stopped(d, entry)
            return
        if not policy.can_fetch(self._settings.user_agent, entry.url):
            await self._mark(entry, mark_skipped_robots(entry, f"robots.txt: {policy.reason}"))
            return
        while True:
            await self._wait_until_due(d, entry)
            result: FetchResult | None = None
            async with d.semaphore:
                if await self._admit(d):
                    result = await self._fetcher.fetch(entry.url)
            if result is None:
                await self._mark_stopped(d, entry)
                return
            entry = await self._record_attempt(d, entry, result)
            if entry.status is not FrontierStatus.PENDING:
                return
            if d.stopped is not None:
                await self._mark_stopped(d, entry)
                return

    async def _wait_until_due(self, d: _Domain, entry: FrontierEntry) -> None:
        due = entry.next_attempt_at
        while due is not None and d.stopped is None:
            remaining = due - self._wall_clock()
            if remaining <= 0:
                return
            await self._nap(d, remaining)

    async def _admit(self, d: _Domain) -> bool:
        """Wait until the bucket and the breaker both let a request through. False: stopped.

        The token is taken before asking the breaker, so a half-open probe is never granted to a
        task that then has to wait for the bucket. A token taken while the breaker says no is
        wasted, which can only make the crawl slower, never faster.
        """
        while True:
            if d.stopped is not None:
                return False
            now = self._clock()
            d.bucket, wait = acquire(d.bucket, now)
            if wait > 0:
                await self._nap(d, wait)
                continue
            d.breaker, allowed = allow(d.breaker, now)
            if allowed:
                return True
            if is_tripped(d.breaker):  # defensive: _record_attempt claims the stop when it trips
                if self._claim_stop(d, describe(d.breaker)):
                    await self._announce_stop(d)
                return False
            if d.breaker.phase is BreakerPhase.OPEN and d.breaker.open_until is not None:
                await self._nap(d, d.breaker.open_until - now)
            else:  # half-open: the probe is out; its result will poke us
                await self._nap(d, None)

    async def _record_attempt(
        self, d: _Domain, entry: FrontierEntry, result: FetchResult
    ) -> FrontierEntry:
        now_epoch = self._wall_clock()
        verdict = classify(
            result.status,
            result.headers,
            result.text(),
            result.error,
            markers=self._markers,
            now_epoch=now_epoch,
        )
        response_class = verdict.response_class
        d.breaker = record(d.breaker, response_class, self._clock())
        if response_class is ResponseClass.RATE_LIMITED:
            d.bucket = slow_down(d.bucket)
        elif response_class is ResponseClass.OK:
            d.bucket = speed_up(d.bucket)
        decision = (
            None
            if response_class is ResponseClass.OK
            else decide(self._policy, verdict, entry.attempts + 1, self._rand())
        )
        new_entry = apply_decision(entry, verdict, decision, now_epoch)
        self._attempts += 1
        self._by_class[response_class] += 1
        if new_entry.status is not FrontierStatus.PENDING:
            self._final[new_entry.status] += 1
        # Claim the stop *before* awaiting the sink: while it writes, other tasks run, and they must
        # already see the domain as stopped (and never send another request to it).
        claimed = False
        if decision is not None and decision.action is RetryAction.STOP_DOMAIN:
            claimed = self._claim_stop(d, new_entry.last_reason or decision.reason)
        elif is_tripped(d.breaker):
            claimed = self._claim_stop(d, describe(d.breaker))
        d.poke()
        await self._sink.on_result(entry, result, verdict, new_entry)
        if claimed:
            await self._announce_stop(d)
        return new_entry

    # --- robots.txt --------------------------------------------------------------------------

    async def _ensure_robots(self, d: _Domain) -> RobotsPolicy:
        async with d.robots_lock:
            if d.policy is None:
                async with d.semaphore:
                    # A new domain's bucket is full, so robots.txt always gets the first token
                    # (and it counts against the budget like every other request).
                    d.bucket, _ = acquire(d.bucket, self._clock())
                    result = await self._fetcher.fetch(d.robots_url)
                # A transport error, even with a status (an oversize body), means we do not know
                # the rules: RobotsPolicy treats "unreachable" as disallow-all.
                status = result.status if result.error is None else None
                policy = RobotsPolicy.from_response(status, result.text())
                crawl_delay = policy.crawl_delay(self._settings.user_agent)
                d.bucket = apply_crawl_delay(d.bucket, crawl_delay)
                d.policy = policy
                # A Crawl-delay beyond what we are willing to wait is the site asking for (almost)
                # no traffic: honour it by stopping, like an over-long Retry-After, instead of
                # crawling at one page an hour.
                claimed = (
                    crawl_delay is not None
                    and crawl_delay > self._settings.max_crawl_delay_s
                    and self._claim_stop(
                        d,
                        f"robots.txt Crawl-delay {crawl_delay:g}s exceeds the "
                        f"{self._settings.max_crawl_delay_s:g}s limit",
                    )
                )
                await self._sink.on_robots(d.name, policy, result)
                if claimed:
                    await self._announce_stop(d)
            return d.policy

    # --- stopping ----------------------------------------------------------------------------

    def _claim_stop(self, d: _Domain, reason: str) -> bool:
        """Stop ``d`` now (no awaits, so no task can slip a request in). True for the first caller."""
        if d.stopped is not None:
            return False
        d.stopped = reason
        d.poke()
        return True

    async def _announce_stop(self, d: _Domain) -> None:
        """Emit ``on_domain_stopped``; entries are marked ``stopped`` only after it went out."""
        assert d.stopped is not None  # noqa: S101 - internal invariant: only called after a claim
        await self._sink.on_domain_stopped(d.name, d.stopped)
        d.stop_reported.set()

    async def _mark_stopped(self, d: _Domain, entry: FrontierEntry) -> None:
        await d.stop_reported.wait()  # on_domain_stopped goes out before the entries it explains
        await self._mark(entry, mark_stopped(entry, f"domain stopped: {d.stopped}"))

    async def _mark(self, entry: FrontierEntry, new_entry: FrontierEntry) -> None:
        self._final[new_entry.status] += 1
        await self._sink.on_marked(entry, new_entry)

    # --- waiting -----------------------------------------------------------------------------

    async def _nap(self, d: _Domain, delay: float | None) -> None:
        """Sleep ``delay`` seconds (forever when ``None``), or until the domain is poked."""
        if delay is not None and delay <= 0:
            return
        waiters: set[asyncio.Future[object]] = {asyncio.ensure_future(d.wake.wait())}
        if delay is not None:
            waiters.add(asyncio.ensure_future(self._sleep(delay)))
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
