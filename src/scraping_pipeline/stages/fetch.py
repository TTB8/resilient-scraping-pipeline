"""Stage 2, fetch: crawl every pending frontier entry politely, store pages, and never lose progress.

This is the only stage that talks to the network, so it is the one built to be interrupted. The
policy (robots.txt, rate limits, retries, breakers, stopping a domain) lives in ``core/`` and is
driven by :class:`~scraping_pipeline.fetch.scheduler.PoliteScheduler`; this module only connects the
scheduler to storage, through a :class:`~scraping_pipeline.fetch.scheduler.FetchSink` backed by the
page cache and the state database.

A run, in order
---------------

1. **Reconcile** (``StateStore.reconcile_with_cache``): a page that reached the cache but whose
   result never reached the state database (a crash between the two writes) is marked ``done``
   without being fetched again.
2. **Retry, if asked** (``--retry-failed`` -> ``StateStore.reset_for_retry``): ``failed_exhausted``
   and ``stopped`` entries go back to ``pending``. Opt-in only: a default run never re-asks a site
   that already said no.
3. **Expand offline** from the cache (the same function ``discover`` uses), then reconcile again:
   links of pages cached by an interrupted run are planned before anything is fetched.
4. **Crawl** every ``pending`` entry (including retries whose time has not come: the scheduler
   waits them out within the run), and expand links *during* the crawl.

Why the cache is written before the state
-----------------------------------------

For an ``ok`` result the sink calls ``cache.put`` (object, then entry) and only then
``state.record_result``. A crash between the two leaves a cached page that is still ``pending``,
which step 1 recovers on the next run without a request. The opposite order could leave a page
marked ``done`` whose body was never stored: it would never be fetched again, and parse would
silently miss it. One order loses nothing; the other loses data.

Each sink call is all-or-nothing per the ``FetchSink`` protocol. The storage calls are synchronous
and the sink never ``await``\\s, so a cancellation (Ctrl-C) can only land between sink calls, never
inside one; each ``StateStore`` call is its own transaction. Every call that touches the frontier
updates it from the scheduler's own ``before`` entry, so a stale copy fails loudly
(``StaleEntryError``) instead of overwriting a newer result.

Expanding links during the crawl
--------------------------------

When a page of a link-bearing kind lands ``ok``, its links are parsed and planned in the same sink
call (``plan_new_links``: same site, not known, depth and page caps), inserted into the frontier,
and fed to the running scheduler. The scheduler reads entries from an async iterator, the
:class:`_Feeder`, which ends exactly when **no entry is queued and none is outstanding**: an entry
is outstanding from the moment it is handed to the scheduler until its single final event
(``on_result`` with a final ``new_entry``, or ``on_marked``). New links are queued *before* their
page is settled, and the feeder only checks for the end after draining the queue, so it cannot end
while a page that might still yield links is in flight, and cannot hang once everything is final.

Run ids, retries and Ctrl-C
---------------------------

``fetch_run_id`` is fresh per run (``fetch-<UTC time>-<hex>``), stored in every cache entry the run
writes and as the run's id in the state database. Fetching is inherently non-deterministic (what a
server answers depends on when you ask), which is exactly why parse derives *its* id from content.

The retry budget is **lifetime**: ``StateStore`` keeps ``attempts`` as the total ever made, and the
scheduler compares the next attempt number with ``max_attempts``. So each ``--retry-failed`` grants
an exhausted entry **one more attempt**; a ``stopped`` entry (usually never attempted) gets what is
left of its budget. Raise ``--max-attempts`` (``PIPELINE_MAX_ATTEMPTS``) for a run to give more.

Ctrl-C: ``asyncio.run`` turns it into a cancellation of the crawl; in-flight requests are abandoned
(their entries stay ``pending``), every result already recorded stays recorded, and the run is
closed as ``interrupted``. The CLI exits with 130. Re-running ``fetch`` resumes where it stopped.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter, deque
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Any

from scraping_pipeline.config import Settings
from scraping_pipeline.core.classify import Classification
from scraping_pipeline.core.frontier import FINAL_STATES, FrontierEntry
from scraping_pipeline.core.robots import RobotsPolicy
from scraping_pipeline.core.types import FrontierStatus, ResponseClass, Site
from scraping_pipeline.core.urls import domain as domain_of
from scraping_pipeline.fetch.protocol import Fetcher, FetchResult
from scraping_pipeline.fetch.scheduler import CrawlSummary, PoliteScheduler
from scraping_pipeline.logging import get_logger
from scraping_pipeline.sites import SiteSpec, get_site
from scraping_pipeline.stages.common import (
    expand_from_cache,
    known_urls,
    new_run_id,
    plan_links,
    stage_run,
    stopped_domains,
)
from scraping_pipeline.storage.cache import FetchMeta, PageCache
from scraping_pipeline.storage.state import (
    AttemptRecord,
    DomainEvent,
    DomainEventKind,
    RunStatus,
    Stage,
    StateStore,
)

log = get_logger(__name__)

_STORABLE_STATUS = range(100, 600)
"""Statuses the attempts table accepts; ``classify`` reads anything else as a network error."""

Runner = Callable[[Coroutine[Any, Any, Any]], Any]
FetcherFactory = Callable[[], Fetcher]
RobotsFetcherFactory = Callable[[], Fetcher | None]


@dataclass(frozen=True, slots=True)
class FetchTiming:
    """Clocks, sleep and randomness for the scheduler. Production leaves every field ``None``
    (real time); tests pass a virtual clock so a crawl with long backoffs runs in milliseconds."""

    clock: Callable[[], float] | None = None
    wall_clock: Callable[[], float] | None = None
    sleep: Callable[[float], Coroutine[Any, Any, None]] | None = None
    rand: Callable[[], float] | None = None


@dataclass(frozen=True, slots=True)
class FetchStageResult:
    run_id: str
    """Also the ``fetch_run_id`` stamped into every page this run cached."""
    recovered: int
    """Entries marked done by reconcile (cached, never recorded): no request was made for them."""
    reset: int
    links_added: int
    attempts: int
    """Page attempts recorded in the frontier."""
    requests: int | None
    """Every HTTP request the site saw (robots.txt and redirect hops included), from the
    scheduler's summary; ``None`` when the run was interrupted before it could report."""
    by_class: dict[ResponseClass, int]
    statuses: dict[FrontierStatus, int]
    """Frontier counts after the run (every status, zero included)."""
    domains_stopped_now: dict[str, str]
    """Domains this run stopped, with the reason."""
    stopped_domains: dict[str, str]
    """Domains with ``stopped`` entries after the run (this run's stops or earlier ones)."""
    interrupted: bool
    scheme_downgrades: int | None = 0
    """Redirect hops followed from ``https`` to ``http`` (ADR 0010), from the scheduler's
    summary; ``None`` when the run was interrupted before it could report."""


class _Feeder:
    """The scheduler's entry stream: ends when nothing is queued and nothing is outstanding.

    Single-threaded by construction (one event loop), so the counter needs no lock; the only
    suspension point is ``await self._changed.wait()``, entered after the queue was drained and
    the outstanding count read, with the event cleared first so no wake-up can be missed.
    """

    def __init__(self) -> None:
        self._queue: deque[FrontierEntry] = deque()
        self._fed: set[str] = set()
        self._outstanding: set[str] = set()
        self._changed = asyncio.Event()

    def add(self, entries: Iterable[FrontierEntry]) -> None:
        for entry in entries:
            # The scheduler ignores a URL it has already seen and would never settle it again:
            # feeding a duplicate would leave it outstanding forever.
            if entry.url not in self._fed:
                self._fed.add(entry.url)
                self._queue.append(entry)
        self._changed.set()

    def settle(self, url: str) -> None:
        self._outstanding.discard(url)
        self._changed.set()

    async def entries(self) -> AsyncIterator[FrontierEntry]:
        while True:
            while self._queue:
                entry = self._queue.popleft()
                self._outstanding.add(entry.url)
                yield entry
            if not self._outstanding:
                return
            self._changed.clear()
            await self._changed.wait()


class _StoreSink:
    """A :class:`~scraping_pipeline.fetch.scheduler.FetchSink` that persists every event.

    Every method is synchronous inside (no ``await``), which is what makes each call atomic with
    respect to cancellation.
    """

    def __init__(
        self,
        *,
        store: StateStore,
        cache: PageCache,
        spec: SiteSpec,
        settings: Settings,
        run_id: str,
        feeder: _Feeder,
        wall_clock: Callable[[], float],
    ) -> None:
        self._store = store
        self._cache = cache
        self._spec = spec
        self._settings = settings
        self._run_id = run_id
        self._feeder = feeder
        self._wall = wall_clock
        self._known = known_urls(store)
        self._count = len(store.entries(spec.site))
        self._last_class: dict[str, ResponseClass | None] = {}
        self.attempts = 0
        self.by_class: Counter[ResponseClass] = Counter()
        self.links_added = 0
        self.domains_stopped: dict[str, str] = {}

    async def on_robots(self, domain: str, policy: RobotsPolicy, result: FetchResult) -> None:  # noqa: ARG002
        self._last_class[domain] = None  # a stop announced next comes from robots, not a page
        self._store.record_domain_event(
            DomainEvent(
                run_id=self._run_id,
                site=self._spec.site,
                domain=domain,
                event=DomainEventKind.ROBOTS,
                reason=policy.reason,
                at=self._wall(),
            )
        )

    async def on_result(
        self,
        entry: FrontierEntry,
        result: FetchResult,
        classification: Classification,
        new_entry: FrontierEntry,
    ) -> None:
        now = self._wall()
        response_class = classification.response_class
        content_sha256: str | None = None
        if response_class is ResponseClass.OK and result.status is not None:
            # 1. The page, durably (object, then entry) ...
            cached = self._cache.put(
                entry.url,
                result.body,
                FetchMeta(
                    final_url=result.final_url,
                    status=result.status,
                    fetched_at=datetime.fromtimestamp(now, UTC).isoformat(),
                    fetch_run_id=self._run_id,
                    fetcher=self._spec.fetcher,
                    headers=result.headers,
                ),
            )
            content_sha256 = cached.content_sha256
        # 2. ... and only then the result that says it is done (see the module docstring).
        status = result.status if result.status in _STORABLE_STATUS else None
        self._store.record_result(
            self._run_id,
            entry,
            new_entry,
            AttemptRecord(
                attempt=new_entry.attempts,
                at=now,
                response_class=response_class,
                reason=new_entry.last_reason or classification.reason,
                http_status=status,
                elapsed_s=max(0.0, result.elapsed_s),
                content_sha256=content_sha256,
            ),
        )
        self.attempts += 1
        self.by_class[response_class] += 1
        self._last_class[domain_of(entry.url)] = response_class
        if content_sha256 is not None:
            self._expand(entry, result)
        if new_entry.status in FINAL_STATES:
            self._feeder.settle(entry.url)

    def _expand(self, entry: FrontierEntry, result: FetchResult) -> None:
        new = plan_links(
            self._spec,
            self._settings,
            known=self._known,
            entry=entry,
            page_url=result.final_url,
            html=result.text(),
            current_count=self._count,
        )
        if not new:
            return
        self._store.add_entries(new, self._run_id)
        self._known.update(e.url for e in new)
        self._count += len(new)
        self.links_added += len(new)
        self._feeder.add(new)  # queued before the parent is settled: the feeder cannot end early

    async def on_marked(self, entry: FrontierEntry, new_entry: FrontierEntry) -> None:
        self._store.record_result(self._run_id, entry, new_entry, None)
        self._feeder.settle(entry.url)

    async def on_breaker_opened(self, domain: str, reason: str) -> None:
        self._store.record_domain_event(
            DomainEvent(
                run_id=self._run_id,
                site=self._spec.site,
                domain=domain,
                event=DomainEventKind.BREAKER_OPENED,
                reason=reason,
                at=self._wall(),
                response_class=self._last_class.get(domain),
            )
        )
        log.warning("breaker_opened", domain=domain, reason=reason)

    async def on_domain_stopped(self, domain: str, reason: str) -> None:
        self.domains_stopped[domain] = reason
        self._store.record_domain_event(
            DomainEvent(
                run_id=self._run_id,
                site=self._spec.site,
                domain=domain,
                event=DomainEventKind.DOMAIN_STOPPED,
                reason=reason,
                at=self._wall(),
                response_class=self._last_class.get(domain),
            )
        )
        log.warning("domain_stopped", domain=domain, reason=reason)


def _no_fetcher() -> Fetcher | None:
    return None


def _pending(store: StateStore, site: Site) -> list[FrontierEntry]:
    """Every pending entry, never-tried first, then by retry time, then URL (like ``due``)."""
    entries = store.entries(site, {FrontierStatus.PENDING})
    return sorted(
        entries,
        key=lambda e: (e.next_attempt_at is not None, e.next_attempt_at or 0.0, e.url),
    )


def run_fetch(
    settings: Settings,
    site: Site,
    *,
    retry_failed: bool = False,
    fetcher_factory: FetcherFactory | None = None,
    robots_fetcher_factory: RobotsFetcherFactory | None = None,
    timing: FetchTiming | None = None,
    runner: Runner = asyncio.run,
) -> FetchStageResult:
    """Fetch every pending entry of ``site``; see the module docstring for the steps.

    ``fetcher_factory`` defaults to the site's fetcher (``SiteSpec.make_fetcher``).
    ``robots_fetcher_factory`` builds the robots.txt fetcher (``None`` from it: the page fetcher
    reads robots.txt); it defaults to ``SiteSpec.make_robots_fetcher`` when ``fetcher_factory`` is
    not given, and to "the page fetcher" when it is, so a scripted fetcher scripts robots.txt too.
    ``timing`` and ``runner`` exist for tests (a scripted fetcher, a virtual clock). Returns
    normally on Ctrl-C with ``interrupted=True``.
    """
    spec = get_site(site)
    cache = PageCache(settings.cache_dir)
    timing = timing or FetchTiming()
    wall = timing.wall_clock or time.time
    factory = fetcher_factory or (lambda: spec.make_fetcher(settings))
    robots_factory: RobotsFetcherFactory
    if robots_fetcher_factory is not None:
        robots_factory = robots_fetcher_factory
    elif fetcher_factory is None:
        robots_factory = partial(spec.make_robots_fetcher, settings)
    else:
        robots_factory = _no_fetcher
    with (
        StateStore.open(settings.state_db) as store,
        stage_run(store, settings, site, Stage.FETCH, run_id=new_run_id(Stage.FETCH)) as run,
    ):
        recovered = store.reconcile_with_cache(site, cache, run.run_id, wall())
        reset = store.reset_for_retry(site, run.run_id) if retry_failed else 0
        planned = expand_from_cache(store, cache, spec, settings, run.run_id)
        recovered += store.reconcile_with_cache(site, cache, run.run_id, wall())
        initial = _pending(store, site)
        sinks: list[_StoreSink] = []

        async def crawl() -> CrawlSummary:
            feeder = _Feeder()
            sink = _StoreSink(
                store=store,
                cache=cache,
                spec=spec,
                settings=settings,
                run_id=run.run_id,
                feeder=feeder,
                wall_clock=wall,
            )
            sinks.append(sink)
            feeder.add(initial)
            options: dict[str, Any] = {
                name: value
                for name in ("clock", "wall_clock", "sleep", "rand")
                if (value := getattr(timing, name)) is not None
            }
            async with AsyncExitStack() as stack:
                fetcher = await stack.enter_async_context(factory())
                robots = robots_factory()
                if robots is not None:
                    robots = await stack.enter_async_context(robots)
                scheduler = PoliteScheduler(
                    fetcher, settings, sink, robots_fetcher=robots, **options
                )
                return await scheduler.run(feeder.entries())

        interrupted = False
        summary: CrawlSummary | None = None
        if initial:
            try:
                summary = runner(crawl())
            except (KeyboardInterrupt, asyncio.CancelledError):
                interrupted = True
        sink = sinks[0] if sinks else None
        result = FetchStageResult(
            run_id=run.run_id,
            recovered=recovered,
            reset=reset,
            links_added=len(planned) + (sink.links_added if sink else 0),
            attempts=sink.attempts if sink else 0,
            requests=summary.requests if summary is not None else (None if interrupted else 0),
            by_class=dict(sorted((sink.by_class if sink else Counter()).items())),
            statuses=store.counts(site),
            domains_stopped_now=dict(sorted((sink.domains_stopped if sink else {}).items())),
            stopped_domains=stopped_domains(store, site),
            interrupted=interrupted,
            scheme_downgrades=(
                summary.scheme_downgrades if summary is not None else (None if interrupted else 0)
            ),
        )
        run.status = RunStatus.INTERRUPTED if interrupted else RunStatus.SUCCEEDED
        run.summary = {
            "fetch_run_id": result.run_id,
            "recovered": result.recovered,
            "reset": result.reset,
            "links_added": result.links_added,
            "attempts": result.attempts,
            "requests": result.requests,
            "by_class": {k.value: v for k, v in result.by_class.items()},
            "statuses": {k.value: v for k, v in result.statuses.items()},
            "domains_stopped_now": result.domains_stopped_now,
            "stopped_domains": result.stopped_domains,
            "interrupted": interrupted,
            "scheme_downgrades": result.scheme_downgrades,
        }
    return result
