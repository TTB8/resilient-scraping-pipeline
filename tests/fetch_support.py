"""Shared helpers for fetch-layer tests: virtual time, a recording sink, settings without .env.

Virtual time
------------

:class:`VirtualTime` gives the scheduler a ``clock``, a ``wall_clock`` and a ``sleep`` where sleeping
costs no real time. ``vt.run(coro)`` drives the coroutine: it lets every ready task run until the
loop is quiet (a fixed number of zero-sleeps; nothing in these tests waits on real I/O), then jumps
the clock to the earliest pending ``sleep`` deadline and wakes it. Ties wake in the order the sleeps
were registered, so a run is deterministic. If the loop goes quiet with no sleeper left while the
coroutine is unfinished, that is a deadlock and ``run`` raises instead of hanging the suite.
"""

from __future__ import annotations

import asyncio
import heapq
import os
from collections.abc import Coroutine, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scraping_pipeline.config import Settings
from scraping_pipeline.core.classify import Classification
from scraping_pipeline.core.frontier import FINAL_STATES, FrontierEntry
from scraping_pipeline.core.robots import RobotsPolicy
from scraping_pipeline.core.types import PageKind, Site
from scraping_pipeline.fetch.protocol import FetchResult

EPOCH = 1_800_000_000.0  # wall clock at virtual time 0 (a fixed date in 2027)
_QUIET_SPINS = 64
_DEADLOCK_ROUNDS = 3


class DeadlockError(RuntimeError):
    """The driven coroutine is waiting on something virtual time can never deliver."""


class VirtualTime:
    """A deterministic clock for asyncio tests. See the module docstring."""

    def __init__(self) -> None:
        self.now = 0.0
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._seq = 0

    def clock(self) -> float:
        return self.now

    def wall(self) -> float:
        return EPOCH + self.now

    async def sleep(self, delay: float) -> None:
        if delay <= 0:
            await asyncio.sleep(0)
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (self.now + delay, self._seq, future))
        self._seq += 1
        await future

    async def _quiesce(self, task: asyncio.Future[Any]) -> None:
        for _ in range(_QUIET_SPINS):
            if task.done():
                return
            await asyncio.sleep(0)

    async def run[T](self, coro: Coroutine[Any, Any, T]) -> T:
        task = asyncio.ensure_future(coro)
        idle_rounds = 0
        while not task.done():
            await self._quiesce(task)
            if task.done():
                break
            while self._sleepers and self._sleepers[0][2].done():
                heapq.heappop(self._sleepers)  # cancelled sleeps
            if not self._sleepers:
                idle_rounds += 1
                if idle_rounds >= _DEADLOCK_ROUNDS:
                    task.cancel()
                    raise DeadlockError("nothing left to wake: the driven coroutine is stuck")
                continue
            idle_rounds = 0
            deadline, _, future = heapq.heappop(self._sleepers)
            self.now = max(self.now, deadline)
            future.set_result(None)
        return await task


@dataclass(frozen=True, slots=True)
class Event:
    """One sink call, as recorded."""

    kind: str
    at: float
    url: str | None = None
    domain: str | None = None
    new_entry: FrontierEntry | None = None
    classification: Classification | None = None
    result: FetchResult | None = None
    reason: str | None = None


@dataclass
class RecordingSink:
    """A :class:`~scraping_pipeline.fetch.scheduler.FetchSink` that records every call in order."""

    clock: Any = None
    events: list[Event] = field(default_factory=list)
    policies: dict[str, RobotsPolicy] = field(default_factory=dict)

    def _now(self) -> float:
        return float(self.clock()) if self.clock is not None else 0.0

    async def on_robots(self, domain: str, policy: RobotsPolicy, result: FetchResult) -> None:
        self.policies[domain] = policy
        self.events.append(Event("robots", self._now(), domain=domain, result=result))

    async def on_result(
        self,
        entry: FrontierEntry,
        result: FetchResult,
        classification: Classification,
        new_entry: FrontierEntry,
    ) -> None:
        self.events.append(
            Event(
                "result",
                self._now(),
                url=entry.url,
                new_entry=new_entry,
                classification=classification,
                result=result,
            )
        )

    async def on_marked(self, entry: FrontierEntry, new_entry: FrontierEntry) -> None:
        self.events.append(Event("marked", self._now(), url=entry.url, new_entry=new_entry))

    async def on_domain_stopped(self, domain: str, reason: str) -> None:
        self.events.append(Event("stopped", self._now(), domain=domain, reason=reason))

    # --- views -------------------------------------------------------------------------------

    def of(self, kind: str) -> list[Event]:
        return [e for e in self.events if e.kind == kind]

    def finals(self) -> dict[str, FrontierEntry]:
        """URL -> its final entry. Fails the test if any URL was settled twice."""
        out: dict[str, FrontierEntry] = {}
        for event in self.events:
            if event.new_entry is not None and event.new_entry.status in FINAL_STATES:
                assert event.url is not None
                assert event.url not in out, f"{event.url} settled twice"
                out[event.url] = event.new_entry
        return out


@contextmanager
def no_env_settings() -> Iterator[None]:
    """Hide PIPELINE_* variables while building Settings (``.env`` is read from the cwd)."""
    saved = {k: v for k, v in os.environ.items() if k.startswith("PIPELINE_")}
    for key in saved:
        del os.environ[key]
    try:
        yield
    finally:
        os.environ.update(saved)


def make_settings(**overrides: Any) -> Settings:
    """Settings from defaults plus ``overrides`` only: no environment, no ``.env`` file."""
    with no_env_settings():
        return Settings(_env_file=None, data_dir=Path("unused"), **overrides)


def entry(
    url: str, *, kind: PageKind = PageKind.LISTING, site: Site = Site.QUOTES
) -> FrontierEntry:
    return FrontierEntry(url=url, site=site, kind=kind, depth=0)
