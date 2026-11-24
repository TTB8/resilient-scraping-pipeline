"""Kill the fetch stage midway, re-run it, and get exactly what an uninterrupted run gets.

Real HTTP against the local fixture server, real ``asyncio.run``, and for the first variant a real
SIGINT (what Ctrl-C sends): ``asyncio.run`` turns it into a cancellation of the crawl, the stage
records the run as ``interrupted``, and the next run resumes. Concurrency is 1, so the kill lands
between pages. The fixture server's request log is the witness that nothing was fetched twice.

The second variant crashes at the worst moment: after ``cache.put``, before ``record_result``.
``reconcile_with_cache`` must recover the page on the next run without asking the server again.
"""

from __future__ import annotations

import signal
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from fixture_server import FixtureServer
from scraping_pipeline.config import Settings
from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import FrontierStatus, ResponseClass, Site
from scraping_pipeline.stages import run_discover, run_fetch
from scraping_pipeline.storage.cache import PageCache
from scraping_pipeline.storage.state import RunStatus, StateStore
from stage_support import server_settings

pytestmark = pytest.mark.slow


class SimulatedCrashError(RuntimeError):
    """The process dying at one exact line."""


def _settings(tmp_path: Path, server: FixtureServer) -> Settings:
    return server_settings(tmp_path, server.base_url, max_concurrency_per_domain=1)


def _pages(server: FixtureServer) -> list[str]:
    return [hit.path for hit in server.log() if hit.path != "/robots.txt"]


def _final_state(settings: Settings, base_url: str) -> tuple[list[tuple[Any, ...]], set[Any]]:
    """Frontier and cache with the server origin stripped, so two servers' runs compare."""

    def rel(url: str) -> str:
        return url.removeprefix(base_url)

    with StateStore.open(settings.state_db) as store:
        frontier = [
            (rel(e.url), e.kind, e.depth, e.status, e.attempts, e.last_class)
            for e in store.entries(Site.QUOTES)
        ]
    cache = {(rel(e.url), e.content_sha256) for e in PageCache(settings.cache_dir).iter_entries()}
    return frontier, cache


def _uninterrupted(tmp_path: Path, make: Callable[..., FixtureServer]) -> tuple[Any, Any]:
    server = make("quotes")
    settings = _settings(tmp_path / "reference", server)
    run_discover(settings, Site.QUOTES)
    result = run_fetch(settings, Site.QUOTES)
    assert result.statuses[FrontierStatus.PENDING] == 0
    return _final_state(settings, server.base_url)


def test_ctrl_c_midway_then_resume_fetches_nothing_twice(
    tmp_path: Path,
    make_fixture_server: Callable[..., FixtureServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_fixture_server("quotes")
    settings = _settings(tmp_path / "run", server)
    run_discover(settings, Site.QUOTES)

    real = StateStore.record_result
    committed = [0]

    def sigint_after_four(self: StateStore, *args: Any) -> None:
        real(self, *args)
        committed[0] += 1
        if committed[0] == 4:
            signal.raise_signal(signal.SIGINT)  # Ctrl-C, right after a committed result

    monkeypatch.setattr(StateStore, "record_result", sigint_after_four)
    first = run_fetch(settings, Site.QUOTES)
    monkeypatch.setattr(StateStore, "record_result", real)

    assert first.interrupted
    assert first.statuses[FrontierStatus.PENDING] > 0
    with StateStore.open(settings.state_db) as store:
        assert store.runs(Site.QUOTES)[-1].status is RunStatus.INTERRUPTED
    after_kill = len(_pages(server))

    second = run_fetch(settings, Site.QUOTES)
    assert not second.interrupted
    assert second.statuses[FrontierStatus.PENDING] == 0
    assert len(_pages(server)) > after_kill  # the second run did real work ...
    assert max(Counter(_pages(server)).values()) == 1  # ... and asked for nothing twice
    assert _final_state(settings, server.base_url) == _uninterrupted(tmp_path, make_fixture_server)


def test_crash_between_cache_put_and_record_result_is_recovered_without_refetch(
    tmp_path: Path,
    make_fixture_server: Callable[..., FixtureServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_fixture_server("quotes")
    settings = _settings(tmp_path / "run", server)
    run_discover(settings, Site.QUOTES)
    victim = server.url("/page/2/")

    real = StateStore.record_result

    def crash_on_victim(self: StateStore, run_id: str, before: FrontierEntry, *rest: Any) -> None:
        if before.url == victim:
            raise SimulatedCrashError("killed after cache.put, before record_result")
        real(self, run_id, before, *rest)

    monkeypatch.setattr(StateStore, "record_result", crash_on_victim)
    with pytest.raises(SimulatedCrashError):
        run_fetch(settings, Site.QUOTES)
    monkeypatch.setattr(StateStore, "record_result", real)
    assert PageCache(settings.cache_dir).has(victim)
    assert _pages(server).count("/page/2/") == 1

    second = run_fetch(settings, Site.QUOTES)
    assert second.recovered == 1
    assert _pages(server).count("/page/2/") == 1  # recovered from the cache, not refetched
    assert max(Counter(_pages(server)).values()) == 1
    with StateStore.open(settings.state_db) as store:
        recovered = store.get(victim)
        attempts = store.attempts(victim)
    assert recovered is not None
    assert recovered.status is FrontierStatus.DONE
    assert recovered.last_class is ResponseClass.OK
    assert [a.record.reason for a in attempts] == ["recovered from cache"]
    assert _final_state(settings, server.base_url) == _uninterrupted(tmp_path, make_fixture_server)
