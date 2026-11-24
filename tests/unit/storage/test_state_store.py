import gc
import gzip
import os
import signal
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from scraping_pipeline import storage
from scraping_pipeline.core.classify import Classification
from scraping_pipeline.core.frontier import (
    FrontierEntry,
    IllegalTransitionError,
    apply_decision,
    mark_skipped_robots,
    mark_stopped,
)
from scraping_pipeline.core.retry import RetryAction, RetryDecision
from scraping_pipeline.core.types import FrontierStatus, PageKind, ResponseClass, Site
from scraping_pipeline.storage import state as state_module
from scraping_pipeline.storage.cache import FetchMeta, PageCache
from scraping_pipeline.storage.state import (
    RECOVERED_REASON,
    SCHEMA_VERSION,
    AttemptRecord,
    DomainEvent,
    DomainEventKind,
    RunError,
    RunStatus,
    SchemaVersionError,
    Stage,
    StaleEntryError,
    StateLockedError,
    StateStore,
)

BOOKS = "https://books.toscrape.com/"
T0 = "2026-11-24T10:00:00+00:00"
T1 = "2026-11-24T10:05:00+00:00"
SRC = str(Path(__file__).resolve().parents[3] / "src")


def url(n: int) -> str:
    return f"{BOOKS}catalogue/page-{n}.html"


def entry(n: int, **overrides: object) -> FrontierEntry:
    base = FrontierEntry(url=url(n), site=Site.BOOKS, kind=PageKind.LISTING, depth=1)
    return replace(base, **overrides)  # type: ignore[arg-type]


OK = Classification(ResponseClass.OK, None, "200 ok")
SERVER_ERROR = Classification(ResponseClass.SERVER_ERROR, None, "503 server error")


def ok_attempt(after: FrontierEntry, at: float = 100.0) -> AttemptRecord:
    return AttemptRecord(
        attempt=after.attempts,
        at=at,
        response_class=ResponseClass.OK,
        reason="200 ok",
        http_status=200,
        elapsed_s=0.25,
        content_sha256="a" * 64,
    )


def fail_attempt(after: FrontierEntry, at: float = 100.0) -> AttemptRecord:
    assert after.last_class is not None
    return AttemptRecord(
        attempt=after.attempts, at=at, response_class=after.last_class, reason="failed"
    )


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "data" / "state.duckdb"


@pytest.fixture
def store(db_path: Path) -> Iterator[StateStore]:
    opened = StateStore.open(db_path)
    opened.start_run("r1", Site.BOOKS, Stage.FETCH, T0, '{"max_attempts": 5}')
    yield opened
    opened.close()


def mark_done(store: StateStore, n: int) -> FrontierEntry:
    before = store.get(url(n))
    assert before is not None
    after = apply_decision(before, OK, None, now=50.0)
    store.record_result("r1", before, after, ok_attempt(after))
    return after


def run_python(code: str) -> subprocess.Popen[str]:
    env = {**os.environ, "PYTHONPATH": SRC}
    return subprocess.Popen(  # noqa: S603 - our own interpreter, our own code
        [sys.executable, "-c", textwrap.dedent(code)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


# --- schema and opening ----------------------------------------------------------------------


def test_open_creates_parent_dirs_and_migrations_are_idempotent(db_path: Path) -> None:
    with StateStore.open(db_path) as first:
        assert first.schema_version() == SCHEMA_VERSION
        first.start_run("r1", Site.BOOKS, Stage.DISCOVER, T0, "{}")
        assert first.add_entries([entry(1)], "r1") == 1
    with StateStore.open(db_path) as second:
        assert second.schema_version() == SCHEMA_VERSION
        rows = second._con.execute("SELECT version FROM schema_version").fetchall()
        assert rows == [(SCHEMA_VERSION,)]
        assert second.get(url(1)) == entry(1)


def test_newer_schema_is_refused(db_path: Path) -> None:
    with StateStore.open(db_path) as store:
        store._con.execute("INSERT INTO schema_version VALUES (?)", [SCHEMA_VERSION + 1])
    with pytest.raises(SchemaVersionError, match=f"schema version {SCHEMA_VERSION + 1}"):
        StateStore.open(db_path)
    # The refused open released the file: a raw connection can still get at it.
    con = duckdb.connect(str(db_path))
    con.execute("DELETE FROM schema_version WHERE version > ?", [SCHEMA_VERSION])
    con.close()
    with StateStore.open(db_path) as store:
        assert store.schema_version() == SCHEMA_VERSION


def test_failed_migration_rolls_back_and_releases_the_file(
    db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    broken = ((1, ("CREATE TABLE half_done (x INTEGER)", "CREATE TABLE broken (")),)
    monkeypatch.setattr(state_module, "MIGRATIONS", broken)
    with pytest.raises(duckdb.ParserException):
        StateStore.open(db_path)
    monkeypatch.undo()
    with StateStore.open(db_path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        tables = {row[0] for row in store._con.execute("SHOW TABLES").fetchall()}
        assert "half_done" not in tables
        assert {"runs", "frontier", "fetch_attempts", "domain_events"} <= tables


def test_second_process_gets_state_locked_error(db_path: Path) -> None:
    holder = run_python(
        f"""
        import sys
        from pathlib import Path
        from scraping_pipeline.storage.state import StateStore
        store = StateStore.open(Path({str(db_path)!r}))
        print("ready", flush=True)
        sys.stdin.read()
        store.close()
        """
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        with pytest.raises(StateLockedError, match="another pipeline process is using"):
            StateStore.open(db_path)
    finally:
        holder.communicate("", timeout=30)
    assert holder.returncode == 0
    with StateStore.open(db_path) as store:  # free again once the holder exits
        assert store.schema_version() == SCHEMA_VERSION


def test_second_open_in_one_process_is_refused_until_closed_or_dropped(db_path: Path) -> None:
    first = StateStore.open(db_path)
    with pytest.raises(StateLockedError, match="already open in this process"):
        StateStore.open(db_path)
    first.close()
    first.close()  # idempotent
    second = StateStore.open(db_path)
    del second
    gc.collect()
    with StateStore.open(db_path) as third:
        assert third.schema_version() == SCHEMA_VERSION


def test_io_errors_other_than_the_lock_propagate(tmp_path: Path) -> None:
    directory = tmp_path / "not-a-file"
    directory.mkdir()
    with pytest.raises(duckdb.IOException, match="directory"):
        StateStore.open(directory)


def test_check_constraints_reject_unknown_enum_values(store: StateStore) -> None:
    store.add_entries([entry(1)], "r1")
    with pytest.raises(duckdb.ConstraintException):
        store._con.execute("UPDATE frontier SET status = 'archived' WHERE url = ?", [url(1)])
    with pytest.raises(duckdb.ConstraintException):
        store._con.execute("UPDATE frontier SET last_class = 'teapot' WHERE url = ?", [url(1)])


def test_state_store_is_exported_from_the_package() -> None:
    assert storage.StateStore is StateStore
    assert storage.StaleEntryError is StaleEntryError
    assert "StateLockedError" in storage.__all__


# --- runs ------------------------------------------------------------------------------------


def test_run_lifecycle(store: StateStore) -> None:
    run = store.get_run("r1")
    assert run is not None
    assert (run.status, run.stage, run.finished_at, run.summary_json) == (
        RunStatus.RUNNING,
        Stage.FETCH,
        None,
        None,
    )
    store.finish_run("r1", T1, RunStatus.INTERRUPTED, '{"done": 3}')
    run = store.get_run("r1")
    assert run is not None
    assert (run.status, run.finished_at, run.summary_json) == (
        RunStatus.INTERRUPTED,
        T1,
        '{"done": 3}',
    )
    assert store.get_run("nope") is None


def test_runs_are_listed_in_start_order(store: StateStore) -> None:
    store.start_run("r0", Site.BOOKS, Stage.DISCOVER, "2026-11-24T09:00:00+00:00", "{}")
    store.start_run("q1", Site.QUOTES, Stage.FETCH, T0, "{}")
    assert [run.run_id for run in store.runs(Site.BOOKS)] == ["r0", "r1"]
    assert [run.run_id for run in store.runs(Site.QUOTES)] == ["q1"]


def test_run_errors(store: StateStore) -> None:
    with pytest.raises(RunError, match="already exists"):
        store.start_run("r1", Site.BOOKS, Stage.FETCH, T0, "{}")
    with pytest.raises(RunError, match="unknown run"):
        store.finish_run("nope", T1, RunStatus.FAILED, None)
    with pytest.raises(ValueError, match="final status"):
        store.finish_run("r1", T1, RunStatus.RUNNING, None)
    store.finish_run("r1", T1, RunStatus.SUCCEEDED, None)
    with pytest.raises(RunError, match="already succeeded"):
        store.finish_run("r1", T1, RunStatus.FAILED, None)
    with pytest.raises(RunError, match="start_run first"):
        store.add_entries([entry(1)], "ghost")


@pytest.mark.parametrize(
    ("started_at", "settings_json", "message"),
    [
        ("yesterday", "{}", "ISO 8601"),
        ("2026-11-24T10:00:00", "{}", "UTC offset"),
        (T0, "{not json", "valid JSON"),
    ],
)
def test_run_inputs_are_validated(
    store: StateStore, started_at: str, settings_json: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        store.start_run("r2", Site.BOOKS, Stage.FETCH, started_at, settings_json)
    assert store.get_run("r2") is None


def test_finish_inputs_are_validated(store: StateStore) -> None:
    with pytest.raises(ValueError, match="UTC offset"):
        store.finish_run("r1", "2026-11-24T10:05:00", RunStatus.FAILED, None)
    with pytest.raises(ValueError, match="valid JSON"):
        store.finish_run("r1", T1, RunStatus.FAILED, "{oops")
    run = store.get_run("r1")
    assert run is not None
    assert run.status is RunStatus.RUNNING


# --- discovery -------------------------------------------------------------------------------


def test_add_entries_is_insert_only_and_idempotent(store: StateStore) -> None:
    assert store.add_entries([entry(1), entry(2)], "r1") == 2
    assert store.add_entries([entry(1), entry(2)], "r1") == 0
    done = mark_done(store, 1)
    # Re-discovering a finished URL, even with different fields, never resets or overwrites it.
    assert store.add_entries([entry(1, depth=7, kind=PageKind.DETAIL), entry(3)], "r1") == 1
    assert store.get(url(1)) == done
    assert store.counts(Site.BOOKS)[FrontierStatus.DONE] == 1


def test_add_entries_keeps_the_first_of_duplicates_in_one_batch(store: StateStore) -> None:
    assert store.add_entries([entry(1, depth=1), entry(1, depth=2)], "r1") == 1
    stored = store.get(url(1))
    assert stored is not None
    assert stored.depth == 1


@pytest.mark.parametrize("bad", ["not a url", f"{BOOKS}catalogue/page-1.html#frag"])
def test_add_entries_rejects_non_canonical_urls_and_writes_nothing(
    store: StateStore, bad: str
) -> None:
    with pytest.raises(ValueError, match="frontier URL"):
        store.add_entries([entry(1), replace(entry(2), url=bad)], "r1")
    assert store.entries(Site.BOOKS) == []


@pytest.mark.parametrize("status", list(FrontierStatus))
@pytest.mark.parametrize("last_class", [None, *ResponseClass])
def test_every_field_round_trips(
    store: StateStore, status: FrontierStatus, last_class: ResponseClass | None
) -> None:
    original = FrontierEntry(
        url=url(1),
        site=Site.QUOTES_JS,
        kind=PageKind.AUTHOR,
        depth=3,
        status=status,
        attempts=4,
        next_attempt_at=1_764_000_000.125 if last_class else None,
        last_class=last_class,
        last_reason="why" if last_class else None,
    )
    store.add_entries([original], "r1")
    assert store.get(url(1)) == original
    assert store.entries(Site.QUOTES_JS) == [original]


@pytest.mark.parametrize("site", list(Site))
@pytest.mark.parametrize("kind", list(PageKind))
def test_every_site_and_kind_round_trips(store: StateStore, site: Site, kind: PageKind) -> None:
    original = FrontierEntry(url=url(9), site=site, kind=kind, depth=0)
    store.add_entries([original], "r1")
    assert store.get(url(9)) == original


def test_reads_are_ordered_and_filtered(store: StateStore) -> None:
    store.add_entries([entry(3), entry(1), entry(2)], "r1")
    store.add_entries(
        [FrontierEntry(url=f"{BOOKS}x", site=Site.QUOTES, kind=PageKind.LISTING, depth=0)], "r1"
    )
    mark_done(store, 2)
    assert [e.url for e in store.entries(Site.BOOKS)] == [url(1), url(2), url(3)]
    assert [e.url for e in store.entries(Site.BOOKS, {FrontierStatus.PENDING})] == [url(1), url(3)]
    assert store.entries(Site.BOOKS, set()) == []
    assert store.get(url(99)) is None
    counts = store.counts(Site.BOOKS)
    assert list(counts) == list(FrontierStatus)
    assert counts[FrontierStatus.PENDING] == 2
    assert counts[FrontierStatus.DONE] == 1
    assert sum(counts.values()) == 3


def test_due_filters_by_time_and_status_and_orders_nulls_first(store: StateStore) -> None:
    store.add_entries(
        [
            entry(5, next_attempt_at=15.0),
            entry(4, next_attempt_at=10.0),
            entry(3, next_attempt_at=5.0),
            entry(2),
            entry(1),
            entry(6, status=FrontierStatus.DONE),
            entry(7, status=FrontierStatus.FAILED_EXHAUSTED, next_attempt_at=1.0),
        ],
        "r1",
    )
    store.add_entries(
        [FrontierEntry(url=f"{BOOKS}q", site=Site.QUOTES, kind=PageKind.LISTING, depth=0)], "r1"
    )
    assert [e.url for e in store.due(Site.BOOKS, 10.0)] == [url(1), url(2), url(3), url(4)]
    assert [e.url for e in store.due(Site.BOOKS, 9.99)] == [url(1), url(2), url(3)]
    assert [e.url for e in store.due(Site.BOOKS, 100.0)][-1] == url(5)


# --- results ---------------------------------------------------------------------------------


def test_record_result_retry_then_done(store: StateStore) -> None:
    store.add_entries([entry(1)], "r1")
    first = store.get(url(1))
    assert first is not None
    retry = RetryDecision(RetryAction.RETRY, 2.0, "retry in 2s")
    second = apply_decision(first, SERVER_ERROR, retry, now=100.0)
    store.record_result("r1", first, second, fail_attempt(second))
    assert store.get(url(1)) == second
    assert store.due(Site.BOOKS, 101.0) == []
    store.start_run("r2", Site.BOOKS, Stage.FETCH, T1, "{}")
    third = apply_decision(second, OK, None, now=103.0)
    store.record_result("r2", second, third, ok_attempt(third, at=103.0))
    assert store.get(url(1)) == third
    history = store.attempts(url(1))
    assert [(a.run_id, a.record.attempt, a.record.response_class) for a in history] == [
        ("r1", 1, ResponseClass.SERVER_ERROR),
        ("r2", 2, ResponseClass.OK),
    ]
    assert history[1].record == ok_attempt(third, at=103.0)
    assert history[0].record.http_status is None
    assert history[0].record.elapsed_s is None


def test_record_result_without_attempt_for_robots_and_stops(store: StateStore) -> None:
    store.add_entries([entry(1), entry(2)], "r1")
    for n, mark in ((1, mark_skipped_robots), (2, mark_stopped)):
        before = store.get(url(n))
        assert before is not None
        after = mark(before, "said so")
        store.record_result("r1", before, after, None)
        assert store.get(url(n)) == after
        assert store.attempts(url(n)) == []


def test_stale_before_raises_and_writes_nothing(store: StateStore) -> None:
    store.add_entries([entry(1)], "r1")
    before = store.get(url(1))
    assert before is not None
    after = apply_decision(before, OK, None, now=1.0)
    store.record_result("r1", before, after, ok_attempt(after))
    # A second worker that read the same `before` must not overwrite the newer result.
    other = apply_decision(before, SERVER_ERROR, RetryDecision(RetryAction.RETRY, 1.0, "r"), 2.0)
    with pytest.raises(StaleEntryError, match="stored is status=done attempts=1"):
        store.record_result("r1", before, other, fail_attempt(other))
    assert store.get(url(1)) == after
    assert len(store.attempts(url(1))) == 1


def _record_one_retry(store: StateStore) -> tuple[FrontierEntry, FrontierEntry]:
    """Store `pending, attempts=1` (a retry with attempt row 1); return (stale copy, stored)."""
    store.add_entries([entry(1)], "r1")
    stale = store.get(url(1))
    assert stale is not None
    retried = apply_decision(stale, SERVER_ERROR, RetryDecision(RetryAction.RETRY, 2.0, "r"), 1.0)
    store.record_result("r1", stale, retried, fail_attempt(retried))
    assert (retried.status, retried.attempts) == (FrontierStatus.PENDING, 1)
    return stale, retried


def test_stale_attempts_with_same_status_raises_without_attempt_record(store: StateStore) -> None:
    """Only `attempts` differs (pending in both): the stale writer must not rewind the counter."""
    stale, stored = _record_one_retry(store)
    with pytest.raises(StaleEntryError, match="expected status=pending attempts=0"):
        store.record_result("r1", stale, mark_stopped(stale, "domain stopped"), None)
    assert store.get(url(1)) == stored
    assert [a.record.attempt for a in store.attempts(url(1))] == [1]


def test_stale_attempts_with_same_status_raises_with_attempt_record(store: StateStore) -> None:
    stale, stored = _record_one_retry(store)
    late = apply_decision(stale, OK, None, now=5.0)
    with pytest.raises(StaleEntryError, match="stored is status=pending attempts=1"):
        store.record_result("r1", stale, late, ok_attempt(late))
    assert store.get(url(1)) == stored
    assert [a.record.reason for a in store.attempts(url(1))] == ["failed"]


def test_stale_when_row_is_missing(store: StateStore) -> None:
    before = entry(1)
    after = apply_decision(before, OK, None, now=1.0)
    with pytest.raises(StaleEntryError, match="stored is missing"):
        store.record_result("r1", before, after, ok_attempt(after))
    assert store.attempts(url(1)) == []


def test_record_result_is_atomic(store: StateStore) -> None:
    """The frontier update is written first; a failing attempt insert must take it back."""
    store.add_entries([entry(1)], "r1")
    before = store.get(url(1))
    assert before is not None
    # A leftover attempt row with the number the next attempt will use forces a PK collision.
    store._con.execute(
        "INSERT INTO fetch_attempts (run_id, url, attempt, attempted_at, response_class, reason) "
        "VALUES ('r0', ?, 1, 0.0, 'timeout', 'leftover')",
        [url(1)],
    )
    after = apply_decision(before, OK, None, now=1.0)
    with pytest.raises(duckdb.ConstraintException):
        store.record_result("r1", before, after, ok_attempt(after))
    assert store.get(url(1)) == before
    assert [a.record.reason for a in store.attempts(url(1))] == ["leftover"]
    # The store is still usable after the rollback.
    store.add_entries([entry(2)], "r1")
    assert store.get(url(2)) == entry(2)


def test_illegal_transition_is_refused(store: StateStore) -> None:
    store.add_entries([entry(1)], "r1")
    done = mark_done(store, 1)
    resurrected = replace(done, status=FrontierStatus.PENDING, attempts=2)
    attempt = AttemptRecord(attempt=2, at=1.0, response_class=ResponseClass.OK, reason="again")
    with pytest.raises(IllegalTransitionError, match="done -> pending"):
        store.record_result("r1", done, resurrected, attempt)
    assert store.get(url(1)) == done
    assert len(store.attempts(url(1))) == 1


@pytest.mark.parametrize(
    ("change", "attempt_number", "with_attempt", "message"),
    [
        ({"depth": 9}, 1, True, "cannot change depth"),
        ({"kind": PageKind.DETAIL}, 1, True, "cannot change kind"),
        ({}, 2, True, "does not follow"),
        ({"attempts": 2}, 2, True, "does not follow"),
        ({}, 1, False, "without an attempt record"),
        ({"last_class": ResponseClass.TIMEOUT}, 1, True, "differs from the attempt"),
    ],
)
def test_inconsistent_results_are_refused(
    store: StateStore,
    change: dict[str, object],
    attempt_number: int,
    with_attempt: bool,
    message: str,
) -> None:
    store.add_entries([entry(1)], "r1")
    before = store.get(url(1))
    assert before is not None
    after = replace(apply_decision(before, OK, None, now=1.0), **change)  # type: ignore[arg-type]
    attempt = (
        AttemptRecord(attempt=attempt_number, at=1.0, response_class=ResponseClass.OK, reason="")
        if with_attempt
        else None
    )
    with pytest.raises(ValueError, match=message):
        store.record_result("r1", before, after, attempt)
    assert store.get(url(1)) == before


# --- domain events ---------------------------------------------------------------------------


def test_domain_events_round_trip_in_order(store: StateStore) -> None:
    events = [
        DomainEvent(
            "r1",
            Site.BOOKS,
            "books.toscrape.com",
            DomainEventKind.DOMAIN_STOPPED,
            "blocked: 403",
            20.0,
            ResponseClass.BLOCKED,
        ),
        DomainEvent("r1", Site.BOOKS, "books.toscrape.com", DomainEventKind.ROBOTS, "rules", 1.0),
        DomainEvent("r1", Site.BOOKS, "a.example", DomainEventKind.BREAKER_OPENED, "5 fails", 20.0),
        DomainEvent(
            "r1", Site.QUOTES, "quotes.toscrape.com", DomainEventKind.ROBOTS, "allow_all", 0.5
        ),
    ]
    for event in events:
        store.record_domain_event(event)
    assert store.domain_events(Site.BOOKS) == [events[1], events[2], events[0]]
    assert store.domain_events(Site.QUOTES) == [events[3]]
    with pytest.raises(RunError):
        store.record_domain_event(replace(events[0], run_id="ghost"))


# --- resumability ----------------------------------------------------------------------------


def put(cache: PageCache, n: int, body: bytes = b"<html>page</html>", status: int = 200) -> str:
    meta = FetchMeta(
        final_url=url(n), status=status, fetched_at=T0, fetch_run_id="r1", fetcher="httpx"
    )
    return cache.put(url(n), body, meta).content_sha256


def object_file(cache: PageCache, digest: str) -> Path:
    return cache.root / "objects" / digest[:2] / f"{digest}.gz"


@pytest.mark.parametrize(
    ("status", "recovered"),
    [(200, True), (204, True), (299, True), (199, False), (301, False), (404, False), (503, False)],
)
def test_reconcile_recovers_only_2xx_cache_entries(
    store: StateStore, tmp_path: Path, status: int, recovered: bool
) -> None:
    cache = PageCache(tmp_path / "cache")
    store.add_entries([entry(1)], "r1")
    put(cache, 1, status=status)
    assert store.reconcile_with_cache(Site.BOOKS, cache, "r1", now_epoch=9.0) == int(recovered)
    stored = store.get(url(1))
    assert stored is not None
    if recovered:
        assert stored.status is FrontierStatus.DONE
        assert [a.record.http_status for a in store.attempts(url(1))] == [status]
    else:
        assert stored == entry(1)  # untouched: it will simply be fetched again
        assert store.attempts(url(1)) == []


@pytest.mark.parametrize("damage", ["missing", "not_gzip", "wrong_body"])
def test_reconcile_skips_entries_whose_object_is_missing_or_corrupt(
    store: StateStore, tmp_path: Path, damage: str
) -> None:
    cache = PageCache(tmp_path / "cache")
    store.add_entries([entry(1), entry(2)], "r1")
    digest = put(cache, 1, b"page one")
    put(cache, 2, b"page two")  # intact: recovered
    path = object_file(cache, digest)
    if damage == "missing":
        path.unlink()
    elif damage == "not_gzip":
        path.write_bytes(b"\x00 not gzip")
    else:
        path.write_bytes(gzip.compress(b"some other body"))
    assert store.reconcile_with_cache(Site.BOOKS, cache, "r1", now_epoch=9.0) == 1
    assert store.get(url(1)) == entry(1)
    assert store.attempts(url(1)) == []
    done = store.get(url(2))
    assert done is not None
    assert done.status is FrontierStatus.DONE


def test_reconcile_recovers_exactly_the_cached_pending_entries(
    store: StateStore, tmp_path: Path
) -> None:
    cache = PageCache(tmp_path / "cache")
    retrying = entry(2, attempts=1, next_attempt_at=500.0, last_class=ResponseClass.TIMEOUT)
    store.add_entries(
        [
            entry(1),  # pending, cached -> recovered
            retrying,  # pending with a retry scheduled, cached -> recovered
            entry(3),  # pending, not cached -> untouched
            entry(4, status=FrontierStatus.FAILED_EXHAUSTED, attempts=5),  # final, cached
            entry(5, status=FrontierStatus.STOPPED),  # final, cached
        ],
        "r1",
    )
    sha1 = put(cache, 1)
    put(cache, 2, b"other")
    put(cache, 4)
    put(cache, 5)
    put(cache, 6)  # cached, not in the frontier at all
    store.start_run("r2", Site.BOOKS, Stage.FETCH, T1, "{}")
    before = {e.url: e for e in store.entries(Site.BOOKS)}

    assert store.reconcile_with_cache(Site.BOOKS, cache, "r2", now_epoch=900.0) == 2

    recovered = store.get(url(1))
    assert recovered == replace(
        entry(1),
        status=FrontierStatus.DONE,
        attempts=1,
        last_class=ResponseClass.OK,
        last_reason=RECOVERED_REASON,
    )
    second = store.get(url(2))
    assert second is not None
    assert (second.status, second.attempts, second.next_attempt_at) == (
        FrontierStatus.DONE,
        2,
        None,
    )
    [synthetic] = store.attempts(url(1))
    assert synthetic.run_id == "r2"
    assert synthetic.record == AttemptRecord(
        attempt=1,
        at=900.0,
        response_class=ResponseClass.OK,
        reason=RECOVERED_REASON,
        http_status=200,
        elapsed_s=None,
        content_sha256=sha1,
    )
    for n in (3, 4, 5):
        assert store.get(url(n)) == before[url(n)]
        assert store.attempts(url(n)) == []
    assert store.get(url(6)) is None
    assert store.reconcile_with_cache(Site.BOOKS, cache, "r2", now_epoch=901.0) == 0


def test_reset_for_retry_touches_only_the_given_statuses(store: StateStore) -> None:
    store.add_entries(
        [
            entry(
                1,
                status=FrontierStatus.FAILED_EXHAUSTED,
                attempts=5,
                last_class=ResponseClass.TIMEOUT,
                last_reason="gave up",
            ),
            entry(2, status=FrontierStatus.STOPPED, attempts=1),
            entry(3, status=FrontierStatus.FAILED_PERMANENT, attempts=1),
            entry(4, status=FrontierStatus.DONE, attempts=1),
            entry(5, status=FrontierStatus.SKIPPED_ROBOTS),
            entry(6, next_attempt_at=50.0, attempts=1),
        ],
        "r1",
    )
    store.add_entries(
        [
            FrontierEntry(
                url=f"{BOOKS}q",
                site=Site.QUOTES,
                kind=PageKind.LISTING,
                depth=0,
                status=FrontierStatus.STOPPED,
            )
        ],
        "r1",
    )
    untouched = {n: store.get(url(n)) for n in (3, 4, 5, 6)}
    store.start_run("r2", Site.BOOKS, Stage.FETCH, T1, "{}")

    assert store.reset_for_retry(Site.BOOKS, "r2") == 2

    reset = store.get(url(1))
    assert reset == replace(
        entry(1),
        attempts=5,  # kept: history is never renumbered
        last_class=ResponseClass.TIMEOUT,
        last_reason="reset for retry from failed_exhausted",
    )
    stopped = store.get(url(2))
    assert stopped is not None
    assert stopped.status is FrontierStatus.PENDING
    for n, original in untouched.items():
        assert store.get(url(n)) == original
    quotes = store.get(f"{BOOKS}q")
    assert quotes is not None
    assert quotes.status is FrontierStatus.STOPPED

    # The next attempt continues the numbering.
    after = apply_decision(reset, OK, None, now=1.0)
    store.record_result("r2", reset, after, ok_attempt(after))
    assert [a.record.attempt for a in store.attempts(url(1))] == [6]

    assert store.reset_for_retry(Site.BOOKS, "r2", {FrontierStatus.FAILED_PERMANENT}) == 1
    assert store.reset_for_retry(Site.BOOKS, "r2", set()) == 0


@pytest.mark.parametrize("status", [FrontierStatus.DONE, FrontierStatus.PENDING])
def test_reset_for_retry_refuses_done_and_pending(
    store: StateStore, status: FrontierStatus
) -> None:
    store.add_entries([entry(1, status=status)], "r1")
    with pytest.raises(ValueError, match=status.value):
        store.reset_for_retry(Site.BOOKS, "r1", {status, FrontierStatus.STOPPED})
    stored = store.get(url(1))
    assert stored is not None
    assert stored.status is status


# --- durability ------------------------------------------------------------------------------


def test_committed_rows_survive_sigkill(db_path: Path) -> None:
    writer = run_python(
        f"""
        import sys, time
        from pathlib import Path
        from scraping_pipeline.core.frontier import FrontierEntry
        from scraping_pipeline.core.types import PageKind, Site
        from scraping_pipeline.storage.state import Stage, StateStore
        store = StateStore.open(Path({str(db_path)!r}))
        store.start_run("r1", Site.BOOKS, Stage.DISCOVER, {T0!r}, "{{}}")
        urls = [f"https://books.toscrape.com/catalogue/page-{{n}}.html" for n in range(50)]
        store.add_entries(
            [FrontierEntry(url=u, site=Site.BOOKS, kind=PageKind.LISTING, depth=1) for u in urls],
            "r1",
        )
        print("written", flush=True)
        time.sleep(60)  # never closes: killed below
        """
    )
    assert writer.stdout is not None
    assert writer.stdout.readline().strip() == "written"
    writer.send_signal(signal.SIGKILL)
    writer.wait(timeout=30)
    assert writer.returncode == -signal.SIGKILL
    with StateStore.open(db_path) as store:
        assert len(store.entries(Site.BOOKS)) == 50
        run = store.get_run("r1")
        assert run is not None
        assert run.status is RunStatus.RUNNING  # it never finished: the report can say so
