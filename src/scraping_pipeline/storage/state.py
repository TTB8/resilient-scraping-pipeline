"""The run-state store: one DuckDB file that records how far the pipeline has got, and why.

**The database is the source of truth for progress.** The frontier (every known URL and its status),
every fetch attempt, every domain event (robots outcomes, opened breakers, stopped domains) and every
run record live here. Nothing about progress is kept only in memory, so a crash or a Ctrl-C loses at
most the attempt that was in flight, and a re-run asks :meth:`StateStore.due` what is still missing
instead of starting over.

Four rules make that safe:

* **Insert-only discovery.** :meth:`StateStore.add_entries` never overwrites or resets a URL it
  already knows. Running ``discover`` twice, or after ``fetch`` has finished half the frontier,
  therefore cannot turn a ``DONE`` page back into ``PENDING`` or forget why a page failed.
* **One transaction per result, with optimistic concurrency.** :meth:`StateStore.record_result`
  updates the frontier row and inserts the attempt row together or not at all, and the update only
  matches when the row still has the ``attempts`` and ``status`` the caller read
  (``UPDATE ... WHERE url = ? AND attempts = ? AND status = ?``). A caller holding a stale copy gets
  :class:`StaleEntryError` instead of silently overwriting a newer result. The ``before -> after`` edge
  is checked against :data:`~scraping_pipeline.core.frontier.TRANSITIONS` here as well as in the core
  (defence in depth: storage must never be the place a final page is resurrected).
* **The reconcile seam.** The fetcher writes the page to the cache *before* it records the result
  here. A crash between the two leaves a page that is cached but still ``PENDING``.
  :meth:`StateStore.reconcile_with_cache` closes that gap at the start of a run: such entries are
  marked ``DONE`` without a refetch, with a synthetic attempt row (reason
  :data:`RECOVERED_REASON`) so lineage says honestly where the ``DONE`` came from.
* **Final means final, unless an operator says otherwise.** Default runs never re-fetch final
  entries. :meth:`StateStore.reset_for_retry` is the one, opt-in, way back to ``PENDING``.

**Why DuckDB.** A single file next to the cache, no server to run; SQL over the frontier and the
attempts is exactly what the run report needs (counts by status, failures by class); and its
columnar engine exports any table to Parquet in one statement when someone wants to analyse a crawl.
DuckDB allows one writing process per file; a second process gets :class:`StateLockedError`, and so
does a second :meth:`StateStore.open` of the same file inside one process (two stores in one process
would share one database instance and quietly race).

**Time.** No clock is read here; every timestamp is passed in. Frontier, attempt and domain-event
times are **Unix epoch seconds** (``float``), the unit :mod:`scraping_pipeline.core.frontier` uses.
Run ``started_at`` / ``finished_at`` are **ISO 8601 strings with a UTC offset**, like the run report
and the page cache.

**Schema.** Versioned: :data:`MIGRATIONS` is an ordered list applied idempotently on open, each in
its own transaction together with its ``schema_version`` row. A database written by newer code
(a higher version than :data:`SCHEMA_VERSION`) is refused with :class:`SchemaVersionError` rather
than misread. A shipped migration is never edited; a change is a new migration. Enum CHECK
constraints spell their values out for that reason, and a test round-trips every enum member so a
new value without a migration fails loudly.

All SQL takes values as parameters only. All reads are ordered, so results never depend on storage
order.
"""

import json
import threading
import weakref
from collections.abc import Iterable, Iterator, Sequence
from collections.abc import Set as AbstractSet
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import duckdb

from scraping_pipeline.core.frontier import (
    FINAL_STATES,
    TRANSITIONS,
    FrontierEntry,
    IllegalTransitionError,
)
from scraping_pipeline.core.types import FrontierStatus, PageKind, ResponseClass, Site
from scraping_pipeline.core.urls import InvalidUrlError, canonicalize
from scraping_pipeline.storage.cache import CacheEntry, CorruptCacheError, PageCache

__all__ = [
    "DEFAULT_RESET_STATUSES",
    "MIGRATIONS",
    "RECOVERED_REASON",
    "SCHEMA_VERSION",
    "AttemptRecord",
    "DomainEvent",
    "DomainEventKind",
    "RunError",
    "RunRecord",
    "RunStatus",
    "SchemaVersionError",
    "Stage",
    "StaleEntryError",
    "StateError",
    "StateLockedError",
    "StateStore",
    "StoredAttempt",
]

#: ``last_reason`` and attempt ``reason`` of an entry recovered by ``reconcile_with_cache``.
RECOVERED_REASON = "recovered from cache"

#: What :meth:`StateStore.reset_for_retry` re-queues when the caller names no statuses.
DEFAULT_RESET_STATUSES: frozenset[FrontierStatus] = frozenset(
    {FrontierStatus.FAILED_EXHAUSTED, FrontierStatus.STOPPED}
)


class Stage(StrEnum):
    """The pipeline stage a run record belongs to."""

    DISCOVER = "discover"
    FETCH = "fetch"
    PARSE = "parse"
    VALIDATE = "validate"
    EXPORT = "export"


class RunStatus(StrEnum):
    """Lifecycle of a run record. ``RUNNING`` until :meth:`StateStore.finish_run`."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"  # Ctrl-C or a signal: progress so far is kept, re-run resumes


class DomainEventKind(StrEnum):
    """Something that happened to a whole domain rather than to one URL."""

    ROBOTS = "robots"  # robots.txt fetched or unavailable; ``reason`` says which and the mode
    BREAKER_OPENED = "breaker_opened"
    DOMAIN_STOPPED = "domain_stopped"  # blocked, challenged, breaker tripped, Retry-After too long


# --- errors ----------------------------------------------------------------------------------


class StateError(Exception):
    """Base class for run-state store errors."""


class StateLockedError(StateError):
    """Another pipeline process (or another open store in this one) holds the database."""


class SchemaVersionError(StateError):
    """The database was written by newer code than this one."""


class StaleEntryError(StateError):
    """``record_result`` was given a ``before`` that no longer matches the stored row."""


class RunError(StateError):
    """A run id is unknown, already used, or not in the state the call needs."""


# --- records ---------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """One finished fetch attempt of one URL. ``at`` is epoch seconds; ``attempt`` is 1-based.

    ``elapsed_s`` is ``None`` for synthetic attempts (recovered from the cache); ``http_status`` is
    ``None`` when no response arrived (timeout, network error); ``content_sha256`` is set when a body
    was stored in the cache.
    """

    attempt: int
    at: float
    response_class: ResponseClass
    reason: str
    http_status: int | None = None
    elapsed_s: float | None = None
    content_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class StoredAttempt:
    """An :class:`AttemptRecord` as read back, with the run and URL it belongs to."""

    run_id: str
    url: str
    record: AttemptRecord


@dataclass(frozen=True, slots=True)
class DomainEvent:
    """A domain-level event. ``at`` is epoch seconds."""

    run_id: str
    site: Site
    domain: str
    event: DomainEventKind
    reason: str
    at: float
    response_class: ResponseClass | None = None


@dataclass(frozen=True, slots=True)
class RunRecord:
    """One run of one stage for one site. Timestamps are ISO 8601 with a UTC offset."""

    run_id: str
    site: Site
    stage: Stage
    started_at: str
    status: RunStatus
    settings_json: str
    finished_at: str | None = None
    summary_json: str | None = None


# --- schema ----------------------------------------------------------------------------------

_V1 = (
    """
    CREATE TABLE runs (
        run_id        VARCHAR PRIMARY KEY CHECK (length(run_id) > 0),
        site          VARCHAR NOT NULL CHECK (site IN ('books', 'quotes', 'quotes-js')),
        stage         VARCHAR NOT NULL
                      CHECK (stage IN ('discover', 'fetch', 'parse', 'validate', 'export')),
        started_at    VARCHAR NOT NULL,
        finished_at   VARCHAR,
        status        VARCHAR NOT NULL
                      CHECK (status IN ('running', 'succeeded', 'failed', 'interrupted')),
        settings_json VARCHAR NOT NULL,
        summary_json  VARCHAR
    )
    """,
    """
    CREATE TABLE frontier (
        url               VARCHAR PRIMARY KEY,
        site              VARCHAR NOT NULL CHECK (site IN ('books', 'quotes', 'quotes-js')),
        kind              VARCHAR NOT NULL CHECK (kind IN ('listing', 'detail', 'author')),
        depth             INTEGER NOT NULL CHECK (depth >= 0),
        status            VARCHAR NOT NULL CHECK (status IN (
                              'pending', 'done', 'failed_permanent', 'failed_exhausted',
                              'skipped_robots', 'stopped')),
        attempts          INTEGER NOT NULL CHECK (attempts >= 0),
        next_attempt_at   DOUBLE,
        last_class        VARCHAR CHECK (last_class IN (
                              'ok', 'not_found', 'client_error', 'rate_limited', 'blocked',
                              'challenge', 'server_error', 'timeout', 'network_error')),
        last_reason       VARCHAR,
        discovered_run_id VARCHAR NOT NULL,
        updated_run_id    VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE fetch_attempts (
        run_id         VARCHAR NOT NULL,
        url            VARCHAR NOT NULL,
        attempt        INTEGER NOT NULL CHECK (attempt >= 1),
        attempted_at   DOUBLE NOT NULL,
        response_class VARCHAR NOT NULL CHECK (response_class IN (
                           'ok', 'not_found', 'client_error', 'rate_limited', 'blocked',
                           'challenge', 'server_error', 'timeout', 'network_error')),
        http_status    INTEGER CHECK (http_status BETWEEN 100 AND 599),
        reason         VARCHAR NOT NULL,
        elapsed_s      DOUBLE CHECK (elapsed_s >= 0),
        content_sha256 VARCHAR CHECK (regexp_full_match(content_sha256, '[0-9a-f]{64}')),
        PRIMARY KEY (url, attempt)
    )
    """,
    """
    CREATE TABLE domain_events (
        run_id         VARCHAR NOT NULL,
        site           VARCHAR NOT NULL CHECK (site IN ('books', 'quotes', 'quotes-js')),
        domain         VARCHAR NOT NULL,
        event          VARCHAR NOT NULL
                       CHECK (event IN ('robots', 'breaker_opened', 'domain_stopped')),
        reason         VARCHAR NOT NULL,
        response_class VARCHAR CHECK (response_class IN (
                           'ok', 'not_found', 'client_error', 'rate_limited', 'blocked',
                           'challenge', 'server_error', 'timeout', 'network_error')),
        occurred_at    DOUBLE NOT NULL
    )
    """,
    "CREATE INDEX frontier_site_status ON frontier (site, status)",
)

MIGRATIONS: tuple[tuple[int, tuple[str, ...]], ...] = ((1, _V1),)
"""``(version, statements)`` in order. Never edit a shipped entry; append a new one."""

SCHEMA_VERSION = MIGRATIONS[-1][0]
"""The newest schema this code understands."""


_SELECT_FRONTIER = "SELECT url, site, kind, depth, status, attempts, next_attempt_at, last_class, last_reason FROM frontier "

# One open store per database file per process (see the module docstring).
_OPEN: weakref.WeakValueDictionary[Path, "StateStore"] = weakref.WeakValueDictionary()
_OPEN_LOCK = threading.Lock()


class StateStore:
    """The run-state database. Create with :meth:`open`; use as a context manager."""

    def __init__(self, path: Path, con: duckdb.DuckDBPyConnection) -> None:
        # Use StateStore.open(): it takes the lock, registers the store and runs migrations.
        self.path = path
        self._con = con
        self._closed = False

    # --- lifecycle ---------------------------------------------------------------------------

    @classmethod
    def open(cls, path: Path) -> Self:
        """Open (creating if needed) the database at ``path`` and bring its schema up to date.

        Raises :class:`StateLockedError` when another process, or another open store in this
        process, uses the file, and :class:`SchemaVersionError` when the file is newer than
        :data:`SCHEMA_VERSION`.
        """
        resolved = Path(path).resolve()
        with _OPEN_LOCK:
            existing = _OPEN.get(resolved)
            if existing is not None and not existing._closed:
                raise StateLockedError(f"{resolved} is already open in this process")
            resolved.parent.mkdir(parents=True, exist_ok=True)
            try:
                con = duckdb.connect(str(resolved))
            except duckdb.IOException as exc:
                if "lock" in str(exc).lower():
                    raise StateLockedError(f"another pipeline process is using {resolved}") from exc
                raise
            store = cls(resolved, con)
            try:
                store._migrate()
            except BaseException:
                con.close()
                raise
            _OPEN[resolved] = store
        return store

    def close(self) -> None:
        """Close the connection. Idempotent. Everything committed is already durable."""
        if not self._closed:
            self._closed = True
            self._con.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def schema_version(self) -> int:
        """The schema version recorded in the database."""
        return self._stored_version()

    def _stored_version(self) -> int:
        row = self._one("SELECT coalesce(max(version), 0) FROM schema_version")
        return int(row[0]) if row is not None else 0

    def _migrate(self) -> None:
        self._con.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
        current = self._stored_version()
        if current > SCHEMA_VERSION:
            raise SchemaVersionError(
                f"{self.path} has schema version {current}; this code knows up to "
                f"{SCHEMA_VERSION}. Upgrade the pipeline before using this state file."
            )
        for version, statements in MIGRATIONS:
            if version <= current:
                continue
            with self._transaction():
                for statement in statements:
                    self._con.execute(statement)
                self._con.execute("INSERT INTO schema_version VALUES (?)", [version])

    # --- runs --------------------------------------------------------------------------------

    def start_run(
        self, run_id: str, site: Site, stage: Stage, started_at: str, settings_json: str
    ) -> RunRecord:
        """Record a new ``RUNNING`` run. ``started_at`` is ISO 8601 with an offset.

        Raises :class:`RunError` when ``run_id`` is already used.
        """
        _require_iso(started_at, "started_at")
        _require_json(settings_json, "settings_json")
        if self._run_status(run_id) is not None:
            raise RunError(f"run {run_id!r} already exists")
        record = RunRecord(
            run_id=run_id,
            site=Site(site),
            stage=Stage(stage),
            started_at=started_at,
            status=RunStatus.RUNNING,
            settings_json=settings_json,
        )
        self._con.execute(
            "INSERT INTO runs (run_id, site, stage, started_at, status, settings_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [run_id, record.site.value, record.stage.value, started_at, "running", settings_json],
        )
        return record

    def finish_run(
        self, run_id: str, finished_at: str, status: RunStatus, summary_json: str | None
    ) -> None:
        """Close a ``RUNNING`` run with a final ``status``. Raises :class:`RunError` otherwise."""
        _require_iso(finished_at, "finished_at")
        if summary_json is not None:
            _require_json(summary_json, "summary_json")
        status = RunStatus(status)
        if status is RunStatus.RUNNING:
            raise ValueError("finish_run needs a final status, not 'running'")
        current = self._run_status(run_id)
        if current is None:
            raise RunError(f"unknown run {run_id!r}")
        if current is not RunStatus.RUNNING:
            raise RunError(f"run {run_id!r} is already {current}")
        self._con.execute(
            "UPDATE runs SET finished_at = ?, status = ?, summary_json = ? WHERE run_id = ?",
            [finished_at, status.value, summary_json, run_id],
        )

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self._one(
            "SELECT run_id, site, stage, started_at, status, settings_json, finished_at, "
            "summary_json FROM runs WHERE run_id = ?",
            [run_id],
        )
        if row is None:
            return None
        return RunRecord(
            run_id=row[0],
            site=Site(row[1]),
            stage=Stage(row[2]),
            started_at=row[3],
            status=RunStatus(row[4]),
            settings_json=row[5],
            finished_at=row[6],
            summary_json=row[7],
        )

    def runs(self, site: Site) -> list[RunRecord]:
        """Every run of ``site``, ordered by ``(started_at, run_id)``."""
        ids = self._con.execute(
            "SELECT run_id FROM runs WHERE site = ? ORDER BY started_at, run_id", [Site(site).value]
        ).fetchall()
        return [run for (run_id,) in ids if (run := self.get_run(run_id)) is not None]

    # --- frontier: discovery and reads -------------------------------------------------------

    def add_entries(self, entries: Iterable[FrontierEntry], run_id: str) -> int:
        """Insert entries whose URL is not known yet; return how many were inserted.

        Insert-only: an existing URL is never overwritten or reset, whatever its status, and a URL
        repeated within ``entries`` keeps its first occurrence. That makes ``discover`` idempotent.
        URLs must be canonical (:func:`~scraping_pipeline.core.urls.canonicalize`).
        """
        self._require_run(run_id)
        batch = list(entries)
        for entry in batch:
            _require_canonical(entry.url)
        inserted = 0
        with self._transaction():
            for entry in batch:
                rows = self._con.execute(
                    "INSERT INTO frontier (url, site, kind, depth, status, attempts, next_attempt_at, last_class, last_reason, "
                    "discovered_run_id, updated_run_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (url) DO NOTHING RETURNING url",
                    [*_entry_params(entry), run_id, run_id],
                ).fetchall()
                inserted += len(rows)
        return inserted

    def get(self, url: str) -> FrontierEntry | None:
        row = self._one(_SELECT_FRONTIER + "WHERE url = ?", [url])
        return None if row is None else _entry_from_row(row)

    def entries(
        self, site: Site, statuses: AbstractSet[FrontierStatus] | None = None
    ) -> list[FrontierEntry]:
        """Entries of ``site`` (optionally only those in ``statuses``), ordered by URL."""
        if statuses is None:
            rows = self._con.execute(
                _SELECT_FRONTIER + "WHERE site = ? ORDER BY url", [Site(site).value]
            ).fetchall()
        else:
            values = sorted(FrontierStatus(status).value for status in statuses)
            rows = self._con.execute(
                _SELECT_FRONTIER + "WHERE site = ? AND list_contains(?, status) ORDER BY url",
                [Site(site).value, values],
            ).fetchall()
        return [_entry_from_row(row) for row in rows]

    def due(self, site: Site, now_epoch: float) -> list[FrontierEntry]:
        """``PENDING`` entries whose retry time has come (``next_attempt_at`` unset or ``<= now``).

        Ordered by ``(next_attempt_at NULLS FIRST, url)``: never-tried URLs first, then the
        longest-waiting retries. Matches :func:`scraping_pipeline.core.frontier.is_due`.
        """
        rows = self._con.execute(
            _SELECT_FRONTIER + "WHERE site = ? AND status = 'pending' "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "ORDER BY next_attempt_at ASC NULLS FIRST, url",
            [Site(site).value, now_epoch],
        ).fetchall()
        return [_entry_from_row(row) for row in rows]

    def counts(self, site: Site) -> dict[FrontierStatus, int]:
        """Entries per status for ``site``. Every status is present (zero when none), in enum order."""
        rows = self._con.execute(
            "SELECT status, count(*) FROM frontier WHERE site = ? GROUP BY status",
            [Site(site).value],
        ).fetchall()
        found = {FrontierStatus(status): int(count) for status, count in rows}
        return {status: found.get(status, 0) for status in FrontierStatus}

    # --- frontier: results -------------------------------------------------------------------

    def record_result(
        self,
        run_id: str,
        before: FrontierEntry,
        after: FrontierEntry,
        attempt: AttemptRecord | None,
    ) -> None:
        """Persist one outcome for one URL in a single transaction.

        ``after`` is what the core produced from ``before`` (:func:`~.frontier.apply_decision`, or
        :func:`~.frontier.mark_stopped` / :func:`~.frontier.mark_skipped_robots`). ``attempt`` is the
        attempt that led there, or ``None`` for outcomes without one (robots, stopped domain).

        Raises :class:`IllegalTransitionError` for an edge outside ``TRANSITIONS``, ``ValueError``
        when ``after`` / ``attempt`` are inconsistent with ``before``, and :class:`StaleEntryError`
        when the stored row no longer has ``before``'s ``attempts`` and ``status``. On any error
        nothing is written.
        """
        self._require_run(run_id)
        _check_result(before, after, attempt)
        with self._transaction():
            self._apply_result(run_id, before, after, attempt)

    def _apply_result(
        self,
        run_id: str,
        before: FrontierEntry,
        after: FrontierEntry,
        attempt: AttemptRecord | None,
    ) -> None:
        # The frontier update comes first and the attempt insert second, inside one transaction:
        # if the insert fails (say, a duplicate attempt number), the update must not survive.
        updated = self._con.execute(
            "UPDATE frontier SET status = ?, attempts = ?, next_attempt_at = ?, last_class = ?, "
            "last_reason = ?, updated_run_id = ? "
            "WHERE url = ? AND attempts = ? AND status = ? RETURNING url",
            [
                after.status.value,
                after.attempts,
                after.next_attempt_at,
                _opt_value(after.last_class),
                after.last_reason,
                run_id,
                before.url,
                before.attempts,
                before.status.value,
            ],
        ).fetchall()
        if not updated:
            stored = self.get(before.url)
            raise StaleEntryError(
                f"{before.url}: expected status={before.status} attempts={before.attempts}, "
                f"stored is {_describe(stored)}"
            )
        if attempt is not None:
            self._con.execute(
                "INSERT INTO fetch_attempts (run_id, url, attempt, attempted_at, response_class, http_status, reason, elapsed_s, content_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    before.url,
                    attempt.attempt,
                    attempt.at,
                    ResponseClass(attempt.response_class).value,
                    attempt.http_status,
                    attempt.reason,
                    attempt.elapsed_s,
                    attempt.content_sha256,
                ],
            )

    def attempts(self, url: str) -> list[StoredAttempt]:
        """Every recorded attempt of ``url``, ordered by attempt number."""
        rows = self._con.execute(
            "SELECT run_id, url, attempt, attempted_at, response_class, http_status, reason, elapsed_s, content_sha256 "
            "FROM fetch_attempts WHERE url = ? ORDER BY attempt",
            [url],
        ).fetchall()
        return [
            StoredAttempt(
                run_id=row[0],
                url=row[1],
                record=AttemptRecord(
                    attempt=row[2],
                    at=row[3],
                    response_class=ResponseClass(row[4]),
                    http_status=row[5],
                    reason=row[6],
                    elapsed_s=row[7],
                    content_sha256=row[8],
                ),
            )
            for row in rows
        ]

    # --- domain events -----------------------------------------------------------------------

    def record_domain_event(self, event: DomainEvent) -> None:
        """Append a domain event (robots outcome, breaker opened, domain stopped)."""
        self._require_run(event.run_id)
        self._con.execute(
            "INSERT INTO domain_events (run_id, site, domain, event, reason, response_class, occurred_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                event.run_id,
                Site(event.site).value,
                event.domain,
                DomainEventKind(event.event).value,
                event.reason,
                _opt_value(event.response_class),
                event.at,
            ],
        )

    def domain_events(self, site: Site) -> list[DomainEvent]:
        """Events of ``site``, ordered by ``(at, domain, event, reason, run_id)``."""
        rows = self._con.execute(
            "SELECT run_id, site, domain, event, reason, response_class, occurred_at FROM domain_events "
            "WHERE site = ? ORDER BY occurred_at, domain, event, reason, run_id, response_class NULLS FIRST",
            [Site(site).value],
        ).fetchall()
        return [
            DomainEvent(
                run_id=row[0],
                site=Site(row[1]),
                domain=row[2],
                event=DomainEventKind(row[3]),
                reason=row[4],
                response_class=None if row[5] is None else ResponseClass(row[5]),
                at=row[6],
            )
            for row in rows
        ]

    # --- resumability ------------------------------------------------------------------------

    def reconcile_with_cache(
        self, site: Site, cache: PageCache, run_id: str, now_epoch: float
    ) -> int:
        """Mark ``DONE`` every ``PENDING`` entry whose page is intact in ``cache``; return the count.

        This repairs a crash between the cache write and :meth:`record_result`. Each recovered entry
        gets one synthetic attempt (``reason`` :data:`RECOVERED_REASON`, ``at = now_epoch``, the
        cached status and body hash, no ``elapsed_s``), because a real attempt did happen; it was
        just never recorded. Final entries are never touched. Running it again recovers 0.

        A cache entry is trusted only when it could have come from a successful fetch *and* its
        body is still there: the cached status is 2xx, and :meth:`PageCache.read_body` returns the
        object (present, valid gzip, hashing to the recorded sha256). Anything else (a cached error
        page, a dangling entry whose object is gone, a corrupt object) is **left exactly as it is**:
        still ``PENDING``, no attempt row, nothing written. The entry is then simply fetched again,
        and that real attempt, with its own row, is the lineage; the fresh ``PageCache.put``
        replaces the bad entry and heals a damaged object. Recording a note instead would invent an
        attempt that never happened, so the store records nothing for an untrusted entry.
        """
        self._require_run(run_id)
        pending = self.entries(site, {FrontierStatus.PENDING})
        recovered: list[tuple[FrontierEntry, FrontierEntry, AttemptRecord]] = []
        for before in pending:
            cached = cache.get_entry(before.url)
            if cached is None or not _recoverable(cache, cached):
                continue
            after = FrontierEntry(
                url=before.url,
                site=before.site,
                kind=before.kind,
                depth=before.depth,
                status=FrontierStatus.DONE,
                attempts=before.attempts + 1,
                next_attempt_at=None,
                last_class=ResponseClass.OK,
                last_reason=RECOVERED_REASON,
            )
            attempt = AttemptRecord(
                attempt=after.attempts,
                at=now_epoch,
                response_class=ResponseClass.OK,
                reason=RECOVERED_REASON,
                http_status=cached.status,
                content_sha256=cached.content_sha256,
            )
            _check_result(before, after, attempt)
            recovered.append((before, after, attempt))
        with self._transaction():
            for before, after, attempt in recovered:
                self._apply_result(run_id, before, after, attempt)
        return len(recovered)

    def reset_for_retry(
        self,
        site: Site,
        run_id: str,
        statuses: AbstractSet[FrontierStatus] = DEFAULT_RESET_STATUSES,
    ) -> int:
        """Operator action: put final entries in ``statuses`` back to ``PENDING``; return the count.

        Opt-in only (a CLI flag); no default code path calls it. It is the one sanctioned exit from
        a final state, so it deliberately bypasses ``TRANSITIONS``. ``DONE`` cannot be reset (that
        would be a re-crawl, not a retry), and neither can ``PENDING`` (nothing to reset).

        **Attempts are kept, not reset.** ``attempts`` stays the lifetime count of attempts, which
        is also the 1-based number of the next attempt row, so history in ``fetch_attempts`` is
        never renumbered or overwritten. The retry budget is therefore lifetime too. Precisely,
        with :func:`scraping_pipeline.core.retry.decide` giving up once ``attempt >= max_attempts``:
        a reset entry with ``attempts = n`` is fetched once more as attempt ``n + 1``. If that ends
        ``ok`` it becomes ``DONE``. If it fails transiently it is retried only while
        ``n + 1 < max_attempts``. A ``FAILED_EXHAUSTED`` entry has ``n >= max_attempts``, so
        under the same ``max_attempts`` it gets **exactly one** more attempt and is exhausted again
        on a transient failure. A ``STOPPED`` entry keeps whatever budget it had left. A permanent
        or refusing response (``not_found``, ``blocked``, ...) is final again whatever the count.
        To grant more retries, run with a higher ``max_attempts``. ``next_attempt_at`` is cleared,
        ``last_class`` is kept, and ``last_reason`` says the entry was reset and from which status.
        """
        self._require_run(run_id)
        wanted = {FrontierStatus(status) for status in statuses}
        refused = wanted - (FINAL_STATES - {FrontierStatus.DONE})
        if refused:
            names = ", ".join(sorted(status.value for status in refused))
            raise ValueError(f"reset_for_retry only re-queues failed/stopped/skipped, not {names}")
        if not wanted:
            return 0
        values = sorted(status.value for status in wanted)
        with self._transaction():
            rows = self._con.execute(
                "UPDATE frontier SET status = 'pending', next_attempt_at = NULL, "
                "last_reason = 'reset for retry from ' || status, updated_run_id = ? "
                "WHERE site = ? AND list_contains(?, status) RETURNING url",
                [run_id, Site(site).value, values],
            ).fetchall()
        return len(rows)

    # --- internals ---------------------------------------------------------------------------

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._con.begin()
        try:
            yield
        except BaseException:
            self._con.rollback()
            raise
        self._con.commit()

    def _one(self, sql: str, params: Sequence[object] = ()) -> tuple[Any, ...] | None:
        return self._con.execute(sql, list(params)).fetchone()

    def _run_status(self, run_id: str) -> RunStatus | None:
        row = self._one("SELECT status FROM runs WHERE run_id = ?", [run_id])
        return None if row is None else RunStatus(row[0])

    def _require_run(self, run_id: str) -> None:
        if self._run_status(run_id) is None:
            raise RunError(f"unknown run {run_id!r}: call start_run first")


# --- helpers ---------------------------------------------------------------------------------


def _check_result(
    before: FrontierEntry, after: FrontierEntry, attempt: AttemptRecord | None
) -> None:
    identity = ("url", "site", "kind", "depth")
    changed = [name for name in identity if getattr(before, name) != getattr(after, name)]
    if changed:
        raise ValueError(f"{before.url}: a result cannot change {', '.join(changed)}")
    if after.status not in TRANSITIONS[before.status]:
        raise IllegalTransitionError(
            f"{before.url}: {before.status} -> {after.status} is not allowed"
        )
    if attempt is None:
        if after.attempts != before.attempts:
            raise ValueError(f"{before.url}: attempts changed without an attempt record")
        return
    if after.attempts != before.attempts + 1 or attempt.attempt != after.attempts:
        raise ValueError(
            f"{before.url}: attempt {attempt.attempt} does not follow attempts={before.attempts} "
            f"-> {after.attempts}"
        )
    if after.last_class is not attempt.response_class:
        raise ValueError(
            f"{before.url}: last_class {after.last_class} differs from the attempt's "
            f"{attempt.response_class}"
        )


def _recoverable(cache: PageCache, cached: CacheEntry) -> bool:
    """A cache entry from a successful fetch whose body is present and verifies."""
    if not 200 <= cached.status < 300:  # noqa: PLR2004 - the HTTP success range
        return False
    try:
        cache.read_body(cached)
    except CorruptCacheError:
        return False
    return True


def _entry_params(entry: FrontierEntry) -> list[object]:
    return [
        entry.url,
        Site(entry.site).value,
        PageKind(entry.kind).value,
        entry.depth,
        FrontierStatus(entry.status).value,
        entry.attempts,
        entry.next_attempt_at,
        _opt_value(entry.last_class),
        entry.last_reason,
    ]


def _entry_from_row(row: tuple[Any, ...]) -> FrontierEntry:
    return FrontierEntry(
        url=row[0],
        site=Site(row[1]),
        kind=PageKind(row[2]),
        depth=row[3],
        status=FrontierStatus(row[4]),
        attempts=row[5],
        next_attempt_at=row[6],
        last_class=None if row[7] is None else ResponseClass(row[7]),
        last_reason=row[8],
    )


def _opt_value(value: StrEnum | None) -> str | None:
    return None if value is None else value.value


def _describe(entry: FrontierEntry | None) -> str:
    if entry is None:
        return "missing"
    return f"status={entry.status} attempts={entry.attempts}"


def _require_canonical(url: str) -> None:
    try:
        canonical = canonicalize(url)
    except InvalidUrlError as exc:
        raise ValueError(f"not a valid frontier URL: {url!r}") from exc
    if canonical != url:
        raise ValueError(f"frontier URLs must be canonical: {url!r} (expected {canonical!r})")


def _require_iso(value: str, name: str) -> None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp, got {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must carry a UTC offset, got {value!r}")


def _require_json(value: str, name: str) -> None:
    try:
        json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be valid JSON") from exc
