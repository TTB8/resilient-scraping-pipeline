"""Plumbing every stage shares: run records, output paths, canonical JSON, the draft codec, links.

Canonical JSON
--------------

Every data file a stage writes is canonical JSON: keys sorted, ``(",", ":")`` separators, UTF-8, one
``\\n`` per line, rows in a documented order. That is the whole trick behind "same cache, same
bytes": no dict insertion order, set order, float formatting or platform newline can leak into a
file. Run records (with their timestamps) live in the state database, never in these files.

The draft codec
---------------

Drafts are lenient: ``price_gbp`` may be a ``Decimal`` *or* the raw ``str`` the parser could not
read, ``born_date`` a ``date`` or a ``str``. The strict contract must see exactly what the parser
produced, so a ``Decimal`` written as ``"51.77"`` must not come back as the string ``"51.77"`` (it
would then be quarantined as a wrong type). The codec therefore tags the two non-JSON types:
``{"decimal": "51.77"}`` and ``{"date": "1879-03-14"}``; tuples become lists and come back as tuples.
Decoding is strict: unknown record types, unknown or missing fields raise :class:`StageInputError`.
"""

from __future__ import annotations

import dataclasses
import json
import secrets
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from scraping_pipeline.config import Settings
from scraping_pipeline.core.frontier import FrontierEntry, plan_new_links
from scraping_pipeline.core.lineage import Lineage
from scraping_pipeline.core.parsers.base import AuthorDraft, BookDraft, Draft, QuoteDraft
from scraping_pipeline.core.types import FrontierStatus, Site
from scraping_pipeline.core.urls import domain as domain_of
from scraping_pipeline.fetch.protocol import FetchResult
from scraping_pipeline.logging import bind_run, get_logger
from scraping_pipeline.sites import SiteSpec
from scraping_pipeline.storage.atomic import atomic_write_text
from scraping_pipeline.storage.cache import CacheEntry, CorruptCacheError, PageCache
from scraping_pipeline.storage.state import DomainEventKind, RunStatus, Stage, StateStore

log = get_logger(__name__)

#: Stand-in for "no limit" in ``plan_new_links``, which takes plain ints.
UNLIMITED = 2**62


class StageInputError(Exception):
    """A stage's input is missing, stale or malformed (run the previous stage first)."""


# --- run records ---------------------------------------------------------------------------------


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def new_run_id(stage: Stage) -> str:
    """``<stage>-<UTC time>-<8 hex>``: unique, sortable by start time, readable in a log line."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{stage.value}-{stamp}-{secrets.token_hex(4)}"


def settings_json(settings: Settings) -> str:
    return json.dumps(settings.model_dump(mode="json"), sort_keys=True)


@dataclass(slots=True)
class RunHandle:
    """What a stage tells :func:`stage_run` about how it ended."""

    run_id: str
    status: RunStatus = RunStatus.SUCCEEDED
    summary: dict[str, Any] = field(default_factory=dict)


@contextmanager
def stage_run(
    store: StateStore,
    settings: Settings,
    site: Site,
    stage: Stage,
    run_id: str | None = None,
) -> Iterator[RunHandle]:
    """Record a run around a stage body: ``interrupted`` on Ctrl-C, ``failed`` on an error.

    Lineage logging is bound for the duration (``run_id``, ``site``, ``stage`` on every event).
    """
    handle = RunHandle(run_id or new_run_id(stage))
    store.start_run(handle.run_id, site, stage, utc_now_iso(), settings_json(settings))
    bind_run(run_id=handle.run_id, site=site.value, stage=stage.value)
    log.info("stage_started")
    try:
        yield handle
    except KeyboardInterrupt:
        store.finish_run(handle.run_id, utc_now_iso(), RunStatus.INTERRUPTED, None)
        log.warning("stage_interrupted")
        raise
    except BaseException as exc:
        summary = json.dumps({"error": f"{type(exc).__name__}: {exc}"}, sort_keys=True)
        store.finish_run(handle.run_id, utc_now_iso(), RunStatus.FAILED, summary)
        log.error("stage_failed", error=str(exc))  # noqa: TRY400 - re-raised with its traceback
        raise
    store.finish_run(
        handle.run_id, utc_now_iso(), handle.status, json.dumps(handle.summary, sort_keys=True)
    )
    log.info("stage_finished", status=handle.status.value, **handle.summary)


# --- paths ---------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SitePaths:
    """Where each stage's files live for one site. Parsed and validated data are intermediate;
    ``output`` holds the deliverables."""

    parsed: Path
    validated: Path
    output: Path

    @classmethod
    def of(cls, settings: Settings, site: Site) -> SitePaths:
        root = settings.data_dir / site.value
        return cls(root / "parsed", root / "validated", settings.output_dir / site.value)

    @property
    def records(self) -> Path:
        return self.parsed / "records.jsonl"

    @property
    def context(self) -> Path:
        return self.parsed / "context.json"

    @property
    def parse_stats(self) -> Path:
        return self.parsed / "parse_stats.json"

    def valid_rows(self, table: str) -> Path:
        return self.validated / f"{table}.jsonl"

    @property
    def quarantine(self) -> Path:
        return self.validated / "quarantine.jsonl"

    @property
    def validation_report(self) -> Path:
        return self.validated / "validation_report.json"


# --- canonical JSON ------------------------------------------------------------------------------


def canonical_json(value: object) -> str:
    """One canonical spelling of a JSON value: sorted keys, no spaces, UTF-8 kept as is."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def write_json(path: Path, value: object) -> None:
    atomic_write_text(path, canonical_json(value) + "\n")


def write_jsonl(path: Path, rows: Iterable[object]) -> None:
    atomic_write_text(path, "".join(canonical_json(row) + "\n" for row in rows))


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise StageInputError(f"{path} is missing: run the previous stage first") from exc
    except json.JSONDecodeError as exc:
        raise StageInputError(f"{path} is not valid JSON: {exc}") from exc


def read_jsonl_lines(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise StageInputError(f"{path} is missing: run the previous stage first") from exc
    return [line for line in text.split("\n") if line]


def read_jsonl(path: Path) -> list[Any]:
    rows = []
    for number, line in enumerate(read_jsonl_lines(path), start=1):
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise StageInputError(f"{path}:{number} is not valid JSON: {exc}") from exc
    return rows


# --- the draft codec -----------------------------------------------------------------------------

DRAFT_TYPES: dict[str, type[BookDraft] | type[QuoteDraft] | type[AuthorDraft]] = {
    "book": BookDraft,
    "quote": QuoteDraft,
    "author": AuthorDraft,
}
_RECORD_TYPE = {cls: name for name, cls in DRAFT_TYPES.items()}


def record_type_of(draft: Draft) -> str:
    return _RECORD_TYPE[type(draft)]


def encode_value(value: object) -> object:
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, tuple | list):
        return [encode_value(item) for item in value]
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"cannot encode {type(value).__name__} in a draft: {value!r}")


def decode_value(value: object) -> object:
    if isinstance(value, dict):
        if set(value) == {"decimal"} and isinstance(value["decimal"], str):
            return Decimal(value["decimal"])
        if set(value) == {"date"} and isinstance(value["date"], str):
            return date.fromisoformat(value["date"])
        raise StageInputError(f"unknown tagged value {value!r}")
    if isinstance(value, list):
        return tuple(decode_value(item) for item in value)
    return value


def encode_draft(draft: Draft) -> dict[str, object]:
    return {f.name: encode_value(getattr(draft, f.name)) for f in dataclasses.fields(draft)}


def decode_draft(record_type: str, data: Mapping[str, object]) -> Draft:
    cls = DRAFT_TYPES.get(record_type)
    if cls is None:
        raise StageInputError(f"unknown record type {record_type!r}")
    names = {f.name for f in dataclasses.fields(cls)}
    if set(data) != names:
        raise StageInputError(
            f"{record_type} draft fields {sorted(data)} differ from {sorted(names)}"
        )
    values: dict[str, Any] = {name: decode_value(value) for name, value in data.items()}
    return cls(**values)


def encode_lineage(lineage: Lineage) -> dict[str, str]:
    return dataclasses.asdict(lineage)


def decode_lineage(data: Mapping[str, object]) -> Lineage:
    names = {f.name for f in dataclasses.fields(Lineage)}
    if set(data) != names or not all(isinstance(v, str) for v in data.values()):
        raise StageInputError(f"malformed lineage {dict(data)!r}")
    return Lineage(**{k: str(v) for k, v in data.items()})


# --- cache reads ---------------------------------------------------------------------------------


def decode_body(entry: CacheEntry, body: bytes) -> str:
    """Decode a cached body exactly as the fetch stage decoded it when it expanded links.

    Reuses :meth:`FetchResult.text` (charset from ``Content-Type``, UTF-8 fallback, replacement on
    bad bytes) so there is one decoding rule, not two that could drift apart.
    """
    headers = {"content-type": entry.content_type} if entry.content_type else {}
    return FetchResult(
        requested_url=entry.url,
        final_url=entry.final_url,
        status=entry.status,
        headers=headers,
        body=body,
        error=None,
        elapsed_s=0.0,
        fetcher=entry.fetcher,
    ).text()


def read_cached_html(cache: PageCache, url: str) -> tuple[CacheEntry, str] | None:
    """The entry and decoded body of ``url``, or ``None`` if absent or its object is corrupt."""
    entry = cache.get_entry(url)
    if entry is None:
        return None
    try:
        body = cache.read_body(entry)
    except CorruptCacheError as exc:
        log.warning("cache_object_corrupt", url=url, error=str(exc))
        return None
    return entry, decode_body(entry, body)


# --- link planning -------------------------------------------------------------------------------


def known_urls(store: StateStore) -> set[str]:
    """Every URL in the frontier, all sites: the primary key is global, so a URL another site
    already owns must never be planned (its row would be updated by the wrong crawl)."""
    return {entry.url for site in Site for entry in store.entries(site)}


def plan_links(
    spec: SiteSpec,
    settings: Settings,
    *,
    known: set[str],
    entry: FrontierEntry,
    page_url: str,
    html: str,
    current_count: int,
) -> list[FrontierEntry]:
    """New frontier entries for the links on one page (empty unless its kind carries links).

    Links resolve against ``page_url`` (where redirects ended), so a redirected listing still
    yields correct absolute URLs. Depth and page caps come from ``Settings`` via
    :func:`~scraping_pipeline.core.frontier.plan_new_links`, which also canonicalizes, deduplicates
    and keeps only same-site URLs.
    """
    if entry.kind not in spec.link_kinds:
        return []
    links = spec.parse(page_url, html, entry.kind).links
    return plan_new_links(
        known,
        links,
        origin=spec.origin(settings),
        site=spec.site,
        depth=entry.depth,
        max_depth=UNLIMITED if settings.max_depth is None else settings.max_depth,
        max_pages=UNLIMITED if settings.max_pages is None else settings.max_pages,
        current_count=current_count,
    )


def expand_from_cache(
    store: StateStore, cache: PageCache, spec: SiteSpec, settings: Settings, run_id: str
) -> list[FrontierEntry]:
    """Plan links from every cached link-bearing page of the site, offline, until nothing is new.

    Pages are visited in ``(depth, url)`` order, so with a page cap the surviving links depend only
    on the cache, never on the order pages happened to be fetched. Insert-only (``add_entries``),
    so running it again adds nothing. Newly planned entries are visited too: a frontier rebuilt
    next to an old cache recovers all of it without a request.
    """
    known = known_urls(store)
    queue = sorted(store.entries(spec.site), key=lambda e: (e.depth, e.url))
    count = len(queue)
    added: list[FrontierEntry] = []
    index = 0
    while index < len(queue):
        entry = queue[index]
        index += 1
        cached = read_cached_html(cache, entry.url) if entry.kind in spec.link_kinds else None
        if cached is None:
            continue
        cache_entry, html = cached
        new = plan_links(
            spec,
            settings,
            known=known,
            entry=entry,
            page_url=cache_entry.final_url,
            html=html,
            current_count=count,
        )
        if new:
            store.add_entries(new, run_id)
            known.update(e.url for e in new)
            count += len(new)
            added += new
            queue += new
    return added


# --- stopped domains -----------------------------------------------------------------------------


def stopped_domains(store: StateStore, site: Site) -> dict[str, str]:
    """Domains that currently have ``stopped`` entries, with the latest recorded stop reason.

    Computed from the frontier, not from the last run: a stop from an earlier run still leaves the
    dataset incomplete until someone re-runs ``fetch --retry-failed``.
    """
    stopped = store.entries(site, {FrontierStatus.STOPPED})
    reasons: dict[str, str] = {}
    for event in store.domain_events(site):  # ordered by time: the latest reason wins
        if event.event is DomainEventKind.DOMAIN_STOPPED:
            reasons[event.domain] = event.reason
    out: dict[str, str] = {}
    for entry in stopped:
        name = domain_of(entry.url)
        out.setdefault(name, reasons.get(name) or entry.last_reason or "stopped")
    return dict(sorted(out.items()))
