"""Stage 5, export: publish the valid tables as Parquet and CSV, plus quarantine.csv and the run report.

Only rows that passed the contract are published, and they are re-checked on the way out: each
line of ``validated/<table>.jsonl`` is parsed back through its contract model, so a hand-edited or
stale staging file cannot reach a deliverable. Rows carry their ``parse_run_id``; one that differs
from the current parse means ``validate`` was not re-run after ``parse``, and export refuses.

Deliverables, under ``<data_dir>/output/<site>/``:

* ``<table>.parquet`` and ``<table>.csv`` for every table in ``SiteSpec.tables`` (an empty table is
  still written, with its header and schema, so consumers never have to special-case a file that
  is missing). The Arrow schema is derived from the contract model's fields, in their declared
  order, by :func:`arrow_schema`: ``str`` -> string, ``int`` -> int64, ``bool`` -> bool, ``Decimal``
  -> decimal128 with the contract's ``decimal_places`` as scale, ``date`` -> date32,
  ``tuple[str, ...]`` -> list<string>; ``X | None`` is nullable, everything else is not. A type it
  does not know raises, so a new contract field cannot be exported as a silently guessed type;
* ``quarantine.csv``: one line per failure of every quarantined row;
* ``run_report.json`` and ``run_report.md`` (``core.report``).

**Determinism.** Every data file is byte-identical across re-runs on the same cache
(``storage.outputs`` pins the writer settings; rows are sorted by the site's sort key). The run
report is the exception, on purpose: it says *when* the pipeline ran (``started_at``,
``finished_at``, ``duration_s``), so it is excluded from the determinism guarantee.

Report times: ``started_at`` is the caller's (``pipeline run`` passes its own start) or else the
start of the latest fetch run, ``finished_at`` is now. Page counts come from the frontier:
``discovered`` is every entry, ``fetched`` the ``done`` ones, and a final failed entry is counted
under the class that ended it; a ``stopped`` entry that was never attempted is counted under the
class that stopped its domain, when known. Domains stopped are those with ``stopped`` entries now.
"""

from __future__ import annotations

import types
import typing
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
from pydantic import BaseModel, ValidationError

from scraping_pipeline.config import Settings
from scraping_pipeline.core.report import (
    DomainStop,
    PageCounts,
    ParseCoverage,
    RunReport,
    ValidationSummary,
    render_markdown,
)
from scraping_pipeline.core.types import FrontierStatus, ResponseClass, Site
from scraping_pipeline.core.urls import domain as domain_of
from scraping_pipeline.core.validate import ValidationReport, Verdict
from scraping_pipeline.sites import SiteSpec, TableSpec, get_site
from scraping_pipeline.stages.common import (
    SitePaths,
    StageInputError,
    read_json,
    read_jsonl,
    read_jsonl_lines,
    stage_run,
    stopped_domains,
)
from scraping_pipeline.storage.atomic import atomic_write_text
from scraping_pipeline.storage.outputs import write_csv, write_parquet
from scraping_pipeline.storage.state import DomainEventKind, RunStatus, Stage, StateStore

#: Precision of exported decimals; the scale comes from the contract (``decimal_places``).
DECIMAL_PRECISION = 18

QUARANTINE_COLUMNS = (
    "row",
    "failure",
    "record_type",
    "row_key",
    "source_url",
    "field",
    "rule",
    "message",
    "value_repr",
)
"""``row`` is the row's index in ``quarantine.jsonl``, ``failure`` the failure's index within that
row: together a unique, deterministic sort key (a row can repeat a rule on several fields)."""


@dataclass(frozen=True, slots=True)
class ExportResult:
    run_id: str
    report: RunReport
    markdown: str
    rows_by_table: dict[str, int]
    paths: SitePaths

    @property
    def passed(self) -> bool:
        return self.report.validation is not None and (
            self.report.validation.verdict is Verdict.PASS
        )


# --- schema --------------------------------------------------------------------------------------


def _arrow_type(annotation: Any, metadata: list[Any], where: str) -> tuple[pa.DataType, bool]:
    """Arrow type and nullability of one contract field annotation (see the module docstring)."""
    nullable = False
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) != 1:
            raise TypeError(f"{where}: unsupported union {annotation!r}")
        nullable, annotation = True, args[0]
        origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        annotation = typing.get_args(annotation)[0]
        origin = typing.get_origin(annotation)
    if origin is tuple:
        item, ellipsis = typing.get_args(annotation)
        if item is str and ellipsis is Ellipsis:
            return pa.list_(pa.string()), nullable
    elif annotation is bool:
        return pa.bool_(), nullable
    elif annotation is int:
        return pa.int64(), nullable
    elif annotation is str:
        return pa.string(), nullable
    elif annotation is date:
        return pa.date32(), nullable
    elif annotation is Decimal:
        scales = [
            m.decimal_places for m in metadata if getattr(m, "decimal_places", None) is not None
        ]
        if not scales:
            raise TypeError(f"{where}: a Decimal column needs decimal_places in its contract")
        return pa.decimal128(DECIMAL_PRECISION, scales[0]), nullable
    raise TypeError(f"{where}: no Arrow mapping for {annotation!r}")


def arrow_schema(model: type[BaseModel]) -> pa.Schema:
    """The Parquet schema of a contract model: its fields, in order, typed explicitly."""
    fields = []
    for name, info in model.model_fields.items():
        arrow_type, nullable = _arrow_type(
            info.annotation, info.metadata, f"{model.__name__}.{name}"
        )
        fields.append(pa.field(name, arrow_type, nullable=nullable))
    return pa.schema(fields)


# --- inputs --------------------------------------------------------------------------------------


def _load_table(paths: SitePaths, table: TableSpec, parse_run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    path = paths.valid_rows(table.name)
    for number, line in enumerate(read_jsonl_lines(path), start=1):
        try:
            record = table.model.model_validate_json(line)
        except ValidationError as exc:
            raise StageInputError(f"{path}:{number} breaks the contract: {exc}") from exc
        row = record.model_dump()
        if row.get("parse_run_id") != parse_run_id:
            raise StageInputError(
                f"{path} is from parse run {row.get('parse_run_id')}, not {parse_run_id}: "
                "re-run validate"
            )
        rows.append(row)
    return rows


def _quarantine_rows(paths: SitePaths) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for index, row in enumerate(read_jsonl(paths.quarantine)):
        for number, failure in enumerate(row["failures"]):
            out.append({"row": index, "failure": number, **failure})
    return out


# --- report --------------------------------------------------------------------------------------


def _page_counts(store: StateStore, site: Site) -> PageCounts:
    entries = store.entries(site)
    stop_class: dict[str, ResponseClass] = {}
    for event in store.domain_events(site):
        if event.event is DomainEventKind.DOMAIN_STOPPED and event.response_class is not None:
            stop_class[event.domain] = event.response_class
    failed: dict[ResponseClass, int] = {}
    for entry in entries:
        if entry.status in (FrontierStatus.DONE, FrontierStatus.PENDING):
            continue
        if entry.status is FrontierStatus.SKIPPED_ROBOTS:
            continue  # not a failure: we chose not to ask
        cls = entry.last_class or stop_class.get(domain_of(entry.url))
        if cls is not None:
            failed[cls] = failed.get(cls, 0) + 1
    return PageCounts(
        discovered=len(entries),
        fetched=sum(1 for e in entries if e.status is FrontierStatus.DONE),
        failed_by_class=dict(sorted(failed.items())),
    )


def _domain_stops(store: StateStore, site: Site) -> tuple[DomainStop, ...]:
    classes: dict[str, ResponseClass | None] = {}
    for event in store.domain_events(site):
        if event.event is DomainEventKind.DOMAIN_STOPPED:
            classes[event.domain] = event.response_class
    return tuple(
        DomainStop(domain=name, reason=reason, response_class=classes.get(name))
        for name, reason in stopped_domains(store, site).items()
    )


def _build_report(
    store: StateStore,
    spec: SiteSpec,
    stats: dict[str, Any],
    validation: ValidationReport,
    started_at: str | None,
) -> RunReport:
    site = spec.site
    fetch_runs = [r for r in store.runs(site) if r.stage is Stage.FETCH]
    fetch_run_id = fetch_runs[-1].run_id if fetch_runs else "n/a"
    finished = datetime.now(UTC)
    started_at = started_at or (fetch_runs[-1].started_at if fetch_runs else finished.isoformat())
    duration = max(0.0, (finished - datetime.fromisoformat(started_at)).total_seconds())
    return RunReport(
        site=site,
        fetch_run_id=fetch_run_id,
        parse_run_id=stats["parse_run_id"],
        started_at=started_at,
        finished_at=finished.isoformat(),
        duration_s=duration,
        pages=_page_counts(store, site),
        domains_stopped=_domain_stops(store, site),
        parse=ParseCoverage(
            item_pages_fetched=stats["item_pages_fetched"],
            item_pages_with_records=stats["item_pages_with_records"],
            records_parsed=stats["records_parsed"],
            warnings=stats["warnings_count"],
        ),
        validation=ValidationSummary.from_report(validation),
    )


# --- the stage -----------------------------------------------------------------------------------


def run_export(settings: Settings, site: Site, *, started_at: str | None = None) -> ExportResult:
    """Write the deliverables and the run report for ``site`` under ``<data_dir>/output/<site>/``."""
    spec = get_site(site)
    paths = SitePaths.of(settings, site)
    with (
        StateStore.open(settings.state_db) as store,
        stage_run(store, settings, site, Stage.EXPORT) as run,
    ):
        stats = read_json(paths.parse_stats)
        validation = ValidationReport.model_validate(read_json(paths.validation_report))
        rows_by_table: dict[str, int] = {}
        for table in spec.tables:
            rows = _load_table(paths, table, stats["parse_run_id"])
            schema = arrow_schema(table.model)
            parquet_rows = [
                {k: list(v) if isinstance(v, tuple) else v for k, v in row.items()} for row in rows
            ]
            write_parquet(
                paths.output / f"{table.name}.parquet",
                parquet_rows,
                schema,
                sort_key=table.sort_key,
            )
            write_csv(
                paths.output / f"{table.name}.csv", rows, schema.names, sort_key=table.sort_key
            )
            rows_by_table[table.name] = len(rows)
        write_csv(
            paths.output / "quarantine.csv",
            _quarantine_rows(paths),
            QUARANTINE_COLUMNS,
            sort_key=("row", "failure"),
        )
        report = _build_report(store, spec, stats, validation, started_at)
        markdown = render_markdown(report)
        atomic_write_text(paths.output / "run_report.json", report.model_dump_json(indent=2) + "\n")
        atomic_write_text(paths.output / "run_report.md", markdown)
        verdict = validation.verdict
        run.status = RunStatus.SUCCEEDED if verdict is Verdict.PASS else RunStatus.FAILED
        run.summary = {"verdict": verdict.value, "rows_by_table": rows_by_table}
    return ExportResult(run.run_id, report, markdown, rows_by_table, paths)


__all__ = ["QUARANTINE_COLUMNS", "ExportResult", "arrow_schema", "run_export"]
