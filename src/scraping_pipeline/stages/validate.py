"""Stage 4, validate: hold every draft to the versioned contract; quarantine, never drop.

Why quarantine instead of drop
------------------------------

A scraper that silently drops rows it cannot read reports a smaller dataset and calls it success.
Nobody notices the site changed its price format until a customer asks where half the catalogue
went. Here every input row ends in exactly one place: a valid table, or ``quarantine.jsonl`` with
*every* rule it broke (field, rule, message, the offending value). ``valid + quarantined == input``
always, so a shrinking dataset is visible, and explained, in the report.

The rules themselves live in ``core.contracts`` (row shape) and ``core.validate`` (uniqueness,
referential checks, the verdict). This stage only reads ``parsed/``, builds the site's dataset
(``SiteSpec.build_dataset``) and writes the result. The verdict is ``fail`` when the share of
quarantined rows exceeds ``max_failure_rate`` (default ``0``: fail closed), or when there is
nothing at all to publish. A failing verdict still writes every file, so the reason can be read;
the CLI then exits with 1.

Outputs, under ``<data_dir>/<site>/validated/``, canonical JSON, written atomically:

* ``<table>.jsonl``: valid rows of each table (``SiteSpec.tables``), sorted by the table's key, in
  the contract's own JSON form (decimals as exact strings, dates in ISO form);
* ``quarantine.jsonl``: ``QuarantinedRow.to_dict()`` per rejected row, in validation's canonical
  order;
* ``validation_report.json``: counts, rule counts, every failure and the verdict, plus the
  ``parse_run_id`` of the parse it validated (from ``parse_stats.json``). Export compares it with
  the current parse, so a report, quarantine and verdict from an older parse are refused even when
  there are no valid rows to carry the id.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from scraping_pipeline.config import Settings
from scraping_pipeline.core.types import Site
from scraping_pipeline.core.validate import (
    Candidate,
    ValidationPolicy,
    ValidationReport,
    Verdict,
    validate,
)
from scraping_pipeline.sites import get_site
from scraping_pipeline.stages.common import (
    SitePaths,
    StageInputError,
    canonical_json,
    decode_draft,
    decode_lineage,
    read_json,
    read_jsonl,
    stage_run,
    write_json,
    write_jsonl,
)
from scraping_pipeline.storage.atomic import atomic_write_text
from scraping_pipeline.storage.state import RunStatus, Stage, StateStore


@dataclass(frozen=True, slots=True)
class ValidateStageResult:
    run_id: str
    report: ValidationReport
    rows_by_table: dict[str, int]
    paths: SitePaths

    @property
    def passed(self) -> bool:
        return self.report.verdict is Verdict.PASS


def load_candidates(paths: SitePaths) -> list[Candidate[Any]]:
    """The parse stage's drafts with their lineage, decoded back to the exact parser types."""
    candidates: list[Candidate[Any]] = []
    for row in read_jsonl(paths.records):
        if not isinstance(row, dict) or set(row) != {"record_type", "draft", "lineage"}:
            raise StageInputError(f"{paths.records}: malformed row {row!r}")
        candidates.append(
            Candidate(
                draft=decode_draft(row["record_type"], row["draft"]),
                lineage=decode_lineage(row["lineage"]),
            )
        )
    return candidates


def _row_json(record: BaseModel) -> str:
    # The contract's own JSON (exact decimals, ISO dates), re-spelt canonically.
    return canonical_json(json.loads(record.model_dump_json()))


def run_validate(settings: Settings, site: Site) -> ValidateStageResult:
    """Validate ``parsed/`` into ``validated/``. Every file is written whatever the verdict."""
    spec = get_site(site)
    paths = SitePaths.of(settings, site)
    with (
        StateStore.open(settings.state_db) as store,
        stage_run(store, settings, site, Stage.VALIDATE) as run,
    ):
        stats = read_json(paths.parse_stats)
        if not isinstance(stats, dict) or not isinstance(stats.get("parse_run_id"), str):
            raise StageInputError(f"{paths.parse_stats}: no parse_run_id")
        parse_run_id: str = stats["parse_run_id"]
        candidates = load_candidates(paths)
        stale = {c.lineage.parse_run_id for c in candidates} - {parse_run_id}
        if stale:
            raise StageInputError(
                f"{paths.records} holds rows from parse run {sorted(stale)[0]}, not "
                f"{parse_run_id}: re-run parse"
            )
        context = read_json(paths.context)
        if not isinstance(context, dict):
            raise StageInputError(f"{paths.context}: expected an object")
        dataset = spec.build_dataset(candidates, context)
        policy = ValidationPolicy(max_failure_rate=Decimal(str(settings.max_failure_rate)))
        result = validate(dataset, policy)

        rows_by_table: dict[str, int] = {}
        for table in spec.tables:
            records = [r for r in result.valid if isinstance(r, table.model)]
            lines = sorted(
                ((tuple(getattr(r, k) for k in table.sort_key), _row_json(r)) for r in records),
                key=lambda pair: pair[0],
            )
            atomic_write_text(paths.valid_rows(table.name), "".join(f"{j}\n" for _, j in lines))
            rows_by_table[table.name] = len(lines)
        write_jsonl(paths.quarantine, (row.to_dict() for row in result.quarantined))
        write_json(
            paths.validation_report,
            {**result.report.model_dump(mode="json"), "parse_run_id": parse_run_id},
        )

        report = result.report
        run.status = RunStatus.SUCCEEDED if report.verdict is Verdict.PASS else RunStatus.FAILED
        run.summary = {
            "parse_run_id": parse_run_id,
            "verdict": report.verdict.value,
            "verdict_reason": report.verdict_reason,
            "total_rows": report.total_rows,
            "valid_rows": report.valid_rows,
            "quarantined_rows": report.quarantined_rows,
            "rows_by_table": rows_by_table,
        }
    return ValidateStageResult(run.run_id, result.report, rows_by_table, paths)
