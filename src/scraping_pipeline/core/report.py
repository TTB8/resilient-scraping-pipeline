"""The run report: one document that says what a run did, for a human (Markdown) and a machine (JSON).

It is data only. The stages fill it in; this module just defines its shape and renders it. Timestamps
arrive as ISO strings and the duration as a number, both passed in by the caller, because the core
has no clock. ``render_markdown`` is deterministic: every mapping is rendered in sorted order and every
number with a fixed format, so the same report always renders to the same bytes and can be diffed
between runs or checked against a golden file.

Parse coverage is measured per *item page* (a page expected to hold records: a book's product page, a
quotes listing or an author page) as ``item pages that yielded at least one record / item pages
fetched``. For books that is exactly "books parsed / detail pages fetched"; stating it per page keeps
the ratio meaningful for the quotes site too, where one listing page yields ten records.
"""

from decimal import Decimal
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field

from scraping_pipeline.core.types import ResponseClass, Site
from scraping_pipeline.core.validate import ValidationReport, Verdict

_FROZEN = ConfigDict(frozen=True, extra="forbid")
_PERCENT = Decimal("0.01")

Count = Annotated[int, Field(ge=0)]


class PageCounts(BaseModel):
    model_config = _FROZEN

    discovered: Count
    fetched: Count
    failed_by_class: dict[ResponseClass, Count] = Field(default_factory=dict)
    """Final failed pages by the response class that ended them (``ok`` never appears here)."""

    @property
    def failed(self) -> int:
        return sum(self.failed_by_class.values())


class DomainStop(BaseModel):
    """A domain the fetch stage stopped crawling (blocked, challenged, breaker open), and why."""

    model_config = _FROZEN

    domain: str
    reason: str
    response_class: ResponseClass | None = None


class ParseCoverage(BaseModel):
    model_config = _FROZEN

    item_pages_fetched: Count
    item_pages_with_records: Count
    records_parsed: Count
    warnings: Count

    @property
    def coverage(self) -> Decimal | None:
        """``item_pages_with_records / item_pages_fetched``, or ``None`` when nothing was fetched."""
        if self.item_pages_fetched == 0:
            return None
        return Decimal(self.item_pages_with_records) / Decimal(self.item_pages_fetched)


class ValidationSummary(BaseModel):
    """The counts of a ``ValidationReport`` without the per-row failures (those go to quarantine)."""

    model_config = _FROZEN

    contract_version: str
    total_rows: Count
    valid_rows: Count
    quarantined_rows: Count
    failure_rate: Decimal
    max_failure_rate: Decimal
    rule_counts: dict[str, Count]
    verdict: Verdict
    verdict_reason: str

    @classmethod
    def from_report(cls, report: ValidationReport) -> Self:
        return cls(
            contract_version=report.contract_version,
            total_rows=report.total_rows,
            valid_rows=report.valid_rows,
            quarantined_rows=report.quarantined_rows,
            failure_rate=report.failure_rate,
            max_failure_rate=report.max_failure_rate,
            rule_counts=report.rule_counts,
            verdict=report.verdict,
            verdict_reason=report.verdict_reason,
        )


class RunReport(BaseModel):
    """Everything one run of one site did. ``parse`` and ``validation`` are ``None`` until those stages run."""

    model_config = _FROZEN

    site: Site
    fetch_run_id: str
    parse_run_id: str | None = None
    started_at: str
    finished_at: str
    duration_s: Annotated[float, Field(ge=0)]
    pages: PageCounts
    domains_stopped: tuple[DomainStop, ...] = ()
    parse: ParseCoverage | None = None
    validation: ValidationSummary | None = None


def render_markdown(report: RunReport) -> str:
    """Render the report as Markdown. Same report, same bytes; ends with a single newline."""
    lines = [
        f"# Run report: {report.site.value}",
        "",
        "| | |",
        "|---|---|",
        f"| Fetch run | `{report.fetch_run_id}` |",
        f"| Parse run | `{report.parse_run_id or 'n/a'}` |",
        f"| Started | {report.started_at} |",
        f"| Finished | {report.finished_at} |",
        f"| Duration | {report.duration_s:.2f} s |",
        "",
        *_pages(report.pages),
        "",
        *_domains(report.domains_stopped),
        "",
        *_parse(report.parse),
        "",
        *_validation(report.validation),
    ]
    return "\n".join(lines) + "\n"


def _pages(pages: PageCounts) -> list[str]:
    lines = [
        "## Pages",
        "",
        f"- Discovered: {pages.discovered}",
        f"- Fetched: {pages.fetched}",
        f"- Failed: {pages.failed}",
    ]
    if pages.failed_by_class:
        lines += ["", "| Response class | Pages |", "|---|---:|"]
        lines += [
            f"| {cls.value} | {n} |"
            for cls, n in sorted(pages.failed_by_class.items(), key=lambda kv: kv[0].value)
        ]
    return lines


def _domains(stops: tuple[DomainStop, ...]) -> list[str]:
    lines = ["## Domains stopped", ""]
    if not stops:
        return [*lines, "None."]
    lines += ["| Domain | Class | Reason |", "|---|---|---|"]
    for stop in sorted(stops, key=lambda s: (s.domain, s.reason)):
        cls = stop.response_class.value if stop.response_class else "n/a"
        lines.append(f"| {stop.domain} | {cls} | {_cell(stop.reason)} |")
    return lines


def _parse(parse: ParseCoverage | None) -> list[str]:
    lines = ["## Parse", ""]
    if parse is None:
        return [*lines, "Not run."]
    coverage = parse.coverage
    shown = "n/a" if coverage is None else f"{(coverage * 100).quantize(_PERCENT)}%"
    return [
        *lines,
        f"- Item pages fetched: {parse.item_pages_fetched}",
        f"- Item pages with records: {parse.item_pages_with_records} (coverage {shown})",
        f"- Records parsed: {parse.records_parsed}",
        f"- Warnings: {parse.warnings}",
    ]


def _validation(summary: ValidationSummary | None) -> list[str]:
    lines = ["## Validation", ""]
    if summary is None:
        return [*lines, "Not run."]
    lines += [
        f"- Contract: `{summary.contract_version}`",
        f"- Verdict: **{summary.verdict.value.upper()}**: {summary.verdict_reason}",
        f"- Rows: {summary.total_rows} total, {summary.valid_rows} valid, "
        f"{summary.quarantined_rows} quarantined",
        f"- Failure rate: {summary.failure_rate} (max {summary.max_failure_rate})",
    ]
    if summary.rule_counts:
        lines += ["", "| Rule | Failures |", "|---|---:|"]
        lines += [f"| {rule} | {n} |" for rule, n in sorted(summary.rule_counts.items())]
    return lines


def _cell(text: str) -> str:
    """Keep free text inside one Markdown table cell."""
    return " ".join(text.split()).replace("|", "\\|")
