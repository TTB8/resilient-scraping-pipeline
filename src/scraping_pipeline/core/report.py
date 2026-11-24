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
    http_requests: Count | None = None
    """HTTP requests the **latest** fetch run sent, robots.txt and redirect hops included. Pages
    and requests differ on purpose: the politeness bound is about requests (ADR 0009). The page
    counts above are cumulative over the frontier, so after a re-run (which fetches nothing new)
    this can be 0 next to many fetched pages; ``http_requests_total`` is the cumulative figure."""
    http_requests_total: Count | None = None
    """HTTP requests summed over every fetch run that produced the parsed pages
    (``RunReport.fetch_run_ids``), so it is comparable with ``fetched``."""
    http_requests_unreported_runs: Count = 0
    """How many of those fetch runs recorded no request count (interrupted before reporting): when
    non-zero, ``http_requests_total`` is a lower bound."""
    scheme_downgrades: Count | None = None
    """Same-host ``https`` to ``http`` redirect hops followed, summed over the same fetch runs
    (ADR 0010). Those bodies crossed the network unencrypted; shown only when there were any."""

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


class Deliverables(BaseModel):
    """The data files export published. They hold only rows that passed the contract.

    ``verdict_failed`` marks a batch whose validation verdict was FAIL: the files are still
    written (failing rows are in ``quarantine.csv``, never dropped), and the report says so in
    words, so nobody mistakes a partial batch for a clean one.
    """

    model_config = _FROZEN

    published: tuple[str, ...] = ()
    verdict_failed: bool = False


class RunReport(BaseModel):
    """Everything one run of one site did. ``parse`` and ``validation`` are ``None`` until those stages run."""

    model_config = _FROZEN

    site: Site
    fetch_run_id: str
    """The latest fetch run of the site (the one ``pipeline run`` just did)."""
    fetch_run_ids: tuple[str, ...] = ()
    """The distinct fetch runs whose pages were parsed (the ``fetch_run_id`` values on the rows),
    sorted. After a resumed or repeated crawl there are several, and the latest may be none of
    them."""
    parse_run_id: str | None = None
    started_at: str
    finished_at: str
    duration_s: Annotated[float, Field(ge=0)]
    pages: PageCounts
    domains_stopped: tuple[DomainStop, ...] = ()
    parse: ParseCoverage | None = None
    validation: ValidationSummary | None = None
    deliverables: Deliverables | None = None


def render_markdown(report: RunReport) -> str:
    """Render the report as Markdown. Same report, same bytes; ends with a single newline."""
    lines = [
        f"# Run report: {report.site.value}",
        "",
        "| | |",
        "|---|---|",
        f"| Latest fetch run | `{report.fetch_run_id}` |",
        *(
            [f"| Pages fetched by | {', '.join(f'`{r}`' for r in sorted(report.fetch_run_ids))} |"]
            if report.fetch_run_ids
            else []
        ),
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
        *_deliverables(report.deliverables),
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
    if pages.http_requests is not None:
        lines.append(
            "- HTTP requests, latest fetch run (robots.txt and redirect hops included): "
            f"{pages.http_requests}"
        )
    if pages.http_requests_total is not None:
        total = f"{pages.http_requests_total}"
        if pages.http_requests_unreported_runs:
            total = (
                f"at least {total} ({pages.http_requests_unreported_runs} fetch runs "
                "reported no count)"
            )
        lines.append(f"- HTTP requests, all fetch runs behind these pages: {total}")
    if pages.scheme_downgrades:
        lines.append(
            "- Same-host https to http redirect hops followed (ADR 0010): "
            f"{pages.scheme_downgrades}"
        )
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


def _deliverables(deliverables: Deliverables | None) -> list[str]:
    if deliverables is None:
        return []
    lines = ["", "## Deliverables", ""]
    if deliverables.verdict_failed:
        lines += [
            "**This batch failed validation.** The files hold only the rows that passed the "
            "contract; every other row is in `quarantine.csv` with its reasons.",
            "",
        ]
    return [
        *lines,
        *(f"- `{name}`" for name in sorted(deliverables.published)),
        "- `quarantine.csv`",
    ]


def _cell(text: str) -> str:
    """Keep free text inside one Markdown table cell."""
    return " ".join(text.split()).replace("|", "\\|")
