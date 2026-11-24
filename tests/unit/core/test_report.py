"""The run report model and its deterministic Markdown rendering (golden strings)."""

from decimal import Decimal

from scraping_pipeline.core.report import (
    Deliverables,
    DomainStop,
    PageCounts,
    ParseCoverage,
    RunReport,
    ValidationSummary,
    render_markdown,
)
from scraping_pipeline.core.types import ResponseClass, Site
from scraping_pipeline.core.validate import ValidationReport, Verdict

FULL = RunReport(
    site=Site.BOOKS,
    fetch_run_id="fetch-20261124",
    parse_run_id="9cf173d472b1ea7b",
    started_at="2026-11-24T10:00:00Z",
    finished_at="2026-11-24T10:35:12Z",
    duration_s=2112.5,
    pages=PageCounts(
        discovered=1052,
        fetched=1049,
        failed_by_class={ResponseClass.TIMEOUT: 1, ResponseClass.NOT_FOUND: 2},
    ),
    domains_stopped=(
        DomainStop(
            domain="z.test",
            reason="403 | access denied\non every page",
            response_class=ResponseClass.BLOCKED,
        ),
        DomainStop(domain="a.test", reason="breaker open"),
    ),
    parse=ParseCoverage(
        item_pages_fetched=1000, item_pages_with_records=999, records_parsed=999, warnings=3
    ),
    validation=ValidationSummary(
        contract_version="books/1.0.0",
        total_rows=999,
        valid_rows=997,
        quarantined_rows=2,
        failure_rate=Decimal("0.0020"),
        max_failure_rate=Decimal("0.01"),
        rule_counts={"unknown_category": 1, "duplicate_key": 1},
        verdict=Verdict.PASS,
        verdict_reason="2 of 999 rows quarantined, within max rate 0.01",
    ),
)

GOLDEN_FULL = """\
# Run report: books

| | |
|---|---|
| Latest fetch run | `fetch-20261124` |
| Parse run | `9cf173d472b1ea7b` |
| Started | 2026-11-24T10:00:00Z |
| Finished | 2026-11-24T10:35:12Z |
| Duration | 2112.50 s |

## Pages

- Discovered: 1052
- Fetched: 1049
- Failed: 3

| Response class | Pages |
|---|---:|
| not_found | 2 |
| timeout | 1 |

## Domains stopped

| Domain | Class | Reason |
|---|---|---|
| a.test | n/a | breaker open |
| z.test | blocked | 403 \\| access denied on every page |

## Parse

- Item pages fetched: 1000
- Item pages with records: 999 (coverage 99.90%)
- Records parsed: 999
- Warnings: 3

## Validation

- Contract: `books/1.0.0`
- Verdict: **PASS**: 2 of 999 rows quarantined, within max rate 0.01
- Rows: 999 total, 997 valid, 2 quarantined
- Failure rate: 0.0020 (max 0.01)

| Rule | Failures |
|---|---:|
| duplicate_key | 1 |
| unknown_category | 1 |
"""

EMPTY = RunReport(
    site=Site.QUOTES_JS,
    fetch_run_id="f",
    started_at="a",
    finished_at="b",
    duration_s=0,
    pages=PageCounts(discovered=0, fetched=0),
)

GOLDEN_EMPTY = """\
# Run report: quotes-js

| | |
|---|---|
| Latest fetch run | `f` |
| Parse run | `n/a` |
| Started | a |
| Finished | b |
| Duration | 0.00 s |

## Pages

- Discovered: 0
- Fetched: 0
- Failed: 0

## Domains stopped

None.

## Parse

Not run.

## Validation

Not run.
"""


def test_full_report_matches_golden() -> None:
    assert render_markdown(FULL) == GOLDEN_FULL


def test_report_before_parse_and_validate_matches_golden() -> None:
    assert render_markdown(EMPTY) == GOLDEN_EMPTY


def test_rendering_ignores_mapping_insertion_order() -> None:
    shuffled = FULL.model_copy(
        update={
            "pages": PageCounts(
                discovered=1052,
                fetched=1049,
                failed_by_class={ResponseClass.NOT_FOUND: 2, ResponseClass.TIMEOUT: 1},
            ),
            "domains_stopped": tuple(reversed(FULL.domains_stopped)),
        }
    )
    assert render_markdown(shuffled) == GOLDEN_FULL


def test_report_round_trips_through_json() -> None:
    assert RunReport.model_validate_json(FULL.model_dump_json()) == FULL


def test_coverage_ratio() -> None:
    assert FULL.parse is not None
    assert FULL.parse.coverage == Decimal("0.999")
    assert FULL.pages.failed == 3
    none = ParseCoverage(
        item_pages_fetched=0, item_pages_with_records=0, records_parsed=0, warnings=0
    )
    assert none.coverage is None
    shown = render_markdown(FULL.model_copy(update={"parse": none}))
    assert "- Item pages with records: 0 (coverage n/a)" in shown


def test_summary_from_validation_report_drops_row_failures() -> None:
    report = ValidationReport(
        contract_version="quotes/1.0.0",
        total_rows=1,
        valid_rows=0,
        quarantined_rows=1,
        failure_rate=Decimal("1.0000"),
        max_failure_rate=Decimal(0),
        rule_counts={"missing": 1},
        failures=(),
        verdict=Verdict.FAIL,
        verdict_reason="1 of 1 rows quarantined, above max rate 0",
    )
    summary = ValidationSummary.from_report(report)
    assert summary.model_dump() == report.model_dump(exclude={"failures"})
    shown = render_markdown(FULL.model_copy(update={"validation": summary}))
    assert "- Verdict: **FAIL**: 1 of 1 rows quarantined, above max rate 0" in shown
    assert "| missing | 1 |" in shown


def test_optional_sections_are_absent_unless_set() -> None:
    # The golden strings above pin that a report without them renders byte-identically.
    shown = render_markdown(FULL)
    assert "HTTP requests" not in shown
    assert "## Deliverables" not in shown


def test_http_requests_line_counts_requests_not_pages() -> None:
    pages = FULL.pages.model_copy(update={"http_requests": 1061})
    shown = render_markdown(FULL.model_copy(update={"pages": pages}))
    assert "- HTTP requests, latest fetch run (robots.txt and redirect hops included): 1061\n" in (
        shown
    )
    assert "all fetch runs" not in shown


def test_cumulative_requests_and_the_fetch_runs_behind_the_pages() -> None:
    """R2-11: after a re-run the latest run may have sent nothing; say so, and give the total."""
    pages = FULL.pages.model_copy(update={"http_requests": 0, "http_requests_total": 1061})
    shown = render_markdown(
        FULL.model_copy(update={"pages": pages, "fetch_run_ids": ("fetch-b", "fetch-a")})
    )
    assert (
        "| Latest fetch run | `fetch-20261124` |\n| Pages fetched by | `fetch-a`, `fetch-b` |\n"
        in (shown)
    )
    assert "- HTTP requests, latest fetch run (robots.txt and redirect hops included): 0\n" in shown
    assert "- HTTP requests, all fetch runs behind these pages: 1061\n" in shown

    partial = pages.model_copy(update={"http_requests_unreported_runs": 1})
    shown = render_markdown(FULL.model_copy(update={"pages": partial}))
    assert (
        "- HTTP requests, all fetch runs behind these pages: at least 1061 "
        "(1 fetch runs reported no count)\n"
    ) in shown


def test_deliverables_section_lists_files_sorted() -> None:
    delivered = Deliverables(published=("books.parquet", "books.csv"))
    shown = render_markdown(FULL.model_copy(update={"deliverables": delivered}))
    assert shown.endswith(
        "## Deliverables\n\n- `books.csv`\n- `books.parquet`\n- `quarantine.csv`\n"
    )
    assert "failed validation" not in shown


def test_deliverables_section_says_when_the_batch_failed_validation() -> None:
    delivered = Deliverables(published=("books.csv",), verdict_failed=True)
    shown = render_markdown(FULL.model_copy(update={"deliverables": delivered}))
    assert "**This batch failed validation.**" in shown
    assert "every other row is in `quarantine.csv`" in shown


def test_scheme_downgrades_are_shown_only_when_there_were_any() -> None:
    """ADR 0010: followed https to http hops are named in the report, silent when zero."""
    line = "- Same-host https to http redirect hops followed (ADR 0010): 50\n"
    some = FULL.pages.model_copy(update={"scheme_downgrades": 50})
    assert line in render_markdown(FULL.model_copy(update={"pages": some}))
    for none in (0, None):
        quiet = FULL.pages.model_copy(update={"scheme_downgrades": none})
        assert "redirect hops followed" not in render_markdown(
            FULL.model_copy(update={"pages": quiet})
        )
