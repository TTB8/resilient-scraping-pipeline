"""Validation: apply the strict contract to lenient drafts, plus the rules only a whole dataset can check.

Contracts over silent dropping. Every input row ends up in exactly one of two places: ``valid`` (a
contract model instance) or ``quarantined`` (the original draft and lineage, with *every* failure that
applied to it). Nothing is dropped, so ``len(valid) + len(quarantined) == input rows`` always holds,
and the quarantine output explains each rejected row: row key, field, rule, message and the offending
value's ``repr``.

Rules, in the order they run:

1. **Row contract** (``core.contracts``): types, ranges, shapes, cross-field consistency. A ``None``
   draft field is passed as *absent*, so it is reported as rule ``missing`` rather than a type error.
2. **Uniqueness** (``duplicate_key``) on ``book_id`` / ``quote_id`` / ``author_slug`` among the rows
   that passed the contract. Rows are first put in a canonical order (key, source URL, position,
   content hash, then the ``repr`` of draft and lineage as a last tie-break), and the first row of each key is kept; later ones are quarantined with a pointer to
   the kept row's source. The choice is therefore deterministic whatever order the stage passes rows in.
3. **Referential**: a book's category must be in the category set discovered on the home page
   (``unknown_category``); a quote's ``author_slug`` must name a *valid* author row (``unknown_author``),
   and must be present (``author_link_missing``) unless the dataset is the JavaScript variant, which
   renders no author links (``QuotesDataset.require_author_links=False``).
4. **Verdict**: ``fail`` when the dataset is empty, or when ``quarantined / total`` exceeds
   ``ValidationPolicy.max_failure_rate``. The default rate is ``0``: fail closed, any quarantined row
   fails the run unless the operator has decided a tolerance.

Everything in the result is sorted by stable keys, so the same inputs give an equal result.
"""

import dataclasses
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from scraping_pipeline.core.contracts import Author, Book, Quote, Record, books_v1, quotes_v1
from scraping_pipeline.core.lineage import Lineage
from scraping_pipeline.core.parsers.base import AuthorDraft, BookDraft, Draft, QuoteDraft

_RATE_PLACES = Decimal("0.0001")


class Verdict(StrEnum):
    PASS = "pass"  # noqa: S105 - a verdict, not a password
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class Candidate[D: Draft]:
    """A draft record plus the lineage the parse stage attached to it: one input row."""

    draft: D
    lineage: Lineage


@dataclass(frozen=True, slots=True)
class BooksDataset:
    books: tuple[Candidate[BookDraft], ...]
    categories: frozenset[str]
    """Category names from the home page side nav (``parsers.books.category_names``)."""


@dataclass(frozen=True, slots=True)
class QuotesDataset:
    quotes: tuple[Candidate[QuoteDraft], ...]
    authors: tuple[Candidate[AuthorDraft], ...]
    require_author_links: bool = True
    """``False`` for the JavaScript variant, whose rendered quotes carry no author link or slug."""


type Dataset = BooksDataset | QuotesDataset


@dataclass(frozen=True, slots=True)
class ValidationPolicy:
    max_failure_rate: Decimal = Decimal(0)
    """Largest tolerated ``quarantined / total``, in ``[0, 1]``. Exceeding it fails the verdict."""

    def __post_init__(self) -> None:
        if not Decimal(0) <= self.max_failure_rate <= Decimal(1):
            raise ValueError(f"max_failure_rate must be in [0, 1], got {self.max_failure_rate}")


class Failure(BaseModel):
    """One broken rule on one row."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    record_type: str
    row_key: str
    source_url: str
    field: str
    rule: str
    message: str
    value_repr: str


class ValidationReport(BaseModel):
    """Counts, every failure (sorted), and the verdict for one dataset."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    contract_version: str
    total_rows: int
    valid_rows: int
    quarantined_rows: int
    failure_rate: Decimal
    max_failure_rate: Decimal
    rule_counts: dict[str, int]
    """Failures per rule, keys sorted. A row can break several rules, so this can exceed the rows."""
    failures: tuple[Failure, ...]
    verdict: Verdict
    verdict_reason: str


@dataclass(frozen=True, slots=True)
class QuarantinedRow:
    """A rejected input row, kept whole so the quarantine output can show what came in."""

    record_type: str
    row_key: str
    draft: Draft
    lineage: Lineage
    failures: tuple[Failure, ...]

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready form (``Decimal`` and ``date`` as strings), keys in a fixed order."""
        return {
            "record_type": self.record_type,
            "row_key": self.row_key,
            "draft": {k: _jsonable(v) for k, v in _fields(self.draft).items()},
            "lineage": _fields(self.lineage),
            "failures": [f.model_dump() for f in self.failures],
        }


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: tuple[Record, ...]
    quarantined: tuple[QuarantinedRow, ...]
    report: ValidationReport

    @property
    def books(self) -> tuple[Book, ...]:
        return tuple(r for r in self.valid if isinstance(r, Book))

    @property
    def quotes(self) -> tuple[Quote, ...]:
        return tuple(r for r in self.valid if isinstance(r, Quote))

    @property
    def authors(self) -> tuple[Author, ...]:
        return tuple(r for r in self.valid if isinstance(r, Author))


def validate(dataset: Dataset, policy: ValidationPolicy | None = None) -> ValidationResult:
    """Validate a books or quotes dataset; see the module docstring for the rules and their order."""
    policy = policy or ValidationPolicy()
    if isinstance(dataset, BooksDataset):
        rows = _contract_rows("book", dataset.books, _book_row, Book, "book_id")
        _unique(rows, "book_id")
        for row in rows:
            if isinstance(row.record, Book) and row.record.category not in dataset.categories:
                row.fail(
                    "category",
                    "unknown_category",
                    "not a category on the home page",
                    row.record.category,
                )
        return _result(rows, books_v1.CONTRACT_VERSION, policy)

    authors = _contract_rows("author", dataset.authors, _author_row, Author, "author_slug")
    _unique(authors, "author_slug")
    known = {
        r.record.author_slug for r in authors if isinstance(r.record, Author) and not r.failures
    }
    quotes = _contract_rows("quote", dataset.quotes, _quote_row, Quote, "quote_id")
    _unique(quotes, "quote_id")
    if dataset.require_author_links:
        for row in quotes:
            if not isinstance(row.record, Quote):
                continue
            slug = row.record.author_slug
            if slug is None:
                row.fail("author_slug", "author_link_missing", "quote has no author link", slug)
            elif slug not in known:
                row.fail("author_slug", "unknown_author", "no valid author row has this slug", slug)
    return _result(authors + quotes, quotes_v1.CONTRACT_VERSION, policy)


@dataclass(slots=True)
class _Row:
    record_type: str
    key: str
    order: tuple[str, int, str, str]
    candidate: Candidate[Any]
    record: Record | None
    failures: list[Failure] = field(default_factory=list)

    def fail(self, field_name: str, rule: str, message: str, value: object) -> None:
        self.failures.append(
            Failure(
                record_type=self.record_type,
                row_key=self.key,
                source_url=self.candidate.lineage.source_url,
                field=field_name,
                rule=rule,
                message=message,
                value_repr=repr(value),
            )
        )


def _contract_rows[D: Draft](
    record_type: str,
    candidates: tuple[Candidate[D], ...],
    to_row: Callable[[D], dict[str, object]],
    model: type[Book] | type[Quote] | type[Author],
    key_field: str,
) -> list[_Row]:
    rows: list[_Row] = []
    for candidate in candidates:
        draft_columns = to_row(candidate.draft)
        data = {
            k: v for k, v in (draft_columns | _fields(candidate.lineage)).items() if v is not None
        }
        key_value = draft_columns.get(key_field)
        key = str(key_value) if key_value is not None else _fallback_key(candidate)
        position = candidate.draft.position if isinstance(candidate.draft, QuoteDraft) else 0
        row = _Row(
            record_type=record_type,
            key=key,
            # A total order: two rows that differ anywhere (draft or lineage) still sort the same way
            # whatever order they arrive in (found by the property test on input order).
            order=(
                candidate.lineage.source_url,
                position,
                candidate.lineage.content_sha256,
                repr(candidate),
            ),
            candidate=candidate,
            record=None,
        )
        try:
            row.record = model.model_validate(data)
        except ValidationError as exc:
            for error in exc.errors():
                _record_error(row, error, data)
        rows.append(row)
    rows.sort(key=lambda r: (r.key, r.order))
    return rows


def _record_error(row: _Row, error: Mapping[str, Any], data: Mapping[str, object]) -> None:
    loc = error["loc"]
    ctx = error.get("ctx") or {}
    field_name = ".".join(str(part) for part in loc) if loc else str(ctx.get("field", "__row__"))
    if error["type"] == "missing":
        value: object = None
    elif loc:
        value = error["input"]
    else:
        value = data.get(field_name)
    row.fail(field_name, error["type"], error["msg"], value)


def _unique(rows: list[_Row], key_field: str) -> None:
    kept: dict[str, _Row] = {}
    for row in rows:  # already in canonical order
        if row.record is None:
            continue
        first = kept.setdefault(row.key, row)
        if first is not row:
            row.fail(
                key_field,
                "duplicate_key",
                f"duplicate of the row from {first.candidate.lineage.source_url}",
                row.key,
            )


def _result(rows: list[_Row], contract_version: str, policy: ValidationPolicy) -> ValidationResult:
    valid: list[Record] = []
    quarantined: list[QuarantinedRow] = []
    for row in rows:
        if row.record is not None and not row.failures:
            valid.append(row.record)
        else:
            quarantined.append(
                QuarantinedRow(
                    record_type=row.record_type,
                    row_key=row.key,
                    draft=row.candidate.draft,
                    lineage=row.candidate.lineage,
                    failures=tuple(sorted(row.failures, key=_failure_order)),
                )
            )
    failures = sorted((f for q in quarantined for f in q.failures), key=_failure_order)
    counts: dict[str, int] = {}
    for failure in failures:
        counts[failure.rule] = counts.get(failure.rule, 0) + 1
    total = len(rows)
    rate = (
        (Decimal(len(quarantined)) / Decimal(total)).quantize(_RATE_PLACES) if total else Decimal(0)
    )
    if total == 0:
        verdict, reason = Verdict.FAIL, "empty dataset: nothing to publish"
    elif Decimal(len(quarantined)) > policy.max_failure_rate * Decimal(total):
        verdict = Verdict.FAIL
        reason = f"{len(quarantined)} of {total} rows quarantined, above max rate {policy.max_failure_rate}"
    else:
        verdict = Verdict.PASS
        reason = f"{len(quarantined)} of {total} rows quarantined, within max rate {policy.max_failure_rate}"
    report = ValidationReport(
        contract_version=contract_version,
        total_rows=total,
        valid_rows=len(valid),
        quarantined_rows=len(quarantined),
        failure_rate=rate,
        max_failure_rate=policy.max_failure_rate,
        rule_counts=dict(sorted(counts.items())),
        failures=tuple(failures),
        verdict=verdict,
        verdict_reason=reason,
    )
    return ValidationResult(valid=tuple(valid), quarantined=tuple(quarantined), report=report)


def _failure_order(f: Failure) -> tuple[str, ...]:
    return (f.record_type, f.row_key, f.source_url, f.field, f.rule, f.message, f.value_repr)


def _fallback_key(candidate: Candidate[Any]) -> str:
    draft = candidate.draft
    suffix = f"#{draft.position}" if isinstance(draft, QuoteDraft) else ""
    return f"<no key> {candidate.lineage.source_url}{suffix}"


def _book_row(d: BookDraft) -> dict[str, object]:
    return {
        "book_id": d.upc,
        "title": d.title,
        "price_gbp": d.price_gbp,
        "stock_count": d.stock_count,
        "in_stock": d.in_stock,
        "rating": d.rating,
        "category": d.category,
        "description": d.description,
        "product_url": d.product_url,
        "image_url": d.image_url,
    }


def _quote_row(d: QuoteDraft) -> dict[str, object]:
    return {
        "quote_id": d.quote_id,
        "text": d.text,
        "author_name": d.author_name,
        "author_slug": d.author_slug,
        "tags": d.tags,
        "page_url": d.page_url,
    }


def _author_row(d: AuthorDraft) -> dict[str, object]:
    return {
        "author_slug": d.author_slug,
        "name": d.name,
        "born_date": d.born_date,
        "born_location": d.born_location,
        "bio": d.bio,
    }


def _fields(obj: Any) -> dict[str, Any]:
    return {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}


def _jsonable(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    return value
