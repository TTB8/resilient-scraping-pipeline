"""Every validation rule with one failing case, the verdict, determinism, and the real fixtures end to end."""

import dataclasses
import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from scraping_pipeline.core.contracts import books_v1, quotes_v1
from scraping_pipeline.core.lineage import Lineage, derive_parse_run_id
from scraping_pipeline.core.parsers import books, quotes
from scraping_pipeline.core.parsers.base import AuthorDraft, BookDraft, QuoteDraft
from scraping_pipeline.core.text import quote_id
from scraping_pipeline.core.types import PageKind
from scraping_pipeline.core.validate import (
    BooksDataset,
    Candidate,
    QuotesDataset,
    ValidationPolicy,
    ValidationResult,
    Verdict,
    validate,
)

SITES = Path(__file__).parents[2] / "fixtures" / "sites"
BOOKS = "https://books.toscrape.com/"
QUOTES = "https://quotes.toscrape.com/"
PARSE_RUN = derive_parse_run_id("0" * 64, "test", "test")


def lineage(url: str, contract: str, body: str = "") -> Lineage:
    return Lineage(
        source_url=url,
        content_sha256=hashlib.sha256(body.encode()).hexdigest(),
        fetch_run_id="fetch-1",
        parse_run_id=PARSE_RUN,
        parser_version="test-parser/1",
        contract_version=contract,
    )


def book(n: int, **change: object) -> Candidate[BookDraft]:
    url = f"{BOOKS}catalogue/b_{n}/index.html"
    draft = BookDraft(
        upc=f"{n:016x}",
        title=f"Book {n}",
        price_gbp=Decimal("10.00"),
        stock_count=1,
        in_stock=True,
        rating=3,
        category="Poetry",
        description="",
        product_url=url,
        image_url=f"{BOOKS}media/{n}.jpg",
    )
    return Candidate(dataclasses.replace(draft, **change), lineage(url, books_v1.CONTRACT_VERSION))  # type: ignore[arg-type]


def books_ds(*rows: Candidate[BookDraft], categories: frozenset[str] | None = None) -> BooksDataset:
    return BooksDataset(books=rows, categories=categories or frozenset({"Poetry", "Travel"}))


def author(slug: str, **change: object) -> Candidate[AuthorDraft]:
    draft = AuthorDraft(
        author_slug=slug, name=slug, born_date=date(1900, 1, 1), born_location="X", bio="Bio."
    )
    return Candidate(
        dataclasses.replace(draft, **change),  # type: ignore[arg-type]
        lineage(f"{QUOTES}author/{slug}", quotes_v1.CONTRACT_VERSION),
    )


def quote(
    text: str | None, slug: str | None = "A", position: int = 0, **change: object
) -> Candidate[QuoteDraft]:
    draft = QuoteDraft(
        quote_id=quote_id(text, "Name") if text else None,
        text=text,
        author_name="Name",
        author_slug=slug,
        tags=("life",),
        page_url=QUOTES,
        position=position,
    )
    return Candidate(
        dataclasses.replace(draft, **change),  # type: ignore[arg-type]
        lineage(QUOTES, quotes_v1.CONTRACT_VERSION),
    )


def rules(result: ValidationResult) -> list[tuple[str, str, str]]:
    return [(f.row_key, f.field, f.rule) for f in result.report.failures]


def test_clean_books_pass() -> None:
    result = validate(books_ds(book(2), book(1)))
    assert result.report.verdict is Verdict.PASS
    assert [b.book_id for b in result.books] == [f"{1:016x}", f"{2:016x}"]  # sorted by key
    assert result.quarantined == ()
    assert result.report.failures == ()
    assert result.report.rule_counts == {}
    assert result.report.contract_version == "books/1.0.0"
    assert result.quotes == ()
    assert result.authors == ()


def test_row_contract_failure_reports_field_rule_and_value() -> None:
    result = validate(books_ds(book(1), book(2, price_gbp="$3.00")))
    (row,) = result.quarantined
    (failure,) = row.failures
    assert (failure.row_key, failure.field, failure.rule) == (
        f"{2:016x}",
        "price_gbp",
        "is_instance_of",
    )
    assert failure.value_repr == "'$3.00'"
    assert failure.source_url == f"{BOOKS}catalogue/b_2/index.html"
    assert result.report.verdict is Verdict.FAIL


def test_missing_field_is_reported_as_missing_with_none() -> None:
    result = validate(books_ds(book(1, image_url=None)))
    (failure,) = result.report.failures
    assert (failure.field, failure.rule, failure.value_repr) == ("image_url", "missing", "None")


def test_row_without_key_gets_a_fallback_key() -> None:
    result = validate(books_ds(book(1, upc=None)))
    (row,) = result.quarantined
    assert row.row_key == f"<no key> {BOOKS}catalogue/b_1/index.html"
    assert rules(result) == [(row.row_key, "book_id", "missing")]


def test_cross_field_failure_names_the_field_and_its_value() -> None:
    result = validate(books_ds(book(1, in_stock=False)))
    (failure,) = result.report.failures
    assert (failure.field, failure.rule, failure.value_repr) == (
        "in_stock",
        "in_stock_mismatch",
        "False",
    )


def test_duplicate_book_id_keeps_the_first_in_canonical_order() -> None:
    a = book(1)
    b = dataclasses.replace(
        book(1), lineage=lineage(f"{BOOKS}catalogue/z_9/index.html", books_v1.CONTRACT_VERSION)
    )
    for order in ((a, b), (b, a)):
        result = validate(books_ds(*order))
        assert [x.source_url for x in result.books] == [a.lineage.source_url]
        (failure,) = result.report.failures
        assert failure.rule == "duplicate_key"
        assert failure.field == "book_id"
        assert failure.source_url == b.lineage.source_url
        assert a.lineage.source_url in failure.message


def test_unknown_category() -> None:
    result = validate(books_ds(book(1, category="Cooking")))
    assert rules(result) == [(f"{1:016x}", "category", "unknown_category")]
    assert result.report.failures[0].value_repr == "'Cooking'"


def test_empty_category_set_fails_closed() -> None:
    result = validate(BooksDataset(books=(book(1),), categories=frozenset()))
    assert rules(result) == [(f"{1:016x}", "category", "unknown_category")]


def test_quotes_and_authors_pass() -> None:
    result = validate(
        QuotesDataset(quotes=(quote("B."), quote("A.", position=1)), authors=(author("A"),))
    )
    assert result.report.verdict is Verdict.PASS
    assert len(result.quotes) == 2
    assert [a.author_slug for a in result.authors] == ["A"]
    assert result.report.total_rows == 3
    assert result.report.contract_version == "quotes/1.0.0"


def test_unknown_author() -> None:
    result = validate(QuotesDataset(quotes=(quote("A.", slug="Nobody"),), authors=(author("A"),)))
    assert [f.rule for f in result.report.failures] == ["unknown_author"]


def test_quote_of_an_invalid_author_is_unknown_too() -> None:
    result = validate(
        QuotesDataset(quotes=(quote("A."),), authors=(author("A", born_date="soon"),))
    )
    assert sorted(f.rule for f in result.report.failures) == ["date_type", "unknown_author"]


def test_author_link_missing_on_the_static_site() -> None:
    result = validate(QuotesDataset(quotes=(quote("A.", slug=None),), authors=(author("A"),)))
    assert [f.rule for f in result.report.failures] == ["author_link_missing"]


def test_js_variant_needs_no_author_links() -> None:
    dataset = QuotesDataset(
        quotes=(quote("A.", slug=None),), authors=(), require_author_links=False
    )
    result = validate(dataset)
    assert result.report.verdict is Verdict.PASS
    assert len(result.quotes) == 1


def test_duplicate_quote_and_author() -> None:
    dup_quote = quote("A.", position=3)
    result = validate(
        QuotesDataset(
            quotes=(quote("A."), dup_quote), authors=(author("A"), author("A", bio="Other."))
        )
    )
    assert sorted((f.record_type, f.rule) for f in result.report.failures) == [
        ("author", "duplicate_key"),
        ("quote", "duplicate_key"),
    ]
    assert len(result.valid) == 2


def test_quote_without_id_uses_page_and_position_as_key() -> None:
    result = validate(QuotesDataset(quotes=(quote(None, position=4),), authors=(author("A"),)))
    (row,) = result.quarantined
    assert row.row_key == f"<no key> {QUOTES}#4"
    assert {f.field for f in row.failures} == {"quote_id", "text"}


def test_failure_rate_threshold() -> None:
    rows = (book(1), book(2), book(3), book(4, rating=9))
    assert validate(books_ds(*rows)).report.verdict is Verdict.FAIL  # default policy: 0
    lenient = validate(books_ds(*rows), ValidationPolicy(max_failure_rate=Decimal("0.25")))
    assert lenient.report.verdict is Verdict.PASS
    assert lenient.report.failure_rate == Decimal("0.2500")
    assert "within max rate 0.25" in lenient.report.verdict_reason
    strict = validate(books_ds(*rows), ValidationPolicy(max_failure_rate=Decimal("0.24")))
    assert strict.report.verdict is Verdict.FAIL
    assert "above max rate 0.24" in strict.report.verdict_reason


def test_empty_dataset_fails() -> None:
    result = validate(books_ds(), ValidationPolicy(max_failure_rate=Decimal(1)))
    assert result.report.verdict is Verdict.FAIL
    assert result.report.verdict_reason.startswith("empty dataset")
    assert result.report.failure_rate == Decimal(0)


@pytest.mark.parametrize("rate", [Decimal("-0.01"), Decimal("1.01")])
def test_policy_rejects_rates_outside_0_1(rate: Decimal) -> None:
    with pytest.raises(ValueError, match="max_failure_rate"):
        ValidationPolicy(max_failure_rate=rate)


def test_rule_counts_and_failure_order_are_sorted() -> None:
    result = validate(
        books_ds(book(3, rating=0, title=""), book(1, category="X"), book(2, rating=7))
    )
    keys = [(f.row_key, f.field, f.rule) for f in result.report.failures]
    assert keys == sorted(keys)
    assert list(result.report.rule_counts) == sorted(result.report.rule_counts)
    assert result.report.rule_counts == {
        "greater_than_equal": 1,
        "less_than_equal": 1,
        "string_too_short": 1,
        "unknown_category": 1,
    }
    assert result.report.quarantined_rows == 3


def test_result_does_not_depend_on_input_order() -> None:
    rows = [book(1), book(2, rating=0), book(3), book(1), book(4, category="X")]
    assert validate(books_ds(*rows)) == validate(books_ds(*reversed(rows)))


def test_quarantined_row_is_json_ready() -> None:
    result = validate(QuotesDataset(quotes=(), authors=(author("A", born_date="soon", bio=None),)))
    payload = result.quarantined[0].to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert payload["draft"]["born_date"] == "soon"
    assert payload["lineage"]["source_url"] == f"{QUOTES}author/A"
    assert [(f["field"], f["rule"]) for f in payload["failures"]] == [
        ("bio", "missing"),
        ("born_date", "date_type"),
    ]
    zero = validate(books_ds(book(1, price_gbp=Decimal("0")))).quarantined[0].to_dict()
    assert zero["draft"]["price_gbp"] == "0"
    assert validate(
        QuotesDataset(quotes=(quote("A.", tags=("B",)),), authors=(author("A"),))
    ).quarantined[0].to_dict()["draft"]["tags"] == ["B"]


# --- the real fixtures, parsed and validated end to end ------------------------------------------


def _fixture(site: str, rel: str) -> str:
    return (SITES / site / rel).read_text(encoding="utf-8")


def test_saved_books_validate_cleanly() -> None:
    categories = frozenset(books.category_names(_fixture("books", "index.html")))
    rows = []
    for rel in sorted(
        p.relative_to(SITES / "books").as_posix()
        for p in SITES.glob("books/catalogue/*_*/index.html")
    ):
        html = _fixture("books", rel)
        url = BOOKS + rel
        for draft in books.parse(url, html, PageKind.DETAIL).drafts:
            rows.append(Candidate(draft, lineage(url, books_v1.CONTRACT_VERSION, html)))
    result = validate(BooksDataset(books=tuple(rows), categories=categories))
    assert result.report.failures == ()
    assert result.report.verdict is Verdict.PASS
    assert {b.title for b in result.books} == {
        "A Light in the Attic",
        "It's Only the Himalayas",
        "Sharp Objects",
    }
    attic = next(b for b in result.books if b.book_id == "a897fe39b1053632")
    assert attic.price_gbp == Decimal("51.77")
    assert (
        attic.content_sha256
        == hashlib.sha256(
            _fixture("books", "catalogue/a-light-in-the-attic_1000/index.html").encode()
        ).hexdigest()
    )


def test_saved_quotes_page_with_its_two_saved_authors() -> None:
    html = _fixture("quotes", "index.html")
    quote_rows = tuple(
        Candidate(d, lineage(QUOTES, quotes_v1.CONTRACT_VERSION, html))
        for d in quotes.parse(QUOTES, html, PageKind.LISTING).drafts
        if isinstance(d, QuoteDraft)
    )
    author_rows = []
    for slug in ("Albert-Einstein", "J-K-Rowling"):
        page = _fixture("quotes", f"author/{slug}/index.html")
        url = f"{QUOTES}author/{slug}"
        for d in quotes.parse(url, page, PageKind.AUTHOR).drafts:
            assert isinstance(d, AuthorDraft)
            author_rows.append(Candidate(d, lineage(url, quotes_v1.CONTRACT_VERSION, page)))
    result = validate(QuotesDataset(quotes=quote_rows, authors=tuple(author_rows)))
    # Only two of the eight authors on page 1 are saved: the others' quotes are quarantined, not lost.
    assert len(result.authors) == 2
    assert {q.author_slug for q in result.quotes} == {"Albert-Einstein", "J-K-Rowling"}
    assert set(result.report.rule_counts) == {"unknown_author"}
    assert len(result.valid) + len(result.quarantined) == 12
