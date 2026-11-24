"""Each contract rule, one failing case each, plus the shape guarantees (strict, frozen, column order)."""

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from scraping_pipeline.core.contracts import Author, Book, Quote, books_v1, quotes_v1
from scraping_pipeline.core.text import quote_id

LINEAGE: dict[str, Any] = {
    "source_url": "https://books.toscrape.com/catalogue/x_1/index.html",
    "content_sha256": "0" * 64,
    "fetch_run_id": "fetch-1",
    "parse_run_id": "0123456789abcdef",
    "parser_version": "books-parser/1.0.0",
}

BOOK: dict[str, Any] = {
    "book_id": "a897fe39b1053632",
    "title": "A Light in the Attic",
    "price_gbp": Decimal("51.77"),
    "stock_count": 22,
    "in_stock": True,
    "rating": 3,
    "category": "Poetry",
    "description": "",
    "product_url": "https://books.toscrape.com/catalogue/x_1/index.html",
    "image_url": "https://books.toscrape.com/media/x.jpg",
    **LINEAGE,
    "contract_version": books_v1.CONTRACT_VERSION,
}

QUOTE: dict[str, Any] = {
    "quote_id": quote_id("Be.", "A B"),
    "text": "Be.",
    "author_name": "A B",
    "author_slug": "A-B",
    "tags": ("a", "b"),
    "page_url": "https://quotes.toscrape.com/",
    **LINEAGE,
    "contract_version": quotes_v1.CONTRACT_VERSION,
}

AUTHOR: dict[str, Any] = {
    "author_slug": "A-B",
    "name": "A B",
    "born_date": date(1879, 3, 14),
    "born_location": "Ulm, Germany",
    "bio": "Bio.",
    **LINEAGE,
    "contract_version": quotes_v1.CONTRACT_VERSION,
}


def rule_of(model: type[Book] | type[Quote] | type[Author], data: dict[str, Any]) -> set[str]:
    with pytest.raises(ValidationError) as exc:
        model.model_validate(data)
    return {e["type"] for e in exc.value.errors()}


def test_valid_rows_validate() -> None:
    book = Book.model_validate(BOOK)
    assert Quote.model_validate(QUOTE).author_slug == "A-B"
    assert Author.model_validate(AUTHOR).born_date == date(1879, 3, 14)
    assert book.description == ""  # a missing description is allowed, as the empty string
    assert Quote.model_validate({**QUOTE, "author_slug": None}).author_slug is None


def test_price_is_quantized_to_two_places() -> None:
    assert str(Book.model_validate({**BOOK, "price_gbp": Decimal("51.7")}).price_gbp) == "51.70"


def test_record_columns_come_before_lineage_columns() -> None:
    assert list(Book.model_fields)[:2] == ["book_id", "title"]
    assert list(Book.model_fields)[-6:] == [
        "source_url",
        "content_sha256",
        "fetch_run_id",
        "parse_run_id",
        "parser_version",
        "contract_version",
    ]


def test_rows_are_frozen() -> None:
    book = Book.model_validate(BOOK)
    with pytest.raises(ValidationError):
        book.title = "x"


@pytest.mark.parametrize(
    ("change", "rule"),
    [
        ({"price_gbp": "51.77"}, "is_instance_of"),  # strict: no coercion from str
        ({"price_gbp": Decimal("0")}, "greater_than"),
        ({"price_gbp": Decimal("1.234")}, "decimal_max_places"),
        ({"stock_count": -1}, "greater_than_equal"),
        ({"stock_count": True}, "int_type"),
        ({"rating": 0}, "greater_than_equal"),
        ({"rating": 6}, "less_than_equal"),
        ({"rating": "Three"}, "int_type"),
        ({"book_id": "ABC"}, "string_pattern_mismatch"),
        ({"title": ""}, "string_too_short"),
        ({"title": " padded "}, "string_pattern_mismatch"),
        ({"category": ""}, "string_too_short"),
        ({"product_url": "ftp://books.toscrape.com/x"}, "url_not_http"),
        ({"image_url": "media/x.jpg"}, "url_not_http"),
        ({"image_url": "https://Books.toscrape.com/x"}, "url_not_canonical"),
        ({"in_stock": False}, "in_stock_mismatch"),
        ({"content_sha256": "xyz"}, "string_pattern_mismatch"),
        ({"fetch_run_id": ""}, "string_too_short"),
        ({"parse_run_id": "not-derived"}, "parse_run_id_shape"),
        ({"contract_version": "books/2.0.0"}, "contract_version_mismatch"),
        ({"surprise": 1}, "extra_forbidden"),
    ],
)
def test_book_rules(change: dict[str, Any], rule: str) -> None:
    assert rule in rule_of(Book, {**BOOK, **change})


def test_book_missing_field() -> None:
    data = dict(BOOK)
    del data["image_url"]
    assert rule_of(Book, data) == {"missing"}


@pytest.mark.parametrize(
    ("change", "rule"),
    [
        ({"quote_id": quote_id("Other.", "A B")}, "quote_id_mismatch"),
        ({"quote_id": "short"}, "string_pattern_mismatch"),
        ({"tags": ("b", "a")}, "tags_not_sorted_unique"),
        ({"tags": ("a", "a")}, "tags_not_sorted_unique"),
        ({"tags": ("A",)}, "tag_not_lowercase"),
        ({"tags": ("",)}, "tag_blank"),
        ({"tags": [" a"]}, "tuple_type"),
        ({"author_slug": "a/b"}, "string_pattern_mismatch"),
        ({"page_url": "https://quotes.toscrape.com"}, "url_not_canonical"),
        ({"contract_version": books_v1.CONTRACT_VERSION}, "contract_version_mismatch"),
    ],
)
def test_quote_rules(change: dict[str, Any], rule: str) -> None:
    assert rule in rule_of(Quote, {**QUOTE, **change})


def test_quote_id_mismatch_names_its_field() -> None:
    with pytest.raises(ValidationError) as exc:
        Quote.model_validate({**QUOTE, "text": "Changed."})
    (error,) = exc.value.errors()
    assert error["loc"] == ()
    assert error["ctx"]["field"] == "quote_id"


@pytest.mark.parametrize(
    ("change", "rule"),
    [
        ({"born_date": "March 14, 1879"}, "date_type"),
        ({"born_location": ""}, "string_too_short"),
        ({"author_slug": None}, "string_type"),
        ({"bio": ""}, "string_too_short"),
    ],
)
def test_author_rules(change: dict[str, Any], rule: str) -> None:
    assert rule in rule_of(Author, {**AUTHOR, **change})


def test_contract_versions() -> None:
    assert books_v1.CONTRACT_VERSION == "books/1.0.0"
    assert quotes_v1.CONTRACT_VERSION == "quotes/1.0.0"
    assert Book.CONTRACT_VERSION == books_v1.CONTRACT_VERSION
    assert Quote.CONTRACT_VERSION == Author.CONTRACT_VERSION == quotes_v1.CONTRACT_VERSION


def test_out_of_stock_is_consistent_and_zero_in_stock_is_not() -> None:
    assert Book.model_validate({**BOOK, "stock_count": 0, "in_stock": False}).in_stock is False
    assert rule_of(Book, {**BOOK, "stock_count": 0, "in_stock": True}) == {"in_stock_mismatch"}


def test_http_fixture_server_urls_validate_and_other_schemes_do_not() -> None:
    # The integration tests crawl a local fixture server over plain http.
    local = "http://127.0.0.1:8765/catalogue/x_1/index.html"
    row = {**BOOK, "source_url": local, "product_url": local}
    row["image_url"] = "http://127.0.0.1:8765/media/x.jpg"
    assert Book.model_validate(row).source_url == local
    ftp = {**row, "source_url": "ftp://127.0.0.1:8765/catalogue/x_1/index.html"}
    assert rule_of(Book, ftp) == {"url_not_http"}
