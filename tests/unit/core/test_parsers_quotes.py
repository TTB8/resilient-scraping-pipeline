"""The quotes parser against every saved quotes.toscrape.com page, plus synthetic edge cases."""

from datetime import date
from pathlib import Path

import pytest

from scraping_pipeline.core.parsers import quotes
from scraping_pipeline.core.parsers.base import AuthorDraft, ParseResult, QuoteDraft
from scraping_pipeline.core.text import quote_id
from scraping_pipeline.core.types import Link, PageKind

SITES = Path(__file__).parents[2] / "fixtures" / "sites"
BASE = "https://quotes.toscrape.com"

# fixture file -> the real URL it was captured from
PAGES = {
    "index.html": "/",
    "page/2/index.html": "/page/2/",
    "page/10/index.html": "/page/10/",
    "tag/love/index.html": "/tag/love/",
    "js/index.html": "/js/",
    "js/page/2/index.html": "/js/page/2/",
    "author/Albert-Einstein/index.html": "/author/Albert-Einstein",
    "author/J-K-Rowling/index.html": "/author/J-K-Rowling",
}


def parse_fixture(rel: str, kind: PageKind) -> ParseResult[quotes.QuotesDraft]:
    html = (SITES / "quotes" / rel).read_text(encoding="utf-8")
    return quotes.parse(BASE + PAGES[rel], html, kind)


def listing(rel: str) -> tuple[list[QuoteDraft], ParseResult[quotes.QuotesDraft]]:
    result = parse_fixture(rel, PageKind.LISTING)
    drafts = [d for d in result.drafts if isinstance(d, QuoteDraft)]
    assert len(drafts) == len(result.drafts)
    return drafts, result


def author(rel: str) -> AuthorDraft:
    result = parse_fixture(rel, PageKind.AUTHOR)
    assert result.warnings == ()
    assert result.links == ()
    (draft,) = result.drafts
    assert isinstance(draft, AuthorDraft)
    return draft


def test_first_page() -> None:
    drafts, result = listing("index.html")
    assert result.warnings == ()
    assert len(drafts) == 10
    first = drafts[0]
    assert first.text == (
        "The world as we have created it is a process of our thinking. "
        "It cannot be changed without changing our thinking."
    )
    assert first.author_name == "Albert Einstein"
    assert first.author_slug == "Albert-Einstein"
    assert first.tags == ("change", "deep-thoughts", "thinking", "world")
    assert first.page_url == BASE + "/"
    assert first.position == 0
    assert first.quote_id == quote_id(first.text, "Albert Einstein")
    assert [d.position for d in drafts] == list(range(10))
    assert all(d.quote_id and d.author_slug for d in drafts)


def test_first_page_links_distinct_authors_then_next() -> None:
    _, result = listing("index.html")
    authors = [link for link in result.links if link.kind is PageKind.AUTHOR]
    assert len(authors) == 8  # ten quotes, eight distinct authors
    assert authors[0] == Link(BASE + "/author/Albert-Einstein", PageKind.AUTHOR)
    assert result.links[-1] == Link(BASE + "/page/2/", PageKind.LISTING)
    assert not any("/tag/" in link.url for link in result.links)  # tag pages are not followed


def test_middle_page_has_next() -> None:
    drafts, result = listing("page/2/index.html")
    assert len(drafts) == 10
    assert drafts[0].author_name == "Marilyn Monroe"
    assert result.links[-1] == Link(BASE + "/page/3/", PageKind.LISTING)


def test_last_page_has_no_next_link() -> None:
    drafts, result = listing("page/10/index.html")
    assert len(drafts) == 10
    assert [link.kind for link in result.links] == [PageKind.AUTHOR] * 10
    # Only the outer pair of curly quotes is stripped; inner punctuation stays.
    assert drafts[0].text == (
        'The truth." Dumbledore sighed. "It is a beautiful and terrible thing, '
        "and should therefore be treated with great caution."
    )
    assert drafts[0].tags == ("truth",)


def test_tag_page_parses_like_a_listing() -> None:
    drafts, result = listing("tag/love/index.html")
    assert len(drafts) == 10
    assert drafts[0].author_name == "André Gide"
    assert drafts[0].author_slug == "Andre-Gide"
    assert result.links[-1] == Link(BASE + "/tag/love/page/2/", PageKind.LISTING)


@pytest.mark.parametrize(
    ("rel", "next_url"), [("js/index.html", "/js/page/2/"), ("js/page/2/index.html", "/js/page/3/")]
)
def test_js_variant_unrendered_has_no_quotes(rel: str, next_url: str) -> None:
    # The inline `var data = [...]` script is deliberately not read: the browser renders it.
    drafts, result = listing(rel)
    assert drafts == []
    assert result.warnings == ("page: no quotes found",)
    assert result.links[-1] == Link(BASE + next_url, PageKind.LISTING)


def test_js_variant_rendered_dom_uses_the_same_parser() -> None:
    # Markup exactly as the page's document.write template produces it (rendering is a browser test).
    rendered = (
        "<div class='quote'><span class='text'>“The world as we have created it is a process "
        "of our thinking. It cannot be changed without changing our thinking.”</span><span>by "
        "<small class='author'>Albert Einstein</small></span><div class='tags'>Tags: "
        "<a class='tag'>change</a> <a class='tag'>deep-thoughts</a> <a class='tag'>thinking</a> "
        "<a class='tag'>world</a></div></div>"
        '<nav><ul class="pager"><li class="next"><a href="/js/page/2/">Next</a></li></ul></nav>'
    )
    result = quotes.parse(BASE + "/js/", rendered, PageKind.LISTING)
    (draft,) = result.drafts
    assert isinstance(draft, QuoteDraft)
    static, _ = listing("index.html")
    assert draft.quote_id == static[0].quote_id  # same id across variants
    assert draft.tags == static[0].tags
    assert draft.author_slug is None  # the template renders no "(about)" link
    assert result.links == (Link(BASE + "/js/page/2/", PageKind.LISTING),)


def test_einstein() -> None:
    draft = author("author/Albert-Einstein/index.html")
    assert draft.author_slug == "Albert-Einstein"
    assert draft.name == "Albert Einstein"
    assert draft.born_date == date(1879, 3, 14)
    assert draft.born_location == "Ulm, Germany"
    assert draft.bio is not None
    assert draft.bio.startswith("In 1879, Albert Einstein was born in Ulm, Germany.")
    assert "\n" not in draft.bio


def test_rowling() -> None:
    draft = author("author/J-K-Rowling/index.html")
    assert (draft.author_slug, draft.name, draft.born_date) == (
        "J-K-Rowling",
        "J.K. Rowling",
        date(1965, 7, 31),
    )
    assert draft.born_location == "Yate, South Gloucestershire, England, The United Kingdom"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("March 14, 1879", date(1879, 3, 14)),
        ("July 1, 2000", date(2000, 7, 1)),
        ("February 30, 1900", "February 30, 1900"),
        ("Smarch 14, 1879", "Smarch 14, 1879"),
        ("14 March 1879", "14 March 1879"),
    ],
)
def test_parse_born_date(raw: str, expected: date | str) -> None:
    assert quotes.parse_born_date(raw) == expected


@pytest.mark.parametrize(
    ("url", "slug"),
    [
        (BASE + "/author/Albert-Einstein", "Albert-Einstein"),
        (BASE + "/author/Albert-Einstein/", "Albert-Einstein"),
        (BASE + "/author/Andr%C3%A9", "André"),
        (BASE + "/page/2/", None),
        ("http://[broken", None),
    ],
)
def test_author_slug_from_url(url: str, slug: str | None) -> None:
    assert quotes.author_slug_from_url(url) == slug


def test_empty_author_page_warns_for_every_field() -> None:
    result = quotes.parse(BASE + "/page/1/", "<html></html>", PageKind.AUTHOR)
    (draft,) = result.drafts
    assert draft == AuthorDraft(None, None, None, None, None)
    assert result.warnings == (
        f"author_slug: not an author URL '{BASE}/page/1/'",
        "name: missing",
        "born_date: missing",
        "born_location: missing",
        "bio: missing",
    )


def test_author_page_with_odd_values() -> None:
    html = (
        '<h3 class="author-title">X</h3><span class="author-born-date">sometime</span>'
        '<span class="author-born-location">in </span><div class="author-description">b</div>'
    )
    result = quotes.parse(BASE + "/author/X", html, PageKind.AUTHOR)
    draft = result.drafts[0]
    assert isinstance(draft, AuthorDraft)
    assert draft.born_date == "sometime"
    assert draft.born_location is None
    assert result.warnings == (
        "born_date: unrecognised date 'sometime'",
        "born_location: empty after removing 'in'",
    )
    # "in" is only removed as a whole word: "Indiana" keeps its "In".
    html = html.replace("in </span>", "Indiana</span>")
    located = quotes.parse(BASE + "/author/X", html, PageKind.AUTHOR).drafts[0]
    assert isinstance(located, AuthorDraft)
    assert located.born_location == "Indiana"


def test_odd_quotes_degrade_to_warnings() -> None:
    html = (
        '<div class="quote"><span class="text">“”</span></div>'
        '<div class="quote"><span class="text">Unquoted</span><small class="author">A</small>'
        '<a href="mailto:x@y.test/author/">(about)</a>'
        '<div class="tags"><a class="tag">b</a><a class="tag">a</a><a class="tag">b</a>'
        '<a class="tag"> </a></div></div>'
    )
    result = quotes.parse(BASE + "/", html, PageKind.LISTING)
    first, second = result.drafts
    assert isinstance(first, QuoteDraft)
    assert isinstance(second, QuoteDraft)
    assert (first.text, first.author_name, first.quote_id) == (None, None, None)
    assert second.text == "Unquoted"
    assert second.author_slug is None
    assert second.tags == ("a", "b")
    assert result.links == ()
    assert result.warnings == (
        "quote[0].text: missing",
        "quote[0].author_name: missing",
        "quote[1].author link: unresolvable href 'mailto:x@y.test/author/'",
    )


def test_detail_kind_is_not_a_quotes_page() -> None:
    result = quotes.parse(BASE + "/", "<html></html>", PageKind.DETAIL)
    assert result.drafts == ()
    assert result.warnings == ("page: the quotes site has no 'detail' pages",)


def test_every_fixture_page_is_covered() -> None:
    saved = {p.relative_to(SITES / "quotes").as_posix() for p in SITES.glob("quotes/**/*.html")}
    assert saved == set(PAGES)


def test_parser_version_is_set() -> None:
    assert quotes.PARSER_VERSION == "quotes-parser/1.0.0"
