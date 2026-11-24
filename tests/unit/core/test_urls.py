import pytest
from hypothesis import given
from hypothesis import strategies as st

from scraping_pipeline.core.urls import InvalidUrlError, canonicalize, domain, resolve, same_site


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("HTTPS://Books.ToScrape.com/index.html", "https://books.toscrape.com/index.html"),
        ("https://books.toscrape.com:443/", "https://books.toscrape.com/"),
        ("http://books.toscrape.com:80/a", "http://books.toscrape.com/a"),
        ("http://books.toscrape.com:8080/a", "http://books.toscrape.com:8080/a"),
        ("https://quotes.toscrape.com", "https://quotes.toscrape.com/"),
        ("https://quotes.toscrape.com/page/2/#top", "https://quotes.toscrape.com/page/2/"),
        ("https://x.test/a/./b/../c", "https://x.test/a/c"),
        ("https://x.test/a/b/", "https://x.test/a/b/"),
        ("https://x.test/?b=2&a=1&a=0", "https://x.test/?a=0&a=1&b=2"),
        ("https://x.test/Path/Case", "https://x.test/Path/Case"),
        ("  https://x.test/a  ", "https://x.test/a"),
    ],
)
def test_canonicalize(raw: str, expected: str) -> None:
    assert canonicalize(raw) == expected


@pytest.mark.parametrize(
    "bad", ["", "/relative", "ftp://x.test/", "mailto:a@b.test", "https://", "http://x.test:99999/"]
)
def test_canonicalize_rejects_non_http_urls(bad: str) -> None:
    with pytest.raises(InvalidUrlError):
        canonicalize(bad)


def test_resolve_relative_links_like_the_books_site() -> None:
    base = "https://books.toscrape.com/catalogue/category/books/travel_2/index.html"
    href = "../../../its-only-the-himalayas_981/index.html"
    assert resolve(base, href) == (
        "https://books.toscrape.com/catalogue/its-only-the-himalayas_981/index.html"
    )


def test_domain_and_same_site() -> None:
    assert domain("https://Books.ToScrape.com/x") == "books.toscrape.com"
    assert same_site("https://books.toscrape.com/a", "https://BOOKS.toscrape.com/")
    assert not same_site("https://quotes.toscrape.com/", "https://books.toscrape.com/")
    assert not same_site("http://books.toscrape.com/", "https://books.toscrape.com/")
    with pytest.raises(InvalidUrlError):
        domain("/no-host")


_segment = st.text(alphabet="abcXYZ09-_.", min_size=1, max_size=6)


@given(
    host=st.sampled_from(["books.toscrape.com", "QUOTES.toscrape.com", "x.test"]),
    segments=st.lists(_segment, max_size=5),
    trailing=st.booleans(),
    query=st.lists(st.tuples(_segment, _segment), max_size=3),
)
def test_canonicalize_is_idempotent(
    host: str, segments: list[str], trailing: bool, query: list[tuple[str, str]]
) -> None:
    path = "/" + "/".join(segments) + ("/" if trailing and segments else "")
    qs = "&".join(f"{k}={v}" for k, v in query)
    url = f"https://{host}{path}" + (f"?{qs}" if qs else "")
    once = canonicalize(url)
    assert canonicalize(once) == once
