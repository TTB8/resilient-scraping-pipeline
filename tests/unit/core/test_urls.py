import pytest
from hypothesis import given
from hypothesis import strategies as st

from scraping_pipeline.core.urls import (
    InvalidUrlError,
    canonicalize,
    domain,
    link_base,
    normalize_percent_encoding,
    remove_dot_segments,
    resolve,
    same_site,
)


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
        # Stable sort by key: repeated keys keep their order (R1-13; this row used to reorder them).
        ("https://x.test/?b=2&a=1&a=0", "https://x.test/?a=1&a=0&b=2"),
        ("https://x.test/?a=2&a=1", "https://x.test/?a=2&a=1"),
        ("https://x.test/p?foo", "https://x.test/p?foo"),  # bare key stays bare
        ("https://x.test/p?b&a=", "https://x.test/p?a=&b"),
        ("https://x.test/p?q=%zz", "https://x.test/p?q=%zz"),  # bad escape not double-encoded
        ("https://x.test/p?q=%7e&r=a%20b", "https://x.test/p?q=%7e&r=a%20b"),  # query is raw
        ("https://x.test/p?&&a=1&", "https://x.test/p?a=1"),
        ("https://x.test/p?", "https://x.test/p"),
        ("http://[::1]:8080/a", "http://[::1]:8080/a"),
        ("http://[2001:DB8::1]/", "http://[2001:db8::1]/"),
        ("https://[::1]:443/", "https://[::1]/"),
        ("https://x.test/a//b", "https://x.test/a//b"),  # '//' is a distinct path
        ("https://x.test//a/./b/../c", "https://x.test//a/c"),
        ("https://x.test/a/b/../../../c", "https://x.test/c"),
        ("https://x.test/a/..", "https://x.test/"),
        ("https://x.test/a/.", "https://x.test/a/"),
        ("https://x.test/a/%2E%2E/b", "https://x.test/b"),
        ("https://x.test/%7Ejoe/%41", "https://x.test/~joe/A"),
        ("https://x.test/a%2fb%c3%a9", "https://x.test/a%2Fb%C3%A9"),
        ("https://x.test/a b", "https://x.test/a%20b"),
        ("https://x.test/a%20b", "https://x.test/a%20b"),
        ("https://x.test/caf\u00e9", "https://x.test/caf%C3%A9"),
        ("https://x.test/100%", "https://x.test/100%"),
        ("https://x.test/%zz", "https://x.test/%zz"),
        ("https://B\u00dcCHER.example/", "https://xn--bcher-kva.example/"),
        ("https://user:pw@x.test/", "https://x.test/"),
        ("https://x.test/Path/Case", "https://x.test/Path/Case"),
        ("  https://x.test/a  ", "https://x.test/a"),
    ],
)
def test_canonicalize(raw: str, expected: str) -> None:
    assert canonicalize(raw) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "/relative",
        "ftp://x.test/",
        "mailto:a@b.test",
        "https://",
        "http://x.test:99999/",
        "http://[::1/",
        "https://\u00e9..b/",
        "https://\u00e9" + "a" * 70 + ".test/",
    ],
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


def test_invalid_ipv6_is_rejected_by_domain_and_same_site() -> None:
    with pytest.raises(InvalidUrlError):
        domain("http://[::1/")
    assert not same_site("http://[::1/", "http://[::1]/")
    assert domain("http://[::1]:8080/") == "::1"
    assert same_site("http://[::1]:8080/a", "http://[::1]:8080/")
    assert not same_site("http://[::1]:8080/a", "http://[::1]/")


@pytest.mark.parametrize(
    ("url", "origin", "expected"),
    [
        ("https://x.test:8080/a", "https://x.test/", False),  # another port is another server
        ("http://x.test:8080/a", "http://x.test:80/", False),
        ("https://x.test:443/a", "https://x.test/", True),  # explicit default port
        ("https://x.test/a", "https://x.test:443/", True),
        ("http://x.test:80/a", "http://x.test/", True),
        ("http://x.test:443/a", "https://x.test/", False),  # scheme still counts
        ("https://x.test:8443/a", "https://x.test:8443/b", True),
        ("https://B\u00dcCHER.example/", "https://xn--bcher-kva.example/", True),
        ("https://x.test:99999/", "https://x.test/", False),  # unparseable port: fail closed
        ("/relative", "/relative", False),  # no host: fail closed
        ("https://\u00e9..b/", "https://\u00e9..b/", False),  # invalid IDNA host
    ],
)
def test_same_site_compares_effective_port(url: str, origin: str, expected: bool) -> None:
    assert same_site(url, origin) is expected


def test_resolve_keeps_ipv6_and_double_slash() -> None:
    assert resolve("http://[::1]:8080/a/b", "../c//d") == "http://[::1]:8080/c//d"


BASE = "http://a.test/b/c/d;p?q"


@pytest.mark.parametrize(
    ("href", "expected"),
    [  # RFC 3986 section 5.4 reference resolution examples (normal and abnormal)
        ("g", "http://a.test/b/c/g"),
        ("./g", "http://a.test/b/c/g"),
        ("g/", "http://a.test/b/c/g/"),
        ("/g", "http://a.test/g"),
        ("//g.test", "http://g.test/"),
        ("?y", "http://a.test/b/c/d;p?y"),
        ("g?y", "http://a.test/b/c/g?y"),
        ("#s", "http://a.test/b/c/d;p?q"),
        ("", "http://a.test/b/c/d;p?q"),
        ("?", "http://a.test/b/c/d;p"),
        (".", "http://a.test/b/c/"),
        ("..", "http://a.test/b/"),
        ("../g", "http://a.test/b/g"),
        ("../../../../g", "http://a.test/g"),
        ("/./g", "http://a.test/g"),
        ("g..", "http://a.test/b/c/g.."),
        ("./g/.", "http://a.test/b/c/g/"),
        ("g;x=1/../y", "http://a.test/b/c/y"),
        ("https://other.test/x", "https://other.test/x"),
        ("  g  ", "http://a.test/b/c/g"),
    ],
)
def test_resolve_rfc3986_examples(href: str, expected: str) -> None:
    assert resolve(BASE, href) == expected


def test_resolve_against_host_only_base() -> None:
    assert resolve("https://x.test", "a") == "https://x.test/a"


def test_resolve_rejects_unresolvable() -> None:
    with pytest.raises(InvalidUrlError):
        resolve("http://[::1/", "a")
    with pytest.raises(InvalidUrlError):
        resolve(BASE, "mailto:a@b.test")


@pytest.mark.parametrize(
    ("path", "expected"),
    [  # RFC 3986 section 5.2.4 examples, plus the edges of each rule
        ("/a/b/c/./../../g", "/a/g"),
        ("mid/content=5/../6", "mid/6"),
        ("../a", "a"),
        ("./a", "a"),
        (".", ""),
        ("..", ""),
        ("/.", "/"),
        ("/..", "/"),
        ("/../../a", "/a"),
        ("//a//", "//a//"),
        ("/a/.b/..c", "/a/.b/..c"),
        ("", ""),
    ],
)
def test_remove_dot_segments(path: str, expected: str) -> None:
    assert remove_dot_segments(path) == expected


_url_chars = st.sampled_from(
    [*"aZ09-._~:@!$&'()*+,;=/%", "%7e", "%2F", "%zz", "%4", " ", "é", "ツ", "..", "."]
)
_path_strategy = st.lists(_url_chars, max_size=12).map(lambda xs: "/" + "".join(xs))
_query_key = st.sampled_from(["a", "b", "foo", "%61", "a%20b", "ké", ""])
_query_value = st.one_of(st.none(), st.sampled_from(["", "1", "2", "%zz", "x y", "%7E", "a=b"]))


@given(text=st.text(max_size=40))
def test_percent_normalization_is_idempotent(text: str) -> None:
    once = normalize_percent_encoding(text)
    assert normalize_percent_encoding(once) == once
    assert once.isascii()


@given(
    host=st.sampled_from(
        [
            "books.toscrape.com",
            "QUOTES.toscrape.com",
            "x.test",
            "[::1]",
            "[2001:DB8::1]",
            "[::1]:8080",
            "x.test:443",
            "x.test:8443",
            "BÜCHER.example",
        ]
    ),
    path=_path_strategy,
    query=st.lists(st.tuples(_query_key, _query_value), max_size=4),
    fragment=st.sampled_from(["", "#top", "#a?b"]),
)
def test_canonicalize_is_idempotent_on_awkward_urls(
    host: str, path: str, query: list[tuple[str, str | None]], fragment: str
) -> None:
    qs = "&".join(k if v is None else f"{k}={v}" for k, v in query)
    url = f"https://{host}{path}" + (f"?{qs}" if qs else "") + fragment
    once = canonicalize(url)
    assert canonicalize(once) == once
    assert "#" not in once
    if host.startswith("["):
        assert "://[" in once
    # The query is never re-encoded: every non-empty raw pair survives, as written.
    raw_pairs = sorted(p for p in qs.split("&") if p)
    kept = once.partition("?")[2]
    assert sorted(p for p in kept.split("&") if p) == raw_pairs


@pytest.mark.parametrize(
    ("page_url", "entry_url", "base"),
    [
        # A same-host scheme switch on default ports: back onto the entry's origin (ADR 0010).
        ("http://h.test/dir/new/", "https://h.test/old", "https://h.test/dir/new/"),
        ("http://h.test:80/a?q=1", "https://h.test/a", "https://h.test/a?q=1"),
        ("https://h.test/x/", "http://h.test/x", "http://h.test/x/"),
        # Everything else is left alone: same origin, other host, other port.
        ("https://h.test/dir/new/", "https://h.test/old", "https://h.test/dir/new/"),
        ("http://other.test/a/", "https://h.test/a", "http://other.test/a/"),
        ("http://h.test:8080/a/", "https://h.test/a", "http://h.test:8080/a/"),
        ("http://h.test/a/", "https://h.test:8443/a", "http://h.test/a/"),
        ("not a url", "https://h.test/a", "not a url"),
    ],
)
def test_link_base_rebases_only_a_same_host_scheme_switch(
    page_url: str, entry_url: str, base: str
) -> None:
    assert link_base(page_url, entry_url) == base
