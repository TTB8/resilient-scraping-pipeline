"""URL canonicalization: one spelling per page, so the frontier can deduplicate by string equality.

Two links are the same page when they differ only in things a server ignores: the case of the scheme
and host, a default port, a fragment, ``.``/``..`` segments, or the order of query parameters. Anything
else (path case, trailing slash, ``index.html``) is kept, because servers do treat those differently
and guessing wrong would merge distinct pages.
"""

import posixpath
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

_DEFAULT_PORTS = {"http": 80, "https": 443}


class InvalidUrlError(ValueError):
    """The input is not an absolute http(s) URL."""


def canonicalize(url: str) -> str:
    """Return the canonical spelling of an absolute http(s) URL.

    >>> canonicalize("HTTPS://Books.ToScrape.com:443/catalogue/../index.html#top")
    'https://books.toscrape.com/index.html'
    >>> canonicalize("https://quotes.toscrape.com?b=2&a=1")
    'https://quotes.toscrape.com/?a=1&b=2'
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS or not parts.hostname:
        raise InvalidUrlError(f"not an absolute http(s) URL: {url!r}")

    host = parts.hostname.lower()
    try:
        port = parts.port
    except ValueError as exc:
        raise InvalidUrlError(f"invalid port in {url!r}") from exc
    netloc = host if port in (None, _DEFAULT_PORTS[scheme]) else f"{host}:{port}"

    path = _normalize_path(parts.path)
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunsplit((scheme, netloc, path, query, ""))


def resolve(base: str, href: str) -> str:
    """Resolve a link found on page ``base`` and return it canonicalized."""
    return canonicalize(urljoin(base, href.strip()))


def domain(url: str) -> str:
    """The lower-cased host of a URL, the key for rate limits, breakers and robots rules."""
    host = urlsplit(url).hostname
    if not host:
        raise InvalidUrlError(f"no host in {url!r}")
    return host.lower()


def same_site(url: str, origin: str) -> bool:
    """True when ``url`` is on the same scheme and host as ``origin`` (the crawl never leaves a site)."""
    a, b = urlsplit(url), urlsplit(origin)
    return (
        a.scheme.lower() == b.scheme.lower()
        and (a.hostname or "").lower() == (b.hostname or "").lower()
    )


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    normalized = posixpath.normpath(path)
    # normpath keeps a leading '//' and drops a trailing slash; restore the slash, which servers honour.
    normalized = "/" + normalized.lstrip("/")
    if path.endswith("/") and not normalized.endswith("/"):
        normalized += "/"
    return normalized
