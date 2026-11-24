"""URL canonicalization: one spelling per page, so the frontier can deduplicate by string equality.

Two failure modes pull in opposite directions. *Merging* two URLs that a server treats differently
loses pages; *splitting* one page into two spellings fetches it twice. So canonicalization applies
only the normalizations RFC 3986 section 6 says preserve meaning, plus two that servers universally
ignore:

* scheme and host are lower-cased; a non-ASCII host is IDNA-encoded (punycode); an IPv6 literal
  keeps its brackets; a default port is dropped, any other port is kept; userinfo is dropped (a
  crawled URL never carries credentials);
* the fragment is dropped (it never reaches the server);
* the path gets RFC 3986 section 5.2.4 dot-segment removal (``/a/./b/../c`` -> ``/a/c``). Unlike
  ``posixpath.normpath`` this keeps ``//``, which is a distinct path to a server;
* percent-encoding in the path is normalized (section 6.2.2.2): escapes of unreserved characters are
  decoded (``%7E`` -> ``~``), other escapes get upper-case hex, and characters that may not appear
  in a URI (space, non-ASCII, ``"<>\\^`{|}``) are UTF-8 percent-encoded. A ``%`` that does not start
  a valid escape is left as written, never turned into ``%25``;
* the query is kept as written, byte for byte, except that its ``&``-separated pairs are sorted by
  key with a *stable* sort and empty pairs are dropped. Repeated keys keep their relative order
  (``?a=2&a=1`` stays as is: some servers read them as an ordered list), bare keys stay bare
  (``?foo`` is not ``?foo=``) and nothing is decoded or re-encoded.

Everything else (path case, trailing slash, ``index.html``) is kept, because servers do treat those
differently and guessing wrong would merge distinct pages. ``canonicalize`` is idempotent: its
output is a fixed point, which the property tests check.
"""

import re
from urllib.parse import urlsplit, urlunsplit

_DEFAULT_PORTS = {"http": 80, "https": 443}

_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
# Characters a URI may contain literally: unreserved plus reserved (gen-delims and sub-delims).
_URI_CHARS = _UNRESERVED | frozenset(":/?#[]@!$&'()*+,;=")
_ESCAPE = re.compile(r"%([0-9A-Fa-f]{2})")


class InvalidUrlError(ValueError):
    """The input is not an absolute http(s) URL."""


def canonicalize(url: str) -> str:
    """Return the canonical spelling of an absolute http(s) URL.

    >>> canonicalize("HTTPS://Books.ToScrape.com:443/catalogue/../index.html#top")
    'https://books.toscrape.com/index.html'
    >>> canonicalize("https://quotes.toscrape.com?b=2&a=1")
    'https://quotes.toscrape.com/?a=1&b=2'
    >>> canonicalize("http://[::1]:8080/a//b?foo")
    'http://[::1]:8080/a//b?foo'
    """
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError as exc:  # bad port, unbalanced IPv6 brackets
        raise InvalidUrlError(f"invalid URL {url!r}: {exc}") from exc
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS or not parts.hostname:
        raise InvalidUrlError(f"not an absolute http(s) URL: {url!r}")

    host = _canonical_host(parts.hostname, url)
    netloc = host if port in (None, _DEFAULT_PORTS[scheme]) else f"{host}:{port}"
    path = remove_dot_segments(normalize_percent_encoding(parts.path)) or "/"
    return urlunsplit((scheme, netloc, path, _canonical_query(parts.query), ""))


def resolve(base: str, href: str) -> str:
    """Resolve a link found on page ``base`` (RFC 3986 section 5.2.2) and canonicalize it.

    Implemented here rather than with ``urllib.parse.urljoin``, which drops empty path segments
    (``../c//d`` would lose its ``//``) and so would split one page into two spellings.
    """
    ref_text = href.strip()
    try:
        ref, ctx = urlsplit(ref_text), urlsplit(base.strip())
    except ValueError as exc:
        raise InvalidUrlError(f"cannot resolve {href!r} against {base!r}: {exc}") from exc
    if ref.scheme:
        return canonicalize(ref_text)
    if ref.netloc:
        netloc, path, query = ref.netloc, ref.path, ref.query
    elif not ref.path:
        has_query = "?" in ref_text.split("#", 1)[0]
        netloc, path, query = ctx.netloc, ctx.path, ref.query if has_query else ctx.query
    elif ref.path.startswith("/"):
        netloc, path, query = ctx.netloc, ref.path, ref.query
    else:
        directory = "/" if ctx.netloc and not ctx.path else ctx.path[: ctx.path.rfind("/") + 1]
        netloc, path, query = ctx.netloc, directory + ref.path, ref.query
    return canonicalize(urlunsplit((ctx.scheme, netloc, remove_dot_segments(path), query, "")))


def domain(url: str) -> str:
    """The lower-cased host of a URL, the key for rate limits, breakers and robots rules."""
    try:
        host = urlsplit(url).hostname
    except ValueError as exc:
        raise InvalidUrlError(f"invalid URL {url!r}: {exc}") from exc
    if not host:
        raise InvalidUrlError(f"no host in {url!r}")
    return host.lower()


def same_site(url: str, origin: str) -> bool:
    """True when ``url`` is on the same scheme and host as ``origin``.

    This is the confinement rule the frontier applies to discovered links.
    """
    try:
        a, b = urlsplit(url), urlsplit(origin)
        host_a, host_b = a.hostname, b.hostname
    except ValueError:
        return False
    return a.scheme.lower() == b.scheme.lower() and (host_a or "").lower() == (host_b or "").lower()


def normalize_percent_encoding(text: str) -> str:
    """RFC 3986 section 6.2.2.2 percent-encoding normalization, safe to apply repeatedly.

    Escapes of unreserved characters are decoded, other escapes are upper-cased, characters that may
    not appear in a URI are UTF-8 percent-encoded, and a stray ``%`` is kept as written. robots.txt
    matching uses the same function on both the rule and the path (RFC 9309 section 2.2.2).
    """
    out: list[str] = []
    i = 0
    while i < len(text):
        char = text[i]
        escape = _ESCAPE.match(text, i) if char == "%" else None
        if escape is not None:
            decoded = chr(int(escape.group(1), 16))
            out.append(decoded if decoded in _UNRESERVED else "%" + escape.group(1).upper())
            i += 3
            continue
        if char == "%" or char in _URI_CHARS:
            out.append(char)
        else:
            out.append("".join(f"%{byte:02X}" for byte in char.encode("utf-8", "surrogatepass")))
        i += 1
    return "".join(out)


def remove_dot_segments(path: str) -> str:
    """RFC 3986 section 5.2.4: resolve ``.`` and ``..`` segments, keeping empty ones (``//``)."""
    output: list[str] = []
    rest = path
    while rest:
        if rest.startswith("../"):
            rest = rest[3:]
        elif rest.startswith("./"):
            rest = rest[2:]
        elif rest.startswith("/./") or rest == "/.":
            rest = "/" + rest[3:]
        elif rest.startswith("/../") or rest == "/..":
            rest = "/" + rest[4:]
            if output:
                output.pop()
        elif rest in (".", ".."):
            rest = ""
        else:
            end = rest.find("/", 1)
            segment = rest if end == -1 else rest[:end]
            output.append(segment)
            rest = rest[len(segment) :]
    return "".join(output)


def _canonical_host(hostname: str, url: str) -> str:
    host = hostname.lower()
    if ":" in host:  # IPv6 literal: urlsplit strips the brackets, the URL needs them back
        return f"[{host}]"
    if host.isascii():
        return host
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise InvalidUrlError(f"host of {url!r} is not a valid IDNA name") from exc


def _canonical_query(query: str) -> str:
    pairs = [pair for pair in query.split("&") if pair]
    return "&".join(sorted(pairs, key=lambda pair: pair.partition("=")[0]))
