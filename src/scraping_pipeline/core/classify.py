"""Response classification: turn one fetch outcome into a :class:`ResponseClass` and a short reason.

Everything downstream (retry policy, circuit breaker, rate limiter, frontier) reacts to the class and
never to raw status codes. That keeps the failure policy in one place, and it means a quirk of one
site (a 503 that really means "slow down", a challenge page served with 200) is handled once, here.

Status rules, in order:

* a transport error wins over any status: ``timeout`` -> ``timeout``, every other transport error ->
  ``network_error``;
* 2xx -> ``ok``, unless the page matches a challenge or block marker (some sites serve their
  interstitial with 200);
* 404 / 410 -> ``not_found``;
* 401 / 403 -> ``blocked``, or ``challenge`` when the body carries a challenge marker;
* 429 -> ``rate_limited``; 503 with a valid ``Retry-After`` -> ``rate_limited``;
* other 5xx -> ``server_error``; other 4xx -> ``client_error``;
* 1xx / 3xx -> ``client_error``: the fetcher is expected to follow redirects, so seeing one here is a
  bug on our side, and retrying it would not help;
* a status outside 100-599 -> ``network_error``: the peer is not speaking HTTP correctly, which we
  treat like a broken connection (transient, retried with backoff).

Marker detection is deliberately conservative, because a false positive stops a whole domain:

* markers are multi-word phrases (enforced by :class:`BlockMarkers`), so a single word such as
  "robot" in a book description can never trigger them;
* only the first ``scan_limit_chars`` characters of the body are scanned, and only the ``<title>`` and
  the visible text (scripts, styles and tags are stripped), so the cost is bounded per response;
* on a 2xx page, markers count only when the page is short (under ``short_page_chars`` visible
  characters), title included. Interstitials are short; real content pages are not, and a book
  titled "Access Denied" or a blurb quoting "are you a robot" must not stop the crawl. On 401/403 the
  status already says "refused", so the whole scanned text is searched, only to tell ``challenge``
  from ``blocked``.

This module only *detects*. The project never tries to get past a block or a challenge: it records
the reason and stops the domain (ADR 0005).
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from http import HTTPStatus

from scraping_pipeline.core.retry import parse_delta_seconds, parse_http_date, parse_retry_after
from scraping_pipeline.core.types import ResponseClass


class TransportError(StrEnum):
    """Why a fetch produced no HTTP response at all. The fetcher maps its exceptions onto these."""

    TIMEOUT = "timeout"  # connect, read or overall deadline expired
    CONNECT = "connect"  # connection refused or unreachable
    DNS = "dns"  # name resolution failed
    RESET = "reset"  # connection reset or closed mid-response
    PROTOCOL = "protocol"  # malformed HTTP from the peer
    TLS = "tls"  # handshake or certificate failure


@dataclass(frozen=True, slots=True)
class BlockMarkers:
    """Case-insensitive phrases that identify challenge and block pages.

    Every phrase must contain at least two words: that is the structural guarantee that one common
    word in real content cannot stop a domain. Phrases are stored case-folded with whitespace
    collapsed, and matched against case-folded, whitespace-collapsed text.
    """

    challenge: tuple[str, ...]
    blocked: tuple[str, ...]
    scan_limit_chars: int = 64 * 1024
    short_page_chars: int = 3000

    def __post_init__(self) -> None:
        object.__setattr__(self, "challenge", _normalize_phrases(self.challenge, "challenge"))
        object.__setattr__(self, "blocked", _normalize_phrases(self.blocked, "blocked"))
        if self.scan_limit_chars < 1:
            raise ValueError(f"scan_limit_chars must be >= 1, got {self.scan_limit_chars!r}")
        if self.short_page_chars < 0:
            raise ValueError(f"short_page_chars must be >= 0, got {self.short_page_chars!r}")


def _normalize_phrases(phrases: tuple[str, ...], field: str) -> tuple[str, ...]:
    normalized = []
    for phrase in phrases:
        words = phrase.casefold().split()
        if len(words) < 2:  # noqa: PLR2004 - "two words" is the rule itself
            raise ValueError(f"{field} marker {phrase!r} must have at least two words")
        normalized.append(" ".join(words))
    return tuple(normalized)


DEFAULT_MARKERS = BlockMarkers(
    challenge=(
        "verify you are human",
        "verify that you are human",
        "checking your browser",
        "checking if the site connection is secure",
        "complete the security check",
        "are you a robot",
        "prove you are not a robot",
        "enable javascript and cookies to continue",
    ),
    blocked=(
        "access denied",
        "request blocked",
        "you have been blocked",
        "your ip has been blocked",
        "access to this page has been denied",
        "the request could not be satisfied",
    ),
)


@dataclass(frozen=True, slots=True)
class Classification:
    """The verdict for one fetch attempt.

    ``retry_after_s`` is the parsed ``Retry-After`` (seconds) whenever the response carried a valid
    one, whatever the class; :func:`~.retry.decide` uses it only for ``rate_limited``. ``reason`` is a
    short human sentence for the run report.
    """

    response_class: ResponseClass
    retry_after_s: float | None
    reason: str


_SCRIPT_OR_STYLE = re.compile(r"<(script|style|noscript)\b[^>]*>.*?</\1\s*>", re.I | re.S)
_TITLE = re.compile(r"<title\b[^>]*>(.*?)</title\s*>", re.I | re.S)
_COMMENT = re.compile(r"<!--.*?-->", re.S)
_TAG = re.compile(r"<[^>]*>")
_ENTITIES = {"&nbsp;": " ", "&amp;": "&", "&#39;": "'", "&apos;": "'", "&quot;": '"'}

_MIN_STATUS, _MAX_STATUS = 100, 599
_NOT_FOUND = frozenset({HTTPStatus.NOT_FOUND, HTTPStatus.GONE})
_DENIED = frozenset({HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN})


def classify(
    status: int | None,
    headers: Mapping[str, str],
    body: str | None,
    error: TransportError | None,
    *,
    markers: BlockMarkers = DEFAULT_MARKERS,
    now_epoch: float | None = None,
) -> Classification:
    """Classify one fetch attempt. See the module docstring for the rules.

    ``now_epoch`` (Unix seconds) is needed only to turn an HTTP-date ``Retry-After`` into a delay;
    without it only the delta-seconds form yields ``retry_after_s`` (a date still marks a 503 as
    ``rate_limited``). Exactly one of ``status`` and ``error`` describes the outcome; when both are
    given the transport error wins, and when neither is given this raises ``ValueError``.
    """
    if error is not None:
        if error is TransportError.TIMEOUT:
            return Classification(ResponseClass.TIMEOUT, None, "timed out")
        return Classification(ResponseClass.NETWORK_ERROR, None, f"transport error: {error}")
    if status is None:
        raise ValueError("classify() needs a status or a transport error")
    if not _MIN_STATUS <= status <= _MAX_STATUS:
        return Classification(ResponseClass.NETWORK_ERROR, None, f"invalid HTTP status {status}")

    raw_retry_after = _header(headers, "retry-after")
    retry_after = (
        parse_delta_seconds(raw_retry_after)
        if now_epoch is None
        else parse_retry_after(raw_retry_after, now_epoch)
    )
    response_class, reason = _classify_status(status, raw_retry_after, retry_after, body, markers)
    return Classification(response_class, retry_after, reason)


def _classify_status(
    status: int,
    raw_retry_after: str | None,
    retry_after: float | None,
    body: str | None,
    markers: BlockMarkers,
) -> tuple[ResponseClass, str]:
    family = status // 100
    if family == 2:  # noqa: PLR2004 - HTTP status families
        return _classify_success(status, body, markers)
    if status in _NOT_FOUND:
        return ResponseClass.NOT_FOUND, f"{status} not found"
    if status in _DENIED:
        hit = _find_marker(body, markers, short_pages_only=False)
        if hit is not None and hit[0] is ResponseClass.CHALLENGE:
            return ResponseClass.CHALLENGE, f"{status} challenge: {hit[1]!r}"
        return ResponseClass.BLOCKED, f"{status} access refused"
    if status == HTTPStatus.TOO_MANY_REQUESTS or (
        status == HTTPStatus.SERVICE_UNAVAILABLE and _is_valid_retry_after(raw_retry_after)
    ):
        return ResponseClass.RATE_LIMITED, f"{status} {_rate_reason(retry_after)}"
    return _classify_other(status)


def _classify_success(
    status: int, body: str | None, markers: BlockMarkers
) -> tuple[ResponseClass, str]:
    hit = _find_marker(body, markers, short_pages_only=True)
    if hit is not None:
        return hit[0], f"{status} page says {hit[1]!r}"
    return ResponseClass.OK, f"{status}"


def _classify_other(status: int) -> tuple[ResponseClass, str]:
    family = status // 100
    if family == 5:  # noqa: PLR2004
        return ResponseClass.SERVER_ERROR, f"{status} server error"
    if family == 4:  # noqa: PLR2004
        return ResponseClass.CLIENT_ERROR, f"{status} client error"
    return ResponseClass.CLIENT_ERROR, f"unexpected {status} (redirects are not followed)"


def _header(headers: Mapping[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.casefold() == name:
            return value
    return None


def _is_valid_retry_after(value: str | None) -> bool:
    return parse_delta_seconds(value) is not None or parse_http_date(value) is not None


def _rate_reason(retry_after: float | None) -> str:
    if retry_after is None:
        return "rate limited"
    return f"rate limited, Retry-After {retry_after:g}s"


def _find_marker(
    body: str | None, markers: BlockMarkers, *, short_pages_only: bool
) -> tuple[ResponseClass, str] | None:
    """Return the first matching marker, challenge phrases before blocked ones."""
    if not body:
        return None
    head = body[: markers.scan_limit_chars]
    title_match = _TITLE.search(head)
    title = _normalize_text(title_match.group(1)) if title_match else ""
    visible = _normalize_text(_TAG.sub(" ", _COMMENT.sub(" ", _SCRIPT_OR_STYLE.sub(" ", head))))
    if short_pages_only and len(visible) >= markers.short_page_chars:
        return None
    for response_class, phrases in (
        (ResponseClass.CHALLENGE, markers.challenge),
        (ResponseClass.BLOCKED, markers.blocked),
    ):
        for phrase in phrases:
            if phrase in title or phrase in visible:
                return response_class, phrase
    return None


def _normalize_text(text: str) -> str:
    for entity, replacement in _ENTITIES.items():
        text = text.replace(entity, replacement)
    return " ".join(text.casefold().split())
