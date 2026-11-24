"""Shared vocabulary of the pipeline: the small enums every stage agrees on.

They are ``StrEnum`` so their values are stable strings in DuckDB, Parquet and the run report. Never
rename a value: stored state and published outputs refer to it.
"""

from dataclasses import dataclass
from enum import StrEnum


class Site(StrEnum):
    """A configured target. ``QUOTES_JS`` is the JavaScript-rendered variant of the quotes site."""

    BOOKS = "books"
    QUOTES = "quotes"
    QUOTES_JS = "quotes-js"


class PageKind(StrEnum):
    """What a frontier URL is expected to contain, which decides the parser that reads it."""

    LISTING = "listing"  # a paginated list of items (also category pages)
    DETAIL = "detail"  # one item (a book's product page)
    AUTHOR = "author"  # an author page on the quotes site


class ResponseClass(StrEnum):
    """The outcome of one fetch attempt, as decided by ``core.classify``.

    The fetch scheduler reacts to the class, never to raw status codes, so the policy for each kind of
    failure lives in one place.
    """

    OK = "ok"
    NOT_FOUND = "not_found"  # 404 / 410: permanent, don't retry
    CLIENT_ERROR = "client_error"  # other 4xx: a bug on our side, don't retry
    RATE_LIMITED = "rate_limited"  # 429, or 503 with Retry-After: slow down, retry later
    BLOCKED = "blocked"  # 401/403 or an access-denied page: stop the domain
    CHALLENGE = "challenge"  # an interstitial asking to prove we're human: stop the domain
    SERVER_ERROR = "server_error"  # 5xx: transient, retry with backoff
    TIMEOUT = "timeout"  # no response in time: transient, retry with backoff
    NETWORK_ERROR = "network_error"  # DNS, connection refused/reset: transient, retry with backoff


class FrontierStatus(StrEnum):
    """Lifecycle of a URL in the frontier. ``DONE`` and the ``FAILED_*`` / ``SKIPPED_*`` states are final."""

    PENDING = "pending"  # discovered, not fetched yet (or retry scheduled)
    DONE = "done"  # body stored in the cache
    FAILED_PERMANENT = "failed_permanent"  # not_found / client_error: retrying won't help
    FAILED_EXHAUSTED = "failed_exhausted"  # transient errors until the retry budget ran out
    SKIPPED_ROBOTS = "skipped_robots"  # robots.txt disallows it
    STOPPED = "stopped"  # its domain was stopped (blocked, challenge, breaker open)


@dataclass(frozen=True, slots=True)
class Link:
    """A URL found on a page, with the kind of page it should lead to."""

    url: str
    kind: PageKind
