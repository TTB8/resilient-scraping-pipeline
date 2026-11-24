"""Frontier rules: which discovered links become work, and how a URL's status may change.

The frontier is the pipeline's to-do list, persisted so that a run can stop and resume. Two kinds of
bug hurt most here, and both are prevented in this module rather than in storage code:

* **Unbounded or duplicated work.** :func:`plan_new_links` canonicalizes every link, keeps only
  same-site URLs, drops what is already known or repeated in the batch, and enforces the depth and
  page limits. Its output is sorted by URL, so the result (including *which* links survive the
  page cap) depends only on the set of inputs, never on the order a parser happened to emit them.
  A resumed run therefore plans exactly what an uninterrupted one would have.
* **Resurrected or lost URLs.** :func:`apply_decision` moves an entry through an explicit transition
  table. ``DONE`` and the ``FAILED_*`` / ``SKIPPED_*`` / ``STOPPED`` states are final: an attempt to
  leave one raises :class:`IllegalTransitionError` instead of silently re-fetching a page or
  overwriting why it failed.

Times here (``now``, ``next_attempt_at``) are **Unix epoch seconds**, not monotonic: they are stored
with the frontier and compared again after a restart, when a monotonic clock would mean nothing.
"""

from collections.abc import Iterable
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, replace

from scraping_pipeline.core.classify import Classification
from scraping_pipeline.core.retry import RetryAction, RetryDecision
from scraping_pipeline.core.types import FrontierStatus, Link, PageKind, ResponseClass, Site
from scraping_pipeline.core.urls import InvalidUrlError, canonicalize, same_site


class IllegalTransitionError(ValueError):
    """A frontier entry was asked to move along an edge the transition table does not have."""


@dataclass(frozen=True, slots=True)
class FrontierEntry:
    """One URL in the frontier. ``url`` is canonical; ``attempts`` counts finished fetch attempts."""

    url: str
    site: Site
    kind: PageKind
    depth: int
    status: FrontierStatus = FrontierStatus.PENDING
    attempts: int = 0
    next_attempt_at: float | None = None
    last_class: ResponseClass | None = None
    last_reason: str | None = None


FINAL_STATES = frozenset(
    {
        FrontierStatus.DONE,
        FrontierStatus.FAILED_PERMANENT,
        FrontierStatus.FAILED_EXHAUSTED,
        FrontierStatus.SKIPPED_ROBOTS,
        FrontierStatus.STOPPED,
    }
)

TRANSITIONS: dict[FrontierStatus, frozenset[FrontierStatus]] = {
    FrontierStatus.PENDING: frozenset({FrontierStatus.PENDING} | FINAL_STATES),
    **{state: frozenset() for state in FINAL_STATES},
}
"""Allowed ``from -> to`` edges. Only ``PENDING`` has any; every other state is final."""

_ACTION_TO_STATUS = {
    RetryAction.RETRY: FrontierStatus.PENDING,
    RetryAction.GIVE_UP_PERMANENT: FrontierStatus.FAILED_PERMANENT,
    RetryAction.GIVE_UP_EXHAUSTED: FrontierStatus.FAILED_EXHAUSTED,
    RetryAction.STOP_DOMAIN: FrontierStatus.STOPPED,
}

_KIND_ORDER = {kind: index for index, kind in enumerate(PageKind)}


def plan_new_links(
    known_urls: AbstractSet[str],
    links: Iterable[Link],
    *,
    origin: str,
    site: Site,
    depth: int,
    max_depth: int,
    max_pages: int,
    current_count: int,
) -> list[FrontierEntry]:
    """New frontier entries for the links found on a page at ``depth``.

    ``links`` carry absolute URLs (parsers resolve hrefs with :func:`~.urls.resolve`); URLs that are
    not http(s) are dropped. New entries get ``depth + 1``; nothing is planned past ``max_depth``, and
    at most ``max_pages - current_count`` entries are returned. ``known_urls`` holds canonical URLs.
    When one URL arrives with different kinds, the kind declared first in :class:`PageKind` wins (a
    deterministic tie-break; it indicates a parser disagreement worth a look).
    """
    for name, value in (("depth", depth), ("max_depth", max_depth), ("max_pages", max_pages)):
        if value < 0:
            raise ValueError(f"{name} must be >= 0, got {value!r}")
    child_depth = depth + 1
    budget = max_pages - current_count
    if child_depth > max_depth or budget <= 0:
        return []

    chosen: dict[str, PageKind] = {}
    for link in links:
        try:
            url = canonicalize(link.url)
        except InvalidUrlError:
            continue
        if url in known_urls or not same_site(url, origin):
            continue
        previous = chosen.get(url)
        if previous is None or _KIND_ORDER[link.kind] < _KIND_ORDER[previous]:
            chosen[url] = link.kind

    return [
        FrontierEntry(url=url, site=site, kind=chosen[url], depth=child_depth)
        for url in sorted(chosen)[:budget]
    ]


def apply_decision(
    entry: FrontierEntry,
    classification: Classification,
    decision: RetryDecision | None,
    now: float,
) -> FrontierEntry:
    """Record a finished fetch attempt on ``entry``.

    ``decision`` is ``None`` exactly when the attempt was ``ok`` (the entry becomes ``DONE``);
    otherwise it is what :func:`~.retry.decide` returned. A ``RETRY`` keeps the entry ``PENDING``
    with ``next_attempt_at = now + delay_s``. Raises :class:`IllegalTransitionError` for an entry
    that is already final, and ``ValueError`` when classification and decision disagree.
    """
    is_ok = classification.response_class is ResponseClass.OK
    if is_ok and decision is not None:
        raise ValueError("an ok attempt takes no retry decision")
    if decision is None:
        if not is_ok:
            raise ValueError(f"a {classification.response_class} attempt needs a retry decision")
        target, next_at, reason = FrontierStatus.DONE, None, classification.reason
    else:
        target = _ACTION_TO_STATUS[decision.action]
        next_at = None
        if decision.action is RetryAction.RETRY:
            if decision.delay_s is None or decision.delay_s < 0:
                raise ValueError("a retry decision needs a delay_s >= 0")
            next_at = now + decision.delay_s
        reason = f"{classification.reason}; {decision.reason}"
    return _transition(
        entry,
        target,
        attempts=entry.attempts + 1,
        next_attempt_at=next_at,
        last_class=classification.response_class,
        last_reason=reason,
    )


def mark_skipped_robots(entry: FrontierEntry, reason: str) -> FrontierEntry:
    """robots.txt disallows the URL: final, without an attempt."""
    return _transition(
        entry, FrontierStatus.SKIPPED_ROBOTS, next_attempt_at=None, last_reason=reason
    )


def mark_stopped(entry: FrontierEntry, reason: str) -> FrontierEntry:
    """The URL's domain was stopped (blocked, challenge, breaker tripped): final, without an attempt."""
    return _transition(entry, FrontierStatus.STOPPED, next_attempt_at=None, last_reason=reason)


def is_due(entry: FrontierEntry, now: float) -> bool:
    """True when the entry is ``PENDING`` and its retry time (if any) has come."""
    if entry.status is not FrontierStatus.PENDING:
        return False
    return entry.next_attempt_at is None or now >= entry.next_attempt_at


def _transition(
    entry: FrontierEntry,
    target: FrontierStatus,
    *,
    next_attempt_at: float | None,
    last_reason: str,
    attempts: int | None = None,
    last_class: ResponseClass | None = None,
) -> FrontierEntry:
    if target not in TRANSITIONS[entry.status]:
        raise IllegalTransitionError(f"{entry.url}: {entry.status} -> {target} is not allowed")
    return replace(
        entry,
        status=target,
        attempts=entry.attempts if attempts is None else attempts,
        next_attempt_at=next_attempt_at,
        last_class=entry.last_class if last_class is None else last_class,
        last_reason=last_reason,
    )
