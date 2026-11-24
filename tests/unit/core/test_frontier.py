import pytest

from scraping_pipeline.core.classify import Classification
from scraping_pipeline.core.frontier import (
    FINAL_STATES,
    TRANSITIONS,
    FrontierEntry,
    IllegalTransitionError,
    apply_decision,
    is_due,
    mark_skipped_robots,
    mark_stopped,
    plan_new_links,
)
from scraping_pipeline.core.retry import RetryAction, RetryDecision
from scraping_pipeline.core.types import FrontierStatus, Link, PageKind, ResponseClass, Site

ORIGIN = "https://books.toscrape.com/"
BASE = "https://books.toscrape.com/catalogue/"


def plan(
    links: list[Link],
    known: frozenset[str] = frozenset(),
    *,
    depth: int = 0,
    max_depth: int = 5,
    max_pages: int = 100,
    current_count: int = 0,
) -> list[FrontierEntry]:
    return plan_new_links(
        known,
        links,
        origin=ORIGIN,
        site=Site.BOOKS,
        depth=depth,
        max_depth=max_depth,
        max_pages=max_pages,
        current_count=current_count,
    )


def link(path: str, kind: PageKind = PageKind.DETAIL) -> Link:
    return Link(BASE + path, kind)


def entry(status: FrontierStatus = FrontierStatus.PENDING) -> FrontierEntry:
    return FrontierEntry(
        url=BASE + "x/", site=Site.BOOKS, kind=PageKind.DETAIL, depth=1, status=status
    )


OK = Classification(ResponseClass.OK, None, "200")
TIMEOUT = Classification(ResponseClass.TIMEOUT, None, "timed out")


# --- plan_new_links --------------------------------------------------------------------------


def test_plans_canonical_same_site_links_sorted() -> None:
    result = plan(
        [
            link("b/index.html"),
            Link("HTTPS://Books.ToScrape.com:443/catalogue/a/index.html#top", PageKind.DETAIL),
            Link("https://quotes.toscrape.com/", PageKind.LISTING),  # other site
            Link("http://books.toscrape.com/catalogue/c/", PageKind.DETAIL),  # other scheme
            Link("mailto:someone@example.test", PageKind.DETAIL),  # not http(s)
            Link("/relative", PageKind.DETAIL),  # not absolute
        ]
    )
    assert [e.url for e in result] == [BASE + "a/index.html", BASE + "b/index.html"]
    assert all(e.depth == 1 and e.site is Site.BOOKS for e in result)
    assert all(e.status is FrontierStatus.PENDING and e.attempts == 0 for e in result)
    assert all(e.next_attempt_at is None and e.last_class is None for e in result)


def test_dedupes_within_batch_and_against_known() -> None:
    result = plan(
        [link("a/"), link("a/#frag"), link("b/"), link("./c/")],
        known=frozenset({BASE + "b/"}),
    )
    assert [e.url for e in result] == [BASE + "a/", BASE + "c/"]


def test_conflicting_kinds_pick_first_declared() -> None:
    for order in ([PageKind.DETAIL, PageKind.LISTING], [PageKind.LISTING, PageKind.DETAIL]):
        result = plan([link("a/", kind) for kind in order])
        assert [e.kind for e in result] == [PageKind.LISTING]


def test_depth_limit() -> None:
    assert plan([link("a/")], depth=4, max_depth=5)[0].depth == 5
    assert plan([link("a/")], depth=5, max_depth=5) == []


def test_page_limit_keeps_smallest_urls() -> None:
    links = [link(name + "/") for name in "edcba"]
    result = plan(links, max_pages=10, current_count=8)
    assert [e.url for e in result] == [BASE + "a/", BASE + "b/"]
    assert plan(links, max_pages=10, current_count=10) == []
    assert plan(links, max_pages=10, current_count=12) == []


@pytest.mark.parametrize("name", ["depth", "max_depth", "max_pages"])
def test_negative_limits_rejected(name: str) -> None:
    limits = {"depth": 0, "max_depth": 1, "max_pages": 1, name: -1}
    with pytest.raises(ValueError, match=name):
        plan(
            [link("a/")],
            depth=limits["depth"],
            max_depth=limits["max_depth"],
            max_pages=limits["max_pages"],
        )


# --- apply_decision ---------------------------------------------------------------------------


def test_ok_marks_done() -> None:
    done = apply_decision(entry(), OK, None, 100.0)
    assert done.status is FrontierStatus.DONE
    assert done.attempts == 1
    assert done.last_class is ResponseClass.OK
    assert done.last_reason == "200"
    assert done.next_attempt_at is None


def test_retry_schedules_next_attempt() -> None:
    retried = apply_decision(
        entry(), TIMEOUT, RetryDecision(RetryAction.RETRY, 2.5, "retry 2"), 100.0
    )
    assert retried.status is FrontierStatus.PENDING
    assert retried.attempts == 1
    assert retried.next_attempt_at == 102.5
    assert retried.last_class is ResponseClass.TIMEOUT
    assert retried.last_reason == "timed out; retry 2"


@pytest.mark.parametrize(
    ("action", "status"),
    [
        (RetryAction.GIVE_UP_PERMANENT, FrontierStatus.FAILED_PERMANENT),
        (RetryAction.GIVE_UP_EXHAUSTED, FrontierStatus.FAILED_EXHAUSTED),
        (RetryAction.STOP_DOMAIN, FrontierStatus.STOPPED),
    ],
)
def test_give_ups_are_final(action: RetryAction, status: FrontierStatus) -> None:
    scheduled = apply_decision(entry(), TIMEOUT, RetryDecision(RetryAction.RETRY, 5.0, "r"), 0.0)
    result = apply_decision(scheduled, TIMEOUT, RetryDecision(action, None, "why"), 10.0)
    assert result.status is status
    assert result.attempts == 2
    assert result.next_attempt_at is None
    assert not is_due(result, 1e12)


@pytest.mark.parametrize("final", sorted(FINAL_STATES))
def test_final_states_cannot_be_left(final: FrontierStatus) -> None:
    stuck = entry(final)
    with pytest.raises(IllegalTransitionError, match=f"{final} -> "):
        apply_decision(stuck, OK, None, 0.0)
    with pytest.raises(IllegalTransitionError):
        apply_decision(stuck, TIMEOUT, RetryDecision(RetryAction.RETRY, 1.0, "r"), 0.0)
    with pytest.raises(IllegalTransitionError):
        mark_skipped_robots(stuck, "robots")
    with pytest.raises(IllegalTransitionError):
        mark_stopped(stuck, "stopped")


def test_transition_table_shape() -> None:
    assert set(TRANSITIONS) == set(FrontierStatus)
    assert TRANSITIONS[FrontierStatus.PENDING] == frozenset(FrontierStatus)
    assert all(not TRANSITIONS[s] for s in FINAL_STATES)


def test_ok_with_decision_is_rejected() -> None:
    with pytest.raises(ValueError, match="no retry decision"):
        apply_decision(entry(), OK, RetryDecision(RetryAction.RETRY, 1.0, "r"), 0.0)


def test_failure_without_decision_is_rejected() -> None:
    with pytest.raises(ValueError, match="needs a retry decision"):
        apply_decision(entry(), TIMEOUT, None, 0.0)


@pytest.mark.parametrize("delay", [None, -1.0])
def test_retry_without_valid_delay_is_rejected(delay: float | None) -> None:
    with pytest.raises(ValueError, match="delay_s"):
        apply_decision(entry(), TIMEOUT, RetryDecision(RetryAction.RETRY, delay, "r"), 0.0)


def test_marks_without_attempt() -> None:
    skipped = mark_skipped_robots(entry(), "disallowed by robots.txt")
    assert skipped.status is FrontierStatus.SKIPPED_ROBOTS
    assert skipped.attempts == 0
    assert skipped.last_reason == "disallowed by robots.txt"
    scheduled = apply_decision(entry(), TIMEOUT, RetryDecision(RetryAction.RETRY, 5.0, "r"), 0.0)
    stopped = mark_stopped(scheduled, "domain stopped: 403")
    assert stopped.status is FrontierStatus.STOPPED
    assert stopped.attempts == 1
    assert stopped.last_class is ResponseClass.TIMEOUT  # kept from the last attempt
    assert stopped.next_attempt_at is None


# --- is_due ----------------------------------------------------------------------------------


def test_is_due() -> None:
    fresh = entry()
    assert is_due(fresh, 0.0)
    scheduled = apply_decision(fresh, TIMEOUT, RetryDecision(RetryAction.RETRY, 10.0, "r"), 100.0)
    assert not is_due(scheduled, 109.99)
    assert is_due(scheduled, 110.0)
    assert not is_due(apply_decision(fresh, OK, None, 0.0), 1e12)
