from datetime import UTC, datetime

import pytest

from scraping_pipeline.core.classify import Classification
from scraping_pipeline.core.retry import (
    RETRY_AFTER_CEILING_S,
    BackoffPolicy,
    RetryAction,
    backoff_delay,
    decide,
    parse_delta_seconds,
    parse_http_date,
    parse_retry_after,
)
from scraping_pipeline.core.types import ResponseClass

DATE = "Wed, 21 Oct 2015 07:28:00 GMT"
DATE_EPOCH = datetime(2015, 10, 21, 7, 28, tzinfo=UTC).timestamp()


def cls(rc: ResponseClass, retry_after: float | None = None) -> Classification:
    return Classification(rc, retry_after, "test")


# --- Retry-After parsing ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("120", 120.0),
        ("0", 0.0),
        ("  30 ", 30.0),
        ("-5", None),
        ("+5", None),
        ("1.5", None),
        ("", None),
        ("soon", None),
        ("١٢", None),  # non-ASCII digits are not DIGIT
        (None, None),
    ],
)
def test_parse_delta_seconds(value: str | None, expected: float | None) -> None:
    assert parse_delta_seconds(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        DATE,  # IMF-fixdate
        "Wednesday, 21-Oct-15 07:28:00 GMT",  # obsolete RFC 850
        "Wed Oct 21 07:28:00 2015",  # obsolete asctime, GMT by definition
    ],
)
def test_parse_http_date_accepts_all_three_forms(value: str) -> None:
    assert parse_http_date(value) == DATE_EPOCH


@pytest.mark.parametrize("value", [None, "", "   ", "120", "garbage", "Wed, 99 Foo 2015"])
def test_parse_http_date_rejects_non_dates(value: str | None) -> None:
    assert parse_http_date(value) is None


def test_parse_retry_after_future_date_is_delta_from_now() -> None:
    assert parse_retry_after(DATE, DATE_EPOCH - 90) == 90.0


def test_parse_retry_after_past_date_is_zero() -> None:
    assert parse_retry_after(DATE, DATE_EPOCH + 1000) == 0.0


def test_parse_retry_after_delta_ignores_now() -> None:
    assert parse_retry_after("7", 1e12) == 7.0


@pytest.mark.parametrize("value", [None, "-1", "garbage", "1e3"])
def test_parse_retry_after_garbage_is_none(value: str | None) -> None:
    assert parse_retry_after(value, DATE_EPOCH) is None


# --- BackoffPolicy validation ----------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_s": 0.0},
        {"base_s": -1.0},
        {"multiplier": 0.5},
        {"cap_s": 0.5, "base_s": 1.0},
        {"max_attempts": 0},
        {"max_retry_after_s": -1.0},
        {"cap_s": float("inf")},
        {"base_s": float("nan")},
    ],
)
def test_policy_validation(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must be"):
        BackoffPolicy(**kwargs)  # type: ignore[arg-type]


def test_policy_defaults_are_valid() -> None:
    policy = BackoffPolicy()
    assert policy.max_attempts == 5


# --- backoff_delay ---------------------------------------------------------------------------


def test_backoff_delay_full_jitter_exponential() -> None:
    policy = BackoffPolicy(base_s=1.0, multiplier=2.0, cap_s=10.0)
    assert backoff_delay(policy, 1, 0.5) == 0.5
    assert backoff_delay(policy, 3, 0.5) == 2.0
    assert backoff_delay(policy, 5, 0.5) == 5.0  # 16 capped to 10
    assert backoff_delay(policy, 1, 0.0) == 0.0


def test_backoff_delay_huge_attempt_does_not_overflow() -> None:
    policy = BackoffPolicy(base_s=1.0, multiplier=10.0, cap_s=60.0)
    assert backoff_delay(policy, 10_000, 0.5) == 30.0


def test_backoff_delay_multiplier_one_is_constant() -> None:
    policy = BackoffPolicy(base_s=2.0, multiplier=1.0, cap_s=60.0)
    assert backoff_delay(policy, 50, 0.5) == 1.0


@pytest.mark.parametrize(("attempt", "rand"), [(0, 0.5), (1, 1.0), (1, -0.1)])
def test_backoff_delay_rejects_bad_inputs(attempt: int, rand: float) -> None:
    with pytest.raises(ValueError, match=r"attempt|rand"):
        backoff_delay(BackoffPolicy(), attempt, rand)


# --- decide ----------------------------------------------------------------------------------

POLICY = BackoffPolicy(
    base_s=1.0, multiplier=2.0, cap_s=60.0, max_attempts=3, max_retry_after_s=120
)


@pytest.mark.parametrize(
    ("rc", "action"),
    [
        (ResponseClass.NOT_FOUND, RetryAction.GIVE_UP_PERMANENT),
        (ResponseClass.CLIENT_ERROR, RetryAction.GIVE_UP_PERMANENT),
        (ResponseClass.BLOCKED, RetryAction.STOP_DOMAIN),
        (ResponseClass.CHALLENGE, RetryAction.STOP_DOMAIN),
        (ResponseClass.SERVER_ERROR, RetryAction.RETRY),
        (ResponseClass.TIMEOUT, RetryAction.RETRY),
        (ResponseClass.NETWORK_ERROR, RetryAction.RETRY),
        (ResponseClass.RATE_LIMITED, RetryAction.RETRY),
    ],
)
def test_decide_first_attempt_by_class(rc: ResponseClass, action: RetryAction) -> None:
    decision = decide(POLICY, cls(rc), 1, 0.5)
    assert decision.action is action
    assert (decision.delay_s is not None) == (action is RetryAction.RETRY)
    assert decision.reason


def test_decide_ok_is_not_a_failure() -> None:
    with pytest.raises(ValueError, match="ok"):
        decide(POLICY, cls(ResponseClass.OK), 1, 0.5)


def test_decide_rejects_attempt_zero() -> None:
    with pytest.raises(ValueError, match="1-based"):
        decide(POLICY, cls(ResponseClass.TIMEOUT), 0, 0.5)


def test_decide_transient_uses_jittered_backoff() -> None:
    assert decide(POLICY, cls(ResponseClass.SERVER_ERROR), 2, 0.5).delay_s == 1.0


def test_decide_exhausts_at_max_attempts() -> None:
    assert decide(POLICY, cls(ResponseClass.TIMEOUT), 2, 0.5).action is RetryAction.RETRY
    exhausted = decide(POLICY, cls(ResponseClass.TIMEOUT), 3, 0.5)
    assert exhausted.action is RetryAction.GIVE_UP_EXHAUSTED
    assert exhausted.delay_s is None


def test_decide_rate_limited_waits_at_least_retry_after() -> None:
    decision = decide(POLICY, cls(ResponseClass.RATE_LIMITED, 45.0), 1, 0.5)
    assert decision.action is RetryAction.RETRY
    assert decision.delay_s == 45.0


def test_decide_rate_limited_backoff_wins_when_longer() -> None:
    policy = BackoffPolicy(base_s=10.0, cap_s=60.0, max_attempts=5)
    assert decide(policy, cls(ResponseClass.RATE_LIMITED, 1.0), 3, 0.9).delay_s == 36.0


def test_decide_rate_limited_without_header_uses_backoff() -> None:
    assert decide(POLICY, cls(ResponseClass.RATE_LIMITED), 1, 0.5).delay_s == 0.5


def test_decide_long_retry_after_stops_domain() -> None:
    decision = decide(POLICY, cls(ResponseClass.RATE_LIMITED, 121.0), 1, 0.5)
    assert decision.action is RetryAction.STOP_DOMAIN
    assert "Retry-After" in decision.reason


def test_decide_retry_after_at_limit_is_waited_out() -> None:
    assert decide(POLICY, cls(ResponseClass.RATE_LIMITED, 120.0), 1, 0.5).delay_s == 120.0


def test_decide_long_retry_after_stops_even_on_last_attempt() -> None:
    decision = decide(POLICY, cls(ResponseClass.RATE_LIMITED, 999.0), 3, 0.5)
    assert decision.action is RetryAction.STOP_DOMAIN


def test_decide_retry_after_ignored_for_server_error() -> None:
    # Retry-After on a plain 500 is informational; only rate_limited honours it.
    assert decide(POLICY, cls(ResponseClass.SERVER_ERROR, 999.0), 1, 0.5).delay_s == 0.5


# --- R1-02: no Retry-After value may raise ----------------------------------------------------


@pytest.mark.parametrize(
    ("digits", "expected"),
    [
        (10, 9_999_999_999.0),  # the longest value still taken literally
        (11, RETRY_AFTER_CEILING_S),
        (400, RETRY_AFTER_CEILING_S),  # used to raise OverflowError
        (5000, RETRY_AFTER_CEILING_S),  # used to raise ValueError (int digit limit)
    ],
)
def test_huge_delta_seconds_are_clamped(digits: int, expected: float) -> None:
    assert parse_delta_seconds("9" * digits) == expected
    assert parse_retry_after("9" * digits, 0.0) == expected


def test_huge_retry_after_stops_the_domain() -> None:
    huge = parse_delta_seconds("9" * 400)
    decision = decide(BackoffPolicy(), cls(ResponseClass.RATE_LIMITED, huge), 1, 0.5)
    assert decision.action is RetryAction.STOP_DOMAIN


@pytest.mark.parametrize(
    "value",
    [
        "Wed, 21 Oct 9999999999999999999999 07:28:00 GMT",  # OverflowError inside the parser
        "Wed, 21 Oct 2015 99999999999999999999:28:00 GMT",
        "Wed, 21 Oct 99999 07:28:00 GMT",  # ValueError: year out of range
    ],
)
def test_overflowing_http_dates_are_none(value: str) -> None:
    assert parse_http_date(value) is None
    assert parse_retry_after(value, 0.0) is None


@pytest.mark.parametrize("limit", [RETRY_AFTER_CEILING_S, 1e12])
def test_policy_rejects_retry_after_limit_at_or_above_ceiling(limit: float) -> None:
    # Otherwise a clamped huge Retry-After could be waited out instead of stopping the domain.
    with pytest.raises(ValueError, match="max_retry_after_s"):
        BackoffPolicy(max_retry_after_s=limit)
