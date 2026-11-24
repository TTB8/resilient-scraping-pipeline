"""Properties of the retry policy that must hold for every configuration, not just the examples."""

from hypothesis import given
from hypothesis import strategies as st

from scraping_pipeline.core.classify import Classification, classify
from scraping_pipeline.core.retry import (
    RETRY_AFTER_CEILING_S,
    BackoffPolicy,
    RetryAction,
    backoff_delay,
    decide,
)
from scraping_pipeline.core.types import ResponseClass

rands = st.floats(min_value=0.0, max_value=1.0, exclude_max=True)


@st.composite
def policies(draw: st.DrawFn) -> BackoffPolicy:
    base = draw(st.floats(min_value=0.001, max_value=100.0))
    return BackoffPolicy(
        base_s=base,
        multiplier=draw(st.floats(min_value=1.0, max_value=10.0)),
        cap_s=draw(st.floats(min_value=base, max_value=10_000.0)),
        max_attempts=draw(st.integers(min_value=1, max_value=20)),
        max_retry_after_s=draw(st.floats(min_value=0.0, max_value=10_000.0)),
    )


@given(policy=policies(), attempt=st.integers(min_value=1, max_value=10_000), rand=rands)
def test_backoff_delay_within_zero_and_cap(
    policy: BackoffPolicy, attempt: int, rand: float
) -> None:
    assert 0.0 <= backoff_delay(policy, attempt, rand) <= policy.cap_s


@given(
    policy=policies(),
    attempt=st.integers(min_value=1, max_value=50),
    rand=rands,
    retry_after=st.one_of(st.none(), st.floats(min_value=0.0, max_value=20_000.0)),
)
def test_rate_limited_delay_never_below_retry_after(
    policy: BackoffPolicy, attempt: int, rand: float, retry_after: float | None
) -> None:
    decision = decide(
        policy, Classification(ResponseClass.RATE_LIMITED, retry_after, ""), attempt, rand
    )
    if decision.action is not RetryAction.RETRY:
        assert decision.delay_s is None
        return
    assert decision.delay_s is not None
    if retry_after is None:
        assert 0.0 <= decision.delay_s <= policy.cap_s
    else:
        assert retry_after <= policy.max_retry_after_s
        assert decision.delay_s >= retry_after
        assert decision.delay_s <= max(policy.cap_s, retry_after)


@given(
    policy=policies(),
    classes=st.lists(
        st.sampled_from(
            [
                ResponseClass.SERVER_ERROR,
                ResponseClass.TIMEOUT,
                ResponseClass.NETWORK_ERROR,
                ResponseClass.RATE_LIMITED,
            ]
        ),
        min_size=1,
        max_size=40,
    ),
    rand=rands,
)
def test_attempts_never_exceed_max_before_give_up(
    policy: BackoffPolicy, classes: list[ResponseClass], rand: float
) -> None:
    # Feed an endless stream of transient failures; the policy must stop by max_attempts.
    attempt = 0
    while True:
        attempt += 1
        assert attempt <= policy.max_attempts
        decision = decide(
            policy, Classification(classes[attempt % len(classes)], None, ""), attempt, rand
        )
        if decision.action is not RetryAction.RETRY:
            assert decision.action is RetryAction.GIVE_UP_EXHAUSTED
            assert attempt == policy.max_attempts
            break


_header_values = st.one_of(
    st.text(),
    st.from_regex(r"\d{1,6000}", fullmatch=True),
    st.from_regex(r"\s*-?\d{1,30}(\.\d{0,5})?\s*", fullmatch=True),
    st.from_regex(
        r"(Mon|Wed|Sun), \d{1,25} (Oct|Foo|Nov) \d{1,25} \d{1,25}:\d{1,3}:\d{1,3} (GMT|[+-]\d{4})",
        fullmatch=True,
    ),
)


@given(
    value=_header_values,
    status=st.sampled_from([200, 403, 429, 500, 503]),
    now=st.one_of(st.none(), st.floats(min_value=0.0, max_value=4e9)),
)
def test_classify_never_raises_on_any_retry_after(
    value: str, status: int, now: float | None
) -> None:
    # R1-02: one bad header must never abort the run.
    result = classify(status, {"Retry-After": value}, None, None, now_epoch=now)
    if result.retry_after_s is not None:
        assert 0.0 <= result.retry_after_s <= RETRY_AFTER_CEILING_S
    if result.response_class is ResponseClass.RATE_LIMITED:
        decide(BackoffPolicy(), result, 1, 0.5)  # and the decision cannot raise either
