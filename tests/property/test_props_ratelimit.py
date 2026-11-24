"""The token bucket's politeness guarantee, over arbitrary call sequences."""

from typing import Literal

from hypothesis import given
from hypothesis import strategies as st

from scraping_pipeline.core.ratelimit import (
    acquire,
    apply_crawl_delay,
    new_bucket,
    slow_down,
    speed_up,
)

Op = Literal["acquire", "slow", "fast"]

steps = st.lists(
    st.tuples(
        st.floats(min_value=0.0, max_value=5.0),  # time advance
        st.sampled_from(["acquire", "acquire", "acquire", "slow", "fast"]),
        st.booleans(),  # on a refusal, retry exactly at the returned wait
    ),
    max_size=150,
)


@given(
    rate=st.floats(min_value=0.05, max_value=20.0),
    burst=st.floats(min_value=1.0, max_value=10.0),
    crawl_delay=st.one_of(st.none(), st.floats(min_value=0.1, max_value=10.0)),
    plan=steps,
)
def test_never_more_than_burst_plus_rate_times_window(
    rate: float, burst: float, crawl_delay: float | None, plan: list[tuple[float, Op, bool]]
) -> None:
    bucket = apply_crawl_delay(new_bucket(rate, burst), crawl_delay)
    ceiling, cap = bucket.max_rate_per_s, bucket.burst
    now = 0.0
    granted: list[float] = []
    for advance, op, follow_wait in plan:
        now += advance
        if op == "slow":
            bucket = slow_down(bucket, 0.5, 0.01)
        elif op == "fast":
            bucket = speed_up(bucket, 0.5)
        else:
            bucket, wait = acquire(bucket, now)
            if wait == 0.0:
                granted.append(now)
            else:
                assert wait > 0.0
                if follow_wait:
                    now += wait
                    bucket, wait = acquire(bucket, now)
                    assert wait == 0.0  # waiting the advertised time is always enough
                    granted.append(now)
        assert bucket.rate_per_s <= ceiling
    # Every window [t_i, t_j] of grants respects burst + ceiling * length.
    for i in range(len(granted)):
        for j in range(i, len(granted)):
            assert j - i + 1 <= cap + ceiling * (granted[j] - granted[i]) + 1e-6
