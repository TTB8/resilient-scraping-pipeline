import pytest

from scraping_pipeline.core.ratelimit import (
    TokenBucket,
    acquire,
    apply_crawl_delay,
    new_bucket,
    slow_down,
    speed_up,
)


def test_new_bucket_starts_full() -> None:
    bucket = new_bucket(2.0, burst=3)
    assert bucket.tokens == 3
    assert bucket.updated_at is None
    assert bucket.max_rate_per_s == 2.0


def test_burst_then_wait() -> None:
    bucket = new_bucket(1.0, burst=2)
    bucket, wait = acquire(bucket, 0.0)
    assert wait == 0.0
    bucket, wait = acquire(bucket, 0.0)
    assert wait == 0.0
    bucket, wait = acquire(bucket, 0.0)
    assert wait == pytest.approx(1.0)
    assert bucket.tokens == 0.0  # waiting did not take a token


def test_wait_then_reacquire_succeeds() -> None:
    bucket = new_bucket(3.0)
    bucket, _ = acquire(bucket, 10.0)
    bucket, wait = acquire(bucket, 10.1)
    assert wait == pytest.approx((1 - 0.3) / 3.0)
    bucket, again = acquire(bucket, 10.1 + wait)
    assert again == 0.0


def test_refill_is_capped_at_burst() -> None:
    bucket = new_bucket(1.0, burst=2)
    bucket, _ = acquire(bucket, 0.0)
    bucket, _ = acquire(bucket, 1000.0)
    assert bucket.tokens == 1.0  # refilled to 2, took 1


def test_clock_going_backwards_adds_nothing() -> None:
    bucket = new_bucket(1.0)
    bucket, _ = acquire(bucket, 100.0)
    bucket, wait = acquire(bucket, 50.0)
    assert wait == pytest.approx(1.0)
    assert bucket.updated_at == 100.0


def test_slow_down_halves_rate_and_empties_bucket() -> None:
    bucket = new_bucket(2.0, burst=5)
    slowed = slow_down(bucket, 0.5, 0.1)
    assert slowed.rate_per_s == 1.0
    assert slowed.tokens == 0.0
    assert slowed.max_rate_per_s == 2.0


def test_slow_down_respects_floor() -> None:
    assert slow_down(new_bucket(0.15), 0.5, 0.1).rate_per_s == 0.1


def test_slow_down_never_raises_rate_below_floor() -> None:
    # A crawl-delay already put the rate under the floor: slowing down must not speed it up.
    assert slow_down(new_bucket(0.01), 0.5, 0.1).rate_per_s == 0.01


@pytest.mark.parametrize(("factor", "floor"), [(0.0, 0.1), (1.0, 0.1), (1.5, 0.1), (0.5, 0.0)])
def test_slow_down_validates(factor: float, floor: float) -> None:
    with pytest.raises(ValueError, match="must be"):
        slow_down(new_bucket(1.0), factor, floor)


def test_speed_up_is_additive_and_capped() -> None:
    bucket = slow_down(new_bucket(1.0), 0.5, 0.1)
    bucket = speed_up(bucket, 0.2)
    assert bucket.rate_per_s == pytest.approx(0.7)
    for _ in range(10):
        bucket = speed_up(bucket, 0.2)
    assert bucket.rate_per_s == 1.0


def test_speed_up_validates() -> None:
    with pytest.raises(ValueError, match="step"):
        speed_up(new_bucket(1.0), 0.0)


def test_crawl_delay_caps_rate_burst_and_ceiling() -> None:
    bucket = apply_crawl_delay(new_bucket(5.0, burst=10), 2.0)
    assert bucket.rate_per_s == 0.5
    assert bucket.max_rate_per_s == 0.5
    assert bucket.burst == 1.0
    assert bucket.tokens == 1.0
    assert speed_up(bucket, 1.0).rate_per_s == 0.5  # recovery cannot exceed the robots cap


def test_crawl_delay_never_raises_a_slower_rate() -> None:
    bucket = apply_crawl_delay(new_bucket(0.1), 2.0)
    assert bucket.rate_per_s == 0.1


def test_crawl_delay_spaces_requests() -> None:
    bucket = apply_crawl_delay(new_bucket(10.0, burst=5), 3.0)
    bucket, first = acquire(bucket, 0.0)
    bucket, second = acquire(bucket, 0.0)
    assert first == 0.0
    assert second == pytest.approx(3.0)


@pytest.mark.parametrize("delay", [None, 0, 0.0])
def test_no_crawl_delay_is_noop(delay: float | None) -> None:
    bucket = new_bucket(2.0, burst=3)
    assert apply_crawl_delay(bucket, delay) is bucket


@pytest.mark.parametrize("delay", [-1.0, float("inf"), float("nan")])
def test_bad_crawl_delay_rejected(delay: float) -> None:
    with pytest.raises(ValueError, match="crawl_delay_s"):
        apply_crawl_delay(new_bucket(1.0), delay)


@pytest.mark.parametrize(
    ("rate", "burst"), [(0.0, 1.0), (-1.0, 1.0), (float("inf"), 1.0), (1.0, 0.5), (1.0, 0.0)]
)
def test_new_bucket_validates(rate: float, burst: float) -> None:
    with pytest.raises(ValueError, match="must be"):
        new_bucket(rate, burst)


def test_state_is_immutable() -> None:
    bucket = new_bucket(1.0)
    with pytest.raises(AttributeError):
        bucket.tokens = 5  # type: ignore[misc]
    assert isinstance(bucket, TokenBucket)


@pytest.mark.parametrize("now", [2.0, None])
def test_slow_down_restarts_the_refill_clock(now: float | None) -> None:
    # Token taken at t=0; the request was slow and its 429 is seen at t=2. Without the restart, the
    # 2 s since the last refill would buy a token at once (R1-11).
    bucket, _ = acquire(new_bucket(1.0), 0.0)
    bucket = slow_down(bucket, 0.5, 0.05, now=now)
    bucket, wait = acquire(bucket, 2.0)
    assert wait == pytest.approx(2.0)  # a full interval at the new rate of 0.5/s
    bucket, wait = acquire(bucket, 4.0)
    assert wait == 0.0


def test_slow_down_ignores_a_clock_that_went_backwards() -> None:
    bucket, _ = acquire(new_bucket(1.0), 10.0)
    assert slow_down(bucket, now=5.0).updated_at == 10.0
    assert slow_down(new_bucket(1.0), now=3.0).updated_at == 3.0
