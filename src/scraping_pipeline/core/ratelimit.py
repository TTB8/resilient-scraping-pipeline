"""Per-domain token bucket, as immutable state: the politeness budget of the crawl.

A token bucket allows short bursts (up to ``burst`` tokens) and then exactly ``rate_per_s`` on
average. Over *any* time window of length ``T`` it grants at most ``burst + rate_per_s * T`` tokens,
which is the guarantee that matters for a polite crawler, and the property the tests check. It
bounds *requests* because the scheduler takes one token per HTTP request it sends: every redirect
hop and every robots.txt attempt included (the fetchers never follow redirects on their own). The
one documented exception is a rendered page's same-origin scripts in the Playwright fetcher, which
ride on the document's token (see ``fetch/playwright_fetcher.py``).

Three adjustments sit on top:

* :func:`apply_crawl_delay` honours robots.txt ``Crawl-delay``: the rate is capped at
  ``1 / crawl_delay`` and the burst at 1, because "N seconds between requests" leaves no room for a
  burst;
* :func:`slow_down` is the multiplicative decrease applied after a ``rate_limited`` response. It also
  empties the bucket *and restarts its refill clock*, so the next request waits a full interval at
  the new, lower rate, counted from the moment the 429 was seen (not from the last token taken,
  which may be long before when the 429'd request was slow);
* :func:`speed_up` is a gentle additive recovery after successes, never above the ceiling (the
  configured rate, or the robots cap if lower). Additive increase with multiplicative decrease
  (AIMD) settles just under what the server tolerates instead of oscillating.

:func:`acquire` does not reserve: it either takes a token (wait ``0.0``) or returns how long to wait,
leaving the state's token count unchanged apart from the refill. The caller sleeps and calls again.
``now`` is monotonic seconds; a clock that goes backwards is treated as no time passing.
"""

import math
from dataclasses import dataclass, replace

# Float slack when re-acquiring exactly ``wait`` seconds later: refill arithmetic can land a hair
# under one token. 1e-9 of a token is far below anything a server could notice.
_EPSILON = 1e-9


@dataclass(frozen=True, slots=True)
class TokenBucket:
    """One domain's bucket. Build it with :func:`new_bucket`; every function returns a new one.

    ``max_rate_per_s`` is the ceiling :func:`speed_up` may climb back to. ``updated_at`` is ``None``
    until the first :func:`acquire`, and the bucket starts full.
    """

    rate_per_s: float
    burst: float
    tokens: float
    max_rate_per_s: float
    updated_at: float | None = None


def new_bucket(rate_per_s: float, burst: float = 1.0) -> TokenBucket:
    """A full bucket allowing ``rate_per_s`` requests per second on average and ``burst`` at once."""
    _check_positive("rate_per_s", rate_per_s)
    _check_positive("burst", burst)
    if burst < 1:
        raise ValueError(f"burst must be >= 1, got {burst!r}")
    return TokenBucket(rate_per_s=rate_per_s, burst=burst, tokens=burst, max_rate_per_s=rate_per_s)


def acquire(state: TokenBucket, now: float) -> tuple[TokenBucket, float]:
    """Try to take one token at time ``now``.

    Returns ``(new_state, 0.0)`` when the request may go now, or ``(new_state, wait_s)`` with the
    seconds until a token will be available; in that case no token was taken.
    """
    refilled = _refill(state, now)
    if refilled.tokens >= 1.0 - _EPSILON:
        return replace(refilled, tokens=max(0.0, refilled.tokens - 1.0)), 0.0
    wait = (1.0 - refilled.tokens) / refilled.rate_per_s
    return refilled, wait


def slow_down(
    state: TokenBucket, factor: float = 0.5, floor: float = 0.05, *, now: float | None = None
) -> TokenBucket:
    """Multiplicative decrease after a ``rate_limited`` response: ``rate * factor``, not below ``floor``.

    Never raises the rate (when the rate is already under ``floor``, it stays where it is). Empties
    the bucket and restarts the refill clock, so the next request waits a full interval at the new
    rate: from ``now`` (when the 429 was seen) if given, else from the next :func:`acquire`. Without
    the restart, the time since the last token was taken would refill the bucket at once.
    """
    if not 0.0 < factor < 1.0:
        raise ValueError(f"factor must be in (0, 1), got {factor!r}")
    _check_positive("floor", floor)
    rate = min(state.rate_per_s, max(floor, state.rate_per_s * factor))
    if now is None:
        restarted = None
    elif state.updated_at is None:
        restarted = now
    else:
        restarted = max(state.updated_at, now)  # a clock going backwards is no time passing
    return replace(state, rate_per_s=rate, tokens=0.0, updated_at=restarted)


def speed_up(state: TokenBucket, step: float = 0.05) -> TokenBucket:
    """Additive recovery after a success: ``rate + step``, never above ``max_rate_per_s``."""
    _check_positive("step", step)
    return replace(state, rate_per_s=min(state.max_rate_per_s, state.rate_per_s + step))


def apply_crawl_delay(state: TokenBucket, crawl_delay_s: float | None) -> TokenBucket:
    """Cap the bucket so requests are at least ``crawl_delay_s`` apart (robots.txt ``Crawl-delay``).

    ``None`` or ``0`` leaves the bucket unchanged. The cap only ever lowers the rate, the ceiling and
    the burst; it never raises a configured rate that is already slower.
    """
    if crawl_delay_s is None or crawl_delay_s == 0:
        return state
    _check_positive("crawl_delay_s", crawl_delay_s)
    cap = 1.0 / crawl_delay_s
    return replace(
        state,
        rate_per_s=min(state.rate_per_s, cap),
        max_rate_per_s=min(state.max_rate_per_s, cap),
        burst=1.0,
        tokens=min(state.tokens, 1.0),
    )


def _refill(state: TokenBucket, now: float) -> TokenBucket:
    if state.updated_at is None:
        return replace(state, updated_at=now)
    elapsed = max(0.0, now - state.updated_at)
    tokens = min(state.burst, state.tokens + elapsed * state.rate_per_s)
    return replace(state, tokens=tokens, updated_at=max(state.updated_at, now))


def _check_positive(name: str, value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite number > 0, got {value!r}")
