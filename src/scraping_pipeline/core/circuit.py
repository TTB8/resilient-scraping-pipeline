"""Per-domain circuit breaker, as immutable state.

Retries handle one URL; the breaker protects the *site*. When a domain keeps failing (5xx, timeouts,
dropped connections, 429s), retrying each of its URLs on its own schedule would keep hitting a server
that is already in trouble. The breaker opens after ``failure_threshold`` consecutive transient
failures and stops all traffic to the domain for a cooldown. Then it lets through exactly one probe
(half-open). If the probe succeeds the domain is closed again; if it fails the breaker reopens with a
longer cooldown. A domain that keeps failing is eventually *tripped*: stopped for the rest of the
run, with a reason in the report, instead of being probed forever.

Rules worth knowing:

* only transient classes count as failures (``server_error``, ``timeout``, ``network_error``,
  ``rate_limited``). ``not_found`` and ``client_error`` are answers, not outages: they neither count
  nor reset the counter while closed, and they close a half-open breaker, because the server
  answered;
* ``ok`` resets the consecutive-failure counter;
* ``blocked`` and ``challenge`` trip the breaker at once. The retry policy already stops the domain
  for these; the breaker records the same decision so no other code path can send another request
  (detection, never evasion);
* results recorded while ``open`` are ignored: they come from requests sent before it opened, and
  only the probe may close the breaker;
* ``max_open_cycles`` is the number of times the breaker may open and then probe. The opening after
  that trips it. With ``max_open_cycles=3`` a domain gets three cooldowns, and the fourth opening
  stops it;
* the escalated cooldown is kept after a successful probe: a domain that has already failed once in
  this run gets the longer pause if it fails again.

``now`` is monotonic seconds (the scheduler's clock). Breaker state lives for one run and is never
persisted, so a monotonic clock is the right one: it never jumps when the wall clock is adjusted.
"""

import math
from dataclasses import dataclass, replace
from enum import StrEnum

from scraping_pipeline.core.types import ResponseClass


class BreakerPhase(StrEnum):
    """Where a domain's breaker is. ``TRIPPED`` is final for the run."""

    CLOSED = "closed"  # traffic flows
    OPEN = "open"  # no traffic until the cooldown ends
    HALF_OPEN = "half_open"  # one probe is in flight; everything else waits for its result
    TRIPPED = "tripped"  # stopped for the rest of the run


@dataclass(frozen=True, slots=True)
class BreakerConfig:
    """Breaker tuning, validated on construction."""

    failure_threshold: int = 5
    cooldown_s: float = 30.0
    cooldown_multiplier: float = 2.0
    max_cooldown_s: float = 600.0
    max_open_cycles: int = 3

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {self.failure_threshold!r}")
        for name in ("cooldown_s", "cooldown_multiplier", "max_cooldown_s"):
            if not math.isfinite(getattr(self, name)):
                raise ValueError(f"{name} must be finite")
        if self.cooldown_s <= 0:
            raise ValueError(f"cooldown_s must be > 0, got {self.cooldown_s!r}")
        if self.cooldown_multiplier < 1:
            raise ValueError(f"cooldown_multiplier must be >= 1, got {self.cooldown_multiplier!r}")
        if self.max_cooldown_s < self.cooldown_s:
            raise ValueError("max_cooldown_s must be >= cooldown_s")
        if self.max_open_cycles < 0:
            raise ValueError(f"max_open_cycles must be >= 0, got {self.max_open_cycles!r}")


@dataclass(frozen=True, slots=True)
class BreakerState:
    """One domain's breaker. Build the first one with :func:`new_breaker`; never mutate it.

    ``open_until`` is set while ``OPEN``. ``open_cycles`` counts openings so far in this run.
    ``reason`` is the latest transition, written for the run report.
    """

    config: BreakerConfig
    phase: BreakerPhase = BreakerPhase.CLOSED
    consecutive_failures: int = 0
    open_until: float | None = None
    cooldown_s: float = 0.0
    open_cycles: int = 0
    reason: str = "closed"


FAILURE_CLASSES = frozenset(
    {
        ResponseClass.SERVER_ERROR,
        ResponseClass.TIMEOUT,
        ResponseClass.NETWORK_ERROR,
        ResponseClass.RATE_LIMITED,
    }
)
_REFUSED = frozenset({ResponseClass.BLOCKED, ResponseClass.CHALLENGE})
_ANSWERED = frozenset({ResponseClass.OK, ResponseClass.NOT_FOUND, ResponseClass.CLIENT_ERROR})


def new_breaker(config: BreakerConfig | None = None) -> BreakerState:
    """A closed breaker for a domain the run has not talked to yet."""
    config = config or BreakerConfig()
    return BreakerState(config=config, cooldown_s=config.cooldown_s)


def allow(state: BreakerState, now: float) -> tuple[BreakerState, bool]:
    """May a request go to this domain now? Returns the (possibly advanced) state and the answer.

    An ``OPEN`` breaker whose cooldown has ended moves to ``HALF_OPEN`` and answers ``True`` once:
    that request is the probe. Every later call answers ``False`` until the probe's result is
    recorded.
    """
    if state.phase is BreakerPhase.CLOSED:
        return state, True
    if state.phase is BreakerPhase.OPEN:
        open_until = state.open_until if state.open_until is not None else now
        if now >= open_until:
            probing = replace(
                state,
                phase=BreakerPhase.HALF_OPEN,
                open_until=None,
                reason="half-open: sending one probe",
            )
            return probing, True
        return state, False
    return state, False  # HALF_OPEN (probe already out) or TRIPPED


def record(state: BreakerState, response_class: ResponseClass, now: float) -> BreakerState:
    """Feed the result of a request to this domain into the breaker."""
    if state.phase in (BreakerPhase.TRIPPED, BreakerPhase.OPEN):
        return state
    if response_class in _REFUSED:
        return replace(
            state,
            phase=BreakerPhase.TRIPPED,
            open_until=None,
            reason=f"tripped: the site answered {response_class}",
        )
    if state.phase is BreakerPhase.HALF_OPEN:
        return _record_probe(state, response_class, now)
    return _record_closed(state, response_class, now)


def _record_probe(state: BreakerState, response_class: ResponseClass, now: float) -> BreakerState:
    if response_class in _ANSWERED:
        return replace(
            state,
            phase=BreakerPhase.CLOSED,
            consecutive_failures=0,
            reason=f"closed: probe answered {response_class}",
        )
    cooldown = min(state.cooldown_s * state.config.cooldown_multiplier, state.config.max_cooldown_s)
    return _open(state, now, cooldown, f"probe failed with {response_class}")


def _record_closed(state: BreakerState, response_class: ResponseClass, now: float) -> BreakerState:
    if response_class is ResponseClass.OK:
        return replace(state, consecutive_failures=0) if state.consecutive_failures else state
    if response_class not in FAILURE_CLASSES:
        return state  # not_found / client_error: neutral
    failures = state.consecutive_failures + 1
    if failures >= state.config.failure_threshold:
        return _open(
            replace(state, consecutive_failures=failures),
            now,
            state.cooldown_s,
            f"{failures} consecutive failures, last {response_class}",
        )
    return replace(state, consecutive_failures=failures)


def describe(state: BreakerState) -> str:
    """A one-line, human summary of the breaker for the run report."""
    return f"{state.phase} ({state.reason}; opened {state.open_cycles}x)"


def is_tripped(state: BreakerState) -> bool:
    """True when the domain is stopped for the rest of the run."""
    return state.phase is BreakerPhase.TRIPPED


def _open(state: BreakerState, now: float, cooldown: float, why: str) -> BreakerState:
    cycles = state.open_cycles + 1
    if cycles > state.config.max_open_cycles:
        return replace(
            state,
            phase=BreakerPhase.TRIPPED,
            open_until=None,
            open_cycles=cycles,
            reason=f"tripped after {state.config.max_open_cycles} open cycles: {why}",
        )
    return replace(
        state,
        phase=BreakerPhase.OPEN,
        open_until=now + cooldown,
        cooldown_s=cooldown,
        open_cycles=cycles,
        reason=f"open for {cooldown:g}s: {why}",
    )
