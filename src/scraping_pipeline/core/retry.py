"""Retry policy: what to do after a failed fetch, and how long to wait before doing it.

The fetch scheduler never looks at status codes. It hands a :class:`~.classify.Classification` to
:func:`decide` and obeys the :class:`RetryDecision` it gets back, so the whole failure policy can be
read (and tested) in this one file:

* permanent failures (``not_found``, ``client_error``) are never retried: retrying cannot change the
  answer and only adds load;
* ``blocked`` and ``challenge`` stop the whole domain: the site has said no, and this project detects
  and records that instead of trying to get around it;
* transient failures (``server_error``, ``timeout``, ``network_error``) are retried with *full
  jitter* exponential backoff (``rand * min(cap, base * multiplier**(attempt-1))``). Full jitter spreads
  retries from many workers over the whole window instead of synchronising them into waves that hit
  a recovering server together;
* ``rate_limited`` is retried no sooner than the server's ``Retry-After``. When that value is longer
  than :attr:`BackoffPolicy.max_retry_after_s`, the site is asking us to go away for a long time: we
  stop the domain and report it rather than park the run or keep knocking.

Randomness and time are arguments (``rand`` in ``[0, 1)``, ``now_epoch`` in Unix seconds), so every
decision is a pure function of its inputs.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from scraping_pipeline.core.types import ResponseClass

if TYPE_CHECKING:
    from scraping_pipeline.core.classify import Classification


class RetryAction(StrEnum):
    """What the scheduler does next with a URL whose fetch did not succeed."""

    RETRY = "retry"  # schedule another attempt after ``delay_s``
    GIVE_UP_PERMANENT = "give_up_permanent"  # the answer will not change: mark failed, never retry
    GIVE_UP_EXHAUSTED = "give_up_exhausted"  # transient errors used up the retry budget
    STOP_DOMAIN = "stop_domain"  # the site said no (or "not for a long while"): stop the domain


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Tuning for retries. Validated on construction so a bad config fails at startup, not mid-run.

    ``max_attempts`` counts *all* attempts, the first one included: ``max_attempts=1`` means never
    retry. ``max_retry_after_s`` is the longest ``Retry-After`` we are willing to wait out.
    """

    base_s: float = 1.0
    multiplier: float = 2.0
    cap_s: float = 60.0
    max_attempts: int = 5
    max_retry_after_s: float = 300.0

    def __post_init__(self) -> None:
        for name in ("base_s", "multiplier", "cap_s", "max_retry_after_s"):
            value = getattr(self, name)
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite, got {value!r}")
        if self.base_s <= 0:
            raise ValueError(f"base_s must be > 0, got {self.base_s!r}")
        if self.multiplier < 1:
            raise ValueError(f"multiplier must be >= 1, got {self.multiplier!r}")
        if self.cap_s < self.base_s:
            raise ValueError(f"cap_s ({self.cap_s!r}) must be >= base_s ({self.base_s!r})")
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts!r}")
        if not 0 <= self.max_retry_after_s < RETRY_AFTER_CEILING_S:
            raise ValueError(
                f"max_retry_after_s must be >= 0 and < {RETRY_AFTER_CEILING_S:g}, "
                f"got {self.max_retry_after_s!r}"
            )


@dataclass(frozen=True, slots=True)
class RetryDecision:
    """The verdict for one failed attempt. ``delay_s`` is set only when ``action`` is ``RETRY``."""

    action: RetryAction
    delay_s: float | None
    reason: str


_PERMANENT = frozenset({ResponseClass.NOT_FOUND, ResponseClass.CLIENT_ERROR})
_REFUSED = frozenset({ResponseClass.BLOCKED, ResponseClass.CHALLENGE})


RETRY_AFTER_CEILING_S = 1e10
"""What a delta-seconds ``Retry-After`` longer than ten digits parses to (about 317 years).

A header value can be arbitrarily long, and ``float(int("9" * 400))`` raises ``OverflowError``
(``int("9" * 5000)`` raises ``ValueError``), which would escape the classifier and abort the run.
Ten digits already mean "centuries", so anything longer is read as what the server is saying: go
away for a very long time. The ceiling is finite (safe to store and print) and larger than any
allowed :attr:`BackoffPolicy.max_retry_after_s`, so :func:`decide` always stops the domain for it.
"""

_MAX_DELTA_DIGITS = 10


def parse_delta_seconds(value: str | None) -> float | None:
    """Parse the delta-seconds form of ``Retry-After`` (RFC 9110 section 10.2.3: ``1*DIGIT``).

    Only non-negative integers are valid; signs, fractions and anything else return ``None``. More
    than ten digits gives :data:`RETRY_AFTER_CEILING_S`. Never raises, whatever the input.
    """
    if value is None:
        return None
    text = value.strip()
    if not text.isascii() or not text.isdigit():
        return None
    if len(text) > _MAX_DELTA_DIGITS:
        return RETRY_AFTER_CEILING_S
    return float(int(text))


def parse_http_date(value: str | None) -> float | None:
    """Parse an HTTP-date (IMF-fixdate, plus the obsolete RFC 850 and asctime forms) to epoch seconds.

    RFC 9110 requires recipients to accept all three forms; asctime carries no zone and is GMT by
    definition. Returns ``None`` for anything that is not a date.
    """
    if value is None:
        return None
    text = value.strip()
    if not text or text.isdigit():
        return None
    try:
        moment = parsedate_to_datetime(text)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        return moment.timestamp()
    except (TypeError, ValueError, IndexError, OverflowError):
        return None


def parse_retry_after(value: str | None, now_epoch: float) -> float | None:
    """Seconds to wait according to a ``Retry-After`` header value.

    Delta-seconds are returned as-is; an HTTP-date becomes ``date - now_epoch``, floored at ``0.0``
    for a date in the past (the server's "later" has already arrived). Negative numbers, fractions and
    garbage return ``None``: an unreadable header is treated as absent, never guessed at.
    """
    delta = parse_delta_seconds(value)
    if delta is not None:
        return delta
    moment = parse_http_date(value)
    if moment is None:
        return None
    return max(0.0, moment - now_epoch)


def backoff_delay(policy: BackoffPolicy, attempt: int, rand: float) -> float:
    """Full-jitter delay before retrying after attempt number ``attempt`` (1-based).

    Always in ``[0, policy.cap_s]``. The exponential term is capped without computing
    ``multiplier**(attempt-1)`` for huge attempts, so it can never overflow.
    """
    if attempt < 1:
        raise ValueError(f"attempt is 1-based, got {attempt!r}")
    if not 0.0 <= rand < 1.0:
        raise ValueError(f"rand must be in [0, 1), got {rand!r}")
    exponent = attempt - 1
    if policy.multiplier == 1.0:
        ceiling = policy.base_s
    elif exponent >= math.log(policy.cap_s / policy.base_s) / math.log(policy.multiplier):
        ceiling = policy.cap_s
    else:
        ceiling = min(policy.cap_s, policy.base_s * policy.multiplier**exponent)
    return rand * ceiling


def decide(
    policy: BackoffPolicy, classification: Classification, attempt: int, rand: float
) -> RetryDecision:
    """Decide what follows attempt number ``attempt`` (1-based) that ended in ``classification``.

    ``rand`` in ``[0, 1)`` feeds the jitter. ``ok`` is not a failure and has no retry decision:
    passing it raises ``ValueError`` (the frontier marks an ``ok`` entry done without one).
    """
    if attempt < 1:
        raise ValueError(f"attempt is 1-based, got {attempt!r}")
    response_class = classification.response_class
    if response_class is ResponseClass.OK:
        raise ValueError("decide() is for failures; an ok response needs no retry decision")
    if response_class in _PERMANENT:
        return RetryDecision(
            RetryAction.GIVE_UP_PERMANENT, None, f"{response_class}: retrying cannot help"
        )
    if response_class in _REFUSED:
        return RetryDecision(
            RetryAction.STOP_DOMAIN, None, f"{response_class}: the site refused us, stopping domain"
        )

    retry_after = classification.retry_after_s
    if (
        response_class is ResponseClass.RATE_LIMITED
        and retry_after is not None
        and retry_after > policy.max_retry_after_s
    ):
        return RetryDecision(
            RetryAction.STOP_DOMAIN,
            None,
            f"Retry-After {retry_after:g}s exceeds the {policy.max_retry_after_s:g}s limit, stopping domain",
        )
    if attempt >= policy.max_attempts:
        return RetryDecision(
            RetryAction.GIVE_UP_EXHAUSTED,
            None,
            f"{response_class} after {attempt} of {policy.max_attempts} attempts",
        )

    delay = backoff_delay(policy, attempt, rand)
    if response_class is ResponseClass.RATE_LIMITED and retry_after is not None:
        delay = max(delay, retry_after)
    # Only the transient classes and rate_limited reach this point.
    return RetryDecision(
        RetryAction.RETRY, delay, f"{response_class}, retry {attempt + 1} in {delay:.2f}s"
    )
