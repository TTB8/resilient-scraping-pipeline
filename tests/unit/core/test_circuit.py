import pytest

from scraping_pipeline.core.circuit import (
    BreakerConfig,
    BreakerPhase,
    BreakerState,
    allow,
    describe,
    generation,
    is_tripped,
    new_breaker,
    record,
)
from scraping_pipeline.core.types import ResponseClass

CFG = BreakerConfig(
    failure_threshold=3,
    cooldown_s=10.0,
    cooldown_multiplier=2.0,
    max_cooldown_s=25.0,
    max_open_cycles=2,
)


def fail(
    state: BreakerState, n: int, now: float = 0.0, rc: ResponseClass = ResponseClass.SERVER_ERROR
) -> BreakerState:
    for _ in range(n):
        state = record(state, rc, now, sent_in=generation(state))
    return state


def opened(now: float = 0.0) -> BreakerState:
    state = fail(new_breaker(CFG), 3, now)
    assert state.phase is BreakerPhase.OPEN
    return state


def test_new_breaker_is_closed_and_allows() -> None:
    state = new_breaker()
    assert state.phase is BreakerPhase.CLOSED
    assert allow(state, 0.0) == (state, True)
    assert state.cooldown_s == BreakerConfig().cooldown_s


@pytest.mark.parametrize(
    "rc",
    [
        ResponseClass.SERVER_ERROR,
        ResponseClass.TIMEOUT,
        ResponseClass.NETWORK_ERROR,
        ResponseClass.RATE_LIMITED,
    ],
)
def test_transient_failures_open_at_threshold(rc: ResponseClass) -> None:
    state = fail(new_breaker(CFG), 2, rc=rc)
    assert state.phase is BreakerPhase.CLOSED
    assert state.consecutive_failures == 2
    state = record(state, rc, 5.0, sent_in=generation(state))
    assert state.phase is BreakerPhase.OPEN
    assert state.open_until == 15.0
    assert state.open_cycles == 1
    assert str(rc) in state.reason


@pytest.mark.parametrize("rc", [ResponseClass.NOT_FOUND, ResponseClass.CLIENT_ERROR])
def test_not_found_and_client_error_are_neutral(rc: ResponseClass) -> None:
    state = fail(new_breaker(CFG), 2)
    after = fail(state, 10, rc=rc)
    assert after == state  # neither counted nor reset


def test_ok_resets_consecutive_counter() -> None:
    state = record(
        fail(new_breaker(CFG), 2),
        ResponseClass.OK,
        0.0,
        sent_in=generation(fail(new_breaker(CFG), 2)),
    )
    assert state.consecutive_failures == 0
    assert fail(state, 2).phase is BreakerPhase.CLOSED


def test_ok_on_clean_breaker_returns_same_state() -> None:
    state = new_breaker(CFG)
    assert record(state, ResponseClass.OK, 0.0, sent_in=generation(state)) is state


def test_open_denies_until_cooldown_then_one_probe() -> None:
    state = opened()
    state, allowed = allow(state, 9.99)
    assert not allowed
    assert state.phase is BreakerPhase.OPEN
    state, allowed = allow(state, 10.0)
    assert allowed
    assert state.phase is BreakerPhase.HALF_OPEN
    for t in (10.0, 11.0, 1000.0):
        state, allowed = allow(state, t)
        assert not allowed  # only one probe


def test_open_without_deadline_allows_probe() -> None:
    # Defensive: a hand-built OPEN state with no deadline probes immediately.
    state = BreakerState(config=CFG, phase=BreakerPhase.OPEN, cooldown_s=10.0)
    assert allow(state, 0.0)[1]


@pytest.mark.parametrize(
    "rc", [ResponseClass.OK, ResponseClass.NOT_FOUND, ResponseClass.CLIENT_ERROR]
)
def test_probe_answer_closes(rc: ResponseClass) -> None:
    state, _ = allow(opened(), 10.0)
    state = record(state, rc, 10.5, sent_in=generation(state))
    assert state.phase is BreakerPhase.CLOSED
    assert state.consecutive_failures == 0
    assert allow(state, 10.6)[1]
    assert state.cooldown_s == 10.0


def test_probe_failure_reopens_with_longer_capped_cooldown() -> None:
    state, _ = allow(opened(), 10.0)
    state = record(state, ResponseClass.TIMEOUT, 11.0, sent_in=generation(state))
    assert state.phase is BreakerPhase.OPEN
    assert state.cooldown_s == 20.0
    assert state.open_until == 31.0
    assert state.open_cycles == 2
    assert "probe failed" in state.reason


def test_cooldown_is_capped() -> None:
    cfg = BreakerConfig(
        failure_threshold=1, cooldown_s=10.0, max_cooldown_s=15.0, max_open_cycles=5
    )
    state = record(
        new_breaker(cfg), ResponseClass.TIMEOUT, 0.0, sent_in=generation(new_breaker(cfg))
    )
    state, _ = allow(state, 10.0)
    state = record(state, ResponseClass.TIMEOUT, 10.0, sent_in=generation(state))
    assert state.cooldown_s == 15.0


def test_trips_after_max_open_cycles() -> None:
    state, _ = allow(opened(), 10.0)
    state = record(
        state, ResponseClass.SERVER_ERROR, 10.0, sent_in=generation(state)
    )  # second opening, cooldown 20
    assert state.phase is BreakerPhase.OPEN
    state, _ = allow(state, 30.0)
    state = record(
        state, ResponseClass.SERVER_ERROR, 30.0, sent_in=generation(state)
    )  # third opening > max_open_cycles=2
    assert state.phase is BreakerPhase.TRIPPED
    assert is_tripped(state)
    assert "tripped after 2 open cycles" in state.reason
    assert not allow(state, 1e9)[1]


def test_openings_count_across_closes() -> None:
    state, _ = allow(opened(), 10.0)
    state = record(state, ResponseClass.OK, 10.0, sent_in=generation(state))
    state = fail(state, 3, 20.0)  # second opening
    assert state.phase is BreakerPhase.OPEN
    assert state.open_cycles == 2
    assert state.cooldown_s == 10.0
    state, _ = allow(state, 30.0)
    state = record(state, ResponseClass.OK, 30.0, sent_in=generation(state))
    assert fail(state, 3, 40.0).phase is BreakerPhase.TRIPPED


def test_zero_open_cycles_trips_on_first_opening() -> None:
    cfg = BreakerConfig(failure_threshold=1, max_open_cycles=0)
    assert (
        record(
            new_breaker(cfg), ResponseClass.TIMEOUT, 0.0, sent_in=generation(new_breaker(cfg))
        ).phase
        is BreakerPhase.TRIPPED
    )


@pytest.mark.parametrize("rc", [ResponseClass.BLOCKED, ResponseClass.CHALLENGE])
@pytest.mark.parametrize("half_open", [False, True])
def test_refusals_trip_immediately(rc: ResponseClass, half_open: bool) -> None:
    state = new_breaker(CFG)
    if half_open:
        state, _ = allow(opened(), 10.0)
    state = record(state, rc, 0.0, sent_in=generation(state))
    assert state.phase is BreakerPhase.TRIPPED
    assert str(rc) in state.reason


def test_results_while_open_are_ignored() -> None:
    state = opened()
    assert record(state, ResponseClass.OK, 1.0, sent_in=generation(state)) is state
    assert record(state, ResponseClass.TIMEOUT, 1.0, sent_in=generation(state)) is state
    assert record(state, ResponseClass.BLOCKED, 1.0, sent_in=generation(state)) is state


def test_tripped_is_final() -> None:
    state = record(
        new_breaker(CFG), ResponseClass.BLOCKED, 0.0, sent_in=generation(new_breaker(CFG))
    )
    for rc in ResponseClass:
        assert record(state, rc, 1.0, sent_in=generation(state)) is state


def test_describe_is_human() -> None:
    text = describe(opened())
    assert text.startswith("open (open for 10s: 3 consecutive failures")
    assert "opened 1x" in text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"failure_threshold": 0},
        {"cooldown_s": 0.0},
        {"cooldown_s": float("inf")},
        {"cooldown_multiplier": 0.5},
        {"max_cooldown_s": 1.0, "cooldown_s": 2.0},
        {"max_open_cycles": -1},
    ],
)
def test_config_validation(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must be"):
        BreakerConfig(**kwargs)  # type: ignore[arg-type]


# --- stale results (R1-10) ------------------------------------------------------------------


def test_stale_ok_does_not_close_a_half_open_breaker() -> None:
    cfg = BreakerConfig(failure_threshold=1, cooldown_s=5.0, max_open_cycles=3)
    state, _ = allow(new_breaker(cfg), 0.0)  # A: sent at t=0, slow
    sent_a = generation(state)
    state, _ = allow(state, 0.1)  # B
    state = record(state, ResponseClass.SERVER_ERROR, 1.0, sent_in=generation(state))  # B opens it
    state, probe = allow(state, 6.0)
    sent_probe = generation(state)
    assert probe
    assert state.phase is BreakerPhase.HALF_OPEN
    stale = record(state, ResponseClass.OK, 7.0, sent_in=sent_a)  # A answers, sent before opening
    assert stale is state  # still half-open: only the probe decides
    closed = record(stale, ResponseClass.OK, 8.0, sent_in=sent_probe)
    assert closed.phase is BreakerPhase.CLOSED


def test_stale_failure_does_not_reopen_or_escalate() -> None:
    cfg = BreakerConfig(failure_threshold=1, cooldown_s=5.0, max_open_cycles=3)
    state = new_breaker(cfg)
    sent_a = generation(state)
    state = record(state, ResponseClass.TIMEOUT, 1.0, sent_in=sent_a)
    state, _ = allow(state, 6.0)
    assert record(state, ResponseClass.TIMEOUT, 7.0, sent_in=sent_a) is state  # half-open
    state = record(state, ResponseClass.OK, 8.0, sent_in=generation(state))  # probe closes it
    assert record(state, ResponseClass.TIMEOUT, 9.0, sent_in=sent_a) is state  # closed: neutral
    assert state.cooldown_s == 5.0


def test_stale_refusal_still_trips() -> None:
    cfg = BreakerConfig(failure_threshold=1, cooldown_s=5.0)
    state = record(new_breaker(cfg), ResponseClass.TIMEOUT, 1.0, sent_in=0)
    state, _ = allow(state, 6.0)
    assert record(state, ResponseClass.BLOCKED, 7.0, sent_in=0).phase is BreakerPhase.TRIPPED


def test_generation_counts_openings() -> None:
    state = opened()
    assert generation(state) == 1
    state, _ = allow(state, 10.0)
    state = record(state, ResponseClass.TIMEOUT, 10.0, sent_in=generation(state))
    assert generation(state) == 2
