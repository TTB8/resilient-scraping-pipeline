"""Breaker safety properties over arbitrary interleavings of allow() and record()."""

from hypothesis import given
from hypothesis import strategies as st

from scraping_pipeline.core.circuit import (
    BreakerConfig,
    BreakerPhase,
    allow,
    generation,
    new_breaker,
    record,
)
from scraping_pipeline.core.types import ResponseClass

configs = st.builds(
    BreakerConfig,
    failure_threshold=st.integers(min_value=1, max_value=5),
    cooldown_s=st.floats(min_value=0.1, max_value=50.0),
    cooldown_multiplier=st.floats(min_value=1.0, max_value=4.0),
    max_cooldown_s=st.just(200.0),
    max_open_cycles=st.integers(min_value=0, max_value=4),
)

ops = st.lists(
    st.tuples(
        st.floats(min_value=0.0, max_value=60.0),  # time advance
        st.one_of(st.none(), st.sampled_from(list(ResponseClass))),  # None = allow()
    ),
    max_size=200,
)


@given(config=configs, steps=ops)
def test_half_open_allows_at_most_one_probe(
    config: BreakerConfig, steps: list[tuple[float, ResponseClass | None]]
) -> None:
    state = new_breaker(config)
    now = 0.0
    probes_out = 0
    for advance, rc in steps:
        now += advance
        if rc is None:
            before = state.phase
            state, allowed = allow(state, now)
            if before is BreakerPhase.TRIPPED:
                assert not allowed
            if allowed and state.phase is BreakerPhase.HALF_OPEN:
                probes_out += 1
            assert probes_out <= 1
        else:
            if state.phase is BreakerPhase.HALF_OPEN:
                probes_out = 0  # the probe's result has come back
            was_tripped = state.phase is BreakerPhase.TRIPPED
            state = record(state, rc, now)
            assert not was_tripped or state.phase is BreakerPhase.TRIPPED  # tripped is final
        assert state.open_cycles <= config.max_open_cycles + 1
        assert state.cooldown_s <= config.max_cooldown_s


@given(config=configs, steps=ops, stale=st.lists(st.sampled_from(list(ResponseClass)), max_size=20))
def test_results_sent_before_an_opening_never_close_it(
    config: BreakerConfig,
    steps: list[tuple[float, ResponseClass | None]],
    stale: list[ResponseClass],
) -> None:
    """A request granted in generation g and answered after a later opening is never the probe."""
    state = new_breaker(config)
    now = 0.0
    for advance, rc in steps:
        now += advance
        if rc is None:
            state, _ = allow(state, now)
        else:
            state = record(state, rc, now, sent_in=generation(state))
        if state.phase is BreakerPhase.HALF_OPEN and generation(state) > 0:
            old = generation(state) - 1
            for late in stale:
                after = record(state, late, now, sent_in=old)
                if late in (ResponseClass.BLOCKED, ResponseClass.CHALLENGE):
                    assert after.phase is BreakerPhase.TRIPPED
                else:
                    assert after is state
