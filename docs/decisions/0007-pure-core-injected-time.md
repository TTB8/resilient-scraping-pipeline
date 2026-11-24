# 0007. A pure core: clocks, randomness and I/O are injected

## Context

The rules that matter most here are about time and failure: backoff with jitter, `Retry-After`
dates, a circuit breaker's cooldown, a token bucket's refill, robots.txt semantics. Rules like that
are usually tested by sleeping, which is slow, flaky, and only covers the cases someone had the
patience to wait for.

## Decision

- `core/` performs no network, filesystem, clock or random calls. Functions take `now` and `rand`
  as arguments and return new immutable state: a frozen dataclass in, a frozen dataclass out.
- The scheduler takes `clock`, `wall_clock`, `sleep` and `rand` as constructor arguments. Tests run
  it in **virtual time**. A `VirtualTime` helper advances the clock when every task is asleep, and
  raises `DeadlockError` instead of hanging when nothing can make progress.
- The `Fetcher` is a protocol with three implementations: httpx for static pages, Playwright for
  the JavaScript-rendered site, and a scripted `FakeFetcher` that simulates timeouts, 429 with
  `Retry-After` (seconds or HTTP date), 5xx, block pages, challenge pages and slow responses.

## Consequences

- The core has 99% line and branch coverage, enforced in CI at 90% or more, plus Hypothesis property tests: backoff never exceeds
  its cap or undercuts `Retry-After`, the breaker never grants two half-open probes, the bucket
  never admits more than `burst + rate x elapsed`, frontier planning is order-independent.
- A ten-minute backoff scenario runs in milliseconds.
- Real-time behaviour is still covered by integration tests against a local fixture server, kept
  small and marked slow where needed.

## Alternatives considered

- **Mock `time.sleep` and `time.time` globally.** Brittle, it leaks between tests, and it does not
  model concurrent tasks waking up.
