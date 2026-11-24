"""The fetch layer: fetchers that turn a URL into a :class:`~.protocol.FetchResult`, and the polite
scheduler that drives them with the pure rules in :mod:`scraping_pipeline.core`.

Everything here does I/O; nothing here decides policy. Classification, retries, the circuit breaker,
the token bucket and robots.txt rules all live in ``core/`` and are only *called* from here.
"""
