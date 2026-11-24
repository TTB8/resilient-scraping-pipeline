# resilient-scraping-pipeline

Instructions for coding agents (and humans) working in this repo. A polite, resumable,
reproducible web data pipeline in Python 3.12. Five stages, each its own re-runnable command:
`discover → fetch → parse → validate → export`. It targets only the scraping practice sites
`books.toscrape.com` and `quotes.toscrape.com`.

## Run

```bash
uv sync
uv run pipeline run --site books
make check                         # ruff, ruff format --check, mypy --strict, the full test suite
```

## Rules

- **Pure core.** `src/scraping_pipeline/core/` does no network or filesystem I/O. Clocks, randomness
  and I/O are injected. Keep it that way: it is what makes the rules unit-testable (ADR 0007).
- **Detection, not evasion.** Classify blocks and rate limits, back off, record, stop the domain.
  Never add stealth plugins, proxy rotation, captcha solving, fingerprint spoofing or a borrowed
  User-Agent (ADR 0005).
- **Parse is offline and deterministic.** It reads only the cache. Same cache → byte-identical
  output (ADR 0003).
- **Tests never touch the network.** Use the saved fixtures and the local fixture server only.
- **A fix comes with a test that fails without it.** For an important rule, break the rule on
  purpose and watch the test fail before calling it done.
- **No company or client names and no credentials** anywhere in the repo.
- One logical change per commit, conventional commit messages. Run `make check` before saying
  done, and show the output.
