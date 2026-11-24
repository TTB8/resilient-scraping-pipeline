# Contributing

Thanks for taking a look. Issues and pull requests are welcome.

## Set up

```bash
uv sync
uv run pre-commit install     # ruff, ruff format, mypy --strict on every commit
make browsers                 # optional: Chromium for the JS-rendered site and its tests
```

## Before opening a pull request

```bash
make check                    # ruff, ruff format --check, mypy --strict, the full test suite
```

CI runs the same checks, plus the browser tests, the offline demo and a Docker build.

## Ground rules

- **Tests never touch the network.** Use the saved pages in `tests/fixtures/sites/` and the local
  fixture server (`tests/fixture_server.py`), which can simulate timeouts, 429, 5xx, redirects,
  block and challenge pages.
- **Keep `core/` pure.** No network, filesystem, clock or randomness there. Pass `now` and `rand`
  in. That is what makes every rule unit-testable ([ADR 0007](docs/decisions/0007-pure-core-injected-time.md)).
- **Parse stays offline and deterministic.** Same cache, same bytes ([ADR 0003](docs/decisions/0003-deterministic-outputs.md)).
- **No evasion.** Pull requests that add stealth plugins, proxy rotation, captcha solving,
  fingerprint spoofing or a borrowed User-Agent will be closed ([ADR 0005](docs/decisions/0005-detection-not-evasion.md)).
- **A bug fix comes with a test that fails without it.** For an important rule, check this by
  breaking the rule on purpose and watching the test fail.
- **Contracts are versioned.** An additive field is a minor bump; renaming, removing or tightening
  is a major bump ([ADR 0004](docs/decisions/0004-contracts-and-quarantine.md)).
- A design decision that a reviewer would ask "why?" about gets an ADR in `docs/decisions/`.
- Conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`...), one logical change per
  commit.

## Adding a site

See "Adapting it to a new site" in the [README](README.md#adapting-it-to-a-new-site) and the
docstring of [`sites/__init__.py`](src/scraping_pipeline/sites/__init__.py).
