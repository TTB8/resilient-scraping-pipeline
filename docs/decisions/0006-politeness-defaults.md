# 0006. Polite defaults with hard ceilings; robots.txt cannot be switched off

## Context

Politeness that depends on someone reading the documentation is not politeness. Crawlers get
tuned under deadline pressure, and a setting that can be turned up will be.

## Decision

Defaults (`config.py`, all validated at start-up, so a bad value fails before the first request):

| Setting | Default | Ceiling |
|---|---|---|
| requests per second, per domain | 1.0 | 5.0 |
| burst | 1 | 10 |
| concurrent requests per domain | 2 | 8 |
| per-request deadline (whole request, not per phase) | 20 s | 120 s |
| attempts per URL | 5 | 10 |
| longest `Retry-After` we wait out | 300 s | then the domain is stopped |
| longest robots.txt `Crawl-delay` we honour | 60 s | then the domain is stopped |

- **robots.txt is always fetched and obeyed**, under RFC 9309: longest-match precedence, `*` and
  `$` wildcards, percent-encoding equivalence, product-token group matching. A 4xx means "no
  rules". A 5xx, 429 or no answer means "disallow everything": it is retried with backoff, and if
  it never answers the domain is stopped. There is no setting to turn robots.txt off; an unknown
  setting is a validation error.
- **The User-Agent is honest.** It must lead with this crawler's own `name/version` token, include
  a contact URL or e-mail address, and not contain a well-known browser or crawler identity (a
  denylist of product tokens, so it catches the common ones, not every crawler's name).
- The ceilings are constants in code. Raising one is a reviewed code change, not an environment
  variable.

## Consequences

- A default run of the books site takes a while: roughly a thousand pages at one per second. That
  is the intended trade.
- Tests run fast anyway, because time is injected (ADR 0007) and the fixture server is local.

## Alternatives considered

- **A "fast mode".** It would become the only mode. Not offered.
- **robots.txt as advisory.** It is the one machine-readable statement of the site's wishes.
  Ignoring it is the first step towards ADR 0005's list.
