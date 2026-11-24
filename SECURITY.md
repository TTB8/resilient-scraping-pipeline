# Security policy

## Reporting a vulnerability

Please report security issues privately, through GitHub's
[private vulnerability reporting](https://github.com/TTB8/resilient-scraping-pipeline/security/advisories/new),
not in a public issue. You will get an answer within a few days.

Useful reports for this project include:

- ways to make the crawler leave its site: redirects, links or browser subresources that reach
  another host or an internal address. Both fetchers report a redirect instead of following it
  (the browser requests the page with redirects off), so every hop gets the same checks;
- ways to make it exceed its rate or concurrency limits, or ignore robots.txt;
- inputs (pages, headers, robots.txt files) that crash a run, hang it, or corrupt the cache, the
  state database or the outputs;
- anything that would let scraped content execute code or reach the operator's credentials.

## Scope and design

The pipeline takes no credentials and stores none. It fetches only public pages, ignores proxy
environment variables, and refuses a User-Agent that carries a well-known browser or crawler
identity (a denylist of their product tokens, checked at start-up). The design decisions
behind this are in [docs/decisions/](docs/decisions/), in particular
[0005](docs/decisions/0005-detection-not-evasion.md) and
[0009](docs/decisions/0009-site-confinement.md).

Known and accepted: the JavaScript-rendered site's same-origin scripts are not rate-limited
separately. Routing requests through Playwright disables the browser's HTTP cache, so every page
downloads them again, without a token. They are bounded by the page itself (a handful of files).

`*.csv` deliverables are data-interchange files: scraped text is written as is, so a cell that
starts with `=`, `+`, `-` or `@` is not escaped. Open them in a spreadsheet only through a text
import, or use the Parquet files.
