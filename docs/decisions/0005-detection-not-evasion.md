# 0005. Detect blocks and rate limits, then stop. Never evade them

## Context

Sites signal "slow down" (429, `Retry-After`) and "stop" (401/403, access-denied pages, "verify
you are human" interstitials). A common scraping playbook answers those signals by evading them:
stealth browser plugins, rotating proxies or residential IPs, captcha-solving services, spoofed
browser fingerprints, a borrowed browser or search-engine User-Agent.

A pipeline a company runs in production needs to answer to its legal, security and reputation
teams, not only to its data consumers. Evasion turns a site's explicit "no" into unauthorised
access. It creates contractual and legal exposure in many jurisdictions, makes the operator's
infrastructure look like an attacker's, and is an arms race the data team cannot win, because
the site changes the rules every week.

## Decision

- **Detect.** Every response is classified into one of `ok`, `not_found`, `client_error`,
  `rate_limited`, `blocked`, `challenge`, `server_error`, `timeout` or `network_error`
  (`core/classify.py`). On a 2xx, a block or challenge marker counts only in the page's title or
  first heading, and only on a short page. That way a book called "Access Denied" is not
  mistaken for a block page.
- **Back off.** Rate limits are retried no sooner than `Retry-After` says, with exponential
  backoff and full jitter, and the domain's request rate is cut multiplicatively.
- **Record and stop.** A block or challenge, a `Retry-After` longer than the configured maximum,
  a robots.txt `Crawl-delay` longer than the maximum, or a circuit breaker that keeps opening
  *stops that domain* for the run. The remaining URLs are marked `stopped` with the reason, and
  the run report names it. Other domains carry on.
- **Never evade.** The project contains no stealth plugins, proxy rotation, captcha solving,
  fingerprint spoofing or User-Agent impersonation. The fetchers ignore proxy environment
  variables. The settings refuse a User-Agent that carries a well-known browser or crawler
  identity (a denylist of product tokens; an unknown crawler's name is not detected). The repository's agent guard blocks adding such packages. A `stopped` domain can be
  re-queued later (`fetch --retry-failed`), which is a human decision to try again politely, not
  a way around the block.

## Consequences

- A blocked crawl ends with less data and a clear explanation, which is the correct outcome.
- The operator can show anyone how the crawler behaves: it says who it is, how to reach its
  operator, what it obeyed and where it stopped.
- If a source matters enough to fight for, the answer is a conversation with the site (an API, a
  feed, a data agreement), not more code here.

## Alternatives considered

- **"Optional" evasion behind a flag.** A flag that exists gets turned on. Not offered.
- **Keep retrying blocked domains slowly.** Retrying a 403 is still ignoring a "no". It's treated
  as final for the run.
