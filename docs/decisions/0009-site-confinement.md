# 0009. The crawl is confined to its site, redirects included

## Context

The first independent review found that redirects were followed by the HTTP client to any host,
including private and cloud-metadata addresses. Every hop bypassed robots.txt, the rate limit and
the circuit breaker, so two promises the code made in writing ("the crawl never leaves a site",
"the rate bound holds") were false.

## Decision

- **Fetchers never follow redirects.** httpx runs with `follow_redirects=False`. Playwright aborts a
  redirected navigation and reports the 3xx.
- **The scheduler follows them itself**, and only within the entry's **origin** (scheme, host and
  effective port). Every hop is robots-checked and takes a token from the bucket and a ticket from
  the breaker, like any other request. A chain is capped at five hops.
- An off-site redirect, a redirect to a non-http(s) scheme, or a loop is a **permanent client
  error** with a reason ("off-site redirect to <host> refused"). It is never retried.
- Links found on pages enter the frontier only if they are on the same origin (`same_site`
  compares scheme, host and effective port), after canonicalization (RFC 3986 dot-segment
  removal, IDNA hosts, percent-encoding normalization).
- The browser loads only the top-level document and same-origin scripts. Images, stylesheets,
  fonts, media and every off-site request are aborted. It runs with no proxy, whatever the
  environment says.
- Every request also has one overall deadline, so a server that drips one byte at a time cannot
  hold a slot forever.

## Consequences

- "The crawl never leaves the site" and "N requests per second" are now true of requests, not just
  of frontier entries, and tests pin both.
- A site that legitimately moves content to another host needs a code change in its `SiteSpec`,
  which is the right place for such a decision.

## Alternatives considered

- **Let httpx follow redirects and check `final_url` afterwards.** By then the off-site request has
  already been sent.
