# 0010. A same-host http<->https redirect is followed (https to http per site), and every downgrade is counted

Supersedes the part of [0009](0009-site-confinement.md) that confines redirects to the entry's
exact origin.

## Context

ADR 0009 let the scheduler follow a redirect only within the entry's origin (scheme, host and
effective port). The first live run against the quotes practice site showed that it answers
`https://.../author/X` with `308` to `http://.../author/X/`. Under 0009 every author page was a
permanent "off-site redirect" failure, although it is the same server under the same name.

The second independent review then found three loose ends around that change: the docs still said
"same origin", links found on a page reached over `http` were resolved against the `http` URL and
dropped by the scheme-strict link rule, and robots.txt redirects were checked by host only, so a
`robots.txt` redirect to port 6379 on the same host was followed and its rules governed the site.

## Decision

- **A redirect stays on the site** when the target has the entry's origin, or when it only
  switches between `http` and `https` on the same host with both sides on their scheme's default
  port (`same_site_origin` in `fetch/scheduler.py`). The `https` to `http` direction needs the
  site's opt-in (see "Per-site opt-in" below). Another host, or any non-default port, is still
  another site and is refused permanently.
- **robots.txt redirects use the same rule.** Another port on the same host is another server,
  and its rules must not govern this site.
- **The downgrade is followed, not upgraded.** An `https` to `http` hop is not rewritten back to
  `https`: the site redirects `https` to `http`, so the upgraded request would get the same `308`
  and loop until the hop cap.
- **Every downgrade is counted.** `CrawlSummary.scheme_downgrades` (and `scheme_downgrades` in the
  fetch run's summary) counts each followed `https` to `http` hop, pages and robots.txt alike, so
  the run report can show how many requests went out in clear text.
- **Links on a scheme-switched page are kept.** When the hops only switched scheme, the page's
  links are resolved against the same path on the entry's scheme (`core.urls.link_base`), so the
  scheme-strict link rule (`same_site`) does not drop them. Links are still confined to the
  entry's exact origin: the scheme switch is accepted for redirects only.
- `Location` is resolved with `core.urls.resolve` (RFC 3986, empty segments kept), like links.

## Consequences

- Author pages on the live quotes site are fetched, and the crawl continues from them.
- A downgraded body crosses the network in clear text and is stored under the `https` entry URL;
  the `http` URL is kept as the cache entry's `final_url`. The count makes this visible instead of
  silent. A practice site serves public pages, so integrity rather than secrecy is what is at
  stake, and the parser treats every body as untrusted anyway.
- The hop is checked against the robots.txt rules fetched for the entry's scheme. RFC 9309 scopes
  robots.txt per scheme and authority, so a site with different rules for `http` would be crawled
  under its `https` rules. That is accepted for a same-host switch and noted here.

## Alternatives considered

- **Keep 0009 as written.** Correct on paper, but it fails every author page of a real target.
- **Upgrade the downgrade hop to `https`.** Loops on the target that motivated the change.
  Upgrading and then falling back to `http` would not loop, but it doubles the requests for every
  such page (about 50 more on the full quotes crawl) and ends in clear text anyway.
- **Treat `http` and `https` as one origin for links too.** It would plan both spellings of every
  page and fetch each twice. Rebasing the links of a switched page onto the entry's origin keeps
  one spelling per page.

## Per-site opt-in (amendment, third review)

Following the downgrade was first a global rule, so a new site adapter would have inherited
clear-text hops without anyone deciding it. It is now a per-site capability:
`SiteSpec.allow_scheme_downgrade`, `False` by default, `True` for `quotes` and `quotes-js` only (the
host that was seen to redirect that way). For any other site, `books` included, a same-host `https`
to `http` redirect is refused as off-site, as under 0009: a permanent `client_error` whose reason
ends "https to http downgrade not allowed for this site", and for robots.txt a stopped domain.
An `http` to `https` upgrade on the same host stays allowed for every site. A downgrade is counted
only once its hop is admitted (a token and the breaker), so a hop the domain stopped before sending
is not counted.
