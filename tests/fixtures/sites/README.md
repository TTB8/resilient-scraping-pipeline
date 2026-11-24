# Saved pages from the scraping practice sites

Captured once, on 2026-11-24, from `books.toscrape.com` and `quotes.toscrape.com`, sites built for
people to practise scraping on. Both answer `404` for `/robots.txt`, which RFC 9309 treats as
"everything allowed". The capture sent one request every two seconds with the project's honest
User-Agent (`resilient-scraping-pipeline/0.1 (+https://github.com/TTB8/resilient-scraping-pipeline)`).

The folder layout mirrors each site's URL paths (`/page/2/` → `page/2/index.html`), so the local
fixture server (`tests/fixture_server.py`) serves them at their real paths. Tests use these files and
that server only. They never touch the network.

Do not edit these files to make a test pass. A parser that breaks here would break on the live site.
Synthetic pages for failure modes (rate limits, block pages, challenge pages) belong to the fixture
server, not in this folder.
