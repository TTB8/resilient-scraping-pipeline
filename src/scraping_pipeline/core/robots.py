"""robots.txt policy per RFC 9309: what the site allows us to fetch, and how slowly.

Why not ``urllib.robotparser``? On Python 3.12 it disagrees with RFC 9309 in three ways that matter,
each pinned by a test in ``tests/unit/core/test_robots.py``:

* it applies the *first* matching rule in file order. RFC 9309 section 2.2.2 says the *longest*
  (most specific) match wins, and that ``allow`` wins a tie. ``Disallow: /a`` followed by
  ``Allow: /a/b`` must allow ``/a/b``; robotparser refuses it;
* it does not implement the ``*`` and ``$`` wildcards (section 2.2.3), so ``Disallow: /*.php$`` never
  matches anything;
* it matches user agents by substring: a group named ``examplebot`` also captures a crawler called
  ``examplebot-news`` (and a group named ``bot`` captures nearly everyone). The RFC matches the
  product token exactly, case-insensitively (section 2.2.1).

The matcher here is small enough to read in one sitting, which is the better trade for a rule that
decides what we may touch.

Status handling (section 2.3.1), with the two places where we are stricter than the RFC:

* 2xx: parse the body (an empty body allows everything);
* 4xx: "unavailable", so everything is allowed. **Except 429**: we treat "too many requests" for
  robots.txt like a server error and allow nothing, because the site is telling us to back off.
  This policy is only the verdict for one answer: the scheduler retries robots.txt with backoff,
  and if it never answers, stops the domain (its entries become ``stopped``, which a later run can
  reset), rather than recording the URLs as disallowed;
* 5xx or unreachable (``status is None``): disallow everything (section 2.3.1.4);
* anything else (1xx, 3xx, out of range): disallow everything. The fetcher must follow up to five
  redirects itself (section 2.3.1.2); a raw 3xx here means it did not, and not knowing the rules is
  not permission.

Other choices:

* only the first 500 KiB of the body are parsed (the RFC's minimum, which also bounds the work); a
  leading UTF-8 byte order mark is dropped, and ``\n``, ``\r\n`` and ``\r`` all end a line;
* rules are compared against the URL's path plus query, and both sides go through the same
  percent-encoding normalization (:func:`~.urls.normalize_percent_encoding`, section 2.2.2):
  ``%7E`` equals ``~``, ``%c3%a9`` equals ``%C3%A9`` equals ``é``, while an escaped reserved
  character such as ``%2F`` stays distinct from ``/``, and ``*`` / ``$`` stay wildcards;
* a group line is reduced to its product token the same way our own User-Agent is, so a group
  written ``User-agent: MyBot/1.0`` applies to ``MyBot``;
* ``/robots.txt`` itself is always allowed;
* ``Crawl-delay`` is not in RFC 9309 but is widely used. We honour it as a minimum gap between
  requests (the largest value if several groups apply).
"""

import math
import re
from dataclasses import dataclass
from enum import StrEnum
from http import HTTPStatus
from urllib.parse import urlsplit

from scraping_pipeline.core.urls import normalize_percent_encoding

MAX_ROBOTS_CHARS = 500 * 1024


class RobotsMode(StrEnum):
    """How the policy decides: a blanket answer, or the parsed rules."""

    ALLOW_ALL = "allow_all"
    DISALLOW_ALL = "disallow_all"
    RULES = "rules"


@dataclass(frozen=True, slots=True)
class RobotsRule:
    """One ``allow`` / ``disallow`` line. ``pattern`` is percent-normalized, wildcards intact."""

    allow: bool
    pattern: str
    regex: re.Pattern[str]


@dataclass(frozen=True, slots=True)
class RobotsGroup:
    """One group: the user agents it names (lower-cased) and its rules, in file order."""

    agents: tuple[str, ...]
    rules: tuple[RobotsRule, ...]
    crawl_delay: float | None


@dataclass(frozen=True, slots=True)
class RobotsPolicy:
    """A site's robots.txt, ready to answer :meth:`can_fetch` and :meth:`crawl_delay`.

    ``reason`` says where the policy came from, for the run report.
    """

    mode: RobotsMode
    groups: tuple[RobotsGroup, ...]
    reason: str

    @classmethod
    def from_response(cls, status: int | None, body: str | None) -> "RobotsPolicy":
        """Build the policy from the robots.txt fetch result (see the module docstring)."""
        if status is None:
            return cls(RobotsMode.DISALLOW_ALL, (), "robots.txt unreachable: disallow all")
        family = status // 100
        if family == 2:  # noqa: PLR2004 - HTTP status families
            return cls.parse(body or "", reason=f"robots.txt {status}: parsed")
        if status == HTTPStatus.TOO_MANY_REQUESTS:
            return cls(
                RobotsMode.DISALLOW_ALL, (), "robots.txt 429: too many requests, disallow all"
            )
        if family == 4:  # noqa: PLR2004
            return cls(RobotsMode.ALLOW_ALL, (), f"robots.txt {status}: unavailable, allow all")
        if family == 5:  # noqa: PLR2004
            return cls(
                RobotsMode.DISALLOW_ALL, (), f"robots.txt {status}: server error, disallow all"
            )
        return cls(
            RobotsMode.DISALLOW_ALL, (), f"robots.txt {status}: unexpected status, disallow all"
        )

    @classmethod
    def parse(cls, text: str, reason: str = "parsed") -> "RobotsPolicy":
        """Parse robots.txt text into groups (RFC 9309 section 2.1).

        A leading UTF-8 byte order mark is dropped: left in, it glues itself to the first key
        (``\ufeffuser-agent``), the first group is lost and its rules silently stop applying.
        """
        text = text.removeprefix("\ufeff")
        return cls(RobotsMode.RULES, _parse_groups(text[:MAX_ROBOTS_CHARS]), reason)

    def can_fetch(self, user_agent: str, url: str) -> bool:
        """May ``user_agent`` fetch ``url``? ``url`` is absolute; only its path and query are used."""
        if self.mode is RobotsMode.ALLOW_ALL:
            return True
        if self.mode is RobotsMode.DISALLOW_ALL:
            return False
        parts = urlsplit(url)
        path = parts.path or "/"
        if path == "/robots.txt":
            return True
        target = normalize_percent_encoding(path + (f"?{parts.query}" if parts.query else ""))
        best: RobotsRule | None = None
        for group in self._groups_for(user_agent):
            for rule in group.rules:
                if rule.regex.match(target) and (
                    best is None
                    or len(rule.pattern) > len(best.pattern)
                    or (len(rule.pattern) == len(best.pattern) and rule.allow and not best.allow)
                ):
                    best = rule
        return best is None or best.allow

    def crawl_delay(self, user_agent: str) -> float | None:
        """The ``Crawl-delay`` (seconds) that applies to ``user_agent``, or ``None``."""
        delays = [g.crawl_delay for g in self._groups_for(user_agent) if g.crawl_delay is not None]
        return max(delays) if delays else None

    def _groups_for(self, user_agent: str) -> list[RobotsGroup]:
        token = product_token(user_agent)
        exact = [g for g in self.groups if token in g.agents]
        return exact or [g for g in self.groups if "*" in g.agents]


def product_token(user_agent: str) -> str:
    """The product token of a User-Agent string: ``"MyBot/1.0 (+url)"`` -> ``"mybot"``."""
    head = user_agent.strip().split("/", 1)[0].split()
    return head[0].casefold() if head else ""


def _parse_groups(text: str) -> tuple[RobotsGroup, ...]:
    groups: list[RobotsGroup] = []
    agents: list[str] = []
    rules: list[RobotsRule] = []
    delay: float | None = None
    in_rules = False

    def flush() -> None:
        if agents:
            groups.append(RobotsGroup(tuple(agents), tuple(rules), delay))

    for raw in text.splitlines():
        key, sep, value = raw.split("#", 1)[0].partition(":")
        if not sep:
            continue
        key, value = key.strip().casefold(), value.strip()
        if key == "user-agent":
            if in_rules:  # a user-agent after rules starts a new group
                flush()
                agents, rules, delay, in_rules = [], [], None, False
            token = product_token(value)  # "MyBot/1.0" in a group line names the token "mybot"
            if token:
                agents.append(token)
        elif key in ("allow", "disallow"):
            in_rules = True
            if value:  # an empty value is "no rule"
                rules.append(_rule(key == "allow", value))
        elif key == "crawl-delay":
            in_rules = True
            delay = _parse_delay(value, delay)
    flush()
    return tuple(groups)


def _rule(allow: bool, value: str) -> RobotsRule:
    pattern = normalize_percent_encoding(value)
    anchored = pattern.endswith("$")
    body = pattern[:-1] if anchored else pattern
    regex = ".*".join(re.escape(piece) for piece in body.split("*"))
    return RobotsRule(allow, pattern, re.compile(regex + (r"\Z" if anchored else "")))


def _parse_delay(value: str, current: float | None) -> float | None:
    try:
        seconds = float(value)
    except ValueError:
        return current
    if not math.isfinite(seconds) or seconds < 0:
        return current
    return seconds
