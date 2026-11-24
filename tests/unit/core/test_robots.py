from urllib.robotparser import RobotFileParser

import pytest

from scraping_pipeline.core.robots import (
    MAX_ROBOTS_CHARS,
    RobotsMode,
    RobotsPolicy,
    product_token,
)

UA = "resilient-scraping-pipeline/0.1 (+https://example.test/about)"
SITE = "https://site.test"


def policy(text: str) -> RobotsPolicy:
    return RobotsPolicy.from_response(200, text)


def allowed(text: str, path: str, ua: str = UA) -> bool:
    return policy(text).can_fetch(ua, SITE + path)


# --- status handling (RFC 9309 section 2.3.1) ------------------------------------------------


@pytest.mark.parametrize(
    ("status", "mode", "can"),
    [
        (200, RobotsMode.RULES, True),
        (204, RobotsMode.RULES, True),
        (400, RobotsMode.ALLOW_ALL, True),
        (401, RobotsMode.ALLOW_ALL, True),
        (403, RobotsMode.ALLOW_ALL, True),
        (404, RobotsMode.ALLOW_ALL, True),
        (410, RobotsMode.ALLOW_ALL, True),
        (429, RobotsMode.DISALLOW_ALL, False),  # stricter than the RFC: back off
        (500, RobotsMode.DISALLOW_ALL, False),
        (503, RobotsMode.DISALLOW_ALL, False),
        (599, RobotsMode.DISALLOW_ALL, False),
        (None, RobotsMode.DISALLOW_ALL, False),
        (301, RobotsMode.DISALLOW_ALL, False),  # redirects are the fetcher's job
        (100, RobotsMode.DISALLOW_ALL, False),
        (999, RobotsMode.DISALLOW_ALL, False),
    ],
)
def test_status_handling(status: int | None, mode: RobotsMode, can: bool) -> None:
    result = RobotsPolicy.from_response(status, None)
    assert result.mode is mode
    assert result.can_fetch(UA, SITE + "/anything") is can
    assert result.reason.startswith("robots.txt")
    assert result.crawl_delay(UA) is None


def test_disallow_all_on_5xx_ignores_body() -> None:
    assert not RobotsPolicy.from_response(500, "User-agent: *\nAllow: /").can_fetch(UA, SITE + "/")


def test_empty_2xx_body_allows_all() -> None:
    assert RobotsPolicy.from_response(200, "").can_fetch(UA, SITE + "/x")
    assert RobotsPolicy.from_response(200, None).can_fetch(UA, SITE + "/x")


# --- precedence (section 2.2.2) --------------------------------------------------------------


def test_longest_match_wins_regardless_of_order() -> None:
    for text in (
        "User-agent: *\nDisallow: /a\nAllow: /a/b",
        "User-agent: *\nAllow: /a/b\nDisallow: /a",
    ):
        assert allowed(text, "/a/b/c")
        assert not allowed(text, "/a/x")


def test_more_specific_disallow_beats_allow() -> None:
    text = "User-agent: *\nAllow: /\nDisallow: /private/"
    assert allowed(text, "/public")
    assert not allowed(text, "/private/x")


def test_tie_goes_to_allow() -> None:
    for text in (
        "User-agent: *\nDisallow: /page\nAllow: /page",
        "User-agent: *\nAllow: /page\nDisallow: /page",
    ):
        assert allowed(text, "/page")


def test_no_matching_rule_allows() -> None:
    assert allowed("User-agent: *\nDisallow: /admin", "/catalogue/")


def test_empty_disallow_allows_everything() -> None:
    assert allowed("User-agent: *\nDisallow:", "/x")


def test_disallow_root_blocks_everything_but_robots_txt() -> None:
    text = "User-agent: *\nDisallow: /"
    assert not allowed(text, "/")
    assert not allowed(text, "/x")
    assert allowed(text, "/robots.txt")


def test_query_is_part_of_the_path() -> None:
    text = "User-agent: *\nDisallow: /search?q="
    assert not allowed(text, "/search?q=x")
    assert allowed(text, "/search")


def test_url_without_path_is_root() -> None:
    assert not policy("User-agent: *\nDisallow: /").can_fetch(UA, "https://site.test")


# --- wildcards (section 2.2.3) ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("rule", "path", "can"),
    [
        ("Disallow: /*.php$", "/index.php", False),
        ("Disallow: /*.php$", "/index.php?x=1", True),
        ("Disallow: /*.php$", "/index.php5", True),
        ("Disallow: /*.php", "/a/b.php5", False),
        ("Disallow: /fish*", "/fish.html", False),
        ("Disallow: /fish*", "/Fish", True),  # paths are case-sensitive
        ("Disallow: /*/private/", "/a/b/private/c", False),
        ("Disallow: /exact$", "/exact", False),
        ("Disallow: /exact$", "/exact/", True),
        ("Disallow: /a.b", "/aXb", True),  # '.' is literal, not a regex
        ("Disallow: /a+b", "/aab", True),
    ],
)
def test_wildcards(rule: str, path: str, can: bool) -> None:
    assert allowed(f"User-agent: *\n{rule}", path) is can


def test_longer_wildcard_allow_beats_shorter_disallow() -> None:
    text = "User-agent: *\nDisallow: /*.html\nAllow: /public/*.html"
    assert allowed(text, "/public/a.html")
    assert not allowed(text, "/x/a.html")


def test_non_ascii_pattern_matches_percent_encoded_path() -> None:
    text = "User-agent: *\nDisallow: /foo/bar/ツ"
    assert not allowed(text, "/foo/bar/%E3%83%84")


# --- user agent groups (section 2.2.1) -------------------------------------------------------


def test_exact_product_token_group_is_used_case_insensitively() -> None:
    text = (
        "User-agent: *\nDisallow: /\n\n"
        "User-agent: Resilient-Scraping-Pipeline\nDisallow: /private\n"
    )
    assert allowed(text, "/catalogue/")
    assert not allowed(text, "/private")
    assert not allowed(text, "/catalogue/", ua="otherbot/1.0")


@pytest.mark.parametrize("group", ["resilient-scraping-pipeline-news", "resilient", "pipeline"])
def test_group_name_is_not_matched_by_substring(group: str) -> None:
    assert allowed(f"User-agent: {group}\nDisallow: /\n", "/x")


def test_star_group_is_fallback() -> None:
    assert not allowed("User-agent: otherbot\nAllow: /\n\nUser-agent: *\nDisallow: /", "/x")


def test_no_matching_group_allows() -> None:
    assert allowed("User-agent: otherbot\nDisallow: /", "/x")


def test_multiple_agents_share_one_group() -> None:
    text = "User-agent: a\nUser-agent: resilient-scraping-pipeline\nDisallow: /x\n"
    assert not allowed(text, "/x")
    assert not allowed(text, "/x", ua="a")


def test_groups_for_the_same_agent_are_merged() -> None:
    text = (
        "User-agent: resilient-scraping-pipeline\nDisallow: /a\n\n"
        "User-agent: otherbot\nDisallow: /b\n\n"
        "User-agent: resilient-scraping-pipeline\nDisallow: /c\n"
    )
    assert not allowed(text, "/a")
    assert allowed(text, "/b")
    assert not allowed(text, "/c")


def test_rules_before_any_user_agent_are_ignored() -> None:
    assert allowed("Disallow: /\nUser-agent: *\nDisallow: /private", "/x")


def test_comments_blank_lines_unknown_keys_and_case() -> None:
    text = (
        "# hello\n"
        "USER-AGENT: *   # everyone\n"
        "Sitemap: https://site.test/sitemap.xml\n"
        "no colon line\n"
        "\n"
        "DISALLOW: /private # keep out\n"
        "User-agent:\n"  # empty agent is ignored
    )
    assert not allowed(text, "/private")
    assert allowed(text, "/public")


def test_body_is_truncated_at_500_kib() -> None:
    text = "User-agent: *\n" + "#" * MAX_ROBOTS_CHARS + "\nDisallow: /"
    assert allowed(text, "/x")


@pytest.mark.parametrize(
    ("ua", "token"),
    [
        ("MyBot/1.0 (+https://x.test)", "mybot"),
        ("MyBot", "mybot"),
        ("  Bot Extra", "bot"),
        ("", ""),
        ("/1.0", ""),
    ],
)
def test_product_token(ua: str, token: str) -> None:
    assert product_token(ua) == token


# --- crawl-delay -----------------------------------------------------------------------------


def test_crawl_delay_from_matching_group() -> None:
    text = "User-agent: *\nCrawl-delay: 2.5\nDisallow: /x"
    assert policy(text).crawl_delay(UA) == 2.5


def test_crawl_delay_takes_the_largest_of_merged_groups() -> None:
    text = (
        "User-agent: resilient-scraping-pipeline\nCrawl-delay: 1\n\n"
        "User-agent: resilient-scraping-pipeline\nCrawl-delay: 4\n"
    )
    assert policy(text).crawl_delay(UA) == 4.0


@pytest.mark.parametrize("value", ["soon", "-1", "inf", "nan", ""])
def test_invalid_crawl_delay_is_ignored(value: str) -> None:
    assert policy(f"User-agent: *\nCrawl-delay: {value}\n").crawl_delay(UA) is None


def test_invalid_crawl_delay_keeps_earlier_valid_one() -> None:
    assert policy("User-agent: *\nCrawl-delay: 3\nCrawl-delay: x\n").crawl_delay(UA) == 3.0


def test_crawl_delay_none_without_groups() -> None:
    assert policy("").crawl_delay(UA) is None


# --- why we do not use urllib.robotparser ----------------------------------------------------
# These pin the stdlib behaviour documented in robots.py. If one starts failing after a Python
# upgrade, the stdlib changed: update the module docstring.


def _stdlib(text: str, path: str, ua: str = "bot") -> bool:
    parser = RobotFileParser()
    parser.parse(text.splitlines())
    return parser.can_fetch(ua, SITE + path)


def test_stdlib_uses_first_match_not_longest() -> None:
    text = "User-agent: *\nDisallow: /a\nAllow: /a/b"
    assert _stdlib(text, "/a/b") is False
    assert allowed(text, "/a/b") is True


def test_stdlib_has_no_wildcards() -> None:
    text = "User-agent: *\nDisallow: /*.php$"
    assert _stdlib(text, "/x.php") is True
    assert allowed(text, "/x.php") is False


def test_stdlib_matches_agents_by_substring() -> None:
    text = "User-agent: examplebot\nDisallow: /"
    assert _stdlib(text, "/", ua="examplebot-news/1.0") is False
    assert allowed(text, "/", ua="examplebot-news/1.0") is True
