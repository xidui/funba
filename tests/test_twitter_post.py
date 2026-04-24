from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.twitter.post import (  # noqa: E402
    _cookie_for_playwright,
    _estimated_tweet_length,
    _extract_status_url_from_text,
    _extract_status_urls_from_page_state,
    _normalize_status_url,
)


class _FakePage:
    def __init__(self, *, url: str, eval_result=None):
        self.url = url
        self._eval_result = eval_result

    def evaluate(self, _script):
        return self._eval_result


class TestTwitterPostHelpers(unittest.TestCase):
    def test_estimated_tweet_length_counts_urls_as_tco_length(self):
        text = "Source: https://funba.app/metrics/max_scoring_run?season=all_4"
        self.assertEqual(_estimated_tweet_length(text), len("Source: ") + 23)

    def test_cookie_for_playwright_prefers_domain_path_over_url(self):
        cookie = _cookie_for_playwright(
            {
                "name": "auth_token",
                "value": "abc",
                "domain": ".x.com",
                "path": "/",
                "secure": True,
                "url": "https://x.com",
                "expires": 123,
            }
        )
        self.assertEqual(cookie["domain"], ".x.com")
        self.assertEqual(cookie["path"], "/")
        self.assertNotIn("url", cookie)
        self.assertEqual(cookie["expires"], 123)

    def test_normalize_status_url_accepts_x_status_url(self):
        self.assertEqual(
            _normalize_status_url("https://x.com/funba_app/status/1915000000000000000?s=20"),
            "https://x.com/funba_app/status/1915000000000000000",
        )

    def test_normalize_status_url_rewrites_twitter_domain(self):
        self.assertEqual(
            _normalize_status_url("https://twitter.com/funba_app/status/1915000000000000000"),
            "https://x.com/funba_app/status/1915000000000000000",
        )

    def test_extract_status_url_from_text_reads_relative_status_path(self):
        self.assertEqual(
            _extract_status_url_from_text('next: "/funba_app/status/1915000000000000000"'),
            "https://x.com/funba_app/status/1915000000000000000",
        )

    def test_extract_status_urls_from_page_state_reads_anchors(self):
        page = _FakePage(
            url="https://x.com/compose/post",
            eval_result={
                "historyState": "",
                "title": "",
                "anchors": ["https://x.com/funba_app/status/1915000000000000000"],
            },
        )
        self.assertEqual(
            _extract_status_urls_from_page_state(page),
            {"https://x.com/funba_app/status/1915000000000000000"},
        )


if __name__ == "__main__":
    unittest.main()
