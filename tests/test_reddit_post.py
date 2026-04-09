from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.reddit.post import (  # noqa: E402
    _build_submit_url,
    _cookie_for_playwright,
    _extract_post_url_from_page_state,
    _extract_post_url_from_text,
    _normalize_post_url,
    _normalize_subreddit,
)


class _FakeLocator:
    def __init__(self, text: str):
        self._text = text

    def inner_text(self, timeout=None):
        return self._text


class _FakePage:
    def __init__(self, *, url: str, body_text: str = "", html: str = "", eval_result=None):
        self.url = url
        self._body_text = body_text
        self._html = html
        self._eval_result = eval_result

    def locator(self, selector):
        if selector != "body":
            raise AssertionError(f"unexpected selector: {selector}")
        return _FakeLocator(self._body_text)

    def content(self):
        return self._html

    def evaluate(self, _script):
        return self._eval_result


class TestRedditPostHelpers(unittest.TestCase):
    def test_normalize_subreddit_strips_r_prefix(self):
        self.assertEqual(_normalize_subreddit("r/nba"), "nba")
        self.assertEqual(_normalize_subreddit("/r/warriors/"), "warriors")

    def test_cookie_for_playwright_prefers_domain_path_over_url(self):
        cookie = _cookie_for_playwright(
            {
                "name": "csrf_token",
                "value": "abc",
                "domain": ".reddit.com",
                "path": "/",
                "secure": True,
                "url": "https://reddit.com",
                "expires": 123,
            }
        )
        self.assertEqual(cookie["domain"], ".reddit.com")
        self.assertEqual(cookie["path"], "/")
        self.assertNotIn("url", cookie)
        self.assertEqual(cookie["expires"], 123)

    def test_build_submit_url_prefills_submit_params(self):
        url = _build_submit_url(subreddit="nba", title="Box Score", content="Line 1\nLine 2")
        self.assertIn("https://www.reddit.com/submit?", url)
        self.assertIn("sr=nba", url)
        self.assertIn("title=Box+Score", url)
        self.assertIn("text=Line+1%0ALine+2", url)

    def test_normalize_post_url_accepts_absolute_comments_url(self):
        self.assertEqual(
            _normalize_post_url("https://www.reddit.com/r/nba/comments/abc123/game_thread_title/"),
            "https://www.reddit.com/r/nba/comments/abc123/game_thread_title",
        )

    def test_extract_post_url_from_text_reads_relative_comments_path(self):
        self.assertEqual(
            _extract_post_url_from_text('next: "/r/nba/comments/abc123/game_thread_title/"'),
            "https://www.reddit.com/r/nba/comments/abc123/game_thread_title",
        )

    def test_extract_post_url_from_page_state_reads_history_state(self):
        page = _FakePage(
            url="https://www.reddit.com/submit?sr=nba",
            eval_result={
                "historyState": '{"url":"https://www.reddit.com/r/nba/comments/abc123/game_thread_title/"}',
                "title": "",
                "anchors": [],
                "resources": [],
            },
        )
        self.assertEqual(
            _extract_post_url_from_page_state(page),
            "https://www.reddit.com/r/nba/comments/abc123/game_thread_title",
        )

    def test_extract_post_url_from_page_state_ignores_resource_links(self):
        page = _FakePage(
            url="https://www.reddit.com/submit?sr=rockets",
            eval_result={
                "historyState": "",
                "title": "",
                "anchors": ["https://www.reddit.com/r/PetPost/comments/1sewyo0/"],
                "resources": ["https://www.reddit.com/r/PetPost/comments/1sewyo0/"],
            },
        )
        self.assertIsNone(_extract_post_url_from_page_state(page))


if __name__ == "__main__":
    unittest.main()
