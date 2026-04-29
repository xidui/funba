from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.twitter.post import (  # noqa: E402
    TWITTER_MAX_IMAGES,
    _cookie_for_playwright,
    _create_context,
    _estimated_tweet_length,
    _extract_status_url_from_text,
    _extract_status_urls_from_page_state,
    _normalize_status_url,
    _resolve_image_paths,
)


class _FakePage:
    def __init__(self, *, url: str, eval_result=None):
        self.url = url
        self._eval_result = eval_result

    def evaluate(self, _script):
        return self._eval_result


class _FakeBrowser:
    def __init__(self):
        self.context_kwargs = None
        self.cookies = None

    def new_context(self, **kwargs):
        self.context_kwargs = kwargs
        return self

    def add_cookies(self, cookies):
        self.cookies = cookies


class _FakeChromium:
    def __init__(self):
        self.headless = None
        self.browser = _FakeBrowser()

    def launch(self, *, headless):
        self.headless = headless
        return self.browser


class _FakePlaywright:
    def __init__(self):
        self.chromium = _FakeChromium()


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

    def test_create_context_defaults_to_headless(self):
        pw = _FakePlaywright()
        with patch("social_media.twitter.post.load_cookies", return_value=[]), patch.dict(
            "os.environ", {}, clear=True
        ):
            _create_context(pw)
        self.assertTrue(pw.chromium.headless)

    def test_create_context_allows_headed_env_override(self):
        pw = _FakePlaywright()
        with patch("social_media.twitter.post.load_cookies", return_value=[]), patch.dict(
            "os.environ", {"FUNBA_TWITTER_HEADLESS": "0"}, clear=True
        ):
            _create_context(pw)
        self.assertFalse(pw.chromium.headless)

    def test_create_context_explicit_headless_overrides_env(self):
        pw = _FakePlaywright()
        with patch("social_media.twitter.post.load_cookies", return_value=[]), patch.dict(
            "os.environ", {"FUNBA_TWITTER_HEADLESS": "0"}, clear=True
        ):
            _create_context(pw, headless=True)
        self.assertTrue(pw.chromium.headless)


class TestResolveImagePaths(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

    def _write(self, name: str, payload: bytes = b"png-bytes") -> Path:
        path = Path(self.tmpdir.name) / name
        path.write_bytes(payload)
        return path

    def test_returns_empty_for_no_images(self):
        self.assertEqual(_resolve_image_paths(None), [])
        self.assertEqual(_resolve_image_paths([]), [])

    def test_deduplicates_resolved_paths(self):
        path = self._write("hero.png")
        resolved = _resolve_image_paths([str(path), str(path)])
        self.assertEqual([p.name for p in resolved], ["hero.png"])

    def test_rejects_missing_file(self):
        missing = Path(self.tmpdir.name) / "ghost.png"
        with self.assertRaises(FileNotFoundError):
            _resolve_image_paths([str(missing)])

    def test_rejects_empty_file(self):
        empty = self._write("empty.png", payload=b"")
        with self.assertRaises(ValueError):
            _resolve_image_paths([str(empty)])

    def test_rejects_too_many_images(self):
        paths = [str(self._write(f"hero{i}.png")) for i in range(TWITTER_MAX_IMAGES + 1)]
        with self.assertRaises(ValueError):
            _resolve_image_paths(paths)


if __name__ == "__main__":
    unittest.main()
