import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tools.hupu_post import (  # noqa: E402
    NBA_COMPOSER_FORUM_ID,
    _extract_thread_url_from_html,
    _forum_label_matches,
    _render_inline_html,
    _resolve_forum,
)


class TestHupuPostUrlExtraction(unittest.TestCase):
    def test_extract_thread_url_from_json_style_url_field(self):
        html = '<script>window.__DATA__={"url":"/638153415.html"}</script>'
        self.assertEqual(
            _extract_thread_url_from_html(html),
            "https://bbs.hupu.com/638153415.html",
        )

    def test_extract_thread_url_from_anchor(self):
        html = '<a href="/638163441.html">帖子</a>'
        self.assertEqual(
            _extract_thread_url_from_html(html),
            "https://bbs.hupu.com/638163441.html",
        )

    def test_returns_none_when_no_thread_url_present(self):
        html = "<html><body>still on editor</body></html>"
        self.assertIsNone(_extract_thread_url_from_html(html))

    def test_resolve_forum_supports_chinese_alias(self):
        key, forum_id, label = _resolve_forum("湖人专区")
        self.assertEqual(key, "lakers")
        self.assertEqual(forum_id, NBA_COMPOSER_FORUM_ID)
        self.assertEqual(label, "湖人专区")

    def test_render_inline_html_supports_bold_and_links(self):
        rendered = _render_inline_html("看**关键数字**，去[funba](https://funba.app)")
        self.assertIn("<strong>关键数字</strong>", rendered)
        self.assertIn('<a href="https://funba.app" target="_blank">funba</a>', rendered)

    def test_resolve_forum_supports_existing_english_key(self):
        key, forum_id, label = _resolve_forum("thunder")
        self.assertEqual(key, "thunder")
        self.assertEqual(forum_id, NBA_COMPOSER_FORUM_ID)
        self.assertEqual(label, "雷霆专区")

    def test_resolve_forum_supports_dynamic_chinese_team_forum(self):
        key, forum_id, label = _resolve_forum("勇士专区")
        self.assertEqual(key, "勇士专区")
        self.assertEqual(forum_id, NBA_COMPOSER_FORUM_ID)
        self.assertEqual(label, "勇士专区")

    def test_forum_label_matches_treats_basketball_court_as_nba_board(self):
        self.assertTrue(_forum_label_matches("篮球场", "NBA版"))
        self.assertTrue(_forum_label_matches("NBA版", "NBA版"))
        self.assertFalse(_forum_label_matches("湖人专区", "NBA版"))


if __name__ == "__main__":
    unittest.main()
