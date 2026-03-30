import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.hupu.post import (  # noqa: E402
    NBA_COMPOSER_FORUM_ID,
    _extract_thread_url_from_html,
    _forum_label_matches,
    _parse_image_placeholder,
    _prepare_placeholder_images,
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
        self.assertEqual(key, "湖人专区")
        self.assertEqual(forum_id, NBA_COMPOSER_FORUM_ID)
        self.assertEqual(label, "湖人专区")

    def test_render_inline_html_supports_bold_and_links(self):
        rendered = _render_inline_html("看**关键数字**，去[funba](https://funba.app)")
        self.assertIn("<strong>关键数字</strong>", rendered)
        self.assertIn('<a href="https://funba.app" target="_blank">funba</a>', rendered)

    def test_render_inline_html_linkifies_bare_urls(self):
        rendered = _render_inline_html("详见 https://funba.app/metrics/scoring_consistency")
        self.assertIn(
            '<a href="https://funba.app/metrics/scoring_consistency" target="_blank">https://funba.app/metrics/scoring_consistency</a>',
            rendered,
        )

    def test_resolve_forum_supports_existing_english_key(self):
        key, forum_id, label = _resolve_forum("thunder")
        self.assertEqual(key, "雷霆专区")
        self.assertEqual(forum_id, NBA_COMPOSER_FORUM_ID)
        self.assertEqual(label, "雷霆专区")

    def test_resolve_forum_supports_dynamic_chinese_team_forum(self):
        key, forum_id, label = _resolve_forum("勇士专区")
        self.assertEqual(key, "勇士专区")
        self.assertEqual(forum_id, NBA_COMPOSER_FORUM_ID)
        self.assertEqual(label, "勇士专区")

    def test_resolve_forum_supports_english_team_key_alias(self):
        key, forum_id, label = _resolve_forum("76ers")
        self.assertEqual(key, "76人专区")
        self.assertEqual(forum_id, NBA_COMPOSER_FORUM_ID)
        self.assertEqual(label, "76人专区")

    def test_forum_label_matches_uses_exact_label_for_general_board(self):
        self.assertTrue(_forum_label_matches("湿乎乎的话题", "湿乎乎的话题"))
        self.assertFalse(_forum_label_matches("篮球场", "湿乎乎的话题"))
        self.assertFalse(_forum_label_matches("湖人专区", "湿乎乎的话题"))

    def test_parse_image_placeholder_extracts_target(self):
        parsed = _parse_image_placeholder(
            "[[IMAGE: type=game_boxscore; target=https://funba.app/games/0022501077; note=PHI vs CHA 比赛数据]]"
        )
        self.assertEqual(parsed["type"], "game_boxscore")
        self.assertEqual(parsed["target"], "https://funba.app/games/0022501077")

    @patch("social_media.hupu.post._capture_compact_screenshot")
    def test_prepare_placeholder_images_autogenerates_missing_images(self, capture_mock):
        content = "[[IMAGE: type=game_boxscore; target=https://funba.app/games/0022501077; note=test]]"
        resolved, temp_paths = _prepare_placeholder_images(content, [])
        self.assertEqual(len(resolved), 1)
        self.assertEqual(len(temp_paths), 1)
        capture_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
