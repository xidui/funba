from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.instagram.post import (  # noqa: E402
    INSTAGRAM_MAX_IMAGES,
    _caption_text_for_instagram,
    _extract_post_url_from_page_state,
    _extract_post_url_from_text,
    _extract_tags_from_content,
    _normalize_post_url,
    _parse_image_placeholder,
    _parse_tags_placeholder,
    _post_url_belongs_to_username,
    _paths_by_priority,
    _render_plain_text_line,
    _resolve_image_paths,
    _wait_for_post_result,
)
from social_media.instagram.hero_highlight import render_hero_highlight  # noqa: E402


class _FakePage:
    def __init__(self, *, url: str, eval_result=None):
        self.url = url
        self._eval_result = eval_result

    def evaluate(self, _script):
        return self._eval_result


class TestInstagramPostHelpers(unittest.TestCase):
    def test_parse_image_placeholder_extracts_slot(self):
        parsed = _parse_image_placeholder("[[IMAGE:slot=poster_ig; type=poster]]")
        self.assertEqual(parsed["slot"], "poster_ig")
        self.assertEqual(parsed["type"], "poster")

    def test_parse_tags_placeholder_extracts_hashtags(self):
        self.assertEqual(_parse_tags_placeholder("[[TAGS:#NBA #Funba #NBA]]"), ["#NBA", "#Funba"])

    def test_extract_tags_from_content_dedupes_and_preserves_order(self):
        tags = _extract_tags_from_content("正文\n[[TAGS:#NBA #活塞 #NBA #助攻]]")
        self.assertEqual(tags, ["#NBA", "#活塞", "#助攻"])

    def test_render_plain_text_line_strips_markdown(self):
        rendered = _render_plain_text_line("See **data** at [Funba](https://funba.app)")
        self.assertEqual(rendered, "See data at Funba https://funba.app")

    def test_caption_text_removes_image_placeholders_and_appends_tags(self):
        caption = _caption_text_for_instagram(
            "First line\n[[IMAGE:slot=poster_ig]]\nSecond line\n[[TAGS:#NBA #Funba]]"
        )
        self.assertEqual(caption, "First line\nSecond line\n\n#NBA #Funba")

    def test_paths_by_priority_prefers_square_instagram_poster(self):
        paths = _paths_by_priority(
            [
                ("poster", "/tmp/poster.png"),
                ("img1", "/tmp/img1.png"),
                ("poster_ig", "/tmp/poster_ig.png"),
            ]
        )
        self.assertEqual(paths, ["/tmp/poster_ig.png", "/tmp/poster.png", "/tmp/img1.png"])

    def test_normalize_post_url_accepts_absolute_url(self):
        self.assertEqual(
            _normalize_post_url("https://www.instagram.com/p/C6abc_DEF12/?img_index=1"),
            "https://www.instagram.com/p/C6abc_DEF12",
        )

    def test_normalize_post_url_accepts_profile_scoped_url(self):
        self.assertEqual(
            _normalize_post_url("https://www.instagram.com/xidui64/p/DXvMVjUG0K3/?img_index=1"),
            "https://www.instagram.com/xidui64/p/DXvMVjUG0K3",
        )

    def test_post_url_belongs_to_username_requires_profile_scoped_url(self):
        self.assertTrue(_post_url_belongs_to_username("https://www.instagram.com/xidui64/p/DXvMVjUG0K3/", "xidui64"))
        self.assertFalse(_post_url_belongs_to_username("https://www.instagram.com/p/DXmJDJkEXk2/", "xidui64"))

    def test_extract_post_url_from_text_reads_relative_path(self):
        self.assertEqual(
            _extract_post_url_from_text('next: "/p/C6abc_DEF12/"'),
            "https://www.instagram.com/p/C6abc_DEF12",
        )

    def test_extract_post_url_from_page_state_reads_anchors(self):
        page = _FakePage(
            url="https://www.instagram.com/",
            eval_result={
                "historyState": "",
                "title": "",
                "anchors": ["https://www.instagram.com/p/C6abc_DEF12/"],
            },
        )
        self.assertEqual(
            _extract_post_url_from_page_state(page),
            "https://www.instagram.com/p/C6abc_DEF12",
        )

    def test_extract_post_url_from_page_state_filters_feed_anchors_by_username(self):
        page = _FakePage(
            url="https://www.instagram.com/",
            eval_result={
                "historyState": "",
                "title": "",
                "anchors": [
                    "https://www.instagram.com/p/DXmJDJkEXk2/",
                    "https://www.instagram.com/xidui64/p/DXvMVjUG0K3/",
                ],
            },
        )
        self.assertEqual(
            _extract_post_url_from_page_state(page, username="xidui64"),
            "https://www.instagram.com/xidui64/p/DXvMVjUG0K3",
        )

    def test_wait_for_post_result_accepts_changed_latest_profile_post_after_timeout(self):
        previous = "https://www.instagram.com/xidui64/p/DXvMVjUG0K3"
        current = "https://www.instagram.com/xidui64/p/DXvNs6Km4Rv"
        with patch("social_media.instagram.post._latest_profile_post_url", return_value=current):
            self.assertEqual(
                _wait_for_post_result(
                    _FakePage(url="https://www.instagram.com/"),
                    username="xidui64",
                    timeout_seconds=0,
                    previous_latest_url=previous,
                ),
                current,
            )

    def test_wait_for_post_result_rejects_unchanged_latest_profile_post_after_timeout(self):
        previous = "https://www.instagram.com/xidui64/p/DXvMVjUG0K3"
        with patch("social_media.instagram.post._latest_profile_post_url", return_value=previous):
            with self.assertRaisesRegex(RuntimeError, "may not have completed"):
                _wait_for_post_result(
                    _FakePage(url="https://www.instagram.com/"),
                    username="xidui64",
                    timeout_seconds=0,
                    previous_latest_url=previous,
                )

    def test_hero_highlight_renderer_uses_square_poster_slot(self):
        class Card:
            metric_name = "Max Scoring Run"
            value_text = "DET 19-0"
            value_time_label = None
            metric_url = "https://funba.app/metrics/max_scoring_run"
            game_url = "https://funba.app/games/20260422-orl-det"
            matchup = "ORL @ DET"
            entity_label = "Detroit"

        rendered = render_hero_highlight(Card())
        self.assertIn("[[IMAGE:slot=poster_ig]]", rendered)
        self.assertIn("#NBA", rendered)
        self.assertIn("#Detroit", rendered)
        self.assertIn("#funba", rendered)


class TestResolveInstagramImages(unittest.TestCase):
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
        self.assertEqual([item.name for item in resolved], ["hero.png"])

    def test_rejects_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            _resolve_image_paths([str(Path(self.tmpdir.name) / "missing.png")])

    def test_rejects_empty_file(self):
        empty = self._write("empty.png", payload=b"")
        with self.assertRaises(ValueError):
            _resolve_image_paths([str(empty)])

    def test_rejects_too_many_images(self):
        paths = [str(self._write(f"hero{i}.png")) for i in range(INSTAGRAM_MAX_IMAGES + 1)]
        with self.assertRaises(ValueError):
            _resolve_image_paths(paths)


if __name__ == "__main__":
    unittest.main()
