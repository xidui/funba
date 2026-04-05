import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.xiaohongshu.post import (  # noqa: E402
    _action_error_from_text,
    _body_text_for_xiaohongshu,
    _content_lines_for_editor,
    _estimated_body_length_for_xiaohongshu,
    _extract_tags_from_content,
    _extract_note_url_from_text,
    _normalize_note_url,
    _parse_image_placeholder,
    _parse_tags_placeholder,
    _render_plain_text_line,
)


class TestXiaohongshuPostHelpers(unittest.TestCase):
    def test_parse_image_placeholder_extracts_slot(self):
        parsed = _parse_image_placeholder("[[IMAGE:slot=img1; type=screenshot; note=test]]")
        self.assertEqual(parsed["slot"], "img1")
        self.assertEqual(parsed["type"], "screenshot")

    def test_render_plain_text_line_strips_markdown_bold_and_links(self):
        rendered = _render_plain_text_line("看**关键数字**，详见[funba](https://funba.app)")
        self.assertEqual(rendered, "看关键数字，详见funba https://funba.app")

    def test_content_lines_for_editor_removes_image_placeholders(self):
        lines = _content_lines_for_editor("第一段\n[[IMAGE:slot=img1]]\n第二段")
        self.assertEqual(lines, ["第一段", "第二段"])

    def test_parse_tags_placeholder_extracts_hashtags(self):
        tags = _parse_tags_placeholder("[[TAGS:#NBA #活塞 #比赛复盘]]")
        self.assertEqual(tags, ["#NBA", "#活塞", "#比赛复盘"])

    def test_extract_tags_from_content_dedupes_and_preserves_order(self):
        tags = _extract_tags_from_content("正文\n[[TAGS:#NBA #活塞 #NBA #助攻]]")
        self.assertEqual(tags, ["#NBA", "#活塞", "#助攻"])

    def test_content_lines_for_editor_appends_tags_as_final_line(self):
        lines = _content_lines_for_editor("第一段\n[[IMAGE:slot=img1]]\n第二段\n[[TAGS:#NBA #活塞]]")
        self.assertEqual(lines, ["第一段", "第二段"])

    def test_body_text_for_xiaohongshu_strips_placeholders_and_keeps_tags(self):
        body = _body_text_for_xiaohongshu(
            "第一段\n[[IMAGE:slot=img1]]\n第二段\n[[TAGS:#NBA #活塞 #比赛复盘]]"
        )
        self.assertEqual(body, "第一段\n第二段")

    def test_estimated_body_length_includes_native_topic_text(self):
        content = "第一段\n第二段\n[[TAGS:#NBA #活塞]]"
        estimated = _estimated_body_length_for_xiaohongshu(content)
        self.assertEqual(estimated, len("第一段\n第二段") + 1 + len("#NBA #活塞"))

    def test_normalize_note_url_accepts_absolute_url(self):
        self.assertEqual(
            _normalize_note_url("https://www.xiaohongshu.com/explore/67f0abc1234def567890abcd"),
            "https://www.xiaohongshu.com/explore/67f0abc1234def567890abcd",
        )

    def test_extract_note_url_from_text_reads_json_note_id(self):
        payload = '{"data":{"noteId":"67f0abc1234def567890abcd"}}'
        self.assertEqual(
            _extract_note_url_from_text(payload),
            "https://www.xiaohongshu.com/explore/67f0abc1234def567890abcd",
        )

    def test_extract_note_url_from_text_reads_escaped_share_url(self):
        payload = '{"shareUrl":"https:\\/\\/www.xiaohongshu.com\\/discovery\\/item\\/67f0abc1234def567890abcd"}'
        self.assertEqual(
            _extract_note_url_from_text(payload),
            "https://www.xiaohongshu.com/discovery/item/67f0abc1234def567890abcd",
        )

    def test_action_error_from_text_detects_blocking_marker(self):
        self.assertEqual(_action_error_from_text("发布失败，请上传图片后重试"), "请上传图片")


if __name__ == "__main__":
    unittest.main()
