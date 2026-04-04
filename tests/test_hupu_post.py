import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.hupu.post import (  # noqa: E402
    NBA_COMPOSER_FORUM_ID,
    _capture_page_error,
    _click_submit,
    _extract_thread_url_from_html,
    _extract_thread_url_from_response_body,
    _forum_label_matches,
    _is_logged_in,
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

    def test_extract_thread_url_from_absolute_url(self):
        html = '<meta property="og:url" content="https://bbs.hupu.com/638233167.html">'
        self.assertEqual(
            _extract_thread_url_from_html(html),
            "https://bbs.hupu.com/638233167.html",
        )

    def test_extract_thread_url_from_response_body_tid(self):
        payload = '{"data":{"tid":"638233167"}}'
        self.assertEqual(
            _extract_thread_url_from_response_body(payload),
            "https://bbs.hupu.com/638233167.html",
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


class _FakeResponse:
    def __init__(self, url: str, body: str, *, status: int | None = None):
        self.url = url
        self._body = body
        self.status = status

    def text(self):
        return self._body


class _FakeLocator:
    def __init__(self, text: str):
        self._text = text

    def inner_text(self, timeout=None):
        return self._text


class _FakeContext:
    def __init__(self, cookies=None):
        self._cookies = list(cookies or [])

    def cookies(self):
        return list(self._cookies)


class _FakePage:
    def __init__(self, *, url: str, body_text: str = "", responses=None, cookies=None):
        self.url = url
        self._body_text = body_text
        self._responses = list(responses or [])
        self._listeners = {}
        self.clicked = []
        self.context = _FakeContext(cookies)

    def on(self, event, handler):
        self._listeners.setdefault(event, []).append(handler)

    def off(self, event, handler):
        self._listeners[event].remove(handler)

    def click(self, selector):
        self.clicked.append(selector)
        for response in self._responses:
            for handler in list(self._listeners.get("response", [])):
                handler(response)

    def locator(self, selector):
        self.assert_selector(selector)
        return _FakeLocator(self._body_text)

    def assert_selector(self, selector):
        if selector != "body":
            raise AssertionError(f"unexpected selector: {selector}")


class TestHupuSubmitFlow(unittest.TestCase):
    @patch("social_media.hupu.post.time.sleep", return_value=None)
    @patch("social_media.hupu.post._wait_for_final_post_url")
    def test_click_submit_prefers_network_captured_url(self, wait_mock, _sleep_mock):
        page = _FakePage(
            url="https://bbs.hupu.com/newpost/179",
            responses=[
                _FakeResponse(
                    "https://bbs.hupu.com/api/post/submit",
                    '{"data":{"tid":"638233167"}}',
                )
            ],
        )

        result = _click_submit(page)

        self.assertEqual(result, "https://bbs.hupu.com/638233167.html")
        self.assertEqual(page.clicked, [".submitVideo"])
        wait_mock.assert_not_called()

    @patch("social_media.hupu.post.time.sleep", return_value=None)
    @patch(
        "social_media.hupu.post._wait_for_final_post_url",
        return_value="https://bbs.hupu.com/638233167.html",
    )
    def test_click_submit_falls_back_to_html_poll(self, wait_mock, _sleep_mock):
        page = _FakePage(url="https://bbs.hupu.com/newpost/179")

        result = _click_submit(page)

        self.assertEqual(result, "https://bbs.hupu.com/638233167.html")
        wait_mock.assert_called_once_with(page)

    @patch("social_media.hupu.post.time.sleep", return_value=None)
    @patch("social_media.hupu.post._wait_for_final_post_url", return_value=None)
    def test_click_submit_raises_when_still_on_compose_page(self, wait_mock, _sleep_mock):
        page = _FakePage(url="https://bbs.hupu.com/newpost/179")

        with self.assertRaisesRegex(RuntimeError, "thread URL was not detected"):
            _click_submit(page)

        wait_mock.assert_called_once_with(page)


class TestHupuLoginState(unittest.TestCase):
    def test_is_logged_in_requires_more_than_auth_cookie_names(self):
        page = _FakePage(
            url="https://bbs.hupu.com/",
            body_text="欢迎访问虎扑，请先 注册 或者 登录\n登录后的世界更精彩",
            cookies=[{"name": "u", "value": "abc"}],
        )

        self.assertFalse(_is_logged_in(page))

    def test_is_logged_in_accepts_logged_in_ui_plus_auth_cookie(self):
        page = _FakePage(
            url="https://bbs.hupu.com/",
            body_text="你好，智趣NBA\n我的首页\n创作者中心\n退出",
            cookies=[{"name": "u", "value": "abc"}, {"name": "us", "value": "def"}],
        )

        self.assertTrue(_is_logged_in(page))


class TestHupuScreenshotGuard(unittest.TestCase):
    def test_capture_page_error_detects_http_500(self):
        page = _FakePage(url="https://funba.app/players/1642843", body_text="Something Went Wrong")
        response = _FakeResponse("https://funba.app/players/1642843", "", status=500)

        self.assertEqual(_capture_page_error(page, response), "Screenshot target returned HTTP 500")

    def test_capture_page_error_detects_rendered_error_page(self):
        page = _FakePage(
            url="https://funba.app/players/1642843",
            body_text="500\nSomething Went Wrong\nAn unexpected error occurred.\nBack to Home",
        )

        self.assertEqual(_capture_page_error(page), "Screenshot target rendered a server error page")


if __name__ == "__main__":
    unittest.main()
