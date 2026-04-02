import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media import images  # noqa: E402


class TestSocialImages(unittest.TestCase):
    def test_resolve_image_supports_player_headshot(self):
        out_dir = Path("/tmp/funba_social_images")
        with patch("social_media.images.ensure_post_media_dir", return_value=out_dir), \
             patch("social_media.images._download_official_player_headshot") as mock_download:
            paths = images.resolve_image(
                {"type": "player_headshot", "player_id": "1629029", "player_name": "Luka Doncic"},
                post_id=42,
                slot="img1",
            )

        self.assertEqual(paths, [str(out_dir / "img1.png")])
        mock_download.assert_called_once_with("1629029", str(out_dir / "img1.png"), player_name="Luka Doncic")

    def test_web_search_query_appends_nba_and_excludes_watermark_sites(self):
        query = images._web_search_query("Luka Doncic celebration")

        self.assertIn("Luka Doncic celebration NBA", query)
        self.assertIn("-site:gettyimages.com", query)
        self.assertIn("-site:alamy.com", query)

    def test_is_good_search_result_rejects_watermarked_domain(self):
        result = {
            "image": "https://media.gettyimages.com/photos/luka-doncic.jpg",
            "url": "https://www.gettyimages.com/detail/news-photo/luka-doncic-news-photo/123",
            "title": "Luka Doncic News Photo",
        }

        self.assertFalse(images._is_good_search_result(result))

    def test_is_good_search_result_rejects_watermark_terms(self):
        result = {
            "image": "https://cdn.example.com/photos/luka-doncic.jpg",
            "url": "https://example.com/gallery/luka-doncic",
            "title": "Luka Doncic via Getty Images",
        }

        self.assertFalse(images._is_good_search_result(result))

    def test_preferred_search_result_detects_official_domain(self):
        result = {
            "image": "https://cdn.nba.com/headshots/nba/latest/1040x760/1629029.png",
            "url": "https://www.nba.com/news/luka-doncic",
            "title": "Luka Doncic",
        }

        self.assertTrue(images._is_good_search_result(result))
        self.assertTrue(images._is_preferred_search_result(result))

    def test_parse_image_review_output_handles_fenced_json(self):
        accepted, reason = images._parse_image_review_output(
            '```json\n{"accepted": false, "reason": "visible Getty watermark"}\n```'
        )

        self.assertFalse(accepted)
        self.assertEqual(reason, "visible Getty watermark")

    def test_review_resolved_image_skips_without_openai_key(self):
        with patch.dict("os.environ", {}, clear=True):
            result = images.review_resolved_image({"type": "web_search", "query": "Luka Doncic celebration"}, "/tmp/x.png")

        self.assertEqual(
            result,
            {"checked": False, "ok": True, "reason": None, "model": None},
        )

    def test_review_resolved_image_rejects_when_model_flags_image(self):
        fake_response = SimpleNamespace(output_text='{"accepted": false, "reason": "visible watermark"}')
        fake_client = SimpleNamespace(
            responses=SimpleNamespace(create=lambda **kwargs: fake_response)
        )

        with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}, clear=True), \
             patch("openai.OpenAI", return_value=fake_client), \
             patch("social_media.images._image_data_url", return_value="data:image/png;base64,abc"):
            result = images.review_resolved_image(
                {"type": "web_search", "query": "Luka Doncic celebration"},
                "/tmp/x.png",
            )

        self.assertTrue(result["checked"])
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "visible watermark")
        self.assertEqual(result["model"], "gpt-5.4-mini")

    def test_review_resolved_image_accepts_non_review_type(self):
        result = images.review_resolved_image({"type": "player_headshot", "player_id": "1629029"}, "/tmp/x.png")

        self.assertEqual(
            result,
            {"checked": False, "ok": True, "reason": None, "model": None},
        )


if __name__ == "__main__":
    unittest.main()
