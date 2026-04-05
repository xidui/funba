import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.funba_imagegen import (  # noqa: E402
    DEFAULT_IMAGE_MODEL,
    _extract_key_from_text,
    _call_images_with_retry,
    _generate_image_bytes,
    generate_image,
    resolve_openai_api_key,
)
from openai import BadRequestError  # noqa: E402


class TestFunbaImagegenKeyResolution(unittest.TestCase):
    def test_extract_key_from_text_supports_plain_assignment(self):
        text = "OPENAI_API_KEY=sk-test123\n"
        self.assertEqual(_extract_key_from_text(text), "sk-test123")

    def test_extract_key_from_text_supports_export_assignment(self):
        text = "export OPENAI_API_KEY='sk-test456'\n"
        self.assertEqual(_extract_key_from_text(text), "sk-test456")

    @patch.dict("os.environ", {"OPENAI_API_KEY": "sk-env"}, clear=False)
    def test_resolve_openai_api_key_prefers_env(self):
        self.assertEqual(resolve_openai_api_key(cwd=REPO_ROOT), "sk-env")

    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_openai_api_key_reads_secrets_md(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "SECRETS.md").write_text("OPENAI_API_KEY=sk-secret\n", encoding="utf-8")
            self.assertEqual(resolve_openai_api_key(cwd=tmpdir), "sk-secret")


class TestFunbaImagegenGenerate(unittest.TestCase):
    @patch("social_media.funba_imagegen.resolve_openai_api_key", return_value="sk-test")
    @patch("social_media.funba_imagegen.OpenAI")
    def test_generate_image_without_reference_uses_generate(self, client_cls, _key_mock):
        mock_client = client_cls.return_value
        mock_client.images.generate.return_value = SimpleNamespace(
            data=[SimpleNamespace(b64_json="aGVsbG8=")]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir, "out.png")
            path = generate_image(prompt="test prompt", output_path=output)
            self.assertEqual(path.read_bytes(), b"hello")
            mock_client.images.generate.assert_called_once()
            self.assertFalse(mock_client.images.edit.called)

    @patch("social_media.funba_imagegen.resolve_openai_api_key", return_value="sk-test")
    @patch("social_media.funba_imagegen.OpenAI")
    def test_generate_image_with_reference_uses_responses_api(self, client_cls, _key_mock):
        mock_client = client_cls.return_value
        mock_client.files.create.side_effect = [
            SimpleNamespace(id="file-1"),
        ]
        mock_client.responses.create.return_value = SimpleNamespace(
            output=[SimpleNamespace(type="image_generation_call", result="aGVsbG8=")]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            ref = Path(tmpdir, "ref.jpg")
            ref.write_bytes(b"ref")
            output = Path(tmpdir, "out.png")
            generate_image(
                prompt="test prompt",
                output_path=output,
                reference_images=[ref],
                model=DEFAULT_IMAGE_MODEL,
            )
            mock_client.files.create.assert_called_once()
            mock_client.responses.create.assert_called_once()
            self.assertFalse(mock_client.images.generate.called)

    def test_call_images_with_retry_drops_unknown_output_format(self):
        calls = []

        def fake_call(**kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                raise BadRequestError(
                    message="bad request",
                    response=httpx.Response(400, request=httpx.Request("POST", "https://api.openai.com/v1/images/edits")),
                    body={"error": {"message": "Unknown parameter: 'output_format'."}},
                )
            return "ok"

        result = _call_images_with_retry(fake_call, prompt="x", output_format="png", size="1024x1024")
        self.assertEqual(result, "ok")
        self.assertEqual(calls[0]["output_format"], "png")
        self.assertNotIn("output_format", calls[1])

    def test_call_images_with_retry_drops_unknown_response_format(self):
        calls = []

        def fake_call(**kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                raise BadRequestError(
                    message="bad request",
                    response=httpx.Response(400, request=httpx.Request("POST", "https://api.openai.com/v1/images/generations")),
                    body={"error": {"message": "Unknown parameter: 'response_format'."}},
                )
            return "ok"

        result = _call_images_with_retry(fake_call, prompt="x", response_format="b64_json", size="1024x1024")
        self.assertEqual(result, "ok")
        self.assertEqual(calls[0]["response_format"], "b64_json")
        self.assertNotIn("response_format", calls[1])

    @patch("social_media.funba_imagegen._read_image_url", return_value=b"hello-url")
    def test_generate_image_bytes_supports_url_response(self, read_url_mock):
        payload = {"url": "https://example.com/out.png"}
        self.assertEqual(_generate_image_bytes(payload), b"hello-url")
        read_url_mock.assert_called_once_with("https://example.com/out.png")

    @patch("social_media.funba_imagegen.resolve_openai_api_key", return_value="sk-test")
    @patch("social_media.funba_imagegen._read_image_url", return_value=b"hello-url")
    @patch("social_media.funba_imagegen.OpenAI")
    def test_generate_image_without_reference_accepts_url_response(self, client_cls, _read_url_mock, _key_mock):
        mock_client = client_cls.return_value
        mock_client.images.generate.return_value = SimpleNamespace(
            data=[SimpleNamespace(url="https://example.com/out.png")]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir, "out.png")
            path = generate_image(prompt="test prompt", output_path=output)
            self.assertEqual(path.read_bytes(), b"hello-url")
            mock_client.images.generate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
