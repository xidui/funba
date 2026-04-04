import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media import images  # noqa: E402


class TestSocialImages(unittest.TestCase):
    def test_store_prepared_image_copies_file_into_post_media(self):
        with tempfile.TemporaryDirectory(prefix="funba_images_src_") as src_dir, \
             tempfile.TemporaryDirectory(prefix="funba_images_dst_") as dst_dir:
            source = Path(src_dir) / "flagg.png"
            source.write_bytes(b"png-data")

            with patch.object(images, "MEDIA_ROOT", Path(dst_dir)):
                stored = Path(images.store_prepared_image(str(source), post_id=42, slot="img8"))

            self.assertEqual(stored.parent, Path(dst_dir) / "42")
            self.assertEqual(stored.name, "img8.png")
            self.assertTrue(stored.exists())
            self.assertEqual(stored.read_bytes(), b"png-data")

    def test_store_prepared_image_adds_suffix_on_collision(self):
        with tempfile.TemporaryDirectory(prefix="funba_images_src_") as src_dir, \
             tempfile.TemporaryDirectory(prefix="funba_images_dst_") as dst_dir:
            source_a = Path(src_dir) / "a.png"
            source_b = Path(src_dir) / "b.png"
            source_a.write_bytes(b"a")
            source_b.write_bytes(b"b")

            with patch.object(images, "MEDIA_ROOT", Path(dst_dir)):
                first = Path(images.store_prepared_image(str(source_a), post_id=42, slot="img8"))
                second = Path(images.store_prepared_image(str(source_b), post_id=42, slot="img8"))

            self.assertEqual(first.name, "img8.png")
            self.assertEqual(second.name, "img8_1.png")
            self.assertEqual(second.read_bytes(), b"b")

    def test_store_prepared_image_requires_existing_file(self):
        with tempfile.TemporaryDirectory(prefix="funba_images_dst_") as dst_dir:
            with patch.object(images, "MEDIA_ROOT", Path(dst_dir)):
                with self.assertRaisesRegex(FileNotFoundError, "Prepared image file not found"):
                    images.store_prepared_image("/tmp/does-not-exist.png", post_id=42, slot="img1")


if __name__ == "__main__":
    unittest.main()
