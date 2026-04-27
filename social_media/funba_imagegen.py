from __future__ import annotations

import argparse
import base64
import os
import re
import sys
from pathlib import Path
from typing import Iterable

from openai import BadRequestError, OpenAI


DEFAULT_IMAGE_MODEL = "gpt-image-2"
DEFAULT_RESPONSES_MODEL = "gpt-5.5"
FALLBACK_RESPONSES_MODEL = "gpt-5"
DEFAULT_SIZE = "1536x1024"
DEFAULT_QUALITY = "high"
DEFAULT_FORMAT = "png"
DEFAULT_BACKGROUND = "opaque"
_API_KEY_PATTERNS = (
    re.compile(r"^\s*OPENAI_API_KEY\s*=\s*(.+?)\s*$"),
    re.compile(r"^\s*export\s+OPENAI_API_KEY\s*=\s*(.+?)\s*$"),
)


def _clean_secret_value(raw: str) -> str:
    value = (raw or "").strip().strip('"').strip("'")
    if not value or value.startswith("[") or value == "...":
        return ""
    return value


def _extract_key_from_text(text: str) -> str | None:
    for line in text.splitlines():
        for pattern in _API_KEY_PATTERNS:
            match = pattern.match(line)
            if match:
                value = _clean_secret_value(match.group(1))
                if value:
                    return value
    return None


def resolve_openai_api_key(*, cwd: str | Path | None = None) -> str:
    env_value = _clean_secret_value(os.environ.get("OPENAI_API_KEY", ""))
    if env_value:
        return env_value

    base_dir = Path(cwd or os.getcwd())
    for candidate in (base_dir / ".env", base_dir / "SECRETS.md"):
        try:
            if not candidate.exists():
                continue
            value = _extract_key_from_text(candidate.read_text(encoding="utf-8"))
            if value:
                return value
        except Exception:
            continue

    raise RuntimeError(
        "OPENAI_API_KEY not found. Set it in the environment, `.env`, or local `SECRETS.md`."
    )


def _decode_image_payload(item) -> bytes:
    b64_value = getattr(item, "b64_json", None) or (item.get("b64_json") if isinstance(item, dict) else None)
    if not b64_value:
        raise RuntimeError("Image response did not include b64_json output.")
    return base64.b64decode(b64_value)


def _read_image_url(url: str) -> bytes:
    import urllib.request

    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read()


def _generate_image_bytes(item) -> bytes:
    b64_value = getattr(item, "b64_json", None) or (item.get("b64_json") if isinstance(item, dict) else None)
    if b64_value:
        return base64.b64decode(b64_value)
    url_value = getattr(item, "url", None) or (item.get("url") if isinstance(item, dict) else None)
    if url_value:
        return _read_image_url(str(url_value))
    raise RuntimeError("OpenAI image response did not include b64_json or url output.")


def _save_image_bytes(image_bytes: bytes, output_path: str | Path) -> Path:
    path = Path(output_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(image_bytes)
    return path


def _call_images_with_retry(fn, **kwargs):
    try:
        return fn(**kwargs)
    except BadRequestError as exc:
        message = ""
        try:
            payload = exc.body or {}
            message = str((payload.get("error") or {}).get("message") or "")
        except Exception:
            message = str(exc)
        lowered = message.lower()
        retry_kwargs = dict(kwargs)
        changed = False
        for param in ("output_format", "background", "response_format"):
            if f"unknown parameter: '{param}'" in lowered and param in retry_kwargs:
                retry_kwargs.pop(param, None)
                changed = True
        if not changed:
            raise
        return fn(**retry_kwargs)


def _upload_reference_files(client: OpenAI, refs: list[Path]) -> list[str]:
    file_ids: list[str] = []
    for path in refs:
        with path.open("rb") as handle:
            file_obj = client.files.create(
                file=handle,
                purpose="vision",
            )
        file_ids.append(file_obj.id)
    return file_ids


def _response_image_bytes(response) -> bytes:
    outputs = getattr(response, "output", None) or []
    for output in outputs:
        output_type = getattr(output, "type", None) or (output.get("type") if isinstance(output, dict) else None)
        if output_type != "image_generation_call":
            continue
        result = getattr(output, "result", None) or (output.get("result") if isinstance(output, dict) else None)
        if result:
            return base64.b64decode(result)
    raise RuntimeError("Responses API did not return any image_generation_call output.")


def _call_responses_image_generation(
    client: OpenAI,
    *,
    prompt: str,
    file_ids: list[str],
    size: str,
    quality: str,
    background: str,
    model: str,
):
    content = [{"type": "input_text", "text": prompt}]
    for file_id in file_ids:
        content.append({"type": "input_image", "file_id": file_id})

    tool = {
        "type": "image_generation",
        "quality": quality,
        "size": size,
    }
    if background:
        tool["background"] = background

    try:
        return client.responses.create(
            model=model,
            input=[{"role": "user", "content": content}],
            tools=[tool],
        )
    except BadRequestError as exc:
        message = str(exc).lower()
        if model == DEFAULT_RESPONSES_MODEL and ("model" in message or "unsupported" in message or "invalid" in message):
            return client.responses.create(
                model=FALLBACK_RESPONSES_MODEL,
                input=[{"role": "user", "content": content}],
                tools=[tool],
            )
        if "unknown parameter" in message:
            slim_tool = {"type": "image_generation", "quality": quality}
            return client.responses.create(
                model=FALLBACK_RESPONSES_MODEL if model == DEFAULT_RESPONSES_MODEL else model,
                input=[{"role": "user", "content": content}],
                tools=[slim_tool],
            )
        raise


def generate_image(
    *,
    prompt: str,
    output_path: str | Path,
    reference_images: Iterable[str | Path] | None = None,
    model: str = DEFAULT_IMAGE_MODEL,
    size: str = DEFAULT_SIZE,
    quality: str = DEFAULT_QUALITY,
    output_format: str = DEFAULT_FORMAT,
    background: str = DEFAULT_BACKGROUND,
    cwd: str | Path | None = None,
) -> Path:
    api_key = resolve_openai_api_key(cwd=cwd)
    client = OpenAI(api_key=api_key)
    refs = [Path(p).expanduser() for p in (reference_images or [])]

    if refs:
        missing = [str(p) for p in refs if not p.exists()]
        if missing:
            raise FileNotFoundError(f"reference image(s) not found: {', '.join(missing)}")
        uploaded_file_ids = _upload_reference_files(client, refs)
        response = _call_responses_image_generation(
            client,
            prompt=prompt,
            file_ids=uploaded_file_ids,
            size=size,
            quality=quality,
            background=background,
            model=DEFAULT_RESPONSES_MODEL,
        )
        return _save_image_bytes(_response_image_bytes(response), output_path)
    else:
        response = _call_images_with_retry(
            client.images.generate,
            model=model,
            prompt=prompt,
            quality=quality,
            size=size,
            output_format=output_format,
            background=background,
        )

    image_item = response.data[0] if getattr(response, "data", None) else None
    if image_item is None:
        raise RuntimeError("OpenAI image response did not include any image data.")

    return _save_image_bytes(_generate_image_bytes(image_item), output_path)


def cmd_generate(args: argparse.Namespace) -> None:
    prompt = (args.prompt or "").strip()
    output = (args.output or "").strip()
    if not prompt or not output:
        print("ERROR: --prompt and --output are required", file=sys.stderr)
        sys.exit(1)

    path = generate_image(
        prompt=prompt,
        output_path=output,
        reference_images=list(args.reference_image or []),
        model=(args.model or DEFAULT_IMAGE_MODEL).strip(),
        size=(args.size or DEFAULT_SIZE).strip(),
        quality=(args.quality or DEFAULT_QUALITY).strip(),
        output_format=(args.output_format or DEFAULT_FORMAT).strip(),
        background=(args.background or DEFAULT_BACKGROUND).strip(),
        cwd=os.getcwd(),
    )
    mode = "edit" if args.reference_image else "generate"
    print(f"Generated ({mode}): {path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m social_media.funba_imagegen",
        description="Generate Funba image assets with OpenAI's strongest image model and optional real-photo references.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_generate = sub.add_parser("generate", help="Generate one supporting image asset.")
    p_generate.add_argument("--prompt", required=True, help="Image prompt.")
    p_generate.add_argument(
        "--reference-image",
        action="append",
        help="Optional local reference image path. Repeat to provide multiple real-game reference photos.",
    )
    p_generate.add_argument("--output", required=True, help="Output file path.")
    p_generate.add_argument("--model", default=DEFAULT_IMAGE_MODEL, help="Image model to use.")
    p_generate.add_argument("--size", default=DEFAULT_SIZE, help="Image size, for example 1536x1024.")
    p_generate.add_argument("--quality", default=DEFAULT_QUALITY, help="Image quality.")
    p_generate.add_argument("--output-format", default=DEFAULT_FORMAT, help="png, jpeg, or webp.")
    p_generate.add_argument("--background", default=DEFAULT_BACKGROUND, help="opaque, transparent, or auto.")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    {"generate": cmd_generate}[args.command](args)


if __name__ == "__main__":
    main()
