from __future__ import annotations

import argparse
import sys
import time
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

try:
    from playwright.sync_api import sync_playwright, Page
except ModuleNotFoundError:
    sync_playwright = None
    Page = Any


REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
PUBLIC_BASE_URL = "https://funba.app"
DEFAULT_BASE_URL = PUBLIC_BASE_URL
_LOCAL_CAPTURE_HOSTS = {"127.0.0.1", "::1", "localhost"}
_SCREENSHOT_ERROR_TEXT_MARKERS = (
    "Something Went Wrong",
    "An unexpected error occurred",
    "Back to Home",
)
_COMMON_CHROME_REMOVE_SELECTORS = [
    ".topbar",
    "#mobile-nav",
    "#mobile-nav-scrim",
]


def _playwright():
    if sync_playwright is None:
        raise RuntimeError("Playwright is required for Funba capture commands. Install it with `pip install playwright`.")
    return sync_playwright()


def _normalize_base_url(base_url: str) -> str:
    return (base_url or DEFAULT_BASE_URL).rstrip("/")


def _canonicalize_capture_url(url: str, *, allow_private_hosts: bool = False) -> str:
    raw_url = str(url or "").strip()
    if not raw_url:
        return raw_url
    parts = urlsplit(raw_url)
    host = (parts.hostname or "").strip().lower()
    if allow_private_hosts or host not in _LOCAL_CAPTURE_HOSTS:
        return raw_url
    public_parts = urlsplit(_normalize_base_url(DEFAULT_BASE_URL))
    return urlunsplit(
        (
            public_parts.scheme,
            public_parts.netloc,
            parts.path,
            parts.query,
            parts.fragment,
        )
    )


def _page_text(page: Page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


def _response_status_code(response: Any) -> int | None:
    value = getattr(response, "status", None)
    if callable(value):
        try:
            value = value()
        except Exception:
            return None
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _capture_page_error(page: Page, response: Any | None = None) -> str | None:
    status_code = _response_status_code(response)
    if status_code is not None and status_code >= 500:
        return f"Screenshot target returned HTTP {status_code}"
    text = _page_text(page)
    if any(marker in text for marker in _SCREENSHOT_ERROR_TEXT_MARKERS):
        return "Screenshot target rendered a server error page"
    return None


def _url_with_query(base_url: str, path: str, query: dict[str, str | None] | None = None) -> str:
    base = _normalize_base_url(base_url)
    query = {k: v for k, v in (query or {}).items() if v not in (None, "")}
    suffix = f"?{urlencode(query)}" if query else ""
    return f"{base}{path}{suffix}"


def _set_query_param(url: str, key: str, value: str | None) -> str:
    if value in (None, ""):
        return url
    parts = urlsplit(url)
    query_items = dict()
    if parts.query:
        for chunk in parts.query.split("&"):
            if not chunk:
                continue
            if "=" in chunk:
                q_key, q_value = chunk.split("=", 1)
            else:
                q_key, q_value = chunk, ""
            query_items[q_key] = q_value
    query_items[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query_items), parts.fragment))


def _capture_with_plan(page: Page, output_path: str, plan: dict[str, object]) -> bool:
    selectors = list(plan.get("selectors") or [])
    if not selectors:
        return False

    def _collect_boxes() -> list[dict[str, float]]:
        boxes = []
        selector_height_limits = plan.get("selector_height_limits") or {}
        for idx, selector in enumerate(selectors):
            locator = page.locator(selector).first
            if locator.count() == 0:
                if idx == 0:
                    return []
                continue
            try:
                box = locator.bounding_box()
            except Exception:
                box = None
            if not box:
                if idx == 0:
                    return []
                continue
            if selector in selector_height_limits:
                box = dict(box)
                box["height"] = min(box["height"], float(selector_height_limits[selector]))
            boxes.append(box)
        return boxes

    boxes = _collect_boxes()
    if not boxes:
        return False

    viewport = getattr(page, "viewport_size", None) or {"width": 1280, "height": 900}
    top = min(box["y"] for box in boxes)
    bottom = max(box["y"] + box["height"] for box in boxes)
    if top < 0 or bottom > float(viewport.get("height", 900)):
        scroll_target = max(top - float(plan.get("pad_top", 0)) - 16.0, 0.0)
        page.evaluate("(y) => window.scrollTo(0, y)", scroll_target)
        time.sleep(0.15)
        boxes = _collect_boxes()
        if not boxes:
            return False

    left = min(box["x"] for box in boxes)
    top = min(box["y"] for box in boxes)
    right = max(box["x"] + box["width"] for box in boxes)
    bottom = max(box["y"] + box["height"] for box in boxes)

    clip_x = max(left - float(plan.get("pad_x", 0)), 0)
    clip_y = max(top - float(plan.get("pad_top", 0)), 0)
    width = right - left + float(plan.get("pad_x", 0)) * 2
    height = bottom - top + float(plan.get("pad_top", 0)) + float(plan.get("pad_bottom", 0))

    width = min(max(width, float(plan.get("min_width", 720))), float(plan.get("max_width", 1100)))
    height = min(max(height, float(plan.get("min_height", 320))), float(plan.get("max_height", 720)))

    page.screenshot(
        path=output_path,
        clip={
            "x": clip_x,
            "y": clip_y,
            "width": width,
            "height": height,
        },
    )
    return True


def _apply_adjustments(page: Page, adjustments: dict[str, object] | None) -> None:
    if not adjustments:
        return

    remove_selectors = list(adjustments.get("remove_selectors") or [])
    limit_table_rows = adjustments.get("limit_table_rows") or {}
    limit_grid_cards = adjustments.get("limit_grid_cards") or {}
    style_updates = adjustments.get("style_updates") or {}
    if not remove_selectors and not limit_table_rows and not limit_grid_cards and not style_updates:
        return

    page.evaluate(
        """payload => {
        const removeSelectors = Array.isArray(payload.remove_selectors) ? payload.remove_selectors : [];
        const limitTableRows = payload.limit_table_rows && typeof payload.limit_table_rows === "object"
          ? payload.limit_table_rows
          : {};
        const limitGridCards = payload.limit_grid_cards && typeof payload.limit_grid_cards === "object"
          ? payload.limit_grid_cards
          : {};
        const styleUpdates = payload.style_updates && typeof payload.style_updates === "object"
          ? payload.style_updates
          : {};

        removeSelectors.forEach((selector) => {
          document.querySelectorAll(selector).forEach((el) => el.remove());
        });

        Object.entries(limitTableRows).forEach(([selector, maxRows]) => {
          const limit = Number(maxRows);
          if (!Number.isFinite(limit) || limit < 1) return;
          document.querySelectorAll(selector).forEach((tbody) => {
            Array.from(tbody.querySelectorAll("tr")).forEach((row, index) => {
              if (index >= limit) row.remove();
            });
          });
        });

        Object.entries(limitGridCards).forEach(([selector, maxCards]) => {
          const limit = Number(maxCards);
          if (!Number.isFinite(limit) || limit < 1) return;
          document.querySelectorAll(selector).forEach((grid) => {
            Array.from(grid.children).forEach((card, index) => {
              if (index >= limit) card.remove();
            });
          });
        });

        Object.entries(styleUpdates).forEach(([selector, styles]) => {
          if (!styles || typeof styles !== "object") return;
          document.querySelectorAll(selector).forEach((el) => {
            Object.entries(styles).forEach(([prop, value]) => {
              el.style.setProperty(prop, String(value));
            });
          });
        });
      }""",
        {
            "remove_selectors": remove_selectors,
            "limit_table_rows": limit_table_rows,
            "limit_grid_cards": limit_grid_cards,
            "style_updates": style_updates,
        },
    )


def _capture_generic_fallback(page: Page, output_path: str) -> None:
    selectors = [
        ".rankings-table-wrap",
        ".rankings-table",
        '[class*="rankings-table"]',
        '[class*="leaderboard"]',
        '[class*="ranking"]',
        '[class*="game-metrics"]',
        '[class*="metric-strip"]',
        '[class*="boxscore"]',
        '[class*="team-stats"]',
        '[class*="player-stats"]',
        '[class*="game"]',
        "main",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() == 0:
                continue
            box = locator.bounding_box()
            if not box:
                continue
            page.screenshot(
                path=output_path,
                clip={
                    "x": max(box["x"], 0),
                    "y": max(box["y"], 0),
                    "width": min(max(box["width"], 720), 1100),
                    "height": min(max(box["height"], 280), 640),
                },
            )
            return
        except Exception:
            continue
    page.screenshot(path=output_path)


def _with_page(url: str, *, wait_ms: int = 4000):
    class _PageSession:
        def __enter__(self_nonlocal):
            pw = _playwright()
            self_nonlocal._pw_manager = pw
            self_nonlocal._pw = pw.__enter__()
            self_nonlocal._browser = self_nonlocal._pw.chromium.launch(headless=True)
            self_nonlocal._context = self_nonlocal._browser.new_context(
                viewport={"width": 1280, "height": 900},
                locale="zh-CN",
                user_agent=REAL_BROWSER_UA,
            )
            self_nonlocal.page = self_nonlocal._context.new_page()
            self_nonlocal.response = self_nonlocal.page.goto(url, wait_until="domcontentloaded", timeout=15000)
            time.sleep(wait_ms / 1000)
            page_error = _capture_page_error(self_nonlocal.page, self_nonlocal.response)
            if page_error:
                raise RuntimeError(page_error)
            return self_nonlocal.page

        def __exit__(self_nonlocal, exc_type, exc, tb):
            self_nonlocal._context.close()
            self_nonlocal._browser.close()
            self_nonlocal._pw_manager.__exit__(exc_type, exc, tb)
            return False

    return _PageSession()


def _player_summary_plan() -> dict[str, object]:
    return {
        "selectors": [".player-header"],
        "pad_x": 12,
        "pad_top": 16,
        "pad_bottom": 28,
        "min_width": 760,
        "max_width": 980,
        "min_height": 280,
        "max_height": 420,
    }


def _player_metrics_plan(*, cards: int = 4) -> dict[str, object]:
    card_index = max(1, int(cards))
    return {
        "selectors": [
            ".metrics-section .section-title",
            ".metrics-section .metrics-columns .metrics-col:first-child .metrics-col-title",
            ".metrics-section .metrics-columns .metrics-col:first-child .metrics-grid",
            f".metrics-section .metrics-columns .metrics-col:first-child .metric-card:nth-child({card_index})",
        ],
        "pad_x": 12,
        "pad_top": 12,
        "pad_bottom": 18,
        "min_width": 760,
        "max_width": 980,
        "min_height": 320,
        "max_height": 520,
    }


def _game_boxscore_plan() -> dict[str, object]:
    return {
        "selectors": [".scoreboard", "#bs-team", "#bs-players .box-score-grid"],
        "pad_x": 12,
        "pad_top": 12,
        "pad_bottom": 18,
        "min_width": 760,
        "max_width": 1220,
        "min_height": 520,
        "max_height": 920,
        "selector_height_limits": {
            "#bs-team": 320,
            "#bs-players .box-score-grid": 420,
        },
    }


def _game_metrics_plan(*, cards: int = 4) -> dict[str, object]:
    card_index = max(1, int(cards))
    return {
        "selectors": [
            "#game-metrics-panel .analytics-header",
            "#game-metrics-panel .game-metrics-grid",
            f"#game-metrics-panel .game-metrics-grid .gmc:nth-child({card_index})",
        ],
        "pad_x": 12,
        "pad_top": 12,
        "pad_bottom": 18,
        "min_width": 760,
        "max_width": 1220,
        "min_height": 340,
        "max_height": 760,
    }


def _metric_page_plan(*, top_n: int = 5) -> dict[str, object]:
    row_index = max(1, int(top_n))
    return {
        "selectors": [
            ".detail-title",
            ".detail-desc",
            ".rankings-table thead",
            f".rankings-table tbody tr:nth-child({row_index})",
        ],
        "pad_x": 12,
        "pad_top": 16,
        "pad_bottom": 18,
        "min_width": 760,
        "max_width": 1100,
        "min_height": 520,
        "max_height": 680,
    }


def _game_boxscore_adjustments() -> dict[str, object]:
    return {
        "remove_selectors": _COMMON_CHROME_REMOVE_SELECTORS + [
            "#game-metrics-panel",
            "#bs-team .table-wrap:first-child",
            ".card:has(.game-admin-panel)",
            ".game-admin-panel",
        ],
        "limit_table_rows": {
            "#bs-players tbody": 4,
        },
        "style_updates": {
            ".sb-chart-wrap": {
                "height": "160px",
                "padding": "14px 28px 16px",
            },
        },
    }


def _game_metrics_adjustments(*, cards: int = 4) -> dict[str, object]:
    return {
        "remove_selectors": ["#show-more-game-metrics", ".gmc-more-wrap"],
        "limit_grid_cards": {
            "#game-metrics-panel .game-metrics-grid": max(1, int(cards)),
        },
    }


def _player_metrics_adjustments(*, scope: str, cards: int = 6) -> dict[str, object]:
    if scope == "career":
        return {
            "remove_selectors": _COMMON_CHROME_REMOVE_SELECTORS + [
                ".player-section-nav",
                ".metrics-section .metrics-columns .metrics-col:first-child",
                ".metrics-section .metrics-columns .metrics-col:nth-child(n+3)",
                ".metrics-more-wrap",
                "#playin-metrics-block",
            ],
            "limit_grid_cards": {
                ".metrics-section .metrics-columns .metrics-col:first-child .metrics-grid": max(1, int(cards)),
            },
            "style_updates": {
                ".metrics-section .metrics-columns": {
                    "grid-template-columns": "minmax(760px, 900px)",
                    "justify-content": "start",
                },
            },
        }
    return {
        "remove_selectors": _COMMON_CHROME_REMOVE_SELECTORS + [
            ".player-section-nav",
            ".metrics-section .metrics-columns .metrics-col:nth-child(n+2)",
            ".metrics-more-wrap",
            "#playin-metrics-block",
        ],
        "limit_grid_cards": {
            ".metrics-section .metrics-columns .metrics-col:first-child .metrics-grid": max(1, int(cards)),
        },
        "style_updates": {
            ".metrics-section .metrics-columns": {
                "grid-template-columns": "minmax(760px, 900px)",
                "justify-content": "start",
            },
        },
    }


def _metric_page_adjustments(*, top_n: int = 5) -> dict[str, object]:
    return {
        "remove_selectors": _COMMON_CHROME_REMOVE_SELECTORS + [
            ".detail-back",
            ".metric-switch",
            ".season-select",
            "#backfill-panel",
            "#admin-details-drawer",
            "#metric-deep-dive-panel",
            "form[action]",
            ".sub-key-hint",
            ".rankings-table tbody tr.drilldown-row",
        ],
        "limit_table_rows": {
            ".rankings-table tbody": max(1, int(top_n)),
        },
        "style_updates": {
            ".rankings-table-wrap": {
                "margin-top": "12px",
            },
        },
    }


def capture_player_profile(player_id: str, output_path: str, *, base_url: str = DEFAULT_BASE_URL, wait_ms: int = 4000) -> None:
    url = _url_with_query(base_url, f"/players/{player_id}")
    with _with_page(url, wait_ms=wait_ms) as page:
        if not _capture_with_plan(page, output_path, _player_summary_plan()):
            _capture_generic_fallback(page, output_path)


def capture_player_metrics(
    player_id: str,
    output_path: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    scope: str = "season",
    season: str | None = None,
    career_type: str | None = None,
    cards: int = 4,
    wait_ms: int = 4000,
) -> None:
    url = _url_with_query(base_url, f"/players/{player_id}", {"season": season})
    if scope == "career":
        url = _set_query_param(url, "season", career_type or "all_regular")
    with _with_page(url, wait_ms=wait_ms) as page:
        page.wait_for_function(
            """() => {
              const section = document.querySelector('.metrics-section');
              if (!section) return false;
              return section.querySelectorAll('.metric-card').length >= 1;
            }""",
            timeout=max(8000, wait_ms + 4000),
        )
        _apply_adjustments(page, _player_metrics_adjustments(scope=scope, cards=cards))
        time.sleep(0.2)
        if not _capture_with_plan(page, output_path, _player_metrics_plan(cards=cards)):
            _capture_generic_fallback(page, output_path)


def capture_player_season_metrics(
    player_id: str,
    output_path: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    season: str | None = None,
    cards: int = 4,
    wait_ms: int = 4000,
) -> None:
    capture_player_metrics(
        player_id,
        output_path,
        base_url=base_url,
        scope="season",
        season=season,
        cards=cards,
        wait_ms=wait_ms,
    )


def capture_player_career_metrics(
    player_id: str,
    output_path: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    career_type: str | None = None,
    cards: int = 4,
    wait_ms: int = 4000,
) -> None:
    capture_player_metrics(
        player_id,
        output_path,
        base_url=base_url,
        scope="career",
        career_type=career_type,
        cards=cards,
        wait_ms=wait_ms,
    )


def capture_game_boxscore(game_id: str, output_path: str, *, base_url: str = DEFAULT_BASE_URL, wait_ms: int = 4000) -> None:
    url = _url_with_query(base_url, f"/games/{game_id}")
    with _with_page(url, wait_ms=wait_ms) as page:
        _apply_adjustments(page, _game_boxscore_adjustments())
        time.sleep(0.2)
        if not _capture_with_plan(page, output_path, _game_boxscore_plan()):
            _capture_generic_fallback(page, output_path)


def capture_game_metrics(
    game_id: str,
    output_path: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    cards: int = 4,
    wait_ms: int = 4000,
) -> None:
    url = _url_with_query(base_url, f"/games/{game_id}")
    with _with_page(url, wait_ms=wait_ms) as page:
        page.wait_for_function(
            """() => {
              const root = document.querySelector('#game-metrics-panel');
              if (!root) return false;
              return !!root.querySelector('.game-metrics-grid .gmc') && !root.innerText.includes('Loading metrics');
            }""",
            timeout=max(8000, wait_ms + 4000),
        )
        _apply_adjustments(page, _game_metrics_adjustments(cards=cards))
        time.sleep(0.2)
        if not _capture_with_plan(page, output_path, _game_metrics_plan(cards=cards)):
            _capture_generic_fallback(page, output_path)


def capture_metric_page(
    metric_key: str,
    output_path: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    season: str | None = None,
    top_n: int = 5,
    wait_ms: int = 4000,
) -> None:
    url = _url_with_query(base_url, f"/metrics/{metric_key}", {"season": season})
    with _with_page(url, wait_ms=wait_ms) as page:
        _apply_adjustments(page, _metric_page_adjustments(top_n=top_n))
        time.sleep(0.2)
        if not _capture_with_plan(page, output_path, _metric_page_plan(top_n=top_n)):
            _capture_generic_fallback(page, output_path)


def capture_funba_url(
    url: str,
    output_path: str,
    *,
    wait_ms: int = 4000,
    allow_private_hosts: bool = False,
) -> None:
    url = _canonicalize_capture_url(url, allow_private_hosts=allow_private_hosts)
    if "/players/" in url and "/compare" not in url:
        player_id = urlsplit(url).path.rstrip("/").split("/")[-1]
        capture_player_profile(player_id, output_path, base_url=f"{urlsplit(url).scheme}://{urlsplit(url).netloc}", wait_ms=wait_ms)
        return
    if "/games/" in url:
        game_id = urlsplit(url).path.rstrip("/").split("/")[-1]
        capture_game_boxscore(game_id, output_path, base_url=f"{urlsplit(url).scheme}://{urlsplit(url).netloc}", wait_ms=wait_ms)
        return
    if "/metrics/" in url:
        metric_key = urlsplit(url).path.rstrip("/").split("/")[-1]
        query = dict()
        if urlsplit(url).query:
            for chunk in urlsplit(url).query.split("&"):
                if "=" in chunk:
                    key, value = chunk.split("=", 1)
                    query[key] = value
        capture_metric_page(
            metric_key,
            output_path,
            base_url=f"{urlsplit(url).scheme}://{urlsplit(url).netloc}",
            season=query.get("season"),
            top_n=5,
            wait_ms=wait_ms,
        )
        return
    with _with_page(url, wait_ms=wait_ms) as page:
        _capture_generic_fallback(page, output_path)


def cmd_capture(args: argparse.Namespace) -> None:
    command = args.command
    base_url = getattr(args, "base_url", DEFAULT_BASE_URL)
    wait_ms = int(getattr(args, "wait_ms", 4000) or 4000)
    allow_private_hosts = bool(getattr(args, "allow_private_hosts", False))
    output = (getattr(args, "output", "") or "").strip()
    if not output:
        print("ERROR: --output is required")
        sys.exit(1)

    if command == "url":
        capture_funba_url(
            (args.url or "").strip(),
            output,
            wait_ms=wait_ms,
            allow_private_hosts=allow_private_hosts,
        )
    elif command == "player-profile":
        capture_player_profile(str(args.player_id), output, base_url=base_url, wait_ms=wait_ms)
    elif command == "player-metrics":
        capture_player_metrics(
            str(args.player_id),
            output,
            base_url=base_url,
            scope=str(args.scope or "season"),
            season=getattr(args, "season", None),
            career_type=getattr(args, "career_type", None),
            cards=int(getattr(args, "cards", 4) or 4),
            wait_ms=wait_ms,
        )
    elif command == "game-boxscore":
        capture_game_boxscore(str(args.game_id), output, base_url=base_url, wait_ms=wait_ms)
    elif command == "game-metrics":
        capture_game_metrics(
            str(args.game_id),
            output,
            base_url=base_url,
            cards=int(getattr(args, "cards", 4) or 4),
            wait_ms=wait_ms,
        )
    elif command == "metric-page":
        capture_metric_page(
            str(args.metric_key),
            output,
            base_url=base_url,
            season=getattr(args, "season", None),
            top_n=int(getattr(args, "top_n", 5) or 5),
            wait_ms=wait_ms,
        )
    else:
        print(f"ERROR: Unsupported command: {command}")
        sys.exit(1)
    print(f"Captured: {output}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture compact Funba screenshots for social content workflows.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_url = sub.add_parser("url", help="Capture a compact screenshot from a raw Funba URL.")
    p_url.add_argument("--url", required=True, help="Full Funba URL to capture.")
    p_url.add_argument("--output", required=True, help="Output image path.")
    p_url.add_argument("--wait-ms", type=int, default=4000, help="Extra wait time before capture.")
    p_url.add_argument(
        "--allow-private-hosts",
        action="store_true",
        help="Honor raw localhost/private-host URLs instead of rewriting them to the default public Funba base URL.",
    )

    p_player = sub.add_parser("player-profile", help="Capture the player summary/header panel.")
    p_player.add_argument("--player-id", required=True, help="Player ID.")
    p_player.add_argument("--output", required=True, help="Output image path.")
    p_player.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Funba base URL.")
    p_player.add_argument("--wait-ms", type=int, default=4000, help="Extra wait time before capture.")

    p_player_metrics = sub.add_parser("player-metrics", help="Capture the player metrics panel for season or career context.")
    p_player_metrics.add_argument("--player-id", required=True, help="Player ID.")
    p_player_metrics.add_argument("--scope", choices=("season", "career"), default="season", help="Which player metrics block to capture.")
    p_player_metrics.add_argument("--season", help="Season for season-scope capture, for example 22025.")
    p_player_metrics.add_argument("--career-type", default="all_regular", help="Career bucket for career-scope capture, for example all_regular.")
    p_player_metrics.add_argument("--cards", type=int, default=4, help="How many metric cards to keep.")
    p_player_metrics.add_argument("--output", required=True, help="Output image path.")
    p_player_metrics.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Funba base URL.")
    p_player_metrics.add_argument("--wait-ms", type=int, default=4000, help="Extra wait time before capture.")

    p_game_box = sub.add_parser("game-boxscore", help="Capture the game scoreboard plus team/player box score panel.")
    p_game_box.add_argument("--game-id", required=True, help="Game ID.")
    p_game_box.add_argument("--output", required=True, help="Output image path.")
    p_game_box.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Funba base URL.")
    p_game_box.add_argument("--wait-ms", type=int, default=4000, help="Extra wait time before capture.")

    p_game_metrics = sub.add_parser("game-metrics", help="Capture the async game metrics panel.")
    p_game_metrics.add_argument("--game-id", required=True, help="Game ID.")
    p_game_metrics.add_argument("--cards", type=int, default=4, help="How many metric cards to keep visible.")
    p_game_metrics.add_argument("--output", required=True, help="Output image path.")
    p_game_metrics.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Funba base URL.")
    p_game_metrics.add_argument("--wait-ms", type=int, default=4000, help="Extra wait time before capture.")

    p_metric = sub.add_parser("metric-page", help="Capture a metric detail page with the top N rows visible.")
    p_metric.add_argument("--metric-key", required=True, help="Metric key.")
    p_metric.add_argument("--season", help="Season or view param to apply, for example 22025 or all_regular.")
    p_metric.add_argument("--top-n", type=int, default=5, help="How many ranking rows to keep visible.")
    p_metric.add_argument("--output", required=True, help="Output image path.")
    p_metric.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Funba base URL.")
    p_metric.add_argument("--wait-ms", type=int, default=4000, help="Extra wait time before capture.")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    cmd_capture(args)


if __name__ == "__main__":
    main()
