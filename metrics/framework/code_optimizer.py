from __future__ import annotations

import ast
import re


_PLAYER_GAME_STAT_RE = re.compile(
    r"(?P<indent>[ \t]*)(?P<var>\w+)\s*=\s*\(\s*"
    r"session\.query\(PlayerGameStats\)\s*"
    r"\.filter\(\s*"
    r"PlayerGameStats\.player_id == entity_id,\s*"
    r"PlayerGameStats\.game_id == game_id,\s*"
    r"\)\s*\.(?:first|one_or_none)\(\)\s*"
    r"\)",
    re.MULTILINE | re.DOTALL,
)

_TEAM_PLAYER_STATS_RE = re.compile(
    r"(?P<indent>[ \t]*)(?P<var>\w+)\s*=\s*\(\s*"
    r"session\.query\(PlayerGameStats\)\s*"
    r"\.filter\(\s*"
    r"PlayerGameStats\.team_id == entity_id,\s*"
    r"PlayerGameStats\.game_id == game_id,\s*"
    r"\)\s*\.all\(\)\s*"
    r"\)",
    re.MULTILINE | re.DOTALL,
)

_TEAM_GAME_STAT_RE = re.compile(
    r"(?P<indent>[ \t]*)(?P<var>\w+)\s*=\s*\(\s*"
    r"session\.query\(TeamGameStats\)\s*"
    r"\.filter\(\s*"
    r"TeamGameStats\.team_id == entity_id,\s*"
    r"TeamGameStats\.game_id == game_id,\s*"
    r"\)\s*\.(?:first|one_or_none)\(\)\s*"
    r"\)",
    re.MULTILINE | re.DOTALL,
)

_GAME_ROW_RE = re.compile(
    r"(?P<indent>[ \t]*)(?P<var>\w+)\s*=\s*\(\s*"
    r"session\.query\((?P<query>Game(?:[^)]*))\)\s*"
    r"\.filter\(Game\.game_id == (?P<gid>\w+)\)\s*"
    r"\.(?:first|one_or_none)\(\)\s*"
    r"\)",
    re.MULTILINE | re.DOTALL,
)

_PLAYER_ATTEMPTED_SHOTS_RE = re.compile(
    r"(?P<indent>[ \t]*)(?P<var>\w+)\s*=\s*\(\s*"
    r"session\.query\(ShotRecord\)\s*"
    r"\.filter\(\s*"
    r"ShotRecord\.player_id == entity_id,\s*"
    r"ShotRecord\.game_id == game_id,\s*"
    r"ShotRecord\.shot_attempted\.is_\(True\),\s*"
    r"\)\s*"
    r"\.order_by\((?P<order_by>.*?)\)\s*"
    r"\.all\(\)\s*"
    r"\)",
    re.MULTILINE | re.DOTALL,
)


def _ensure_helper_imports(code_python: str, helper_names: set[str]) -> str:
    lines = code_python.splitlines(keepends=True)
    existing: set[str] = set()
    filtered_lines: list[str] = []
    for line in lines:
        if line.startswith("from metrics.helpers import "):
            existing.update(
                part.strip()
                for part in line.split("import", 1)[1].split(",")
                if part.strip()
            )
            continue
        filtered_lines.append(line)

    if not helper_names and not existing:
        return code_python

    lines = filtered_lines
    merged = sorted(existing | helper_names)

    insert_at = 0
    filtered_code = "".join(lines)
    try:
        tree = ast.parse(filtered_code)
    except SyntaxError:
        tree = None

    if tree and tree.body:
        first_stmt = tree.body[0]
        if (
            isinstance(first_stmt, ast.Expr)
            and isinstance(first_stmt.value, ast.Constant)
            and isinstance(first_stmt.value.value, str)
        ):
            insert_at = getattr(first_stmt, "end_lineno", first_stmt.lineno)
    elif lines:
        first_nonblank = 0
        while first_nonblank < len(lines) and not lines[first_nonblank].strip():
            first_nonblank += 1
        if first_nonblank < len(lines):
            stripped = lines[first_nonblank].lstrip()
            for quote in ('"""', "'''"):
                if stripped.startswith(quote):
                    insert_at = first_nonblank + 1
                    if stripped.count(quote) >= 2 and len(stripped) > len(quote):
                        break
                    while insert_at < len(lines):
                        if quote in lines[insert_at]:
                            insert_at += 1
                            break
                        insert_at += 1
                    break

    idx = insert_at
    while idx < len(lines):
        stripped = lines[idx].strip()
        if stripped.startswith("from __future__ import "):
            insert_at = idx + 1
            idx += 1
            continue
        if insert_at and stripped == "":
            insert_at = idx + 1
            idx += 1
            continue
        break

    lines.insert(insert_at, f"from metrics.helpers import {', '.join(merged)}\n")
    return "".join(lines)


def optimize_metric_code(code_python: str) -> str:
    """Rewrite common per-entity game queries to use cached metrics.helpers helpers."""
    helper_names: set[str] = set()
    optimized = code_python

    optimized, n = _PLAYER_GAME_STAT_RE.subn(
        lambda m: f"{m.group('indent')}{m.group('var')} = player_game_stat(session, game_id, entity_id)",
        optimized,
    )
    if n:
        helper_names.add("player_game_stat")

    optimized, n = _TEAM_PLAYER_STATS_RE.subn(
        lambda m: f"{m.group('indent')}{m.group('var')} = team_player_stats(session, game_id, entity_id)",
        optimized,
    )
    if n:
        helper_names.add("team_player_stats")

    optimized, n = _TEAM_GAME_STAT_RE.subn(
        lambda m: f"{m.group('indent')}{m.group('var')} = team_game_stat(session, game_id, entity_id)",
        optimized,
    )
    if n:
        helper_names.add("team_game_stat")

    optimized, n = _GAME_ROW_RE.subn(
        lambda m: f"{m.group('indent')}{m.group('var')} = game_row(session, {m.group('gid')})",
        optimized,
    )
    if n:
        helper_names.add("game_row")

    optimized, n = _PLAYER_ATTEMPTED_SHOTS_RE.subn(
        lambda m: f"{m.group('indent')}{m.group('var')} = player_attempted_shots(session, game_id, entity_id)",
        optimized,
    )
    if n:
        helper_names.add("player_attempted_shots")

    return _ensure_helper_imports(optimized, helper_names)
