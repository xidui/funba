from __future__ import annotations

import ast
import copy


CAREER_KEY_SUFFIX = "_career"
LAST3_KEY_SUFFIX = "_last3"
LAST5_KEY_SUFFIX = "_last5"
WINDOW_SUFFIXES = {
    "career": CAREER_KEY_SUFFIX,
    "last3": LAST3_KEY_SUFFIX,
    "last5": LAST5_KEY_SUFFIX,
}
FAMILY_VARIANT_SEASON = "season"
FAMILY_VARIANT_CAREER = "career"


def family_base_key(key: str) -> str:
    value = str(key or "")
    for suffix in WINDOW_SUFFIXES.values():
        if value.endswith(suffix):
            return value.removesuffix(suffix)
    return value


def window_type_from_key(key: str) -> str | None:
    value = str(key or "")
    for window_type, suffix in WINDOW_SUFFIXES.items():
        if value.endswith(suffix):
            return window_type
    return None


def family_window_key(base_key: str, window_type: str) -> str:
    suffix = WINDOW_SUFFIXES[window_type]
    return family_base_key(base_key) + suffix


def family_career_key(base_key: str) -> str:
    return family_window_key(base_key, "career")


def family_last3_key(base_key: str) -> str:
    return family_window_key(base_key, "last3")


def family_last5_key(base_key: str) -> str:
    return family_window_key(base_key, "last5")


def is_reserved_window_key(key: str) -> bool:
    return window_type_from_key(key) is not None


def is_reserved_career_key(key: str) -> bool:
    return is_reserved_window_key(key)


def derive_window_name(
    name: str,
    window_type: str,
    *,
    suffix: str | None = None,
) -> str:
    if suffix is None:
        suffix = {
            "career": " (Career)",
            "last3": " (Last 3 Seasons)",
            "last5": " (Last 5 Seasons)",
        }[window_type]
    base_name = (name or "").strip()
    return f"{base_name}{suffix}" if base_name else suffix.strip()


def derive_career_name(name: str, suffix: str = " (Career)") -> str:
    return derive_window_name(name, "career", suffix=suffix)


def derive_window_description(description: str, window_type: str) -> str:
    base_description = (description or "").strip()
    suffix = {
        "career": "Computed across seasons of the same type (regular season, playoffs, or play-in).",
        "last3": "Computed across the most recent 3 seasons of the same type (regular season, playoffs, or play-in).",
        "last5": "Computed across the most recent 5 seasons of the same type (regular season, playoffs, or play-in).",
    }[window_type]
    return f"{base_description} {suffix}".strip() if base_description else suffix


def derive_career_description(description: str) -> str:
    return derive_window_description(description, "career")


def derive_window_min_sample(
    min_sample: int,
    window_type: str,
    *,
    career_min_sample: int | None = None,
    season_type: str | None = None,
) -> int:
    """Compute the effective min_sample for a window/season-type combination.

    season_type is one of "regular" / "playoffs" / "playin" (or None to use
    the per-window default, which assumes regular). Playoffs and play-in have
    fewer games per year, so they keep the base threshold (or 1 for play-in)
    instead of being multiplied by the window length.
    """
    base_min = int(min_sample or 1)
    season_type_norm = (season_type or "").strip().lower() or None

    if season_type_norm == "playin":
        return 1
    if season_type_norm == "playoffs":
        return max(base_min, 1)

    if window_type == "career":
        if career_min_sample is not None:
            return int(career_min_sample)
        return max(base_min * 5, base_min)
    if window_type == "last3":
        return max(base_min * 3, base_min)
    if window_type == "last5":
        return max(base_min * 5, base_min)
    raise KeyError(f"Unsupported window_type: {window_type!r}")


def derive_career_min_sample(min_sample: int, career_min_sample: int | None) -> int:
    return derive_window_min_sample(min_sample, "career", career_min_sample=career_min_sample)


def rule_supports_career(definition: dict | None, scope: str | None) -> bool:
    if (scope or "").strip() == "game":
        return False
    payload = definition or {}
    time_scope = str(payload.get("time_scope") or "season").strip().lower()
    return bool(payload.get("supports_career") or time_scope == "season_and_career")


def rule_is_career_variant(definition: dict | None) -> bool:
    payload = definition or {}
    return str(payload.get("time_scope") or "season").strip().lower() == "career"


def build_career_rule_definition(definition: dict | None) -> dict:
    payload = copy.deepcopy(definition or {})
    payload["time_scope"] = "career"
    payload["supports_career"] = False
    return payload


def _is_metric_definition_base(base: ast.expr) -> bool:
    return (
        isinstance(base, ast.Name) and base.id == "MetricDefinition"
    ) or (
        isinstance(base, ast.Attribute) and base.attr == "MetricDefinition"
    )


def _rewrite_metric_attr(code_python: str, attr_name: str, value) -> str:
    try:
        tree = ast.parse(code_python)
    except SyntaxError:
        return code_python

    lines = code_python.splitlines(keepends=True)
    literal = repr(value)

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not any(_is_metric_definition_base(base) for base in node.bases):
            continue

        indent = " " * ((node.body[0].col_offset if node.body else node.col_offset + 4))
        insert_at = node.lineno

        for stmt in node.body:
            target_name = None
            value_node = None
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target_name = stmt.targets[0].id
                value_node = stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target_name = stmt.target.id
                value_node = stmt.value

            if target_name == attr_name and isinstance(value_node, ast.Constant):
                if stmt.end_lineno is None or stmt.end_col_offset is None or stmt.lineno != stmt.end_lineno:
                    return code_python
                lineno = stmt.lineno - 1
                line = lines[lineno]
                prefix = line[:stmt.col_offset]
                lines[lineno] = prefix + f"{attr_name} = {literal}\n"
                return "".join(lines)

            if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
                insert_at = max(insert_at, getattr(stmt, "end_lineno", stmt.lineno))
                indent = " " * stmt.col_offset
            elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                insert_at = min(insert_at, stmt.lineno - 1 if insert_at == node.lineno else insert_at)
                break

        lines.insert(insert_at, f"{indent}{attr_name} = {literal}\n")
        return "".join(lines)

    return code_python


def build_window_code_variant(
    code_python: str,
    *,
    base_key: str,
    name: str,
    description: str,
    min_sample: int,
    window_type: str,
) -> str:
    window_code = code_python
    window_code = _rewrite_metric_attr(window_code, "key", family_window_key(base_key, window_type))
    window_code = _rewrite_metric_attr(window_code, "name", name)
    window_code = _rewrite_metric_attr(window_code, "description", description)
    window_code = _rewrite_metric_attr(window_code, "min_sample", int(min_sample))
    window_code = _rewrite_metric_attr(window_code, "career", True)
    window_code = _rewrite_metric_attr(window_code, "supports_career", False)
    return window_code


def build_career_code_variant(
    code_python: str,
    *,
    base_key: str,
    name: str,
    description: str,
    min_sample: int,
) -> str:
    return build_window_code_variant(
        code_python,
        base_key=base_key,
        name=name,
        description=description,
        min_sample=min_sample,
        window_type="career",
    )
