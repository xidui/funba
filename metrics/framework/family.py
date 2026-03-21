from __future__ import annotations

import ast
import copy


CAREER_KEY_SUFFIX = "_career"
FAMILY_VARIANT_SEASON = "season"
FAMILY_VARIANT_CAREER = "career"


def family_base_key(key: str) -> str:
    return str(key or "").removesuffix(CAREER_KEY_SUFFIX)


def family_career_key(base_key: str) -> str:
    return family_base_key(base_key) + CAREER_KEY_SUFFIX


def is_reserved_career_key(key: str) -> bool:
    return str(key or "").endswith(CAREER_KEY_SUFFIX)


def derive_career_name(name: str, suffix: str = " (Career)") -> str:
    base_name = (name or "").strip()
    return f"{base_name}{suffix}" if base_name else suffix.strip()


def derive_career_description(description: str) -> str:
    base_description = (description or "").strip()
    suffix = "Computed across all seasons."
    return f"{base_description} {suffix}".strip() if base_description else suffix


def derive_career_min_sample(min_sample: int, career_min_sample: int | None) -> int:
    base_min = int(min_sample or 1)
    if career_min_sample is not None:
        return int(career_min_sample)
    return max(base_min * 5, base_min)


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


def build_career_code_variant(
    code_python: str,
    *,
    base_key: str,
    name: str,
    description: str,
    min_sample: int,
) -> str:
    career_code = code_python
    career_code = _rewrite_metric_attr(career_code, "key", family_career_key(base_key))
    career_code = _rewrite_metric_attr(career_code, "name", name)
    career_code = _rewrite_metric_attr(career_code, "description", description)
    career_code = _rewrite_metric_attr(career_code, "min_sample", int(min_sample))
    career_code = _rewrite_metric_attr(career_code, "career", True)
    career_code = _rewrite_metric_attr(career_code, "supports_career", False)
    return career_code
