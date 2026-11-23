"""Next UI — metrics catalog page plugin.

* ``enrich``: category counts for the catalog's category lens.
* ``register``: ``/api/next/metrics/catalog`` — same contract as
  ``/api/metrics/catalog`` (``{ok, html, next_offset, has_more}``) plus an
  optional ``category`` filter, which the classic catalog API doesn't support.
"""
from __future__ import annotations

import time

from flask import jsonify, render_template, request
from sqlalchemy import func

CATEGORY_LABELS_ZH = {
    "scoring": "得分",
    "record": "纪录",
    "efficiency": "效率",
    "aggregate": "累计",
    "conditional": "条件",
    "defense": "防守",
    "playmaking": "组织",
    "rebounding": "篮板",
    "salary": "薪资",
    "passing": "传球",
    "impact": "影响力",
    "shooting": "投篮",
}


def _app_module(api=None):
    """Catalog helpers from web.app, passed in explicitly via ``deps.catalog_helpers``."""
    deps = getattr(api, "deps", None) if api is not None else None
    return getattr(deps, "catalog_helpers", None)


def enrich(ctx: dict, api) -> dict:
    scope = (ctx.get("scope_filter") or "").strip()
    MD = api.models.MetricDefinition

    def build():
        with api.session_local() as session:
            q = session.query(MD.category, func.count(MD.id)).filter(
                MD.status == "published",
                MD.variant == "season",
                MD.category.isnot(None),
                MD.category != "",
            )
            if scope == "player":
                q = q.filter(MD.scope.in_(["player", "player_franchise"]))
            elif scope:
                q = q.filter(MD.scope == scope)
            rows = q.group_by(MD.category).all()
        out = [{"key": cat, "count": int(n), "label_zh": CATEGORY_LABELS_ZH.get(cat)} for cat, n in rows if cat]
        out.sort(key=lambda c: -c["count"])
        return out

    try:
        categories = api.cached(f"nx_metric_categories:{scope}", 600, build)
    except Exception:
        categories = []

    status = (ctx.get("status_filter") or "").strip()
    # The exact catalog total parses every metric's code (seconds when cold), so
    # never compute it during a page render: use the cached value when warm,
    # otherwise the page fetches /api/next/metrics/count and animates in.
    return {
        "categories": categories,
        "category_labels_zh": CATEGORY_LABELS_ZH,
        "catalog_total": _peek_total(scope, status),
        "catalog_total_url": f"/api/next/metrics/count?scope={scope}&status={status}",
    }


_TOTALS: dict[str, tuple[float, int]] = {}
_TOTAL_TTL = 3600.0


def _peek_total(scope: str, status: str) -> int | None:
    hit = _TOTALS.get(f"{scope}|{status}")
    if hit and time.monotonic() - hit[0] < _TOTAL_TTL:
        return hit[1]
    return None


def register(app, api) -> None:
    MD = api.models.MetricDefinition

    def api_next_metrics_catalog():
        A = _app_module(api)
        if A is None:
            return jsonify({"ok": False, "error": "unavailable"}), 503
        category = (request.args.get("category") or "").strip()
        scope = (request.args.get("scope") or "").strip()
        offset = max(0, request.args.get("offset", 0, type=int))
        limit = max(1, min(request.args.get("limit", 24, type=int), 48))
        user = A._current_user()
        user_id = user.id if user else None

        with api.session_local() as session:
            base = A._catalog_metric_base_query(session, scope_filter=scope, status_filter="")
            if category:
                base = base.filter(MD.category == category)
            ordered = A._catalog_metric_ordered_query(base, current_user_id=user_id)
            existing_keys = {key for (key,) in base.with_entities(MD.key).all()}
            page: list[dict] = []
            has_more = False
            index = 0
            for row in ordered.all():
                entries = A._catalog_metric_entries_for_row(
                    row, existing_keys=existing_keys, counts={}, current_user_id=user_id
                )
                for entry in entries:
                    if index >= offset:
                        if len(page) < limit:
                            page.append(entry)
                        else:
                            has_more = True
                            break
                    index += 1
                if has_more:
                    break
            top3 = A._catalog_top3(session, page) if page else {}

        html = render_template("next/_metrics_catalog_cards.html", metrics_list=page, top3_by_metric=top3)
        return jsonify({
            "ok": True,
            "html": html,
            "count": len(page),
            "offset": offset,
            "next_offset": offset + len(page),
            "has_more": has_more,
        })

    def api_next_metrics_count():
        scope = (request.args.get("scope") or "").strip()
        status = (request.args.get("status") or "").strip()
        cached = _peek_total(scope, status)
        if cached is not None:
            return jsonify({"ok": True, "total": cached})
        A = _app_module(api)
        if A is None:
            return jsonify({"ok": False, "error": "unavailable"}), 503
        with api.session_local() as session:
            total = int(A._catalog_metrics_total(session, scope_filter=scope, status_filter=status))
        _TOTALS[f"{scope}|{status}"] = (time.monotonic(), total)
        return jsonify({"ok": True, "total": total})

    limiter = getattr(api.deps, "limiter", None)
    view = limiter.limit("60 per minute")(api_next_metrics_catalog) if limiter else api_next_metrics_catalog
    app.add_url_rule("/api/next/metrics/catalog", endpoint="api_next_metrics_catalog", view_func=view)
    count_view = limiter.limit("30 per minute")(api_next_metrics_count) if limiter else api_next_metrics_count
    app.add_url_rule("/api/next/metrics/count", endpoint="api_next_metrics_count", view_func=count_view)
