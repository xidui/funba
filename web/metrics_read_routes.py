from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable

from flask import abort, jsonify, redirect, request, url_for


def register_metrics_read_routes(
    app,
    *,
    get_session_local: Callable[[], Any],
    get_render_template: Callable[..., Any],
    get_current_user: Callable[[], Any],
    get_catalog_metrics_page: Callable[..., Any],
    get_catalog_top3: Callable[..., Any],
    get_catalog_metrics_total: Callable[..., Any],
    get_feature_access_config: Callable[..., Any],
    get_build_metric_feature_context: Callable[..., Any],
    get_llm_model_for_purpose: Callable[..., Any],
    get_available_llm_models: Callable[[], Any],
    get_metric_definition_model: Callable[[], Any],
    get_game_model: Callable[[], Any],
    get_require_login_page: Callable[[], Any],
    get_require_metric_creator_page: Callable[[], Any],
    get_pick_current_season: Callable[[list[str]], str | None],
    get_family_variant_season: Callable[[], str],
    get_family_variant_career: Callable[[], str],
    get_family_base_key: Callable[[str], str],
    get_metrics_catalog_page_size: Callable[[], int],
):
    def metrics_browse():
        scope_filter = request.args.get("scope", "")
        status_filter = request.args.get("status", "")
        search_query = request.args.get("q", "").strip()

        cur_user = get_current_user()
        SessionLocal = get_session_local()
        with SessionLocal() as session:
            metrics_list, metrics_has_more = get_catalog_metrics_page()(
                session,
                scope_filter=scope_filter,
                status_filter=status_filter,
                current_user_id=cur_user.id if cur_user else None,
            )
            llm_default_model = get_llm_model_for_purpose()(session, "search")
            metrics_total = len(metrics_list) if not metrics_has_more else None
            top3_by_metric = get_catalog_top3()(session, metrics_list)
            feature_access = get_feature_access_config()(session)

        return get_render_template()(
            "metrics.html",
            metrics_list=metrics_list,
            metrics_total=metrics_total,
            metrics_has_more=metrics_has_more,
            metrics_page_size=get_metrics_catalog_page_size(),
            scope_filter=scope_filter,
            status_filter=status_filter,
            search_query=search_query,
            top3_by_metric=top3_by_metric,
            llm_default_model=llm_default_model,
            llm_available_models=get_available_llm_models()(),
            **get_build_metric_feature_context()(feature_access),
        )

    def api_metrics_catalog():
        scope_filter = request.args.get("scope", "")
        status_filter = request.args.get("status", "")
        offset = max(0, request.args.get("offset", 0, type=int))
        limit = request.args.get("limit", get_metrics_catalog_page_size(), type=int)
        limit = max(1, min(limit, 48))

        cur_user = get_current_user()
        SessionLocal = get_session_local()
        with SessionLocal() as session:
            metrics_slice, has_more = get_catalog_metrics_page()(
                session,
                scope_filter=scope_filter,
                status_filter=status_filter,
                current_user_id=cur_user.id if cur_user else None,
                offset=offset,
                limit=limit,
            )
            top3_by_metric = get_catalog_top3()(session, metrics_slice)

        html = get_render_template()(
            "_metrics_catalog_cards.html",
            metrics_list=metrics_slice,
            top3_by_metric=top3_by_metric,
        )
        next_offset = offset + len(metrics_slice)
        return jsonify(
            {
                "ok": True,
                "html": html,
                "count": len(metrics_slice),
                "offset": offset,
                "next_offset": next_offset,
                "has_more": has_more,
                "total": next_offset if not has_more else None,
            }
        )

    def api_metrics_catalog_count():
        scope_filter = request.args.get("scope", "")
        status_filter = request.args.get("status", "")
        SessionLocal = get_session_local()
        with SessionLocal() as session:
            total = get_catalog_metrics_total()(
                session,
                scope_filter=scope_filter,
                status_filter=status_filter,
            )
        return jsonify({"ok": True, "total": total})

    def my_metrics():
        denied = get_require_login_page()()
        if denied:
            return denied

        cur_user = get_current_user()
        if cur_user is None:
            return redirect(url_for("auth_login", next=request.url))

        SessionLocal = get_session_local()
        MetricDefinitionModel = get_metric_definition_model()
        with SessionLocal() as session:
            feature_access = get_feature_access_config()(session)
            drafts = (
                session.query(MetricDefinitionModel)
                .filter(
                    MetricDefinitionModel.created_by_user_id == cur_user.id,
                    MetricDefinitionModel.base_metric_key.is_(None),
                    MetricDefinitionModel.status == "draft",
                )
                .order_by(MetricDefinitionModel.updated_at.desc())
                .all()
            )
            published = (
                session.query(MetricDefinitionModel)
                .filter(
                    MetricDefinitionModel.created_by_user_id == cur_user.id,
                    MetricDefinitionModel.base_metric_key.is_(None),
                    MetricDefinitionModel.status.in_(["published", "disabled"]),
                )
                .order_by(MetricDefinitionModel.created_at.desc())
                .all()
            )

        return get_render_template()(
            "my_metrics.html",
            drafts=drafts,
            published=published,
            total_metrics=len(drafts) + len(published),
            scope_labels={
                "player": "Player",
                "player_franchise": "Player Franchise",
                "team": "Team",
                "game": "Game",
                "season": "Season",
            },
            **get_build_metric_feature_context()(feature_access),
        )

    def metric_new():
        denied = get_require_metric_creator_page()()
        if denied:
            return denied

        initial_expression = request.args.get("expression", "").strip()
        SessionLocal = get_session_local()
        Game = get_game_model()
        with SessionLocal() as session:
            all_seasons = sorted([row[0] for row in session.query(Game.season).distinct().all()], reverse=True)
            current_season = get_pick_current_season(all_seasons)
            llm_default_model = get_llm_model_for_purpose()(session, "generate")
            feature_access = get_feature_access_config()(session)

        return get_render_template()(
            "metric_new.html",
            current_season=current_season,
            all_seasons=all_seasons,
            initial_expression=initial_expression,
            edit_metric=None,
            llm_default_model=llm_default_model,
            llm_available_models=get_available_llm_models()(),
            **get_build_metric_feature_context()(feature_access),
        )

    def metric_edit(metric_key: str):
        denied = get_require_metric_creator_page()()
        if denied:
            return denied

        import json as _json  # kept for behavior parity / future edits

        SessionLocal = get_session_local()
        MetricDefinitionModel = get_metric_definition_model()
        Game = get_game_model()
        FAMILY_VARIANT_SEASON = get_family_variant_season()
        FAMILY_VARIANT_CAREER = get_family_variant_career()
        family_base_key = get_family_base_key

        with SessionLocal() as session:
            from metrics.framework.runtime import get_metric as _get_metric

            metric_row = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
            if metric_row is None:
                abort(404)
            if getattr(metric_row, "managed_family", False) and getattr(metric_row, "variant", FAMILY_VARIANT_SEASON) == FAMILY_VARIANT_CAREER:
                return redirect(
                    url_for(
                        "metric_edit",
                        metric_key=getattr(metric_row, "base_metric_key", None)
                        or getattr(metric_row, "family_key", None)
                        or family_base_key(metric_row.key),
                    )
                )

            all_seasons = sorted([row[0] for row in session.query(Game.season).distinct().all()], reverse=True)
            current_season = get_pick_current_season(all_seasons)
            runtime_metric = _get_metric(metric_key, session=session)

            edit_data = {
                "key": metric_row.key,
                "name": metric_row.name,
                "name_zh": metric_row.name_zh or "",
                "description": metric_row.description or "",
                "description_zh": metric_row.description_zh or "",
                "scope": metric_row.scope,
                "category": metric_row.category or "",
                "code": metric_row.code_python or "",
                "expression": metric_row.expression or "",
                "min_sample": metric_row.min_sample,
                "rank_order": getattr(runtime_metric, "rank_order", "desc"),
                "season_types": list(getattr(runtime_metric, "season_types", ("regular", "playoffs", "playin")) or ()),
                "max_results_per_season": getattr(runtime_metric, "max_results_per_season", None) or metric_row.max_results_per_season,
                "group_key": metric_row.group_key,
                "status": metric_row.status,
            }
            llm_default_model = get_llm_model_for_purpose()(session, "generate")
            feature_access = get_feature_access_config()(session)

        return get_render_template()(
            "metric_new.html",
            current_season=current_season,
            all_seasons=all_seasons,
            initial_expression="",
            edit_metric=edit_data,
            llm_default_model=llm_default_model,
            llm_available_models=get_available_llm_models()(),
            **get_build_metric_feature_context()(feature_access),
        )

    app.add_url_rule("/cn/metrics", endpoint="metrics_browse_zh", view_func=metrics_browse)
    app.add_url_rule("/metrics", endpoint="metrics_browse", view_func=metrics_browse)
    app.add_url_rule("/api/metrics/catalog", endpoint="api_metrics_catalog", view_func=api_metrics_catalog)
    app.add_url_rule("/api/metrics/catalog-count", endpoint="api_metrics_catalog_count", view_func=api_metrics_catalog_count)
    app.add_url_rule("/cn/metrics/mine", endpoint="my_metrics_zh", view_func=my_metrics)
    app.add_url_rule("/metrics/mine", endpoint="my_metrics", view_func=my_metrics)
    app.add_url_rule("/cn/metrics/new", endpoint="metric_new_zh", view_func=metric_new)
    app.add_url_rule("/metrics/new", endpoint="metric_new", view_func=metric_new)
    app.add_url_rule("/cn/metrics/<metric_key>/edit", endpoint="metric_edit_zh", view_func=metric_edit)
    app.add_url_rule("/metrics/<metric_key>/edit", endpoint="metric_edit", view_func=metric_edit)

    return SimpleNamespace(
        metrics_browse=metrics_browse,
        api_metrics_catalog=api_metrics_catalog,
        api_metrics_catalog_count=api_metrics_catalog_count,
        my_metrics=my_metrics,
        metric_new=metric_new,
        metric_edit=metric_edit,
    )
