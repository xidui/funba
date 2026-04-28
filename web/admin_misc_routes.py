from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from types import SimpleNamespace

from flask import abort, jsonify, redirect, request, url_for
from sqlalchemy import and_, case, extract, func, not_, or_


def _format_schedule_interval(schedule) -> str:
    if isinstance(schedule, (int, float)):
        seconds = int(schedule)
        if seconds >= 3600 and seconds % 3600 == 0:
            return f"{seconds // 3600}h"
        if seconds >= 60 and seconds % 60 == 0:
            return f"{seconds // 60}m"
        return f"{seconds}s"

    minute = getattr(schedule, "_orig_minute", None)
    hour = getattr(schedule, "_orig_hour", None)
    day_of_month = getattr(schedule, "_orig_day_of_month", None)
    month_of_year = getattr(schedule, "_orig_month_of_year", None)
    day_of_week = getattr(schedule, "_orig_day_of_week", None)

    if (
        minute is not None
        and hour is not None
        and day_of_month is not None
        and month_of_year is not None
        and day_of_week is not None
    ):
        minute_text = str(minute)
        hour_text = str(hour)
        dom_text = str(day_of_month)
        moy_text = str(month_of_year)
        dow_text = str(day_of_week)

        if dom_text == moy_text == dow_text == "*" and minute_text.isdigit() and hour_text.isdigit():
            return f"daily {int(hour_text):02d}:{int(minute_text):02d}"

        return f"cron {minute_text} {hour_text} {dom_text} {moy_text} {dow_text}"

    return str(schedule or "?")


def register_admin_misc_routes(app, deps):
    def api_data_games():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        date_str = request.args.get("date")
        if not date_str:
            return jsonify({"error": "date required"}), 400
        from tasks.topics import get_games_by_date

        result = get_games_by_date(date.fromisoformat(date_str))
        return jsonify({"date": date_str, "games": result})

    def api_data_boxscore(game_id: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from tasks.topics import get_game_box_score

        return jsonify(get_game_box_score(game_id))

    def api_data_pbp(game_id: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        period = int(request.args.get("period", 4))
        from tasks.topics import get_game_play_by_play

        return jsonify({"game_id": game_id, "period": period, "plays": get_game_play_by_play(game_id, period)})

    def api_data_game_metrics(game_id: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        payload = deps.build_game_metrics_payload()(game_id)
        return jsonify(payload)

    def api_data_metric_top(metric_key: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        season = request.args.get("season")
        limit = min(int(request.args.get("limit", 10)), 100)
        from tasks.topics import get_metric_top_results

        return jsonify({"metric_key": metric_key, "results": get_metric_top_results(metric_key, season, limit)})

    def api_data_triggered_metrics():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        date_str = request.args.get("date")
        if not date_str:
            return jsonify({"error": "date required"}), 400
        from tasks.topics import get_triggered_metrics

        result = get_triggered_metrics(date.fromisoformat(date_str))
        return jsonify({"date": date_str, "metrics": result})

    def admin_fragment(section: str):
        denied = deps.require_admin_page()()
        if denied:
            return denied

        section = (section or "").strip().lower()
        runs_page_size = 25
        recent_page_size = 25
        perf_page_size = 20

        SessionLocal = deps.session_local()
        Game = deps.game_model()
        User = deps.user_model()
        PageView = deps.page_view_model()
        human_page_view_filter = deps.human_page_view_filter()
        PlayerGameStats = deps.player_game_stats_model()
        ShotRecord = deps.shot_record_model()
        MetricRunLog = deps.metric_run_log_model()
        with SessionLocal() as session:
            if section == "visitor-stats":
                now_dt = datetime.utcnow()
                cutoff_24h = now_dt - timedelta(hours=24)
                cutoff_7d = now_dt - timedelta(days=7)
                cutoff_30d = now_dt - timedelta(days=30)

                def _verified_unique(cutoff):
                    """Visitors that either had an external referrer or a multi-page session."""
                    external_ref = and_(
                        PageView.referrer.isnot(None),
                        PageView.referrer != "",
                        not_(PageView.referrer.like("http%://funba.app%")),
                        not_(PageView.referrer.like("http%://www.funba.app%")),
                    )
                    external_visitors = (
                        session.query(PageView.visitor_id)
                        .filter(PageView.created_at >= cutoff, human_page_view_filter(PageView), external_ref)
                        .distinct()
                    )
                    multi_pv_visitors = (
                        session.query(PageView.visitor_id)
                        .filter(PageView.created_at >= cutoff, human_page_view_filter(PageView))
                        .group_by(PageView.visitor_id)
                        .having(func.count(PageView.id) >= 2)
                    )
                    return session.query(func.count(func.distinct(PageView.visitor_id))).filter(
                        PageView.created_at >= cutoff,
                        human_page_view_filter(PageView),
                        or_(
                            PageView.visitor_id.in_(external_visitors),
                            PageView.visitor_id.in_(multi_pv_visitors),
                        ),
                    ).scalar() or 0

                visitor_stats = {
                    "user_count": session.query(func.count(User.id)).scalar() or 0,
                    "views_24h": session.query(func.count(PageView.id)).filter(PageView.created_at >= cutoff_24h, human_page_view_filter(PageView)).scalar() or 0,
                    "views_7d": session.query(func.count(PageView.id)).filter(PageView.created_at >= cutoff_7d, human_page_view_filter(PageView)).scalar() or 0,
                    "views_30d": session.query(func.count(PageView.id)).filter(PageView.created_at >= cutoff_30d, human_page_view_filter(PageView)).scalar() or 0,
                    "unique_24h": session.query(func.count(func.distinct(PageView.visitor_id))).filter(PageView.created_at >= cutoff_24h, human_page_view_filter(PageView)).scalar() or 0,
                    "unique_7d": session.query(func.count(func.distinct(PageView.visitor_id))).filter(PageView.created_at >= cutoff_7d, human_page_view_filter(PageView)).scalar() or 0,
                    "unique_30d": session.query(func.count(func.distinct(PageView.visitor_id))).filter(PageView.created_at >= cutoff_30d, human_page_view_filter(PageView)).scalar() or 0,
                    "verified_24h": _verified_unique(cutoff_24h),
                    "verified_7d": _verified_unique(cutoff_7d),
                    "verified_30d": _verified_unique(cutoff_30d),
                }
                return deps.render_template()("_admin_visitor_stats.html", visitor_stats=visitor_stats)

            if section == "top-pages":
                panel = deps.load_admin_top_pages_panel()(session, request.args.get("window"))
                return deps.render_template()(
                    "_admin_top_pages.html",
                    selected_window=panel["selected_window"],
                    top_pages=panel["top_pages"],
                    top_referrers=panel["top_referrers"],
                )

            if section == "coverage":
                now = deps.time_module().time()
                admin_cache = deps.admin_cache()
                if "coverage" not in admin_cache or now - admin_cache.get("ts", 0) > deps.admin_cache_ttl():
                    from sqlalchemy import text as sa_text

                    coverage_rows = session.execute(
                        sa_text(
                            """
                    SELECT
                        g.season,
                        COUNT(*)           AS total,
                        COUNT(box.game_id) AS has_detail,
                        COUNT(pbp.game_id) AS has_pbp,
                        COUNT(gls.game_id) AS has_line,
                        COUNT(sr.game_id)  AS has_shot,
                        COUNT(pps.game_id) AS has_period,
                        COUNT(mrl.game_id) AS has_metrics,
                        0                  AS active_claims
                    FROM Game g
                    LEFT JOIN (
                        SELECT DISTINCT game_id FROM TeamGameStats
                        UNION
                        SELECT DISTINCT game_id FROM PlayerGameStats
                    ) box ON box.game_id = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM GamePlayByPlay)         pbp ON pbp.game_id = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM GameLineScore)          gls ON gls.game_id = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM ShotRecord)             sr  ON sr.game_id  = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM PlayerGamePeriodStats)  pps ON pps.game_id = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM MetricRunLog)           mrl ON mrl.game_id = g.game_id
                    WHERE g.game_date IS NOT NULL
                    GROUP BY g.season
                    ORDER BY g.season DESC
                """
                        )
                    ).fetchall()
                    coverage_source_rows = session.execute(
                        sa_text(
                            """
                    SELECT
                        g.season,
                        COALESCE(g.data_source, 'unknown') AS data_source,
                        COUNT(DISTINCT g.game_id) AS detail_games
                    FROM Game g
                    JOIN (
                        SELECT DISTINCT game_id FROM TeamGameStats
                        UNION
                        SELECT DISTINCT game_id FROM PlayerGameStats
                    ) box ON box.game_id = g.game_id
                    WHERE g.game_date IS NOT NULL
                    GROUP BY g.season, COALESCE(g.data_source, 'unknown')
                    ORDER BY g.season DESC, data_source ASC
                """
                        )
                    ).fetchall()
                    admin_cache["coverage"] = {"rows": coverage_rows, "sources": coverage_source_rows}
                    admin_cache["ts"] = now
                else:
                    cached_coverage = admin_cache["coverage"]
                    if isinstance(cached_coverage, list):
                        coverage_rows = cached_coverage
                        coverage_source_rows = []
                    else:
                        coverage_rows = cached_coverage["rows"]
                        coverage_source_rows = cached_coverage["sources"]

                from collections import defaultdict

                source_counts_by_season: dict[str, list[dict[str, object]]] = defaultdict(list)
                for row in coverage_source_rows:
                    season_key = str(row.season)
                    source_counts_by_season[season_key].append(
                        {
                            "source": row.data_source,
                            "label": deps.box_score_source_label()(row.data_source),
                            "count": int(row.detail_games or 0),
                        }
                    )
                coverage = [
                    {
                        "season": deps.season_label()(row.season),
                        "season_raw": row.season,
                        "total": row.total,
                        "detail": row.has_detail,
                        "detail_sources": source_counts_by_season.get(str(row.season), []),
                        "detail_remaining": max(int(row.total or 0) - int(row.has_detail or 0), 0),
                        "pbp": row.has_pbp,
                        "line": row.has_line,
                        "shot": row.has_shot,
                        "period": row.has_period,
                        "metrics": row.has_metrics,
                        "active_claims": row.active_claims,
                        "complete": row.total == row.has_detail == row.has_pbp == row.has_shot == row.has_period == row.has_metrics,
                    }
                    for row in coverage_rows
                ]
                return deps.render_template()("_admin_coverage.html", coverage=coverage)

            if section == "compute-runs":
                panel = deps.load_admin_compute_runs_panel()(session, runs_page=deps.admin_page_arg()("runs_page"), runs_page_size=runs_page_size)
                return deps.render_template()(
                    "_admin_compute_runs_card.html",
                    compute_run_counts=panel["compute_run_counts"],
                    compute_runs=panel["compute_runs"],
                    runs_page=panel["runs_page"],
                    runs_total_pages=panel["runs_total_pages"],
                    admin_page_url=deps.admin_page_url(),
                    admin_fragment_url=deps.admin_fragment_url(),
                )

            if section == "recent-runs":
                panel = deps.load_admin_recent_runs_panel()(session, recent_page=deps.admin_page_arg()("recent_page"), recent_page_size=recent_page_size)
                return deps.render_template()(
                    "_admin_recent_runs_card.html",
                    recent=panel["recent"],
                    recent_page=panel["recent_page"],
                    recent_has_prev=panel["recent_has_prev"],
                    recent_has_next=panel["recent_has_next"],
                    admin_page_url=deps.admin_page_url(),
                    admin_fragment_url=deps.admin_fragment_url(),
                )

            if section == "metric-perf":
                panel = deps.load_admin_metric_perf_panel()(session, perf_page=deps.admin_page_arg()("perf_page"), perf_page_size=perf_page_size)
                return deps.render_template()(
                    "_admin_metric_perf.html",
                    perf_data=panel["perf_data"],
                    perf_page=panel["perf_page"],
                    perf_total_pages=panel["perf_total_pages"],
                    perf_has_prev=panel["perf_has_prev"],
                    perf_has_next=panel["perf_has_next"],
                    admin_page_url=deps.admin_page_url(),
                    admin_fragment_url=deps.admin_fragment_url(),
                )

            if section == "missing":
                season_filter = Game.season.like("22024%") | Game.season.like("22025%")

                def _missing(joined_model, joined_col, limit=20):
                    rows = (
                        session.query(Game.game_id, Game.game_date, Game.season)
                        .outerjoin(joined_model, joined_col == Game.game_id)
                        .filter(season_filter, Game.game_date.isnot(None), joined_col.is_(None))
                        .order_by(Game.game_date)
                        .limit(limit + 1)
                        .all()
                    )
                    overflow = len(rows) > limit
                    rows = rows[:limit]
                    total = len(rows) + (1 if overflow else 0)
                    return {
                        "total": total,
                        "overflow": overflow,
                        "rows": [{"game_id": r.game_id, "game_date": r.game_date, "season": deps.season_label()(r.season)} for r in rows],
                    }

                return deps.render_template()(
                    "_admin_missing.html",
                    missing_detail=_missing(PlayerGameStats, PlayerGameStats.game_id),
                    missing_shot=_missing(ShotRecord, ShotRecord.game_id),
                    missing_metrics=_missing(MetricRunLog, MetricRunLog.game_id),
                )

        abort(404)

    def api_admin_infra_status():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        queues = []
        broker_ok = False
        try:
            import redis as _redis

            r = _redis.Redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"), socket_timeout=2)
            r.ping()
            broker_ok = True
        except Exception:
            pass

        workers = []
        try:
            from tasks.celery_app import app as celery_app

            inspector = celery_app.control.inspect(timeout=1.5)
            ping_result = celery_app.control.ping(timeout=1.5)
            active_queues = inspector.active_queues() or {}
            stats = inspector.stats() or {}
            active_tasks = inspector.active() or {}

            pinged = set()
            for entry in ping_result:
                for worker_name in entry:
                    pinged.add(worker_name)

            for worker_name in pinged:
                wq = active_queues.get(worker_name, [])
                queue_names = sorted(set(q["name"] for q in wq if not q["name"].endswith(".pidbox")))
                ws = stats.get(worker_name, {})
                pool = ws.get("pool", {})
                concurrency = pool.get("max-concurrency", None)
                active_count = len(active_tasks.get(worker_name, []))
                role = ", ".join(queue_names) if queue_names else "unknown"
                workers.append({"name": worker_name, "role": role, "concurrency": concurrency, "active": active_count, "ok": True})
            workers.sort(key=lambda w: w["role"])

            if broker_ok:
                import redis as _redis

                r = _redis.Redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"), socket_timeout=2)
                consumer_count: dict[str, int] = {}
                for wq_list in active_queues.values():
                    for q in wq_list:
                        qn = q.get("name", "")
                        if not qn.endswith(".pidbox"):
                            consumer_count[qn] = consumer_count.get(qn, 0) + 1
                for qname in ("ingest", "metrics", "reduce"):
                    length = r.llen(qname) or 0
                    queues.append({"name": qname, "ready": length, "unacked": 0, "consumers": consumer_count.get(qname, 0)})
        except Exception:
            pass

        scheduled = []
        try:
            from tasks.celery_app import app as _celery_app

            for name, entry in (_celery_app.conf.beat_schedule or {}).items():
                schedule = entry.get("schedule", "")
                scheduled.append(
                    {
                        "name": name,
                        "task": entry.get("task", ""),
                        "every": int(schedule) if isinstance(schedule, (int, float)) else None,
                        "display": _format_schedule_interval(schedule),
                    }
                )
        except Exception:
            pass

        return jsonify({"ok": True, "broker_ok": broker_ok, "queues": queues, "workers": workers, "scheduled": scheduled})

    def api_admin_feature_access():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            return jsonify({"ok": True, "features": deps.serialize_feature_access()(session)})

    def api_admin_update_feature_access():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        body = request.get_json(force=True) or {}
        try:
            SessionLocal = deps.session_local()
            with SessionLocal() as session:
                updated = {}
                for descriptor in deps.feature_access_descriptors()():
                    feature_key = descriptor["key"]
                    if feature_key in body:
                        updated[feature_key] = deps.set_feature_access_level()(session, feature_key, body[feature_key])
                session.commit()
                features = deps.serialize_feature_access()(session)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("failed to save feature access config")
            return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, "updated": updated, "features": features})

    def api_admin_model_config():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            payload = {
                "default_model": deps.get_default_llm_model_for_ui()(session),
                "search_model": deps.get_llm_model_for_purpose()(session, "search"),
                "generate_model": deps.get_llm_model_for_purpose()(session, "generate"),
                "curator_model": deps.get_llm_model_for_purpose()(session, "curator"),
                "available_models": deps.available_llm_models()(),
            }
            models_meta = getattr(deps, "available_llm_models_meta", None)
            if callable(models_meta):
                payload["available_models_meta"] = models_meta()()
            get_curator_reasoning = getattr(deps, "get_curator_reasoning_effort", None)
            if callable(get_curator_reasoning):
                payload["curator_reasoning"] = get_curator_reasoning()(session)
            available_reasoning = getattr(deps, "available_reasoning_efforts", None)
            if callable(available_reasoning):
                payload["available_reasoning_efforts"] = list(available_reasoning()())
            return jsonify(payload)

    def api_admin_paperclip_config():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            return jsonify({"ok": True, "issue_base_url": deps.get_paperclip_issue_base_url()(session)})

    def api_admin_update_paperclip_config():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        body = request.get_json(force=True) or {}
        try:
            SessionLocal = deps.session_local()
            with SessionLocal() as session:
                issue_base_url = deps.set_paperclip_issue_base_url()(session, body.get("issue_base_url"))
                session.commit()
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("failed to save paperclip config")
            return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, "issue_base_url": issue_base_url})

    def api_admin_update_model_config():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        body = request.get_json(force=True) or {}
        try:
            SessionLocal = deps.session_local()
            with SessionLocal() as session:
                result = {}
                if "search_model" in body:
                    result["search_model"] = deps.set_llm_model_for_purpose()(session, "search", body["search_model"])
                if "generate_model" in body:
                    result["generate_model"] = deps.set_llm_model_for_purpose()(session, "generate", body["generate_model"])
                if "curator_model" in body:
                    result["curator_model"] = deps.set_llm_model_for_purpose()(session, "curator", body["curator_model"])
                if "curator_reasoning" in body:
                    setter = getattr(deps, "set_curator_reasoning_effort", None)
                    if callable(setter):
                        result["curator_reasoning"] = setter()(session, body["curator_reasoning"])
                if "default_model" in body:
                    result["default_model"] = deps.set_default_llm_model()(session, body["default_model"])
                session.commit()
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("failed to save admin model config")
            return jsonify({"ok": False, "error": str(exc)}), 500
        response_payload = {"ok": True, **result, "available_models": deps.available_llm_models()()}
        models_meta = getattr(deps, "available_llm_models_meta", None)
        if callable(models_meta):
            response_payload["available_models_meta"] = models_meta()()
        return jsonify(response_payload)

    def api_admin_hero_poster_config():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from social_media.hero_poster import (
            DEFAULT_HERO_POSTER_PROMPT_TEMPLATE,
            HERO_POSTER_DEFAULT_MODEL,
            get_hero_poster_model,
            get_hero_poster_prompt_template,
        )

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            template = get_hero_poster_prompt_template(session)
            model = get_hero_poster_model(session)
        return jsonify({
            "ok": True,
            "template": template,
            "default_template": DEFAULT_HERO_POSTER_PROMPT_TEMPLATE,
            "model": model,
            "default_model": HERO_POSTER_DEFAULT_MODEL,
            "placeholders": [
                {"key": "{metric_key}", "desc": "Metric definition key (e.g. best_single_game_blk_per_game)"},
                {"key": "{metric_name}", "desc": "Human-readable metric name"},
                {"key": "{metric_description}", "desc": "Description from MetricDefinition"},
                {"key": "{metric_scope}", "desc": "player | team | game"},
                {"key": "{metric_category}", "desc": "Metric category"},
                {"key": "{season_label}", "desc": "Pretty season label, e.g. '2025-26 NBA Playoffs'"},
                {"key": "{game_score_line}", "desc": "Final scoreline, e.g. 'SAS 114 @ POR 93'"},
                {"key": "{game_date}", "desc": "Formatted game date"},
                {"key": "{game_stage}", "desc": "playoffs | regular season | play-in"},
                {"key": "{game_stage_pill}", "desc": "Compact uppercase pill, e.g. 'PLAYOFFS · APR 26 2026'"},
                {"key": "{trigger_label}", "desc": "Triggering entity name (player, team, game)"},
                {"key": "{trigger_team_full}", "desc": "Full team name of trigger row"},
                {"key": "{trigger_team_abbr}", "desc": "Three-letter team abbr of trigger row"},
                {"key": "{trigger_value_str}", "desc": "Trigger value as string (e.g. '7 blk')"},
                {"key": "{trigger_rank}", "desc": "Trigger row's actual rank in the season"},
                {"key": "{trigger_window}", "desc": "Best ranking window: alltime | season | last5 | last3"},
                {"key": "{trigger_full_line}", "desc": "Player's full game line if scope=player (PTS · REB · AST · …)"},
                {"key": "{trigger_in_topn}", "desc": "Bool — true when trigger is within top N (use in {% if %} blocks)"},
                {"key": "{trigger_appendix_row}", "desc": "Pre-formatted extra row when trigger is outside top N"},
                {"key": "{top_n_table}", "desc": "Top N rows already formatted as plain text"},
                {"key": "{top_n}", "desc": "Number of leaderboard rows (currently 10)"},
                {"key": "{title_line_1}", "desc": "Suggested poster title line 1 (uppercase metric name)"},
                {"key": "{title_line_2}", "desc": "Suggested poster title line 2 (season label · TOP N)"},
                {"key": "{entity_kind}", "desc": "player | team — for visual asset hints"},
            ],
        })

    def api_admin_update_hero_poster_config():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from social_media.hero_poster import (
            DEFAULT_HERO_POSTER_PROMPT_TEMPLATE,
            HERO_POSTER_MODEL_KEY,
            HERO_POSTER_PROMPT_TEMPLATE_KEY,
            set_hero_poster_prompt_template,
        )
        from datetime import datetime as _dt

        from db.models import Setting

        body = request.get_json(force=True) or {}
        SessionLocal = deps.session_local()
        try:
            with SessionLocal() as session:
                result = {}
                if "template" in body:
                    template = body["template"]
                    if template is None or str(template).strip() == "":
                        # Treat empty as "reset to default"
                        row = session.get(Setting, HERO_POSTER_PROMPT_TEMPLATE_KEY)
                        if row is not None:
                            session.delete(row)
                        result["template"] = DEFAULT_HERO_POSTER_PROMPT_TEMPLATE
                    else:
                        result["template"] = set_hero_poster_prompt_template(session, str(template))
                if "model" in body:
                    model = str(body["model"] or "").strip()
                    row = session.get(Setting, HERO_POSTER_MODEL_KEY)
                    if not model:
                        if row is not None:
                            session.delete(row)
                        result["model"] = None
                    else:
                        if row is None:
                            session.add(Setting(key=HERO_POSTER_MODEL_KEY, value=model, updated_at=_dt.utcnow()))
                        else:
                            row.value = model
                            row.updated_at = _dt.utcnow()
                        result["model"] = model
                session.commit()
        except Exception as exc:
            deps.logger().exception("failed to save hero poster config")
            return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, **result})

    def _resolve_game_lookup(session, ref: str):
        """Accept either a slug (e.g. '20260426-sas-por') or a raw game_id."""
        from db.models import Game

        ref = (ref or "").strip()
        if not ref:
            return None
        return (
            session.query(Game).filter(Game.game_id == ref).first()
            or session.query(Game).filter(Game.slug == ref).first()
        )

    def api_admin_hero_poster_preview():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from social_media.hero_poster import (
            build_prompt_context,
            get_hero_poster_prompt_template,
            render_prompt,
        )

        body = request.get_json(force=True) or {}
        game_ref = str(body.get("game_id") or body.get("game_slug") or body.get("game") or "").strip()
        metric_key = str(body.get("metric_key") or "").strip()
        scope = str(body.get("scope") or "game").strip() or "game"
        entity_id = str(body.get("entity_id") or "").strip() or None
        if not game_ref or not metric_key:
            return jsonify({"ok": False, "error": "game (slug or game_id) and metric_key required"}), 400

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            game = _resolve_game_lookup(session, game_ref)
            if game is None:
                return jsonify({"ok": False, "error": "game_not_found"}), 404
            template_override = body.get("template")
            template = template_override if template_override else get_hero_poster_prompt_template(session)
            card = {"metric_key": metric_key, "scope": scope, "entity_id": entity_id}
            ctx = build_prompt_context(session, card=card, game=game)
            rendered = render_prompt(template, ctx)
        return jsonify({"ok": True, "context": ctx, "prompt": rendered, "resolved_game_id": game.game_id})

    def api_admin_hero_poster_regenerate():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from social_media.hero_poster import generate_posters_for_curated_game

        body = request.get_json(force=True) or {}
        game_ref = str(body.get("game_id") or body.get("game_slug") or body.get("game") or "").strip()
        force = bool(body.get("force") or False)
        if not game_ref:
            return jsonify({"ok": False, "error": "game (slug or game_id) required"}), 400

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            game = _resolve_game_lookup(session, game_ref)
            if game is None:
                return jsonify({"ok": False, "error": "game_not_found"}), 404
            try:
                paths = generate_posters_for_curated_game(session, game, force=force)
            except Exception as exc:
                deps.logger().exception("hero poster regenerate failed for %s", game_ref)
                return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, "game_id": game.game_id, "game_slug": game.slug, "paths": [str(p) for p in paths]})

    def admin_assets():
        denied = deps.require_admin_page()()
        if denied:
            return denied
        return deps.render_template()("admin_assets.html")

    def admin_asset_detail(image_id: int):
        denied = deps.require_admin_page()()
        if denied:
            return denied
        return deps.render_template()("admin_asset_detail.html", image_id=int(image_id))

    def _build_asset_view(session, img):
        """Shape one SocialPostImage row into the JSON the assets pages render."""
        import json as _json
        from datetime import datetime as _dt
        from os.path import basename
        from pathlib import Path as _Path
        from db.models import Game, MetricDefinition, Player, SocialPost, Team

        spec = {}
        if img.spec:
            try:
                spec = _json.loads(img.spec) or {}
            except Exception:
                spec = {}

        post = session.query(SocialPost).filter(SocialPost.id == img.post_id).first()

        # Resolve game / metric / entity from spec, falling back to topic parsing.
        game_id = str(spec.get("game_id") or "")
        scope = str(spec.get("scope") or "")
        metric_key = str(spec.get("metric_key") or "")
        entity_id = str(spec.get("entity_id") or "")
        if (not game_id or not scope) and post is not None and post.topic:
            parts = [p.strip() for p in str(post.topic).split("—")]
            if len(parts) >= 5 and parts[0] == "Hero Highlight":
                game_id = game_id or parts[1]
                scope = scope or parts[2]
                metric_key = metric_key or parts[3]
                entity_id = entity_id or parts[4]

        matchup_text = ""
        game_url = ""
        if game_id:
            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game is not None:
                home = session.query(Team).filter(Team.team_id == game.home_team_id).first() if game.home_team_id else None
                road = session.query(Team).filter(Team.team_id == game.road_team_id).first() if game.road_team_id else None
                home_abbr = home.abbr if home else "?"
                road_abbr = road.abbr if road else "?"
                if game.home_team_score is not None and game.road_team_score is not None:
                    matchup_text = f"{road_abbr} {game.road_team_score} @ {home_abbr} {game.home_team_score}"
                else:
                    matchup_text = f"{road_abbr} @ {home_abbr}"
                slug = game.slug or game.game_id
                game_url = f"/games/{slug}"

        metric_name = ""
        metric_url = ""
        if metric_key:
            metric_url = f"/metrics/{metric_key}"
            md = session.query(MetricDefinition).filter(MetricDefinition.key == metric_key).first()
            metric_name = (md.name if md and md.name else metric_key.replace("_", " ").title())

        entity_label = ""
        if scope == "player" and entity_id:
            p = session.query(Player).filter(Player.player_id == entity_id).first()
            entity_label = p.full_name if p and p.full_name else entity_id
        elif scope == "team" and entity_id:
            tid = entity_id.split(":")[1] if ":" in entity_id else entity_id
            t = session.query(Team).filter(Team.team_id == tid).first()
            entity_label = t.full_name if t and t.full_name else entity_id

        # Try sidecar prompt if not in spec.
        prompt = str(spec.get("prompt") or "")
        if not prompt:
            try:
                from social_media.hero_poster import read_prompt_sidecar

                source_path = spec.get("source_poster_path") or img.file_path
                if source_path:
                    prompt = read_prompt_sidecar(source_path) or ""
            except Exception:
                prompt = ""

        url = None
        size_kb = None
        if img.file_path:
            try:
                fname = basename(str(img.file_path))
                url = f"/media/social_posts/{int(img.post_id)}/{fname}"
                fpath = _Path(str(img.file_path))
                if fpath.exists():
                    size_kb = round(fpath.stat().st_size / 1024)
            except Exception:
                pass

        created_human = ""
        if img.created_at:
            try:
                ago = _dt.utcnow() - img.created_at
                hours = int(ago.total_seconds() // 3600)
                if hours < 1:
                    created_human = "just now"
                elif hours < 24:
                    created_human = f"{hours}h ago"
                else:
                    created_human = f"{hours // 24}d ago"
            except Exception:
                created_human = ""

        return {
            "id": int(img.id),
            "post_id": int(img.post_id),
            "slot": img.slot,
            "image_type": img.image_type,
            "is_enabled": bool(img.is_enabled),
            "url": url,
            "file_path": img.file_path,
            "size_kb": size_kb,
            "created_at": img.created_at.isoformat() if img.created_at else None,
            "created_at_human": created_human,
            "scope": scope,
            "metric_key": metric_key,
            "metric_name": metric_name,
            "metric_url": metric_url,
            "matchup_text": matchup_text,
            "game_id": game_id,
            "game_url": game_url,
            "entity_id": entity_id,
            "entity_label": entity_label,
            "value_text": spec.get("value_text") or "",
            "rank_text": spec.get("rank_text") or "",
            "model": spec.get("model") or "gpt-image-2",
            "matchup": matchup_text,
            "prompt": prompt,
        }

    def api_admin_assets_list():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from db.models import Game, SocialPost, SocialPostImage

        page = max(1, request.args.get("page", 1, type=int))
        page_size = 60
        game_filter = (request.args.get("game") or "").strip()
        metric_filter = (request.args.get("metric") or "").strip()
        scope_filter = (request.args.get("scope") or "").strip()
        status_filter = (request.args.get("status") or "").strip()

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            q = (
                session.query(SocialPostImage)
                .filter(SocialPostImage.image_type == "ai_generated")
            )
            if game_filter:
                # accept slug or raw game_id; resolve slug → id when possible
                game = (
                    session.query(Game).filter(Game.game_id == game_filter).first()
                    or session.query(Game).filter(Game.slug == game_filter).first()
                )
                gid = game.game_id if game else game_filter
                q = q.join(SocialPost, SocialPost.id == SocialPostImage.post_id).filter(
                    SocialPost.source_game_ids.like(f"%{gid}%")
                )
            if metric_filter:
                q = q.filter(SocialPostImage.spec.like(f"%{metric_filter}%"))
            if scope_filter:
                q = q.filter(SocialPostImage.spec.like(f'%"scope": "{scope_filter}"%'))
            if status_filter == "enabled":
                q = q.filter(SocialPostImage.is_enabled.is_(True))
            elif status_filter == "disabled":
                q = q.filter(SocialPostImage.is_enabled.is_(False))

            total = q.count()
            import math

            total_pages = max(1, math.ceil(total / page_size))
            page = min(page, total_pages)
            imgs = (
                q.order_by(SocialPostImage.created_at.desc(), SocialPostImage.id.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
                .all()
            )
            items = [_build_asset_view(session, img) for img in imgs]
        return jsonify({
            "ok": True,
            "items": items,
            "total": int(total),
            "page": int(page),
            "total_pages": int(total_pages),
            "page_size": int(page_size),
        })

    def api_admin_asset_detail(image_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from db.models import SocialPostImage

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            img = session.query(SocialPostImage).filter(SocialPostImage.id == int(image_id)).first()
            if img is None:
                return jsonify({"ok": False, "error": "asset_not_found"}), 404
            return jsonify({"ok": True, "meta": _build_asset_view(session, img)})

    def api_admin_asset_replace(image_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        import shutil as _shutil
        from datetime import datetime as _dt
        from pathlib import Path as _Path

        from db.models import SocialPostImage

        upload = request.files.get("file")
        if upload is None or not upload.filename:
            return jsonify({"ok": False, "error": "no_file"}), 400
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            img = session.query(SocialPostImage).filter(SocialPostImage.id == int(image_id)).first()
            if img is None or not img.file_path:
                return jsonify({"ok": False, "error": "asset_not_found"}), 404
            dst = _Path(str(img.file_path))
            try:
                dst.parent.mkdir(parents=True, exist_ok=True)
                upload.save(str(dst))
                # Mark this row as human-replaced so future audits can tell.
                img.image_type = "human_replaced"
                img.review_decision = "keep"
                img.review_source = "human_reviewer"
                img.reviewed_at = _dt.utcnow()
                session.commit()
            except Exception as exc:
                deps.logger().exception("asset replace failed for image_id=%s", image_id)
                return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, "image_id": int(image_id)})

    def api_admin_asset_regenerate(image_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        import json as _json
        from pathlib import Path as _Path

        from db.models import Game, SocialPost, SocialPostImage

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            img = session.query(SocialPostImage).filter(SocialPostImage.id == int(image_id)).first()
            if img is None:
                return jsonify({"ok": False, "error": "asset_not_found"}), 404
            spec = {}
            if img.spec:
                try:
                    spec = _json.loads(img.spec) or {}
                except Exception:
                    spec = {}
            game_id = spec.get("game_id")
            scope = spec.get("scope")
            metric_key = spec.get("metric_key")
            entity_id = spec.get("entity_id")
            if not (game_id and scope and metric_key):
                # Try parsing topic
                post = session.query(SocialPost).filter(SocialPost.id == img.post_id).first()
                if post and post.topic:
                    parts = [p.strip() for p in str(post.topic).split("—")]
                    if len(parts) >= 5 and parts[0] == "Hero Highlight":
                        game_id = game_id or parts[1]
                        scope = scope or parts[2]
                        metric_key = metric_key or parts[3]
                        entity_id = entity_id or parts[4]
            if not (game_id and scope and metric_key):
                return jsonify({"ok": False, "error": "missing_metadata"}), 400

            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game is None:
                return jsonify({"ok": False, "error": "game_not_found"}), 404

            try:
                from social_media.hero_poster import generate_hero_poster, poster_path_for
                import shutil as _shutil

                src = generate_hero_poster(
                    session,
                    card={"metric_key": metric_key, "scope": scope, "entity_id": entity_id},
                    game=game,
                    force=True,
                )
                if src is None:
                    return jsonify({"ok": False, "error": "regen_failed"}), 500
                # Refresh the per-post file copy.
                if img.file_path:
                    dst = _Path(str(img.file_path))
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    _shutil.copy2(str(src), str(dst))
                # Update spec with the fresh prompt.
                from social_media.hero_poster import read_prompt_sidecar

                spec["prompt"] = read_prompt_sidecar(str(src)) or spec.get("prompt", "")
                img.spec = _json.dumps(spec, ensure_ascii=False)
                session.commit()
            except Exception as exc:
                deps.logger().exception("asset regenerate failed for image_id=%s", image_id)
                return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, "image_id": int(image_id)})

    def api_admin_publishing_matrix():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from content_pipeline.publishing_registry import get_publishing_matrix

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            return jsonify({"ok": True, **get_publishing_matrix(session)})

    def api_admin_update_publishing_matrix():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from content_pipeline.publishing_registry import update_publishing_matrix

        body = request.get_json(force=True) or {}
        updates = body.get("updates") or []
        if not isinstance(updates, list) or not updates:
            return jsonify({"ok": False, "error": "updates required (list of {pipeline, platform, action, value})"}), 400
        SessionLocal = deps.session_local()
        try:
            with SessionLocal() as session:
                result = update_publishing_matrix(session, updates)
                session.commit()
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("publishing matrix update failed")
            return jsonify({"ok": False, "error": str(exc)}), 500
        return jsonify({"ok": True, **result})

    def api_admin_runtime_flags():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        return jsonify({"ok": True, "flags": deps.load_runtime_flags()()})

    def api_admin_ai_usage():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            return jsonify({"ok": True, "dashboard": deps.get_ai_usage_dashboard()(session)})

    def api_admin_visitor_timeseries():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        PageView = deps.page_view_model()
        human_page_view_filter = deps.human_page_view_filter()
        SocialPostDelivery = deps.social_post_delivery_model()
        SocialPostVariant = deps.social_post_variant_model()
        SessionLocal = deps.session_local()

        # Manual date range overrides days param
        from_str = (request.args.get("from") or "").strip()
        to_str = (request.args.get("to") or "").strip()
        cutoff = None
        end = None
        if from_str:
            try:
                cutoff = datetime.fromisoformat(from_str)
            except ValueError:
                cutoff = None
        if to_str:
            try:
                end = datetime.fromisoformat(to_str) + timedelta(days=1)  # inclusive
            except ValueError:
                end = None
        if cutoff is None:
            days = min(int(request.args.get("days", 90)), 365)
            cutoff = datetime.utcnow() - timedelta(days=days)
        if end is None:
            end = datetime.utcnow() + timedelta(days=1)

        granularity = (request.args.get("granularity") or "").strip().lower()
        if granularity not in ("hour", "day", "week"):
            # Auto-pick: small ranges → hour, mid → day, large → week
            window_days = (end - cutoff).total_seconds() / 86400
            if window_days <= 3:
                granularity = "hour"
            elif window_days <= 60:
                granularity = "day"
            else:
                granularity = "week"

        external_ref = and_(
            PageView.referrer.isnot(None),
            PageView.referrer != "",
            not_(PageView.referrer.like("http%://funba.app%")),
            not_(PageView.referrer.like("http%://www.funba.app%")),
        )

        if granularity == "hour":
            bucket_cols = [
                extract("year", PageView.created_at).label("k1"),
                extract("month", PageView.created_at).label("k2"),
                extract("day", PageView.created_at).label("k3"),
                extract("hour", PageView.created_at).label("k4"),
            ]
            post_key_fmt = "%Y-%m-%dT%H:00:00Z"
            row_to_date = lambda r: f"{int(r.k1):04d}-{int(r.k2):02d}-{int(r.k3):02d}T{int(r.k4):02d}:00:00Z"
        elif granularity == "week":
            bucket_cols = [
                func.yearweek(PageView.created_at, 3).label("k1"),  # ISO week
            ]
            post_key_fmt = None  # computed manually below
            row_to_date = None
        else:  # day
            bucket_cols = [
                extract("year", PageView.created_at).label("k1"),
                extract("month", PageView.created_at).label("k2"),
                extract("day", PageView.created_at).label("k3"),
            ]
            post_key_fmt = "%Y-%m-%dT00:00:00Z"
            row_to_date = lambda r: f"{int(r.k1):04d}-{int(r.k2):02d}-{int(r.k3):02d}T00:00:00Z"

        with SessionLocal() as session:
            # Per-(bucket, visitor) aggregate to evaluate verified-in-this-bucket.
            visitor_subq = (
                session.query(
                    *bucket_cols,
                    PageView.visitor_id.label("vid"),
                    func.count(PageView.id).label("pv_in_bucket"),
                    func.max(case((external_ref, 1), else_=0)).label("has_ext"),
                )
                .filter(
                    PageView.created_at >= cutoff,
                    PageView.created_at < end,
                    human_page_view_filter(PageView),
                )
                .group_by(*[c.element for c in bucket_cols], PageView.visitor_id)
                .subquery()
            )
            sub_keys = [visitor_subq.c[col.name] for col in bucket_cols]
            rows = (
                session.query(
                    *sub_keys,
                    func.sum(visitor_subq.c.pv_in_bucket).label("views"),
                    func.count().label("unique"),
                    func.sum(
                        case(
                            (or_(visitor_subq.c.has_ext == 1, visitor_subq.c.pv_in_bucket >= 2), 1),
                            else_=0,
                        )
                    ).label("verified"),
                )
                .group_by(*sub_keys)
                .order_by(*sub_keys)
                .all()
            )

            if granularity == "week":
                # yearweek(date, 3) returns yyyyww (ISO). Convert to date of Monday.
                def _yearweek_to_iso(yw: int) -> str:
                    s = str(int(yw))
                    yyyy, ww = int(s[:4]), int(s[4:])
                    monday = datetime.fromisocalendar(yyyy, ww, 1)
                    return monday.strftime("%Y-%m-%dT00:00:00Z")
                data = [
                    {
                        "date": _yearweek_to_iso(r.k1),
                        "views": int(r.views or 0),
                        "unique": int(r.unique or 0),
                        "verified": int(r.verified or 0),
                    }
                    for r in rows
                ]
            else:
                data = [
                    {
                        "date": row_to_date(r),
                        "views": int(r.views or 0),
                        "unique": int(r.unique or 0),
                        "verified": int(r.verified or 0),
                    }
                    for r in rows
                ]

            posts = (
                session.query(SocialPostDelivery.published_at, SocialPostDelivery.platform, SocialPostVariant.title)
                .join(SocialPostVariant, SocialPostDelivery.variant_id == SocialPostVariant.id)
                .filter(
                    SocialPostDelivery.status == "published",
                    SocialPostDelivery.published_at >= cutoff,
                    SocialPostDelivery.published_at < end,
                    SocialPostDelivery.published_at.isnot(None),
                )
                .order_by(SocialPostDelivery.published_at)
                .all()
            )
            from collections import OrderedDict

            def _post_bucket_key(dt):
                if granularity == "hour":
                    return dt.strftime("%Y-%m-%dT%H:00:00Z")
                if granularity == "week":
                    iso = dt.isocalendar()
                    monday = datetime.fromisocalendar(iso.year, iso.week, 1)
                    return monday.strftime("%Y-%m-%dT00:00:00Z")
                return dt.strftime("%Y-%m-%dT00:00:00Z")

            post_buckets: dict[str, dict] = OrderedDict()
            for p in posts:
                key = _post_bucket_key(p.published_at)
                bucket = post_buckets.setdefault(key, {"date": key, "count": 0, "titles": []})
                bucket["count"] += 1
                label = f"[{p.platform}] {(p.title or '')[:60]}"
                bucket["titles"].append(label)
            post_data = list(post_buckets.values())
        return jsonify({"ok": True, "series": data, "posts": post_data, "granularity": granularity})

    def api_admin_update_runtime_flags():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        body = request.get_json(force=True) or {}
        flags = deps.load_runtime_flags()()
        updated = False
        for key in deps.default_runtime_flags()():
            if key in body:
                try:
                    flags = deps.set_runtime_flag()(key, body[key])
                    updated = True
                except KeyError:
                    return jsonify({"ok": False, "error": f"unknown runtime flag: {key}"}), 400
        if not updated:
            return jsonify({"ok": False, "error": "no recognized flags in request body"}), 400
        return jsonify({"ok": True, "flags": flags})

    def admin_backfill(season: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        from tasks.ingest import ingest_game
        from tasks.celery_app import app as celery_app
        from tasks.dispatch import discover_and_insert_games
        from metrics.framework.runtime import get_all_metrics as _get_runtime_metrics

        type_map = {"2": "Regular Season", "4": "Playoffs", "5": "PlayIn", "1": "Pre Season"}
        prefix = season[0] if season else "2"
        year = int(season[1:]) if len(season) > 1 else 0
        nba_season = f"{year}-{(year + 1) % 100:02d}"
        season_type = type_map.get(prefix, "Regular Season")

        game_ids = discover_and_insert_games(season=nba_season, season_types=[season_type])
        if not game_ids:
            return jsonify({"error": f"No games found for season {season}"}), 404

        ingest_q = next(q for q in celery_app.conf.task_queues if q.name == "ingest")
        metric_keys = [m.key for m in _get_runtime_metrics()]
        for gid in game_ids:
            ingest_game.apply_async(args=[gid], kwargs={"metric_keys": metric_keys}, declare=[ingest_q])
        return jsonify({"season": season, "enqueued": len(game_ids)})

    def game_shotchart_backfill(game_id: str):
        denied = deps.require_admin_page()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        Game = deps.game_model()
        with SessionLocal() as session:
            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game is None:
                abort(404, description=f"Game {game_id} not found")
            try:
                count = deps.back_fill_game_shot_record_from_api()(session, game_id, commit=True, replace_existing=False)
                return redirect(url_for("game_page", game_id=game_id, shot_backfill="ok", shot_count=count))
            except Exception:
                session.rollback()
                deps.app().logger.exception("manual shotchart backfill failed for game_id=%s", game_id)
                return redirect(url_for("game_page", game_id=game_id, shot_backfill="error"))

    def game_shotchart_backfill_api(game_id: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        Game = deps.game_model()
        with SessionLocal() as session:
            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game is None:
                return jsonify({"ok": False, "error": f"Game {game_id} not found"}), 404
            try:
                count = deps.back_fill_game_shot_record_from_api()(session, game_id, commit=True, replace_existing=False)
                return jsonify({"ok": True, "game_id": game_id, "shot_count": int(count)})
            except Exception as exc:
                session.rollback()
                deps.app().logger.exception("manual shotchart backfill failed for game_id=%s", game_id)
                return jsonify({"ok": False, "error": str(exc)}), 500

    def admin_users():
        denied = deps.require_admin_page()()
        if denied:
            return denied

        from sqlalchemy import func as _sa_func, or_

        SessionLocal = deps.session_local()
        User = deps.user_model()

        page = max(1, request.args.get("page", 1, type=int))
        page_size = 50
        q_text = (request.args.get("q") or "").strip()
        tier_filter = (request.args.get("tier") or "").strip() or None
        sort = (request.args.get("sort") or "last_login").strip()

        sort_map = {
            "last_login": User.last_login_at.desc(),
            "created_at": User.created_at.desc(),
            "email": User.email.asc(),
            "display_name": User.display_name.asc(),
        }
        order_clause = sort_map.get(sort, User.last_login_at.desc())

        with SessionLocal() as db:
            query = db.query(User)
            if q_text:
                like = f"%{q_text}%"
                query = query.filter(or_(User.email.ilike(like), User.display_name.ilike(like)))
            if tier_filter in {"free", "pro"}:
                query = query.filter(User.subscription_tier == tier_filter)

            total = query.count()
            import math
            total_pages = max(1, math.ceil(total / page_size)) if total else 1
            page = min(page, total_pages)
            rows = (
                query.order_by(order_clause)
                .offset((page - 1) * page_size)
                .limit(page_size)
                .all()
            )

            # Aggregate counts for the top strip.
            total_users = db.query(_sa_func.count(User.id)).scalar() or 0
            total_pro = db.query(_sa_func.count(User.id)).filter(User.subscription_tier == "pro").scalar() or 0
            total_admins = db.query(_sa_func.count(User.id)).filter(User.is_admin.is_(True)).scalar() or 0

            users = [
                {
                    "id": u.id,
                    "email": u.email,
                    "display_name": u.display_name,
                    "avatar_url": u.avatar_url,
                    "is_admin": bool(u.is_admin),
                    "subscription_tier": u.subscription_tier,
                    "subscription_status": u.subscription_status,
                    "subscription_expires_at": u.subscription_expires_at,
                    "created_at": u.created_at,
                    "last_login_at": u.last_login_at,
                    "google_linked": bool(u.google_id),
                }
                for u in rows
            ]

        return deps.render_template()(
            "admin_users.html",
            users=users,
            page=page,
            total_pages=total_pages,
            total=total,
            page_size=page_size,
            q_text=q_text,
            tier_filter=tier_filter,
            sort=sort,
            totals={
                "all": total_users,
                "pro": total_pro,
                "admins": total_admins,
            },
        )

    app.add_url_rule("/admin/users", endpoint="admin_users", view_func=admin_users)
    app.add_url_rule("/api/data/games", endpoint="api_data_games", view_func=api_data_games)
    app.add_url_rule("/api/data/games/<game_id>/boxscore", endpoint="api_data_boxscore", view_func=api_data_boxscore)
    app.add_url_rule("/api/data/games/<game_id>/pbp", endpoint="api_data_pbp", view_func=api_data_pbp)
    app.add_url_rule("/api/data/games/<game_id>/metrics", endpoint="api_data_game_metrics", view_func=api_data_game_metrics)
    app.add_url_rule("/api/data/metrics/<metric_key>/top", endpoint="api_data_metric_top", view_func=api_data_metric_top)
    app.add_url_rule("/api/data/metrics/triggered", endpoint="api_data_triggered_metrics", view_func=api_data_triggered_metrics)
    app.add_url_rule("/admin/fragment/<section>", endpoint="admin_fragment", view_func=admin_fragment)
    app.add_url_rule("/api/admin/infra-status", endpoint="api_admin_infra_status", view_func=api_admin_infra_status)
    app.add_url_rule("/api/admin/feature-access", endpoint="api_admin_feature_access", view_func=api_admin_feature_access)
    app.add_url_rule("/api/admin/feature-access", endpoint="api_admin_update_feature_access", view_func=api_admin_update_feature_access, methods=["POST"])
    app.add_url_rule("/api/admin/model-config", endpoint="api_admin_model_config", view_func=api_admin_model_config)
    app.add_url_rule("/api/admin/paperclip-config", endpoint="api_admin_paperclip_config", view_func=api_admin_paperclip_config)
    app.add_url_rule("/api/admin/paperclip-config", endpoint="api_admin_update_paperclip_config", view_func=api_admin_update_paperclip_config, methods=["POST"])
    app.add_url_rule("/api/admin/model-config", endpoint="api_admin_update_model_config", view_func=api_admin_update_model_config, methods=["POST"])
    app.add_url_rule("/api/admin/runtime-flags", endpoint="api_admin_runtime_flags", view_func=api_admin_runtime_flags)
    app.add_url_rule("/api/admin/ai-usage", endpoint="api_admin_ai_usage", view_func=api_admin_ai_usage)
    app.add_url_rule("/api/admin/visitor-timeseries", endpoint="api_admin_visitor_timeseries", view_func=api_admin_visitor_timeseries)
    app.add_url_rule("/api/admin/runtime-flags", endpoint="api_admin_update_runtime_flags", view_func=api_admin_update_runtime_flags, methods=["POST"])
    app.add_url_rule("/api/admin/hero-poster-config", endpoint="api_admin_hero_poster_config", view_func=api_admin_hero_poster_config)
    app.add_url_rule("/api/admin/hero-poster-config", endpoint="api_admin_update_hero_poster_config", view_func=api_admin_update_hero_poster_config, methods=["POST"])
    app.add_url_rule("/api/admin/hero-poster-preview", endpoint="api_admin_hero_poster_preview", view_func=api_admin_hero_poster_preview, methods=["POST"])
    app.add_url_rule("/api/admin/hero-poster-regenerate", endpoint="api_admin_hero_poster_regenerate", view_func=api_admin_hero_poster_regenerate, methods=["POST"])
    app.add_url_rule("/api/admin/publishing-matrix", endpoint="api_admin_publishing_matrix", view_func=api_admin_publishing_matrix)
    app.add_url_rule("/api/admin/publishing-matrix", endpoint="api_admin_update_publishing_matrix", view_func=api_admin_update_publishing_matrix, methods=["POST"])
    app.add_url_rule("/admin/assets", endpoint="admin_assets", view_func=admin_assets)
    app.add_url_rule("/admin/assets/<int:image_id>", endpoint="admin_asset_detail", view_func=admin_asset_detail)
    app.add_url_rule("/api/admin/assets", endpoint="api_admin_assets_list", view_func=api_admin_assets_list)
    app.add_url_rule("/api/admin/assets/<int:image_id>", endpoint="api_admin_asset_detail", view_func=api_admin_asset_detail)
    app.add_url_rule("/api/admin/assets/<int:image_id>/replace", endpoint="api_admin_asset_replace", view_func=api_admin_asset_replace, methods=["POST"])
    app.add_url_rule("/api/admin/assets/<int:image_id>/regenerate", endpoint="api_admin_asset_regenerate", view_func=api_admin_asset_regenerate, methods=["POST"])
    app.add_url_rule("/admin/backfill/<season>", endpoint="admin_backfill", view_func=admin_backfill, methods=["POST"])
    app.add_url_rule("/games/<game_id>/shotchart/backfill", endpoint="game_shotchart_backfill", view_func=game_shotchart_backfill, methods=["POST"])
    app.add_url_rule("/api/games/<game_id>/shotchart/backfill", endpoint="game_shotchart_backfill_api", view_func=game_shotchart_backfill_api, methods=["POST"])

    return SimpleNamespace(
        admin_users=admin_users,
        api_data_games=api_data_games,
        api_data_boxscore=api_data_boxscore,
        api_data_pbp=api_data_pbp,
        api_data_game_metrics=api_data_game_metrics,
        api_data_metric_top=api_data_metric_top,
        api_data_triggered_metrics=api_data_triggered_metrics,
        admin_fragment=admin_fragment,
        api_admin_infra_status=api_admin_infra_status,
        api_admin_feature_access=api_admin_feature_access,
        api_admin_update_feature_access=api_admin_update_feature_access,
        api_admin_model_config=api_admin_model_config,
        api_admin_paperclip_config=api_admin_paperclip_config,
        api_admin_update_paperclip_config=api_admin_update_paperclip_config,
        api_admin_update_model_config=api_admin_update_model_config,
        api_admin_runtime_flags=api_admin_runtime_flags,
        api_admin_ai_usage=api_admin_ai_usage,
        api_admin_visitor_timeseries=api_admin_visitor_timeseries,
        api_admin_update_runtime_flags=api_admin_update_runtime_flags,
        admin_backfill=admin_backfill,
        game_shotchart_backfill=game_shotchart_backfill,
        game_shotchart_backfill_api=game_shotchart_backfill_api,
        api_admin_hero_poster_config=api_admin_hero_poster_config,
        api_admin_update_hero_poster_config=api_admin_update_hero_poster_config,
        api_admin_hero_poster_preview=api_admin_hero_poster_preview,
        api_admin_hero_poster_regenerate=api_admin_hero_poster_regenerate,
        api_admin_publishing_matrix=api_admin_publishing_matrix,
        api_admin_update_publishing_matrix=api_admin_update_publishing_matrix,
        admin_assets=admin_assets,
        admin_asset_detail=admin_asset_detail,
        api_admin_assets_list=api_admin_assets_list,
        api_admin_asset_detail=api_admin_asset_detail,
        api_admin_asset_replace=api_admin_asset_replace,
        api_admin_asset_regenerate=api_admin_asset_regenerate,
    )
