from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from types import SimpleNamespace

from flask import jsonify, request


def register_metrics_write_routes(app, deps):
    @app.post("/api/metrics/search")
    @deps.limiter().limit("30 per minute")
    def api_metric_search():
        denied = deps.require_feature_json()("metric_search")
        if denied:
            return denied
        from metrics.framework.search import rank_metrics

        body = request.get_json(force=True) or {}
        query = (body.get("query") or "").strip()
        scope_filter = (body.get("scope") or "").strip()
        status_filter = (body.get("status") or "").strip()
        if status_filter == "draft":
            status_filter = ""
        requested_model = (body.get("model") or "").strip() if deps.is_admin()() else None
        if not query:
            return jsonify({"ok": False, "error": "query is required"}), 400
        if len(query) > 200:
            return jsonify({"ok": False, "error": "query too long (max 200 characters)"}), 400

        usage_payload: dict = {}
        started_at = time.perf_counter()
        candidate_count = 0

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            catalog = deps.catalog_metrics()(
                session,
                scope_filter=scope_filter,
                status_filter=status_filter,
            )
            candidate_count = len(catalog)
            try:
                llm_model = deps.resolve_llm_model()(session, requested_model=requested_model, purpose="search")
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400

        try:
            ranked = rank_metrics(
                query,
                catalog,
                limit=8,
                model=llm_model,
                usage_recorder=usage_payload.update,
            )
        except ValueError as exc:
            deps.record_ai_usage_event()(
                feature="metric_search",
                operation="rank",
                model=llm_model,
                usage=usage_payload,
                started_at=started_at,
                success=False,
                http_status=400,
                error_code=type(exc).__name__,
                metadata={
                    "query_chars": len(query),
                    "query_text": deps.ai_usage_preview()(query),
                    "candidate_count": candidate_count,
                    "scope_filter": scope_filter,
                    "status_filter": status_filter,
                },
            )
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("metric search failed")
            deps.record_ai_usage_event()(
                feature="metric_search",
                operation="rank",
                model=llm_model,
                usage=usage_payload,
                started_at=started_at,
                success=False,
                http_status=500,
                error_code=type(exc).__name__,
                metadata={
                    "query_chars": len(query),
                    "query_text": deps.ai_usage_preview()(query),
                    "candidate_count": candidate_count,
                    "scope_filter": scope_filter,
                    "status_filter": status_filter,
                },
            )
            return jsonify({"ok": False, "error": str(exc)}), 500

        by_key = {metric["key"]: metric for metric in catalog}
        matches = []
        for ranked_item in ranked:
            metric = by_key.get(ranked_item["key"])
            if metric is None:
                continue
            matches.append({**metric, "reason": ranked_item["reason"]})

        deps.record_ai_usage_event()(
            feature="metric_search",
            operation="rank",
            model=llm_model,
            usage=usage_payload,
            started_at=started_at,
            success=True,
            http_status=200,
            metadata={
                "query_chars": len(query),
                "query_text": deps.ai_usage_preview()(query),
                "candidate_count": candidate_count,
                "match_count": len(matches),
                "scope_filter": scope_filter,
                "status_filter": status_filter,
            },
        )
        return jsonify({"ok": True, "matches": matches})

    @app.post("/api/metrics/check-similar")
    @deps.limiter().limit("15 per minute")
    def api_metric_check_similar():
        denied = deps.require_metric_creator_json()()
        if denied:
            return denied
        from metrics.framework.search import rank_metrics

        body = request.get_json(force=True) or {}
        expression = (body.get("expression") or "").strip()
        conversation_id = (body.get("conversationId") or "").strip() or None
        requested_model = (body.get("model") or "").strip() if deps.is_admin()() else None
        if not expression:
            return jsonify({"ok": False, "error": "expression is required"}), 400
        usage_payload: dict = {}
        started_at = time.perf_counter()
        candidate_count = 0
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            try:
                llm_model = deps.resolve_llm_model()(session, requested_model=requested_model, purpose="search")
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
            catalog = deps.catalog_metrics()(session, status_filter="published")
            candidate_count = len(catalog)
        try:
            with SessionLocal() as embed_session:
                ranked = rank_metrics(
                    expression,
                    catalog,
                    limit=3,
                    model=llm_model,
                    usage_recorder=usage_payload.update,
                    session=embed_session,
                    mode="similarity",
                )
            by_key = {m["key"]: m for m in catalog}
            similar = []
            for item in ranked:
                m = by_key.get(item["key"])
                if m is None:
                    continue
                similar.append({
                    "key": item["key"],
                    "name": m.get("name", ""),
                    "description": m.get("description", ""),
                    "reason": item.get("reason") or "Similar metric.",
                })
            success = True
            error_code = None
        except Exception as exc:
            deps.logger().exception("check-similar failed")
            similar = []
            success = False
            error_code = type(exc).__name__
        deps.record_ai_usage_event()(
            feature="metric_create",
            operation="check_similar",
            model=llm_model,
            usage=usage_payload,
            started_at=started_at,
            success=success,
            http_status=200,
            error_code=error_code,
            conversation_id=conversation_id,
            metadata={
                "input_chars": len(expression),
                "input_text": deps.ai_usage_preview()(expression),
                "candidate_count": candidate_count,
                "similar_count": len(similar),
            },
        )
        return jsonify({"ok": True, "similar": similar})

    @app.post("/api/metrics/generate")
    @deps.limiter().limit("10 per minute")
    def api_metric_generate():
        denied = deps.require_metric_creator_json()()
        if denied:
            return denied
        from metrics.framework.generator import generate

        body = request.get_json(force=True) or {}
        expression = body.get("expression", "").strip()
        history = body.get("history")
        existing = body.get("existing")
        conversation_id = (body.get("conversationId") or "").strip() or None
        requested_model = (body.get("model") or "").strip() if deps.is_admin()() else None
        if not expression:
            return jsonify({"ok": False, "error": "expression is required"}), 400
        usage_payload: dict = {}
        started_at = time.perf_counter()
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            try:
                llm_model = deps.resolve_llm_model()(session, requested_model=requested_model, purpose="generate")
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
        try:
            spec = generate(expression, history=history, existing=existing, model=llm_model, usage_recorder=usage_payload.update)
            response_type = (spec.get("responseType") or "code") if isinstance(spec, dict) else "code"
            if response_type == "clarification":
                deps.record_ai_usage_event()(
                    feature="metric_create",
                    operation="generate",
                    model=llm_model,
                    usage=usage_payload,
                    started_at=started_at,
                    success=True,
                    http_status=200,
                    conversation_id=conversation_id,
                    metadata={
                        "input_chars": len(expression),
                        "input_text": deps.ai_usage_preview()(expression),
                        "history_turn_count": len(history or []),
                        "is_edit": bool(existing),
                        "response_type": "clarification",
                        "metric_key": (existing or {}).get("key"),
                    },
                )
                return jsonify({"ok": True, "responseType": "clarification", "message": spec.get("message", "")})
            deps.record_ai_usage_event()(
                feature="metric_create",
                operation="generate",
                model=llm_model,
                usage=usage_payload,
                started_at=started_at,
                success=True,
                http_status=200,
                conversation_id=conversation_id,
                metadata={
                    "input_chars": len(expression),
                    "input_text": deps.ai_usage_preview()(expression),
                    "history_turn_count": len(history or []),
                    "is_edit": bool(existing),
                    "response_type": "code",
                    "metric_key": (existing or {}).get("key") or (spec or {}).get("key"),
                },
            )
            return jsonify({"ok": True, "responseType": "code", "spec": spec})
        except ValueError as exc:
            deps.record_ai_usage_event()(
                feature="metric_create",
                operation="generate",
                model=llm_model,
                usage=usage_payload,
                started_at=started_at,
                success=False,
                http_status=400,
                error_code=type(exc).__name__,
                conversation_id=conversation_id,
                metadata={
                    "input_chars": len(expression),
                    "input_text": deps.ai_usage_preview()(expression),
                    "history_turn_count": len(history or []),
                    "is_edit": bool(existing),
                    "metric_key": (existing or {}).get("key"),
                },
            )
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("metric generate failed")
            deps.record_ai_usage_event()(
                feature="metric_create",
                operation="generate",
                model=llm_model,
                usage=usage_payload,
                started_at=started_at,
                success=False,
                http_status=500,
                error_code=type(exc).__name__,
                conversation_id=conversation_id,
                metadata={
                    "input_chars": len(expression),
                    "input_text": deps.ai_usage_preview()(expression),
                    "history_turn_count": len(history or []),
                    "is_edit": bool(existing),
                    "metric_key": (existing or {}).get("key"),
                },
            )
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.post("/api/metrics/preview")
    @deps.limiter().limit("20 per minute")
    def api_metric_preview():
        body = request.get_json(force=True) or {}
        definition = body.get("definition")
        code_python = (body.get("code") or "").strip()
        scope = body.get("scope", "player")
        season = body.get("season", "")
        rank_order = str(body.get("rank_order") or "").strip().lower() or None
        season_types = body.get("season_types")

        if not definition and not code_python:
            return jsonify({"ok": False, "error": "definition or code is required"}), 400

        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            try:
                if code_python:
                    rows = deps.preview_code_metric()(
                        session,
                        code_python,
                        scope,
                        season,
                        limit=25,
                        rank_order_override=rank_order,
                        season_types_override=season_types,
                    )
                else:
                    from metrics.framework.rule_engine import preview as re_preview
                    rows = re_preview(session, definition, scope, season, limit=25)
            except Exception as exc:
                deps.logger().exception("metric preview failed")
                return jsonify({"ok": False, "error": str(exc)}), 400

            entity_ids = [row["entity_id"] for row in rows]
            if scope == "player":
                Player = deps.player_model()
                player_rows = session.query(Player.player_id, Player.full_name, Player.slug).filter(Player.player_id.in_(entity_ids)).all()
                names = {p.player_id: p.full_name for p in player_rows}
                slugs = {p.player_id: p.slug for p in player_rows}
            elif scope == "team":
                tm = deps.team_map()(session)
                names = {team_id: deps.team_name()(tm, team_id) for team_id in entity_ids}
                team_slugs = {team_id: tm[team_id].slug for team_id in entity_ids if team_id in tm and tm[team_id].slug}
            elif scope == "game":
                names, game_dates = deps.resolve_game_entity_names()(session, entity_ids)
                Game = deps.game_model()
                game_slug_rows = session.query(Game.game_id, Game.slug).filter(Game.game_id.in_(entity_ids)).all()
                game_slugs = {g.game_id: g.slug for g in game_slug_rows}
            else:
                names = {}
                game_dates = {}

            for row in rows:
                row["entity_name"] = names.get(row["entity_id"], row.get("value_str") or row["entity_id"])
                if scope == "player":
                    row["entity_slug"] = slugs.get(row["entity_id"], row["entity_id"])
                if scope == "team":
                    row["entity_slug"] = team_slugs.get(row["entity_id"], row["entity_id"])
                if scope == "game":
                    row["date"] = game_dates.get(row["entity_id"], "")
                    row["entity_slug"] = game_slugs.get(row["entity_id"], row["entity_id"])

        return jsonify({"ok": True, "rows": rows})

    @app.post("/api/metrics")
    def api_metric_create():
        denied = deps.require_metric_creator_json()()
        if denied:
            return denied
        from metrics.framework.runtime import get_metric as _get_runtime_metric

        body = request.get_json(force=True) or {}
        key = (body.get("key") or "").strip().lower().replace(" ", "_")
        name = (body.get("name") or "").strip()
        name_zh = (body.get("name_zh") or "").strip()
        scope = (body.get("scope") or "").strip()
        code_python = (body.get("code") or "").strip()
        definition = body.get("definition")
        rank_order_override = str(body.get("rank_order") or "").strip().lower() or None
        season_types_override = body.get("season_types")

        if not code_python and not definition:
            return jsonify({"ok": False, "error": "code or definition is required"}), 400

        source_type = "code" if code_python else "rule"
        code_metadata = None
        if code_python:
            try:
                code_metadata = deps.code_metric_metadata_from_code()(
                    code_python,
                    rank_order_override=rank_order_override,
                    season_types_override=season_types_override,
                )
            except Exception as exc:
                return jsonify({"ok": False, "error": f"Code validation failed: {exc}"}), 400
            code_python = code_metadata["code_python"]
            key = code_metadata["key"]
            name = code_metadata["name"]
            name_zh = code_metadata.get("name_zh", "")
            scope = code_metadata["scope"]
            description = code_metadata["description"]
            description_zh = code_metadata.get("description_zh", "")
            category = code_metadata["category"]
            min_sample = code_metadata["min_sample"]
        else:
            if not key:
                return jsonify({"ok": False, "error": "key is required"}), 400
            if not name or not scope:
                return jsonify({"ok": False, "error": "name and scope are required"}), 400
            description = body.get("description", "")
            description_zh = body.get("description_zh", "")
            category = body.get("category", "")
            min_sample = int(body.get("min_sample", 1))
            definition = definition or {}

        if deps.is_reserved_career_key()(key):
            return jsonify({"ok": False, "error": "Keys ending with managed window suffixes are reserved for sibling metrics"}), 409

        supports_career, career_only, _ = deps.metric_supports_career()(
            source_type,
            scope=scope,
            code_metadata=code_metadata,
            definition=definition if source_type == "rule" else None,
        )

        SessionLocal = deps.session_local()
        MetricDefinitionModel = deps.metric_definition_model()
        with SessionLocal() as session:
            reserved_keys = [key]
            if supports_career and not career_only:
                reserved_keys.append(deps.family_career_key()(key))
            for reserved_key in reserved_keys:
                if _get_runtime_metric(reserved_key, session=session) is not None:
                    return jsonify({"ok": False, "error": f"Key '{reserved_key}' is already published"}), 409
                existing = session.query(MetricDefinitionModel).filter(
                    MetricDefinitionModel.key == reserved_key,
                    MetricDefinitionModel.status == "published",
                ).first()
                if existing:
                    return jsonify({"ok": False, "error": f"Key '{reserved_key}' is already published"}), 409

            now = datetime.utcnow()
            cur_user = deps.current_user()()
            draft_key = deps.make_draft_key()(cur_user.id, key) if cur_user else key
            if code_python:
                code_python = deps.replace_key_in_code()(code_python, key, draft_key)

            m = MetricDefinitionModel(
                key=draft_key,
                family_key=draft_key,
                variant=deps.family_variant_career() if career_only else deps.family_variant_season(),
                base_metric_key=None,
                managed_family=False,
                name=name,
                name_zh=name_zh or None,
                description=description,
                description_zh=description_zh or None,
                scope=scope,
                category=category,
                group_key=body.get("group_key"),
                source_type=source_type,
                status="draft",
                definition_json=json.dumps(definition) if definition else None,
                code_python=code_python or None,
                expression=body.get("expression", ""),
                min_sample=min_sample,
                created_by_user_id=cur_user.id if cur_user else None,
                created_at=now,
                updated_at=now,
            )
            session.add(m)
            session.flush()
            deps.sync_metric_family()(
                session,
                m,
                source_type=source_type,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope=scope,
                category=category,
                group_key=body.get("group_key"),
                expression=body.get("expression", ""),
                min_sample=min_sample,
                code_python=code_python,
                definition=definition if source_type == "rule" else None,
                code_metadata=code_metadata,
                now=now,
            )
            session.commit()
            return jsonify({"ok": True, "key": draft_key}), 201

    @app.post("/api/metrics/<metric_key>/publish")
    def api_metric_publish(metric_key: str):
        denied = deps.require_metric_creator_json()()
        if denied:
            return denied
        from metrics.framework.runtime import get_metric as _get_runtime_metric

        SessionLocal = deps.session_local()
        MetricDefinitionModel = deps.metric_definition_model()
        with SessionLocal() as session:
            metric = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
            if metric is None:
                return jsonify({"ok": False, "error": "Not found"}), 404
            base_row = deps.metric_family_base_row()(session, metric)
            if not getattr(base_row, "key", None):
                base_row.key = metric_key

            old_key = base_row.key
            clean_key = deps.strip_draft_prefix()(old_key)
            needs_rename = deps.is_draft_key()(old_key)

            if needs_rename:
                if _get_runtime_metric(clean_key, session=session) is not None:
                    return jsonify({"ok": False, "error": f"Key '{clean_key}' is already published by another user"}), 409
                existing_published = session.query(MetricDefinitionModel).filter(
                    MetricDefinitionModel.key == clean_key,
                    MetricDefinitionModel.status == "published",
                ).first()
                if existing_published:
                    return jsonify({"ok": False, "error": f"Key '{clean_key}' is already published by another user"}), 409

            family_rows = deps.metric_family_rows()(session, base_row)
            now = datetime.utcnow()
            for row in family_rows:
                if row.status == "archived":
                    continue
                if needs_rename:
                    old_row_key = row.key
                    new_row_key = old_row_key.replace(old_key, clean_key, 1)
                    row.key = new_row_key
                    row.family_key = clean_key
                    if row.base_metric_key and deps.is_draft_key()(row.base_metric_key):
                        row.base_metric_key = deps.strip_draft_prefix()(row.base_metric_key)
                    if row.code_python:
                        row.code_python = deps.replace_key_in_code()(row.code_python, old_row_key, new_row_key)
                row.status = "published"
                row.updated_at = now
            session.commit()
            dispatch_key = clean_key if needs_rename else getattr(base_row, "key", metric_key)
        try:
            deps.dispatch_metric_backfill()(dispatch_key)
        except Exception:
            deps.logger().exception("Failed to enqueue backfill for %s", dispatch_key)
            return jsonify({"ok": True, "key": dispatch_key, "status": "published", "warning": "Metric published but backfill enqueue failed. Run manually."})
        return jsonify({"ok": True, "key": dispatch_key, "status": "published"})

    @app.post("/api/admin/metrics/<metric_key>/toggle-enabled")
    def api_admin_toggle_metric_enabled(metric_key: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied

        SessionLocal = deps.session_local()
        MetricDefinitionModel = deps.metric_definition_model()
        with SessionLocal() as session:
            metric = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
            if metric is None:
                return jsonify({"ok": False, "error": "Not found"}), 404
            base_row = deps.metric_family_base_row()(session, metric)
            if base_row.status not in ("published", "disabled"):
                return jsonify({"ok": False, "error": f"Cannot toggle metric with status '{base_row.status}'"}), 400
            new_status = "disabled" if base_row.status == "published" else "published"
            family_rows = deps.metric_family_rows()(session, base_row)
            now = datetime.utcnow()
            toggled_keys = []
            for row in family_rows:
                if row.status in ("published", "disabled"):
                    row.status = new_status
                    row.updated_at = now
                    toggled_keys.append(row.key)
            session.commit()
        return jsonify({"ok": True, "status": new_status, "toggled_keys": toggled_keys})

    @app.get("/api/metrics/<metric_key>/qualifying-games")
    @deps.limiter().limit("30 per minute")
    def api_qualifying_games(metric_key: str):
        entity_id = request.args.get("entity_id")
        season = request.args.get("season")
        if not entity_id:
            return jsonify({"ok": False, "error": "entity_id is required"}), 400
        page = max(1, int(request.args.get("page", 1) or 1))
        page_size = 10

        SessionLocal = deps.session_local()
        Game = deps.game_model()
        MetricRunLog = deps.metric_run_log_model()
        PlayerGameStats = deps.player_game_stats_model()
        TeamGameStats = deps.team_game_stats_model()
        with SessionLocal() as session:
            from metrics.framework.runtime import _aggregated_career_qualification_game_ids, get_metric as _rt_get_metric

            runtime_metric = _rt_get_metric(metric_key, session=session)
            aggregated_game_ids = _aggregated_career_qualification_game_ids(runtime_metric, session, season, entity_id)
            if aggregated_game_ids is not None:
                games_q = session.query(Game).filter(Game.game_id.in_(aggregated_game_ids))
                total = games_q.count()
                rows = [(None, game) for game in games_q.order_by(Game.game_date.desc(), Game.game_id.desc()).offset((page - 1) * page_size).limit(page_size).all()]
            else:
                base_q = (
                    session.query(MetricRunLog, Game)
                    .join(Game, MetricRunLog.game_id == Game.game_id)
                    .filter(MetricRunLog.metric_key == metric_key, MetricRunLog.entity_id == entity_id, MetricRunLog.qualified == True)
                )
                if season:
                    base_q = base_q.filter(MetricRunLog.season == season)
                total = base_q.count()
                rows = base_q.order_by(Game.game_date.desc(), Game.game_id.desc()).offset((page - 1) * page_size).limit(page_size).all()
            team_map = deps.team_map()(session)
            game_ids = [game.game_id for _, game in rows]

            player_stats_map = {}
            team_stats_map = {}
            if game_ids:
                for ps in session.query(PlayerGameStats).filter(PlayerGameStats.game_id.in_(game_ids), PlayerGameStats.player_id == entity_id).all():
                    player_stats_map[ps.game_id] = ps
                for ts in session.query(TeamGameStats).filter(TeamGameStats.game_id.in_(game_ids), TeamGameStats.team_id == entity_id).all():
                    team_stats_map[ts.game_id] = ts

            game_scores = {}
            if game_ids:
                for ts in session.query(TeamGameStats).filter(TeamGameStats.game_id.in_(game_ids)).all():
                    game_scores.setdefault(ts.game_id, {})[str(ts.team_id)] = int(ts.pts or 0)

            games = []
            for log, game in rows:
                gid = game.game_id
                home_id = str(game.home_team_id)
                road_id = str(game.road_team_id)
                scores = game_scores.get(gid, {})
                home_score = scores.get(home_id)
                road_score = scores.get(road_id)
                entry = {
                    "game_id": gid,
                    "game_slug": game.slug or f"game-{gid}",
                    "game_date": game.game_date.isoformat() if game.game_date else None,
                    "season": game.season,
                    "home_team": deps.team_abbr()(team_map, game.home_team_id),
                    "road_team": deps.team_abbr()(team_map, game.road_team_id),
                    "home_team_id": str(game.home_team_id),
                    "road_team_id": str(game.road_team_id),
                    "home_score": home_score,
                    "road_score": road_score,
                    "delta": json.loads(log.delta_json) if log and log.delta_json else None,
                }
                ps = player_stats_map.get(gid)
                if ps:
                    entry["player_line"] = f"{int(ps.pts or 0)} PTS, {int(ps.reb or 0)} REB, {int(ps.ast or 0)} AST"
                    entity_team_id = str(ps.team_id) if ps.team_id else None
                    if entity_team_id and game.wining_team_id is not None:
                        entry["win"] = str(game.wining_team_id) == entity_team_id
                ts = team_stats_map.get(gid)
                if ts:
                    entry["team_line"] = f"{int(ts.pts or 0)} PTS"
                    entry["win"] = bool(ts.win)
                games.append(entry)

            total_pages = max(1, (total + page_size - 1) // page_size)
            return jsonify({"ok": True, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages, "games": games})

    @app.get("/api/metrics/<metric_key>/backfill-status")
    def api_metric_backfill_status(metric_key: str):
        SessionLocal = deps.session_local()
        with SessionLocal() as session:
            metric_def, backfill = deps.build_metric_backfill_status()(session, metric_key)
            if metric_def is None:
                return jsonify({"ok": False, "error": "Not found"}), 404
            return jsonify({"ok": True, "metric_key": metric_key, "backfill": backfill})

    @app.post("/api/metrics/<metric_key>/update")
    def api_metric_update(metric_key: str):
        denied = deps.require_metric_creator_json()()
        if denied:
            return denied

        body = request.get_json(force=True) or {}
        code_python = (body.get("code") or "").strip()
        code_metadata = None
        rank_order_override = str(body.get("rank_order") or "").strip().lower() or None
        season_types_override = body.get("season_types")

        SessionLocal = deps.session_local()
        MetricDefinitionModel = deps.metric_definition_model()
        MetricResultModel = deps.metric_result_model()
        MetricComputeRun = deps.metric_compute_run_model()
        MetricRunLog = deps.metric_run_log_model()
        engine = deps.engine()
        with SessionLocal() as session:
            metric = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
            if metric is None:
                return jsonify({"ok": False, "error": "Not found"}), 404
            metric = deps.metric_family_base_row()(session, metric)
            if not getattr(metric, "key", None):
                metric.key = metric_key
            result_key = metric.key

            if code_python:
                try:
                    code_metadata = deps.code_metric_metadata_from_code()(
                        code_python,
                        expected_key=metric.key,
                        rank_order_override=rank_order_override,
                        season_types_override=season_types_override,
                    )
                except Exception as exc:
                    return jsonify({"ok": False, "error": f"Code validation failed: {exc}"}), 400
                code_python = code_metadata["code_python"]

            metadata_fields = {"code", "definition", "name", "name_zh", "description", "description_zh", "scope", "category", "min_sample", "group_key", "expression", "rank_order", "season_types"}
            if not any(field in body for field in metadata_fields):
                metric.updated_at = datetime.utcnow()
                session.commit()
            else:
                source_type = "code" if code_python else ("rule" if body.get("definition") is not None else getattr(metric, "source_type", "rule"))
                if source_type == "code":
                    source_code = code_python or getattr(metric, "code_python", "") or ""
                    if code_metadata is None and source_code:
                        code_metadata = deps.code_metric_metadata_from_code()(
                            source_code,
                            expected_key=metric.key,
                            rank_order_override=rank_order_override,
                            season_types_override=body.get("season_types"),
                        )
                        source_code = code_metadata["code_python"]
                    source_definition = None
                    name = body.get("name") or code_metadata["name"]
                    name_zh = body.get("name_zh") if body.get("name_zh") is not None else code_metadata.get("name_zh", "")
                    description = body.get("description") if body.get("description") is not None else code_metadata["description"]
                    description_zh = body.get("description_zh") if body.get("description_zh") is not None else code_metadata.get("description_zh", "")
                    scope = body.get("scope") or code_metadata["scope"]
                    category = body.get("category") or code_metadata["category"]
                    min_sample = int(body.get("min_sample") or code_metadata["min_sample"])
                    if "max_results_per_season" in body:
                        code_metadata["max_results_per_season"] = body["max_results_per_season"]
                else:
                    source_code = None
                    source_definition = body.get("definition")
                    if source_definition is None:
                        try:
                            source_definition = json.loads(getattr(metric, "definition_json", None) or "{}")
                        except Exception:
                            source_definition = {}
                    name = body.get("name", getattr(metric, "name", metric_key))
                    name_zh = body.get("name_zh", getattr(metric, "name_zh", "") or "")
                    description = body["description"] if body.get("description") is not None else (getattr(metric, "description", "") or "")
                    description_zh = body.get("description_zh", getattr(metric, "description_zh", "") or "")
                    scope = body.get("scope", getattr(metric, "scope", "player"))
                    category = body.get("category", getattr(metric, "category", "") or "")
                    min_sample = int(body.get("min_sample", getattr(metric, "min_sample", 1) or 1))

                now = datetime.utcnow()
                deps.sync_metric_family()(
                    session,
                    metric,
                    source_type=source_type,
                    name=name,
                    name_zh=name_zh or "",
                    description=description,
                    description_zh=description_zh or "",
                    scope=scope,
                    category=category,
                    group_key=body.get("group_key", getattr(metric, "group_key", None)),
                    expression=body.get("expression", getattr(metric, "expression", "") or ""),
                    min_sample=min_sample,
                    code_python=source_code,
                    definition=source_definition,
                    code_metadata=code_metadata,
                    now=now,
                )
                session.commit()

            if body.get("rebackfill") and metric.status == "published":
                family_keys = [row.key for row in deps.metric_family_rows()(session, metric)]
                session.query(MetricResultModel).filter(MetricResultModel.metric_key.in_(family_keys)).delete(synchronize_session=False)
                session.query(MetricComputeRun).filter(MetricComputeRun.metric_key.in_(family_keys)).delete(synchronize_session=False)
                session.commit()

                def _delete_run_logs_bg():
                    from sqlalchemy.orm import sessionmaker as _sm
                    _sess = _sm(bind=engine)()
                    try:
                        _sess.query(MetricRunLog).filter(MetricRunLog.metric_key.in_(family_keys)).delete(synchronize_session=False)
                        _sess.commit()
                    except Exception:
                        deps.logger().exception("Background RunLog cleanup failed for %s", family_keys)
                        _sess.rollback()
                    finally:
                        _sess.close()

                threading.Thread(target=_delete_run_logs_bg, daemon=True).start()
                try:
                    deps.dispatch_metric_backfill()(metric.key)
                except Exception:
                    deps.logger().exception("Failed to enqueue backfill for %s", metric.key)
                    return jsonify({"ok": True, "key": result_key, "warning": "Metric updated but backfill enqueue failed. Run manually."})

        return jsonify({"ok": True, "key": result_key})

    return SimpleNamespace(
        api_metric_search=api_metric_search,
        api_metric_check_similar=api_metric_check_similar,
        api_metric_generate=api_metric_generate,
        api_metric_preview=api_metric_preview,
        api_metric_create=api_metric_create,
        api_metric_publish=api_metric_publish,
        api_admin_toggle_metric_enabled=api_admin_toggle_metric_enabled,
        api_qualifying_games=api_qualifying_games,
        api_metric_backfill_status=api_metric_backfill_status,
        api_metric_update=api_metric_update,
    )
