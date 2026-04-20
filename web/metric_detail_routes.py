from __future__ import annotations

import json
from flask import abort, request
from sqlalchemy import and_, func, or_
from types import SimpleNamespace

from metrics.framework.family import (
    derive_window_description,
    derive_window_name,
    family_base_key,
    family_window_key,
    window_type_from_key,
)


def register_metric_detail_routes(app, deps):
    def _resolve_entity_labels(session, rows):
        player_ids = {r.entity_id for r in rows if r.entity_type == "player" and r.entity_id}
        player_franchise_pairs = {
            tuple(r.entity_id.split(":", 1))
            for r in rows
            if r.entity_type == "player_franchise" and r.entity_id and ":" in r.entity_id
        }
        player_ids.update({player_id for player_id, _ in player_franchise_pairs})
        team_ids = {r.entity_id for r in rows if r.entity_type == "team" and r.entity_id}
        team_ids.update({franchise_id for _, franchise_id in player_franchise_pairs})
        game_ids = {r.entity_id.split(":")[0] for r in rows if r.entity_type == "game" and r.entity_id}

        Player = deps.player_model()
        Game = deps.game_model()
        player_info = (
            {
                p.player_id: (
                    p.full_name_zh if deps.is_zh() and getattr(p, "full_name_zh", None) else p.full_name,
                    bool(p.is_active),
                )
                for p in session.query(Player.player_id, Player.full_name, Player.full_name_zh, Player.is_active)
                .filter(Player.player_id.in_(player_ids))
                .all()
            }
            if player_ids
            else {}
        )
        player_names = {pid: info[0] for pid, info in player_info.items()}
        player_active = {pid: info[1] for pid, info in player_info.items()}
        team_map = deps.team_map()(session)
        game_info = (
            {
                g.game_id: (g.game_date, g.home_team_id, g.road_team_id)
                for g in session.query(Game.game_id, Game.game_date, Game.home_team_id, Game.road_team_id)
                .filter(Game.game_id.in_(game_ids))
                .all()
            }
            if game_ids
            else {}
        )

        def _label(entity_type, entity_id):
            if entity_type == "season":
                return deps.season_label()(entity_id)
            if entity_type == "player":
                return player_names.get(entity_id) or entity_id
            if entity_type == "player_franchise" and entity_id and ":" in entity_id:
                player_id, franchise_id = entity_id.split(":", 1)
                player_name = player_names.get(player_id) or player_id
                franchise_name = deps.team_name()(team_map, franchise_id)
                return f"{player_name} — {franchise_name}"
            if entity_type == "team":
                team = team_map.get(entity_id)
                return (deps.display_team_name()(team) or team.abbr) if team else entity_id
            if entity_type == "game":
                parts = entity_id.split(":")
                gid = parts[0]
                if gid in game_info:
                    gdate, home_id, road_id = game_info[gid]
                    matchup = f"{deps.team_abbr()(team_map, road_id)} @ {deps.team_abbr()(team_map, home_id)}"
                    date_str = deps.fmt_date()(gdate)
                    if len(parts) > 1:
                        team_id = parts[1] if len(parts) > 1 else None
                        qualifier = parts[2] if len(parts) > 2 else ""
                        team_label = deps.team_abbr()(team_map, team_id) if team_id else ""
                        return f"{team_label} {qualifier} — {matchup} ({date_str})"
                    return f"{matchup} ({date_str})"
            return entity_id

        labels = {(r.entity_type, r.entity_id): _label(r.entity_type, r.entity_id) for r in rows}
        return labels, player_active, game_info

    def metric_detail(metric_key: str):
        from metrics.framework.base import CAREER_SEASON, is_career_season
        from metrics.framework.runtime import get_metric as _get_metric

        # Same three labels for every window page (career / last3 / last5);
        # the URL's window suffix already disambiguates which time bucket the
        # value lives in, so the dropdown stays "Regular / Playoffs / Play-In".
        WINDOW_SEASON_LABELS = [
            ("regular", deps.t()("Regular Season", "常规赛")),
            ("playoffs", deps.t()("Playoffs", "季后赛")),
            ("playin", deps.t()("Play-In", "附加赛")),
        ]

        selected_season = request.args.get("season", "")
        all_season_type = None
        if selected_season.startswith("all_") and len(selected_season) == 5:
            show_all_seasons = True
            all_season_type = selected_season[4]
        elif selected_season == "all":
            show_all_seasons = True
            all_season_type = "2"
        else:
            show_all_seasons = False
        page = max(1, int(request.args.get("page", 1) or 1))
        search_q = request.args.get("q", "").strip()
        entity_filter_id = request.args.get("entity", "").strip()
        sub_key_filter = request.args.get("sub_key", "").strip()
        active_only = request.args.get("active") == "1"
        expand = request.args.get("expand") == "1"
        sort_by = request.args.get("sort", "")
        page_size = 50

        SessionLocal = deps.session_local()
        MetricDefinitionModel = deps.metric_definition_model()
        MetricResultModel = deps.metric_result_model()
        MetricRunLog = deps.metric_run_log_model()
        MetricPerfLog = deps.metric_perf_log_model()
        Player = deps.player_model()
        Team = deps.team_model()

        with SessionLocal() as session:
            base_metric_key = family_base_key(metric_key)
            db_metric = (
                session.query(MetricDefinitionModel)
                .filter(MetricDefinitionModel.key == base_metric_key, MetricDefinitionModel.status != "archived")
                .first()
            )
            runtime_metric = _get_metric(metric_key, session=session)
            if db_metric is None and runtime_metric is None:
                abort(404, description=f"Metric '{metric_key}' not found.")
            if db_metric and db_metric.status == "disabled" and not deps.is_admin()():
                current_user = deps.current_user()()
                if not (current_user and db_metric.created_by_user_id == current_user.id):
                    abort(404, description=f"Metric '{metric_key}' not found.")

            metric_def = deps.metric_def_view()(runtime_metric or db_metric, source_type=getattr(db_metric, "source_type", None))
            is_career_metric = bool(getattr(runtime_metric, "career", False))
            is_season_scope = metric_def.scope == "season"
            related_metrics_all = deps.related_metric_links()(session, metric_key, runtime_metric, db_metric)
            current_metric_season = None

            current_window_type = window_type_from_key(metric_key)

            # Game-scope last3/last5 are virtual windows: rows live under the
            # base metric key and we filter to the most recent N distinct
            # seasons of the selected season-type at query time. No sibling
            # MetricResult rows get stored.
            is_virtual_game_window = (
                current_window_type in ("last3", "last5")
                and runtime_metric is None
                and db_metric is not None
                and db_metric.scope == "game"
            )
            query_metric_key = base_metric_key if is_virtual_game_window else metric_key

            if is_virtual_game_window:
                window_n = 3 if current_window_type == "last3" else 5
                base_name_en = getattr(metric_def, "name_en", "") or ""
                base_name_zh = getattr(metric_def, "name_zh", "") or ""
                base_desc_en = getattr(metric_def, "description_en", "") or ""
                base_desc_zh = getattr(metric_def, "description_zh", "") or ""
                name_en = derive_window_name(base_name_en, current_window_type)
                name_zh = f"{base_name_zh}（近 {window_n} 季）" if base_name_zh else ""
                desc_en = derive_window_description(base_desc_en, current_window_type)
                desc_zh = f"近 {window_n} 季{base_desc_zh}" if base_desc_zh else ""
                metric_def.name_en = name_en
                metric_def.name_zh = name_zh
                metric_def.description_en = desc_en
                metric_def.description_zh = desc_zh
                # deps.is_zh() reflects the current request language; use it
                # to recompute the localized display strings the template uses.
                is_zh = deps.is_zh()
                metric_def.name = name_zh if (is_zh and name_zh) else name_en
                metric_def.description = desc_zh if (is_zh and desc_zh) else desc_en
                metric_def.key = metric_key

            window_tabs: list[dict] = []

            def _has_results_for(key: str) -> bool:
                return (
                    session.query(MetricResultModel.metric_key)
                    .filter(MetricResultModel.metric_key == key)
                    .limit(1)
                    .first()
                    is not None
                )

            base_has_results = _has_results_for(base_metric_key)
            if base_has_results:
                window_tabs.append(
                    {
                        "window_type": "season",
                        "metric_key": base_metric_key,
                        "label": deps.t()("Season", "赛季"),
                        "is_current": base_metric_key == metric_key,
                    }
                )
            is_game_scope = db_metric is not None and db_metric.scope == "game"
            for candidate_window in ("last3", "last5", "career"):
                candidate_key = family_window_key(base_metric_key, candidate_window)
                # Game-scope window tabs are virtual — their rows share the
                # base key, so _has_results_for won't find them. Fall back to
                # the base's result existence instead. Career is still skipped
                # for game scope (redundant with the All-<type> selector).
                if is_game_scope and candidate_window in ("last3", "last5"):
                    if not base_has_results:
                        continue
                elif not _has_results_for(candidate_key):
                    continue
                elif is_game_scope and candidate_window == "career":
                    continue
                window_tabs.append(
                    {
                        "window_type": candidate_window,
                        "metric_key": candidate_key,
                        "label": {
                            "career": deps.t()("Career", "生涯"),
                            "last5": deps.t()("Last 5 Seasons", "近 5 季"),
                            "last3": deps.t()("Last 3 Seasons", "近 3 季"),
                        }[candidate_window],
                        "is_current": candidate_key == metric_key,
                    }
                )

            window_tab_keys = {tab["metric_key"] for tab in window_tabs}
            related_metrics = [r for r in related_metrics_all if r["metric_key"] not in window_tab_keys]

            season_rows = (
                session.query(MetricResultModel.season)
                .filter(MetricResultModel.metric_key == query_metric_key, MetricResultModel.season.isnot(None))
                .distinct()
                .all()
            )
            season_values = [r.season for r in season_rows]

            virtual_window_seasons: list[str] | None = None
            if is_virtual_game_window:
                show_all_seasons = True
                if not all_season_type:
                    all_season_type = "2"
                window_n = 3 if current_window_type == "last3" else 5
                type_seasons = sorted(
                    [s for s in season_values if s and len(s) == 5 and s.isdigit() and s[0] == all_season_type],
                    reverse=True,
                )
                virtual_window_seasons = type_seasons[:window_n]
                selected_season = f"all_{all_season_type}"
            if is_career_metric:
                show_all_seasons = False
                window_type = window_type_from_key(metric_key) or "career"
                window_prefix = "all_" if window_type == "career" else f"{window_type}_"
                window_season_set = {s for s in season_values if is_career_season(s) and s.startswith(window_prefix)}
                career_season_options = [
                    window_prefix + season_type
                    for season_type, _ in WINDOW_SEASON_LABELS
                    if window_prefix + season_type in window_season_set
                ]
                if not selected_season or selected_season not in career_season_options:
                    selected_season = career_season_options[0] if career_season_options else f"{window_prefix}regular"
                season_options = career_season_options
                season_groups = [
                    {
                        "type_code": "career",
                        "type_name": "Career",
                        "type_name_plural": "Career",
                        "all_value": None,
                        "seasons": [
                            {"value": window_prefix + season_type, "label": label}
                            for season_type, label in WINDOW_SEASON_LABELS
                            if window_prefix + season_type in window_season_set
                        ],
                    }
                ]
            elif is_virtual_game_window:
                # Only expose one "All <type>" option per season-type that has
                # at least one game in the last N seasons of that type.
                from collections import defaultdict

                type_buckets = defaultdict(list)
                for s in season_values:
                    if s and len(s) == 5 and s.isdigit():
                        type_buckets[s[0]].append(s)
                season_options = [f"all_{code}" for code in ["2", "4", "5", "1", "3"] if code in type_buckets]
                season_groups = []
                for type_code in ["2", "4", "5", "1", "3"]:
                    if type_code not in type_buckets:
                        continue
                    season_groups.append(
                        {
                            "type_code": type_code,
                            "type_name": deps.t()(
                                deps.season_type_names().get(type_code, type_code),
                                {
                                    "Regular Season": "常规赛",
                                    "Playoffs": "季后赛",
                                    "PlayIn": "附加赛",
                                    "Pre Season": "季前赛",
                                    "All Star": "全明星",
                                }.get(deps.season_type_names().get(type_code, type_code), deps.season_type_names().get(type_code, type_code)),
                            ),
                            "type_name_plural": deps.t()(
                                deps.season_type_plural().get(type_code, type_code),
                                {
                                    "Regular Seasons": "常规赛",
                                    "Playoffs": "季后赛",
                                    "PlayIn": "附加赛",
                                    "Pre Seasons": "季前赛",
                                    "All Star": "全明星",
                                }.get(deps.season_type_plural().get(type_code, type_code), deps.season_type_plural().get(type_code, type_code)),
                            ),
                            "all_value": f"all_{type_code}",
                            "seasons": [],
                        }
                    )
            elif is_season_scope:
                show_all_seasons = True
                if not all_season_type:
                    all_season_type = "2"
                season_options = sorted([s for s in season_values if not is_career_season(s) and s != CAREER_SEASON], key=deps.season_sort_key(), reverse=True)
                current_metric_season = deps.pick_current_season()(season_options)
                from collections import defaultdict

                type_buckets = defaultdict(list)
                for s in season_options:
                    if len(s) == 5 and s.isdigit():
                        type_buckets[s[0]].append(s)
                season_groups = []
                for type_code in ["2", "4", "5", "1", "3"]:
                    if type_code in type_buckets:
                        season_groups.append(
                            {
                                "type_code": type_code,
                                "type_name": deps.t()(
                                    deps.season_type_names().get(type_code, type_code),
                                    {
                                        "Regular Season": "常规赛",
                                        "Playoffs": "季后赛",
                                        "PlayIn": "附加赛",
                                        "Pre Season": "季前赛",
                                        "All Star": "全明星",
                                    }.get(deps.season_type_names().get(type_code, type_code), deps.season_type_names().get(type_code, type_code)),
                                ),
                                "type_name_plural": deps.t()(
                                    deps.season_type_plural().get(type_code, type_code),
                                    {
                                        "Regular Seasons": "常规赛",
                                        "Playoffs": "季后赛",
                                        "PlayIn": "附加赛",
                                        "Pre Seasons": "季前赛",
                                        "All Star": "全明星",
                                    }.get(deps.season_type_plural().get(type_code, type_code), deps.season_type_plural().get(type_code, type_code)),
                                ),
                                "all_value": f"all_{type_code}",
                                "seasons": [],
                            }
                        )
            else:
                season_options = sorted([s for s in season_values if not is_career_season(s) and s != CAREER_SEASON], key=deps.season_sort_key(), reverse=True)
                current_metric_season = deps.pick_current_season()(season_options)
                from collections import defaultdict

                type_buckets = defaultdict(list)
                for s in season_options:
                    if len(s) == 5 and s.isdigit():
                        type_buckets[s[0]].append(s)
                season_groups = []
                for type_code in ["2", "4", "5", "1", "3"]:
                    if type_code in type_buckets:
                        season_groups.append(
                            {
                                "type_code": type_code,
                                "type_name": deps.t()(
                                    deps.season_type_names().get(type_code, type_code),
                                    {
                                        "Regular Season": "常规赛",
                                        "Playoffs": "季后赛",
                                        "PlayIn": "附加赛",
                                        "Pre Season": "季前赛",
                                        "All Star": "全明星",
                                    }.get(deps.season_type_names().get(type_code, type_code), deps.season_type_names().get(type_code, type_code)),
                                ),
                                "type_name_plural": deps.t()(
                                    deps.season_type_plural().get(type_code, type_code),
                                    {
                                        "Regular Seasons": "常规赛",
                                        "Playoffs": "季后赛",
                                        "PlayIn": "附加赛",
                                        "Pre Seasons": "季前赛",
                                        "All Star": "全明星",
                                    }.get(deps.season_type_plural().get(type_code, type_code), deps.season_type_plural().get(type_code, type_code)),
                                ),
                                "all_value": f"all_{type_code}",
                                "seasons": type_buckets[type_code],
                            }
                        )
                if not show_all_seasons and not selected_season and season_options:
                    selected_season = season_options[0]

            filtered_q = session.query(MetricResultModel).filter(MetricResultModel.metric_key == query_metric_key, MetricResultModel.value_num.isnot(None))
            if is_virtual_game_window:
                if virtual_window_seasons:
                    filtered_q = filtered_q.filter(MetricResultModel.season.in_(virtual_window_seasons))
                else:
                    filtered_q = filtered_q.filter(False)
            elif show_all_seasons and all_season_type:
                filtered_q = filtered_q.filter(MetricResultModel.season.like(f"{all_season_type}%"))
            elif not show_all_seasons and selected_season:
                filtered_q = filtered_q.filter(MetricResultModel.season == selected_season)

            has_sub_keys = (
                session.query(MetricResultModel.id)
                .filter(MetricResultModel.metric_key == query_metric_key, MetricResultModel.sub_key != "")
                .limit(1)
                .first()
            ) is not None

            if has_sub_keys and not expand and not entity_filter_id and not sub_key_filter:
                is_asc_dedup = deps.metric_rank_order()(session, query_metric_key) == "asc"
                dedup_order = MetricResultModel.value_num.asc() if is_asc_dedup else MetricResultModel.value_num.desc()
                dedup_rn = func.row_number().over(
                    partition_by=[MetricResultModel.entity_type, MetricResultModel.entity_id, MetricResultModel.season],
                    order_by=dedup_order,
                ).label("_dedup_rn")
                dedup_sub = filtered_q.with_entities(MetricResultModel.id, dedup_rn).subquery()
                filtered_q = (
                    session.query(MetricResultModel)
                    .join(dedup_sub, MetricResultModel.id == dedup_sub.c.id)
                    .filter(dedup_sub.c._dedup_rn == 1)
                )

            rank_partition = func.coalesce(MetricResultModel.rank_group, "__all__")
            is_asc = deps.metric_rank_order()(session, query_metric_key) == "asc"
            detail_rank_val = -MetricResultModel.value_num if is_asc else MetricResultModel.value_num
            rank_group_fields = [MetricResultModel.metric_key, rank_partition]
            if not show_all_seasons:
                rank_group_fields.insert(1, MetricResultModel.season)
            ranked_q = (
                filtered_q.with_entities(
                    MetricResultModel.id.label("id"),
                    MetricResultModel.entity_type.label("entity_type"),
                    MetricResultModel.entity_id.label("entity_id"),
                    MetricResultModel.season.label("season"),
                    MetricResultModel.sub_key.label("sub_key"),
                    MetricResultModel.rank_group.label("rank_group"),
                    MetricResultModel.value_num.label("value_num"),
                    MetricResultModel.value_str.label("value_str"),
                    MetricResultModel.context_json.label("context_json"),
                    MetricResultModel.computed_at.label("computed_at"),
                    func.rank().over(partition_by=rank_group_fields, order_by=detail_rank_val.desc()).label("rank"),
                    func.count(MetricResultModel.id).over(partition_by=rank_group_fields).label("standing_total"),
                ).subquery()
            )

            if sort_by == "season":
                detail_sort_cols = [ranked_q.c.season.desc(), ranked_q.c.value_num.asc() if is_asc else ranked_q.c.value_num.desc()]
            else:
                detail_sort_cols = [ranked_q.c.value_num.asc() if is_asc else ranked_q.c.value_num.desc(), ranked_q.c.entity_id.asc()]
            base_rows_q = session.query(ranked_q).order_by(*detail_sort_cols)

            if active_only and metric_def.scope in ("player", "player_franchise"):
                active_player_ids = [r[0] for r in session.query(Player.player_id).filter(Player.is_active == True).all()]
                if metric_def.scope == "player":
                    base_rows_q = base_rows_q.filter(ranked_q.c.entity_id.in_(active_player_ids))
                else:
                    active_like_filters = [ranked_q.c.entity_id.like(f"{pid}:%") for pid in active_player_ids]
                    base_rows_q = base_rows_q.filter(or_(*active_like_filters)) if active_like_filters else base_rows_q.filter(False)

            if entity_filter_id:
                base_rows_q = base_rows_q.filter(ranked_q.c.entity_id == entity_filter_id)
            if sub_key_filter:
                base_rows_q = base_rows_q.filter(ranked_q.c.sub_key == sub_key_filter)

            if entity_filter_id or sub_key_filter:
                rows = base_rows_q.limit(500).all()
                total = len(rows)
                total_pages = 1
                page = 1
            elif search_q:
                matching_player_ids = [
                    r[0]
                    for r in session.query(Player.player_id)
                    .filter(or_(Player.full_name.ilike(f"%{search_q}%"), Player.full_name_zh.ilike(f"%{search_q}%")))
                    .all()
                ]
                matching_team_ids = [
                    r[0]
                    for r in session.query(Team.team_id)
                    .filter(or_(Team.full_name.ilike(f"%{search_q}%"), Team.full_name_zh.ilike(f"%{search_q}%")))
                    .all()
                ]
                name_filters = []
                if matching_player_ids:
                    name_filters.append(and_(ranked_q.c.entity_type == "player", ranked_q.c.entity_id.in_(matching_player_ids)))
                if matching_team_ids:
                    name_filters.append(and_(ranked_q.c.entity_type == "team", ranked_q.c.entity_id.in_(matching_team_ids)))
                if name_filters:
                    base_rows_q = base_rows_q.filter(or_(*name_filters))
                else:
                    base_rows_q = base_rows_q.filter(False)
                rows = base_rows_q.limit(200).all()
                total = len(rows)
                total_pages = 1
                page = 1
            else:
                import math

                total = base_rows_q.count() or 0
                total_pages = max(1, math.ceil(total / page_size))
                page = min(page, total_pages)
                offset = (page - 1) * page_size
                rows = base_rows_q.offset(offset).limit(page_size).all()

            labels, player_active, game_info = _resolve_entity_labels(session, rows)
            team_map = deps.team_map()(session)

            sub_key_labels: dict[str, dict] = {}
            sub_key_type = getattr(metric_def, "sub_key_type", None)
            if sub_key_type and rows:
                sub_key_values = {str(r.sub_key) for r in rows if r.sub_key}
                if sub_key_type == "team" and sub_key_values:
                    for tid in sub_key_values:
                        team = team_map.get(tid)
                        if team:
                            sub_key_labels[tid] = {
                                "label": deps.display_team_name()(team) or team.abbr or tid,
                                "abbr": team.abbr,
                                "team_id": tid,
                                "slug": getattr(team, "slug", None),
                            }
                        else:
                            sub_key_labels[tid] = {"label": tid, "abbr": None, "team_id": tid, "slug": None}
                elif sub_key_type == "player" and sub_key_values:
                    pid_rows = (
                        session.query(Player.player_id, Player.full_name, Player.full_name_zh, Player.slug)
                        .filter(Player.player_id.in_(list(sub_key_values)))
                        .all()
                    )
                    for pid, fn, fn_zh, slug in pid_rows:
                        sub_key_labels[str(pid)] = {
                            "label": (fn_zh if deps.is_zh() and fn_zh else fn) or str(pid),
                            "player_id": str(pid),
                            "slug": slug,
                        }
            rank_labels = {1: "Best", 2: "2nd best", 3: "3rd best"}
            scope_label = {"player": "players", "player_franchise": "franchise stints", "team": "teams", "game": "results", "season": "seasons"}.get(metric_def.scope, "entities")
            if is_career_metric:
                period = "across all seasons"
            elif show_all_seasons:
                type_name = deps.t()(
                    deps.season_type_names().get(all_season_type, "").lower(),
                    {
                        "2": "常规赛",
                        "4": "季后赛",
                        "5": "附加赛",
                        "1": "季前赛",
                        "3": "全明星",
                    }.get(all_season_type, deps.season_type_names().get(all_season_type, "").lower()),
                )
                period = f"跨全部{type_name}" if deps.is_zh() and type_name else ("across all seasons" if not type_name else f"across all {type_name} seasons")
            else:
                period = "this season"

            base_key = family_base_key(metric_key)
            detail_db_templates = deps.load_context_label_templates()(session, {base_key})

            # Batch-load game metadata for rows whose context has qualifying_game_ids,
            # so we can render inline drill-down tables like the player page does.
            all_qual_ids: set[str] = set()
            parsed_contexts: list[dict] = []
            for row in rows:
                _ctx = {}
                if row.context_json:
                    try:
                        _ctx = json.loads(row.context_json)
                    except Exception:
                        _ctx = {}
                parsed_contexts.append(_ctx)
                for gid in (_ctx.get("qualifying_game_ids") or []):
                    if gid:
                        all_qual_ids.add(str(gid))
            Game = deps.game_model()
            games_meta_map: dict[str, dict] = {}
            if all_qual_ids:
                game_rows = (
                    session.query(
                        Game.game_id,
                        Game.game_date,
                        Game.season,
                        Game.home_team_id,
                        Game.road_team_id,
                        Game.home_team_score,
                        Game.road_team_score,
                        Game.slug,
                    )
                    .filter(Game.game_id.in_(list(all_qual_ids)))
                    .all()
                )
                for g in game_rows:
                    games_meta_map[str(g.game_id)] = {
                        "game_id": str(g.game_id),
                        "date": g.game_date.isoformat() if g.game_date else "",
                        "season": str(g.season) if g.season else "",
                        "home_team_id": str(g.home_team_id) if g.home_team_id else "",
                        "road_team_id": str(g.road_team_id) if g.road_team_id else "",
                        "home_score": g.home_team_score,
                        "road_score": g.road_team_score,
                        "slug": g.slug,
                    }

            result_rows = []
            for row_idx, row in enumerate(rows):
                ctx = parsed_contexts[row_idx]
                games_counted = (
                    ctx.get("games")
                    or ctx.get("total_games")
                    or ctx.get("games_played")
                    or ctx.get("games_leading_at_half")
                    or ctx.get("games_trailing_at_half")
                    or ctx.get("road_games")
                    or ctx.get("home_games")
                )
                rank_group_label = deps.team_name()(team_map, row.rank_group) if row.rank_group else None
                context_label = deps.resolve_context_label()(base_key, ctx, detail_db_templates)
                rank = int(row.rank or 0)
                standing_total = int(row.standing_total or 0)
                is_notable = standing_total > 0 and rank / standing_total <= 0.25
                label = rank_labels.get(rank, f"#{rank}")
                group_phrase = f" in {rank_group_label}" if rank_group_label else ""
                notable_reason = f"{label} of {standing_total} {scope_label}{group_phrase} {period}."
                player_id_for_active = row.entity_id.split(":")[0] if row.entity_type in ("player", "player_franchise") else None
                game_home_team_id = game_road_team_id = game_road_abbr = game_home_abbr = game_date_str = None
                if row.entity_type == "game" and row.entity_id:
                    gid = row.entity_id.split(":")[0]
                    gi = game_info.get(gid)
                    if gi:
                        game_home_team_id = str(gi[1]) if gi[1] else None
                        game_road_team_id = str(gi[2]) if gi[2] else None
                        game_road_abbr = deps.team_abbr()(team_map, gi[2])
                        game_home_abbr = deps.team_abbr()(team_map, gi[1])
                        game_date_str = deps.fmt_date()(gi[0])
                # Build per-row games_meta from qualifying_game_ids (sorted by date desc)
                qual_ids = ctx.get("qualifying_game_ids") or []
                row_games_meta: list[dict] = []
                for gid in qual_ids:
                    meta = games_meta_map.get(str(gid))
                    if meta:
                        row_games_meta.append(meta)
                row_games_meta.sort(key=lambda m: m.get("date") or "", reverse=True)
                qual_total = ctx.get("qualifying_game_total") or len(qual_ids)

                result_rows.append(
                    {
                        "rank": rank,
                        "total": standing_total,
                        "entity_type": row.entity_type,
                        "entity_id": row.entity_id,
                        "entity_label": labels.get((row.entity_type, row.entity_id), row.entity_id),
                        "is_active": player_active.get(player_id_for_active) if player_id_for_active else None,
                        "home_team_id": game_home_team_id,
                        "road_team_id": game_road_team_id,
                        "road_abbr": game_road_abbr,
                        "home_abbr": game_home_abbr,
                        "game_date_str": game_date_str,
                        "season": deps.season_label()(row.season),
                        "season_raw": row.season,
                        "sub_key": row.sub_key or "",
                        "sub_key_info": sub_key_labels.get(str(row.sub_key)) if row.sub_key else None,
                        "value_num": row.value_num,
                        "value_str": row.value_str,
                        "is_notable": is_notable,
                        "notable_reason": notable_reason if is_notable else None,
                        "context": ctx,
                        "context_label": context_label,
                        "rank_group": row.rank_group,
                        "rank_group_label": rank_group_label,
                        "games_counted": int(games_counted) if games_counted is not None else None,
                        "games_meta": row_games_meta,
                        "qual_total": qual_total,
                    }
                )
            show_rank_group = any(r["rank_group_label"] for r in result_rows)

            _, backfill = deps.build_metric_backfill_status()(session, query_metric_key)
            dd_key = query_metric_key
            if is_career_metric and window_type_from_key(metric_key) is not None:
                from metrics.framework.runtime import _metric_declares_career_reducer as _mcr
                if runtime_metric and _mcr(runtime_metric):
                    dd_key = family_base_key(metric_key)
            has_drilldown = (
                session.query(MetricRunLog.game_id)
                .filter(MetricRunLog.metric_key == dd_key, MetricRunLog.qualified == True)
                .limit(1)
                .first()
            ) is not None
            metric_deep_dive = deps.metric_deep_dive_state()(session, query_metric_key)
            feature_access = deps.get_feature_access_config()(session)

            metric_perf_samples = []
            if deps.is_admin()():
                perf_key = family_base_key(metric_key)
                perf_rows = (
                    session.query(MetricPerfLog.duration_ms, MetricPerfLog.recorded_at)
                    .filter(MetricPerfLog.metric_key == perf_key)
                    .order_by(MetricPerfLog.recorded_at.desc())
                    .limit(5)
                    .all()
                )
                metric_perf_samples = [{"ms": r.duration_ms, "at": r.recorded_at} for r in perf_rows]

        if is_career_metric:
            display_season_label = "Career"
        elif show_all_seasons:
            type_name = deps.t()(
                deps.season_type_plural().get(all_season_type, "Seasons"),
                {
                    "2": "常规赛",
                    "4": "季后赛",
                    "5": "附加赛",
                    "1": "季前赛",
                    "3": "全明星",
                }.get(all_season_type, "赛季"),
            )
            display_season_label = f"全部{type_name}" if deps.is_zh() else f"All {type_name}"
        else:
            display_season_label = deps.season_label()(selected_season)
        current_metric_season_label = deps.season_label()(current_metric_season) if current_metric_season else None
        is_player_scope = metric_def.scope in ("player", "player_franchise")
        return deps.render_template()(
            "metric_detail.html",
            metric_def=metric_def,
            result_rows=result_rows,
            show_rank_group=show_rank_group,
            is_player_scope=is_player_scope,
            is_season_scope=is_season_scope,
            active_only=active_only,
            season_options=season_options,
            season_groups=season_groups,
            selected_season=selected_season,
            show_all_seasons=show_all_seasons,
            all_season_type=all_season_type,
            is_career_metric=is_career_metric,
            current_window_type=current_window_type,
            window_tabs=window_tabs,
            related_metrics=related_metrics,
            season_label=display_season_label,
            current_metric_season=current_metric_season,
            current_metric_season_label=current_metric_season_label,
            fmt_season=deps.season_label(),
            fmt_season_short=deps.season_year_label(),
            page=page,
            total_pages=total_pages,
            total=total,
            page_size=page_size,
            backfill=backfill,
            has_drilldown=has_drilldown,
            search_q=search_q,
            entity_filter_id=entity_filter_id,
            sub_key_filter=sub_key_filter,
            entity_filter_label=labels.get(("player", entity_filter_id)) or labels.get(("team", entity_filter_id)) or entity_filter_id if entity_filter_id else "",
            sub_key_filter_info=sub_key_labels.get(sub_key_filter) if sub_key_filter else None,
            metric_deep_dive=metric_deep_dive,
            has_sub_keys=has_sub_keys,
            expand=expand,
            sort_by=sort_by,
            metric_perf_samples=metric_perf_samples,
            **deps.build_metric_feature_context()(feature_access),
        )

    app.add_url_rule("/cn/metrics/<metric_key>", endpoint="metric_detail_zh", view_func=metric_detail)
    app.add_url_rule("/metrics/<metric_key>", endpoint="metric_detail", view_func=metric_detail)

    return SimpleNamespace(metric_detail=metric_detail)
