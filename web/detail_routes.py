from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Callable

from flask import abort, jsonify, redirect, request
from sqlalchemy import case, func, text

from db.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_LIVE, GAME_STATUS_UPCOMING, get_game_status
from web.live_game_data import build_live_game_stub, fetch_live_card, fetch_live_game_detail, fetch_live_scoreboard_map


def _request_line_score_backfill_enabled() -> bool:
    value = os.getenv("FUNBA_ENABLE_REQUEST_LINE_SCORE_BACKFILL", "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _format_tipoff_et(iso_et: str | None) -> str | None:
    if not iso_et:
        return None
    try:
        text = iso_et.strip()
        if text.endswith("Z"):
            text = text[:-1]
        dt = datetime.fromisoformat(text)
        return dt.strftime("%-I:%M %p ET")
    except Exception:
        return None


def _safe_model_attr(model, attr: str):
    return getattr(model, attr, None)


def _lookup_by_slug_or_id(session, model, slug: str, *, id_attr: str, prefix: str | None = None):
    row = None
    slug_col = _safe_model_attr(model, "slug")
    if slug_col is not None:
        row = session.query(model).filter(slug_col == slug).first()
    if row is not None:
        return row
    legacy_id = slug.removeprefix(prefix) if prefix and slug.startswith(prefix) else slug
    id_col = _safe_model_attr(model, id_attr)
    if id_col is None:
        return None
    return session.query(model).filter(id_col == legacy_id).first()


def _build_upcoming_preview(session, game, teams, live_summary, *, headshot_fn=None):
    """Pre-tipoff context for the game detail page.

    Returns a dict with both teams' season W-L, last 10, head-to-head
    series (plus last 10 H2H across all seasons), each team's top scorer
    from their most recent completed game (with headshot), rest days /
    B2B flag, and a formatted tipoff time. The template renders this in
    place of the empty box-score panels. Pure read; no caching needed
    (low traffic on upcoming pages).
    """
    try:
        from db.models import Game as GameModel, Player, PlayerGameStats
    except (ImportError, AttributeError):
        return None

    if not game.home_team_id or not game.road_team_id or not game.game_date:
        return None

    try:
        # Pick the latest in-progress regular season as the record source.
        current_season_row = (
            session.query(GameModel.season)
            .filter(GameModel.wining_team_id.isnot(None))
            .filter(GameModel.season.like("2%"))
            .order_by(GameModel.game_date.desc())
            .limit(1)
            .first()
        )
        current_season = current_season_row[0] if current_season_row else None
        if not current_season:
            return None

        team_ids = {game.home_team_id, game.road_team_id}
        past_games = (
            session.query(GameModel)
            .filter(
                GameModel.season == current_season,
                GameModel.wining_team_id.isnot(None),
                (GameModel.home_team_id.in_(team_ids)) | (GameModel.road_team_id.in_(team_ids)),
            )
            .order_by(GameModel.game_date.desc(), GameModel.game_id.desc())
            .all()
        )

        team_games: dict[str, list] = defaultdict(list)
        for pg in past_games:
            if pg.home_team_id in team_ids:
                team_games[pg.home_team_id].append(pg)
            if pg.road_team_id in team_ids:
                team_games[pg.road_team_id].append(pg)

        def _team_block(team_id):
            rows = team_games.get(team_id, [])
            wins = sum(1 for pg in rows if pg.wining_team_id == team_id)
            losses = len(rows) - wins
            last10 = ["W" if pg.wining_team_id == team_id else "L" for pg in reversed(rows[:10])]
            rest = None
            if rows:
                days = (game.game_date - rows[0].game_date).days
                if days > 0:
                    rest = {"days": days, "is_b2b": days == 1}
            return {
                "team_id": team_id,
                "wins": wins,
                "losses": losses,
                "last10": last10,
                "rest": rest,
            }

        home_block = _team_block(game.home_team_id)
        road_block = _team_block(game.road_team_id)

        # Head-to-head series this season.
        home_h2h = 0
        road_h2h = 0
        for pg in past_games:
            if {pg.home_team_id, pg.road_team_id} != team_ids:
                continue
            if pg.wining_team_id == game.home_team_id:
                home_h2h += 1
            elif pg.wining_team_id == game.road_team_id:
                road_h2h += 1
        h2h = None
        if home_h2h + road_h2h > 0:
            h2h = {"home_wins": home_h2h, "road_wins": road_h2h}

        tipoff = None
        if live_summary:
            tipoff = _format_tipoff_et(live_summary.get("game_time_et"))

        # Last-10 head-to-head across all seasons — gives a trend chip strip.
        h2h_history_rows = (
            session.query(GameModel)
            .filter(
                GameModel.wining_team_id.isnot(None),
                GameModel.home_team_id.in_(team_ids),
                GameModel.road_team_id.in_(team_ids),
            )
            .filter(GameModel.game_date < game.game_date)
            .order_by(GameModel.game_date.desc(), GameModel.game_id.desc())
            .limit(10)
            .all()
        )
        h2h_history = []
        for pg in reversed(h2h_history_rows):
            if {pg.home_team_id, pg.road_team_id} != team_ids:
                continue
            h2h_history.append(
                {
                    "game_id": pg.game_id,
                    "game_date": pg.game_date,
                    "home_team_id": pg.home_team_id,
                    "road_team_id": pg.road_team_id,
                    "winner_team_id": pg.wining_team_id,
                    "home_score": pg.home_team_score,
                    "road_score": pg.road_team_score,
                }
            )

        # Top scorer from each team's previous completed game (any matchup).
        def _last_game_scorer(team_id):
            prev_rows = team_games.get(team_id, [])
            if not prev_rows:
                return None
            prev = prev_rows[0]  # most recent past game
            row = (
                session.query(PlayerGameStats, Player)
                .outerjoin(Player, Player.player_id == PlayerGameStats.player_id)
                .filter(
                    PlayerGameStats.game_id == prev.game_id,
                    PlayerGameStats.team_id == team_id,
                    PlayerGameStats.pts.isnot(None),
                )
                .order_by(PlayerGameStats.pts.desc())
                .limit(1)
                .one_or_none()
            )
            if not row:
                return None
            pgs, player = row
            return {
                "player_id": pgs.player_id,
                "name": (player.full_name if player else pgs.player_id),
                "slug": (player.slug if player else None),
                "pts": int(pgs.pts or 0),
                "reb": int(pgs.reb or 0) if pgs.reb is not None else None,
                "ast": int(pgs.ast or 0) if pgs.ast is not None else None,
                "headshot_url": headshot_fn(pgs.player_id) if headshot_fn else None,
                "prev_game_id": prev.game_id,
                "prev_game_date": prev.game_date,
            }

        home_block["last_game_scorer"] = _last_game_scorer(game.home_team_id)
        road_block["last_game_scorer"] = _last_game_scorer(game.road_team_id)

        return {
            "home": home_block,
            "road": road_block,
            "h2h": h2h,
            "h2h_history": h2h_history,
            "tipoff_et": tipoff,
        }
    except Exception:
        return None


def _safe_fetch_live_card(game_id: str):
    """Best-effort wrapper around fetch_live_card that never raises."""
    try:
        return fetch_live_card(game_id)
    except Exception:
        return None


def _build_game_leaders(session, game, *, headshot_fn=None):
    """Top scorer / rebounder / assister from each team for a completed game.

    Returns {'home': {...}, 'road': {...}} where each side is a dict of three
    leader rows plus the team_id. Used on the completed game page as a
    visual summary above the full box score.
    """
    from db.models import PlayerGameStats, Player

    if not game.home_team_id or not game.road_team_id:
        return None

    rows = (
        session.query(PlayerGameStats, Player)
        .outerjoin(Player, Player.player_id == PlayerGameStats.player_id)
        .filter(PlayerGameStats.game_id == game.game_id)
        .all()
    )
    if not rows:
        return None

    by_team: dict[str, list] = defaultdict(list)
    for pgs, player in rows:
        by_team[pgs.team_id].append((pgs, player))

    def _pick(team_id, stat_name):
        rows = by_team.get(team_id, [])
        if not rows:
            return None
        best = max(rows, key=lambda pair: int(getattr(pair[0], stat_name, 0) or 0))
        pgs, player = best
        val = int(getattr(pgs, stat_name, 0) or 0)
        if val <= 0:
            return None
        return {
            "player_id": pgs.player_id,
            "name": player.full_name if player else pgs.player_id,
            "slug": player.slug if player else None,
            "value": val,
            "headshot_url": headshot_fn(pgs.player_id) if headshot_fn else None,
        }

    def _side(team_id):
        return {
            "team_id": team_id,
            "scorer": _pick(team_id, "pts"),
            "rebounder": _pick(team_id, "reb"),
            "assister": _pick(team_id, "ast"),
        }

    return {
        "home": _side(game.home_team_id),
        "road": _side(game.road_team_id),
    }


def _build_live_quick_panel(game, *, live_card=None):
    """Flatten the live_card dict from fetch_live_card() into a template-ready
    structure for the in-progress game page. Falls back to an empty shell
    (fields None) so the template can still render placeholder rows that
    the JS refresh loop will fill in on the next poll."""
    if not game.home_team_id or not game.road_team_id:
        return None
    card = live_card or {}
    return {
        "home_team_id": game.home_team_id,
        "road_team_id": game.road_team_id,
        "home_fg_pct": card.get("home_fg_pct"),
        "road_fg_pct": card.get("road_fg_pct"),
        "home_fg3_pct": card.get("home_fg3_pct"),
        "road_fg3_pct": card.get("road_fg3_pct"),
        "home_scorer": card.get("home_scorer"),
        "road_scorer": card.get("road_scorer"),
        "home_rebounder": card.get("home_rebounder"),
        "road_rebounder": card.get("road_rebounder"),
        "home_assister": card.get("home_assister"),
        "road_assister": card.get("road_assister"),
        "home_win_probability": card.get("home_win_probability"),
        "road_win_probability": card.get("road_win_probability"),
        "hot_player_ids": card.get("hot_player_ids") or [],
    }


def register_detail_routes(
    app,
    *,
    get_session_local: Callable[[], Any],
    get_render_template: Callable[..., Any],
    get_player_model: Callable[[], Any],
    get_award_model: Callable[[], Any],
    get_game_model: Callable[[], Any],
    get_shot_record_model: Callable[[], Any],
    get_player_game_stats_model: Callable[[], Any],
    get_player_salary_model: Callable[[], Any],
    get_team_model: Callable[[], Any],
    get_team_game_stats_model: Callable[[], Any],
    get_game_pbp_model: Callable[[], Any],
    get_team_map: Callable[[], Callable[[Any], dict[str, Any]]],
    get_award_order_case: Callable[[], Callable[[Any], Any]],
    get_award_badge_label: Callable[[], Callable[[str], str]],
    get_player_career_summary: Callable[[], Callable[..., tuple[dict[str, str | int], list[dict[str, object]]]]],
    get_build_shot_zone_heatmap: Callable[[], Callable[[list[Any]], tuple[list[dict[str, object]], object | None, object | None]]],
    get_season_sort_key: Callable[[], Callable[[str | None], tuple[int, int]]],
    get_season_label: Callable[[], Callable[[str | None], str]],
    get_is_pro: Callable[[], Callable[[], bool]],
    get_pick_current_season: Callable[[], Callable[[list[str]], str | None]],
    get_team_abbr: Callable[[], Callable[[dict[str, Any], str | None], str]],
    get_fmt_date: Callable[[], Callable[[Any], str]],
    get_player_status: Callable[[], Callable[[Any], str]],
    get_fmt_minutes: Callable[[], Callable[[int | None, int | None], str]],
    get_localized_url_for: Callable[[], Callable[..., str]],
    get_build_game_metrics_payload: Callable[[], Callable[[Any, str, str | None], dict]],
    get_metric_results: Callable[[], Callable[[Any, str, str, str | None], dict]],
    get_game_triggered_entity_metrics: Callable[[], Callable[[Any, str, str | None], dict]],
    get_season_start_year_label: Callable[[], Callable[[int | None], str]],
    get_season_year_label: Callable[[], Callable[[str | None], str]],
    get_coerce_award_season: Callable[[], Callable[[str | int | None], int | None]],
    get_team_name: Callable[[], Callable[[dict[str, Any], str | None], str]],
    get_display_player_name: Callable[[], Callable[[Any], str]],
    get_pbp_event_type_label: Callable[[], Callable[[Any], str]],
    get_pbp_text: Callable[[], Callable[[Any], str]],
    get_paperclip_issue_url: Callable[[], Callable[[str | None], str | None]],
    get_game_analysis_issue_history: Callable[[], Callable[[str], list[Any]]],
    get_player_headshot_url: Callable[[], Callable[[str | None], str | None]],
    get_logger: Callable[[], Any],
):
    def player_page(slug: str):
        SessionLocal = get_session_local()
        Player = get_player_model()
        Award = get_award_model()
        Game = get_game_model()
        ShotRecord = get_shot_record_model()
        PlayerGameStats = get_player_game_stats_model()
        PlayerSalary = get_player_salary_model()

        with SessionLocal() as session:
            player = _lookup_by_slug_or_id(session, Player, slug, id_attr="player_id", prefix="player-")
            if player is not None and getattr(player, "slug", None) and player.slug != slug:
                redirect_params = request.args.to_dict(flat=True)
                return redirect(
                    get_localized_url_for()("player_page", slug=player.slug, **redirect_params),
                    code=302,
                )
            if player is None:
                abort(404, description=f"Player not found")
            player_id = player.player_id

            player_award_rows = (
                session.query(
                    Award.award_type,
                    func.count(Award.id).label("award_count"),
                )
                .filter(Award.player_id == player_id)
                .group_by(Award.award_type)
                .order_by(get_award_order_case()(Award.award_type))
                .all()
            )
            player_awards = [
                {
                    "award_type": row.award_type,
                    "label": get_award_badge_label()(row.award_type),
                    "count": int(row.award_count or 0),
                }
                for row in player_award_rows
            ]

            has_playin_stats = session.query(
                session.query(PlayerGameStats.game_id)
                .join(Game, PlayerGameStats.game_id == Game.game_id)
                .filter(PlayerGameStats.player_id == player_id, Game.season.like("5%"))
                .exists()
            ).scalar()

            _season_prefix_by_kind = {"regular": "2", "playoffs": "4", "playin": "5"}
            _career_label_by_kind = {
                "regular": "Regular Season",
                "playoffs": "Playoffs",
                "playin": "Play-In",
            }
            _allowed_kinds = {"regular", "playoffs"} | ({"playin"} if has_playin_stats else set())
            selected_career_kind = request.args.get("career_kind", "regular")
            if selected_career_kind not in _allowed_kinds:
                selected_career_kind = "regular"
            season_prefix = _season_prefix_by_kind[selected_career_kind]
            career_kind_label = _career_label_by_kind[selected_career_kind]

            teams = get_team_map()(session)
            career_overall, career_season_rows = get_player_career_summary()(
                session,
                player_id,
                season_prefix=season_prefix,
                teams=teams,
            )

            heatmap_season_rows = (
                session.query(Game.season)
                .join(ShotRecord, ShotRecord.game_id == Game.game_id)
                .filter(
                    ShotRecord.player_id == player_id,
                    Game.season.like(f"{season_prefix}%"),
                    ShotRecord.shot_zone_basic.isnot(None),
                )
                .distinct()
                .all()
            )
            heatmap_season_options = sorted([row[0] for row in heatmap_season_rows], key=get_season_sort_key(), reverse=True)
            selected_heatmap_season = request.args.get("heatmap_season", "overall")
            if selected_heatmap_season != "overall" and selected_heatmap_season not in heatmap_season_options:
                selected_heatmap_season = "overall"

            shot_base_filter = [
                ShotRecord.player_id == player_id,
                Game.season.like(f"{season_prefix}%"),
            ]

            shot_query = (
                session.query(ShotRecord.loc_x, ShotRecord.loc_y, ShotRecord.shot_made)
                .join(Game, ShotRecord.game_id == Game.game_id)
                .filter(*shot_base_filter, ShotRecord.loc_x.isnot(None), ShotRecord.loc_y.isnot(None))
            )
            zone_query = (
                session.query(ShotRecord.shot_zone_basic, ShotRecord.shot_zone_area, ShotRecord.shot_made)
                .join(Game, ShotRecord.game_id == Game.game_id)
                .filter(*shot_base_filter, ShotRecord.shot_zone_basic.isnot(None))
            )
            if selected_heatmap_season != "overall":
                shot_query = shot_query.filter(Game.season == selected_heatmap_season)
                zone_query = zone_query.filter(Game.season == selected_heatmap_season)

            shot_rows = shot_query.all()
            shot_dots = [{"x": row.loc_x, "y": row.loc_y, "made": bool(row.shot_made)} for row in shot_rows]
            shot_attempts = len(shot_dots)
            shot_made_count = sum(1 for dot in shot_dots if dot["made"])
            heatmap_zones, _, _ = get_build_shot_zone_heatmap()(zone_query.all())
            heatmap_scope_label = (
                f"Overall {career_kind_label}"
                if selected_heatmap_season == "overall"
                else get_season_label()(selected_heatmap_season)
            )

            seasons = (
                session.query(Game.season)
                .join(PlayerGameStats, Game.game_id == PlayerGameStats.game_id)
                .filter(PlayerGameStats.player_id == player_id, Game.season.isnot(None))
                .distinct()
                .all()
            )
            season_options = sorted([row[0] for row in seasons], key=get_season_sort_key(), reverse=True)
            if not get_is_pro()():
                current = get_pick_current_season()(season_options)
                if current:
                    season_options = [current]
            selected_season = request.args.get("season")
            if selected_season not in season_options:
                selected_season = get_pick_current_season()(season_options)

            game_rows = []
            if selected_season is not None:
                rows = (
                    session.query(PlayerGameStats, Game)
                    .join(Game, PlayerGameStats.game_id == Game.game_id)
                    .filter(PlayerGameStats.player_id == player_id, Game.season == selected_season)
                    .order_by(Game.game_date.desc(), Game.game_id.desc())
                    .all()
                )
                from web.historical_team_locations import get_era_abbr_for_year as _era_abbr
                for stat, game in rows:
                    # Season start year first, so the row's matchup/abbrs can
                    # use era-appropriate codes (SEA in 2007, not OKC).
                    _season_str = str(game.season or "")
                    _season_year = int(_season_str[1:]) if len(_season_str) == 5 and _season_str.isdigit() else None

                    def _abbr(tid):
                        if _season_year is not None:
                            historic = _era_abbr(tid, _season_year)
                            if historic:
                                return historic
                        return get_team_abbr()(teams, tid)

                    if stat.team_id == game.home_team_id:
                        opponent_id = game.road_team_id
                        matchup = f"{_abbr(stat.team_id)} vs {_abbr(opponent_id)}"
                    else:
                        opponent_id = game.home_team_id
                        matchup = f"{_abbr(stat.team_id)} @ {_abbr(opponent_id)}"

                    if game.wining_team_id:
                        result = "W" if game.wining_team_id == stat.team_id else "L"
                    else:
                        result = "-"

                    is_home = stat.team_id == game.home_team_id
                    player_team_score = game.home_team_score if is_home else game.road_team_score
                    opponent_score = game.road_team_score if is_home else game.home_team_score

                    game_rows.append(
                        {
                            "game_id": game.game_id,
                            "game_date": get_fmt_date()(game.game_date),
                            "matchup": matchup,
                            "result": result,
                            "status": get_player_status()(stat),
                            "minutes": get_fmt_minutes()(stat.min, stat.sec),
                            "pts": stat.pts if stat.pts is not None else "-",
                            "reb": stat.reb if stat.reb is not None else "-",
                            "ast": stat.ast if stat.ast is not None else "-",
                            "player_team_id": stat.team_id,
                            "player_team_abbr": _abbr(stat.team_id),
                            "player_team_href": get_localized_url_for()("team_page", slug=teams[stat.team_id].slug) if stat.team_id and stat.team_id in teams and teams[stat.team_id].slug else None,
                            "opponent_id": opponent_id,
                            "opponent_abbr": _abbr(opponent_id),
                            "opponent_href": get_localized_url_for()("team_page", slug=teams[opponent_id].slug) if opponent_id and opponent_id in teams and teams[opponent_id].slug else None,
                            "is_home": is_home,
                            "player_team_score": player_team_score,
                            "opponent_score": opponent_score,
                            "season_year": _season_year,
                        }
                    )

            player_current_team_id = game_rows[0]["player_team_id"] if game_rows else None
            if not player_current_team_id:
                latest_team = (
                    session.query(PlayerGameStats.team_id)
                    .join(Game, PlayerGameStats.game_id == Game.game_id)
                    .filter(PlayerGameStats.player_id == player_id)
                    .order_by(Game.game_date.desc())
                    .first()
                )
                if latest_team:
                    player_current_team_id = latest_team[0]

            player_current_team = teams.get(player_current_team_id) if player_current_team_id else None

            player_metrics = get_metric_results()(session, "player", player_id, selected_season)

            # ── Team stint timeline ──
            player_stint_timeline = []
            try:
                from db.models import TeamRosterStint as _TRS
            except (ImportError, AttributeError):
                _TRS = None
            if _TRS is not None:
                from web.historical_team_locations import get_era_abbr_for_year as _tl_abbr
                from web.historical_team_locations import get_era_name_for_year as _tl_name
                stint_rows = (
                    session.query(_TRS)
                    .filter(_TRS.player_id == player_id)
                    .order_by(_TRS.joined_at.asc())
                    .all()
                )
                # Merge adjacent stints on the same team (can happen if game-derived +
                # snapshot both produced rows with a tiny overlap or touching span).
                merged: list[dict] = []
                from datetime import date as _today_date
                _current_yr = _today_date.today().year
                for s in stint_rows:
                    team_obj = teams.get(s.team_id)
                    if merged and merged[-1]["team_id"] == s.team_id:
                        prev = merged[-1]
                        prev_left = prev["left_at"]
                        new_left = s.left_at
                        if prev_left is None or new_left is None:
                            prev["left_at"] = None
                            prev["left_year"] = None
                            prev["is_active"] = True
                        elif new_left > prev_left:
                            prev["left_at"] = new_left
                            prev["left_year"] = new_left.year
                        # Recompute era + abbr from the (possibly updated) end year
                        era_year = prev["left_year"] if prev["left_year"] is not None else _current_yr
                        prev["era_year"] = era_year
                        era_name = _tl_name(prev["team_id"], era_year)
                        era_abbr = _tl_abbr(prev["team_id"], era_year)
                        team_obj = teams.get(prev["team_id"])
                        if era_name:
                            prev["team_name"] = era_name
                        if era_abbr:
                            prev["team_abbr"] = era_abbr
                        continue
                    joined_year = s.joined_at.year if s.joined_at else None
                    left_year = s.left_at.year if s.left_at else None
                    # Era lookup year = end of stint (or current year if still active).
                    # Using the end year gives the "most recent" era label — e.g. a
                    # 2007-2016 stint labels as OKC (2016), not SEA (2007).
                    era_year = left_year if left_year is not None else _current_yr
                    era_name = None
                    era_abbr = None
                    if era_year is not None:
                        era_name = _tl_name(s.team_id, era_year)
                        era_abbr = _tl_abbr(s.team_id, era_year)
                    merged.append({
                        "team_id": s.team_id,
                        "team_slug": team_obj.slug if team_obj else None,
                        "team_name": era_name or (team_obj.full_name if team_obj else "?"),
                        "team_abbr": era_abbr or (team_obj.abbr if team_obj else "?"),
                        "joined_at": s.joined_at,
                        "left_at": s.left_at,
                        "joined_year": joined_year,
                        "left_year": left_year,
                        "era_year": era_year,
                        "is_active": s.left_at is None,
                        "how_acquired": s.how_acquired,
                    })
                # Season code (5-digit, regular season) of the last season the
                # player spent with that team — used to deep-link the team page
                # to the era the stint actually covered, instead of today.
                for entry in merged:
                    left_at_final = entry.get("left_at")
                    if left_at_final is None:
                        entry["last_season_code"] = None
                    else:
                        start_year = left_at_final.year if left_at_final.month >= 10 else left_at_final.year - 1
                        entry["last_season_code"] = f"2{start_year}"
                player_stint_timeline = merged

            salary_records = (
                session.query(PlayerSalary)
                .filter(PlayerSalary.player_id == player_id)
                .order_by(PlayerSalary.season.desc())
                .all()
            )
            salary_rows = [
                SimpleNamespace(
                    season=row.season,
                    season_label=get_season_start_year_label()(row.season),
                    salary_usd=row.salary_usd,
                )
                for row in salary_records
            ]

        return get_render_template()(
            "player.html",
            player=player,
            selected_career_kind=selected_career_kind,
            career_kind_label=career_kind_label,
            has_playin_stats=has_playin_stats,
            career_overall=career_overall,
            career_season_rows=career_season_rows,
            selected_heatmap_season=selected_heatmap_season,
            heatmap_season_options=heatmap_season_options,
            heatmap_scope_label=heatmap_scope_label,
            shot_dots=shot_dots,
            shot_attempts=shot_attempts,
            shot_made_count=shot_made_count,
            heatmap_zones=heatmap_zones,
            player_current_team_id=player_current_team_id,
            player_current_team=player_current_team,
            season_options=season_options,
            selected_season=selected_season,
            game_rows=game_rows,
            player_metrics=player_metrics,
            player_awards=player_awards,
            salary_rows=salary_rows,
            player_stint_timeline=player_stint_timeline,
        )

    def team_page(slug: str):
        SessionLocal = get_session_local()
        Team = get_team_model()
        Award = get_award_model()
        Game = get_game_model()
        TeamGameStats = get_team_game_stats_model()

        with SessionLocal() as session:
            team = _lookup_by_slug_or_id(session, Team, slug, id_attr="team_id", prefix="team-")
            if team is not None and getattr(team, "slug", None) and team.slug != slug:
                redirect_params = request.args.to_dict(flat=True)
                return redirect(
                    get_localized_url_for()("team_page", slug=team.slug, **redirect_params),
                    code=302,
                )
            if team is None:
                abort(404, description=f"Team not found")
            team_id = team.team_id

            canonical_team = None
            if getattr(team, "canonical_team_id", None) and team.canonical_team_id != team.team_id:
                canonical_team = session.query(Team).filter(Team.team_id == team.canonical_team_id).first()

            championship_rows = (
                session.query(Award.season)
                .filter(Award.award_type == "champion", Award.team_id == team_id)
                .order_by(Award.season.desc())
                .all()
            )
            team_championships = [
                {
                    "season": int(row.season),
                    "season_label": get_season_year_label()(str(row.season)),
                }
                for row in championship_rows
                if get_coerce_award_season()(row.season) is not None
            ]

            season_summary_rows = (
                session.query(
                    Game.season.label("season"),
                    func.sum(case((TeamGameStats.win.is_(True), 1), else_=0)).label("wins"),
                    func.sum(case((TeamGameStats.win.is_(False), 1), else_=0)).label("losses"),
                    func.count(TeamGameStats.game_id).label("games"),
                    func.sum(func.coalesce(TeamGameStats.fgm, 0)).label("fgm"),
                    func.sum(func.coalesce(TeamGameStats.fga, 0)).label("fga"),
                    func.sum(func.coalesce(TeamGameStats.fg3m, 0)).label("fg3m"),
                    func.sum(func.coalesce(TeamGameStats.fg3a, 0)).label("fg3a"),
                    func.sum(func.coalesce(TeamGameStats.ftm, 0)).label("ftm"),
                    func.sum(func.coalesce(TeamGameStats.fta, 0)).label("fta"),
                )
                .join(Game, TeamGameStats.game_id == Game.game_id)
                .filter(TeamGameStats.team_id == team_id, Game.season.isnot(None))
                .group_by(Game.season)
                .order_by(Game.season.desc())
                .all()
            )
            season_summary = [
                {
                    "season": row.season,
                    "wins": int(row.wins or 0),
                    "losses": int(row.losses or 0),
                    "games": int(row.games or 0),
                    "fg_pct": f"{int(row.fgm or 0) / int(row.fga or 1):.3f}" if int(row.fga or 0) > 0 else "0.000",
                    "fg3_pct": f"{int(row.fg3m or 0) / int(row.fg3a or 1):.3f}" if int(row.fg3a or 0) > 0 else "0.000",
                    "ft_pct": f"{int(row.ftm or 0) / int(row.fta or 1):.3f}" if int(row.fta or 0) > 0 else "0.000",
                }
                for row in season_summary_rows
            ]
            season_summary.sort(key=lambda row: get_season_sort_key()(row["season"]), reverse=True)

            season_kind = request.args.get("season_kind", "regular")
            if season_kind not in {"regular", "playoffs"}:
                season_kind = "regular"

            if season_kind == "regular":
                season_summary_view = [row for row in season_summary if str(row["season"]).startswith("2")]
            else:
                season_summary_view = [row for row in season_summary if str(row["season"]).startswith("4")]

            current_season = get_pick_current_season()([row["season"] for row in season_summary])
            season_options = [row["season"] for row in season_summary]

            # `?season=<5-digit-code>` is the canonical param; `?games_season=`
            # is still accepted as a legacy alias from older links/bookmarks.
            _req_season = request.args.get("season") or request.args.get("games_season")
            if _req_season and _req_season not in season_options:
                # If the request specified a year without a valid 5-digit code
                # (e.g., `?season=22010` is valid but `?season=2010` is not),
                # try to promote a 4-digit year to the regular-season code.
                if _req_season.isdigit() and len(_req_season) == 4:
                    promoted = f"2{_req_season}"
                    if promoted in season_options:
                        _req_season = promoted
                    else:
                        _req_season = None
                else:
                    _req_season = None
            selected_season = _req_season or current_season
            selected_games_season = selected_season  # legacy var name

            teams = get_team_map()(session)
            current_games = []
            if selected_games_season is not None:
                rows = (
                    session.query(TeamGameStats, Game)
                    .join(Game, TeamGameStats.game_id == Game.game_id)
                    .filter(TeamGameStats.team_id == team_id, Game.season == selected_games_season)
                    .order_by(Game.game_date.desc(), Game.game_id.desc())
                    .all()
                )
                for stat, game in rows:
                    if stat.team_id == game.home_team_id:
                        opponent_id = game.road_team_id
                        where = "Home"
                        team_score = game.home_team_score
                        opp_score = game.road_team_score
                    else:
                        opponent_id = game.home_team_id
                        where = "Away"
                        team_score = game.road_team_score
                        opp_score = game.home_team_score

                    score = f"{team_score}-{opp_score}" if team_score is not None and opp_score is not None else "-"
                    if stat.win is None:
                        result = "-"
                        status = "Not finished"
                    elif stat.win:
                        result = "W"
                        status = "Win"
                    else:
                        result = "L"
                        status = "Loss"

                    opponent_team = teams.get(opponent_id) if opponent_id else None
                    current_games.append(
                        {
                            "game_id": game.game_id,
                            "game_date": get_fmt_date()(game.game_date),
                            "opponent_id": opponent_id,
                            "opponent_slug": opponent_team.slug if opponent_team else None,
                            "opponent_name": get_team_name()(teams, opponent_id),
                            "where": where,
                            "result": result,
                            "score": score,
                            "status": status,
                        }
                    )

            team_metrics = get_metric_results()(session, "team", team_id, selected_season)

            # ── Roster + coaching staff for the selected games season ──
            roster_players: list[dict] = []
            roster_coaches: list[dict] = []
            roster_season_label = None
            _sel_year = None
            if selected_games_season:
                s = str(selected_games_season)
                if len(s) == 5 and s.isdigit():
                    _sel_year = int(s[1:])
                roster_season_label = get_season_label()(selected_games_season) if selected_games_season else None

            # Date bounds for the selected season for this team (fallback Oct-Jun).
            try:
                from db.models import TeamRosterStint, TeamCoachStint, Player as _PlayerM
            except (ImportError, AttributeError):
                TeamRosterStint = TeamCoachStint = _PlayerM = None

            if _sel_year is not None and TeamRosterStint is not None and TeamCoachStint is not None and _PlayerM is not None:
                bounds_row = session.execute(text("""
                    SELECT MIN(g.game_date) AS lo, MAX(g.game_date) AS hi
                    FROM Game g
                    WHERE (g.home_team_id = :tid OR g.road_team_id = :tid)
                      AND CAST(SUBSTRING(CAST(g.season AS CHAR), 2) AS UNSIGNED) = :yr
                """), {"tid": team_id, "yr": _sel_year}).first()
                season_lo = bounds_row.lo if bounds_row and bounds_row.lo else None
                season_hi = bounds_row.hi if bounds_row and bounds_row.hi else None
                from datetime import date as _date
                if season_lo is None or season_hi is None:
                    season_lo = _date(_sel_year, 10, 1)
                    season_hi = _date(_sel_year + 1, 6, 30)

                # For an in-progress season, "still on the team" = still on
                # today (don't list players already traded away). For a past
                # season, "still on the team at end of season" — players
                # traded mid-season are credited to whichever team they
                # finished on, not both.
                today = _date.today()
                effective_end = min(season_hi, today)

                player_stint_rows = (
                    session.query(TeamRosterStint, _PlayerM)
                    .outerjoin(_PlayerM, _PlayerM.player_id == TeamRosterStint.player_id)
                    .filter(
                        TeamRosterStint.team_id == team_id,
                        TeamRosterStint.joined_at <= effective_end,
                        (TeamRosterStint.left_at.is_(None)) | (TeamRosterStint.left_at >= effective_end),
                    )
                    .order_by(TeamRosterStint.joined_at.asc())
                    .all()
                )
                seen_pids: set[str] = set()
                for stint, pl in player_stint_rows:
                    if stint.player_id in seen_pids:
                        continue
                    seen_pids.add(stint.player_id)
                    roster_players.append({
                        "player_id": stint.player_id,
                        "slug": pl.slug if pl else None,
                        "full_name": pl.full_name if pl else f"#{stint.player_id}",
                        "full_name_zh": getattr(pl, "full_name_zh", None) if pl else None,
                        "jersey": stint.jersey or (getattr(pl, "jersey", None) if pl else None),
                        "position": stint.position or (getattr(pl, "position", None) if pl else None),
                        "height": getattr(pl, "height", None) if pl else None,
                        "weight": getattr(pl, "weight", None) if pl else None,
                        "how_acquired": stint.how_acquired,
                        "joined_at": stint.joined_at,
                        "left_at": stint.left_at,
                        "is_active": stint.left_at is None,
                        # Per-game stats (filled in below)
                        "gp": None, "mpg": None, "ppg": None, "rpg": None, "apg": None,
                        "fg_pct": None, "fg3_pct": None,
                    })

                # Per-player season averages for the selected season
                if selected_games_season and roster_players:
                    PGS = get_player_game_stats_model()
                    stat_rows = (
                        session.query(
                            PGS.player_id,
                            func.count(PGS.game_id).label("gp"),
                            func.sum(func.coalesce(PGS.min, 0)).label("tmin"),
                            func.sum(func.coalesce(PGS.sec, 0)).label("tsec"),
                            func.sum(func.coalesce(PGS.pts, 0)).label("tpts"),
                            func.sum(func.coalesce(PGS.reb, 0)).label("treb"),
                            func.sum(func.coalesce(PGS.ast, 0)).label("tast"),
                            func.sum(func.coalesce(PGS.fgm, 0)).label("tfgm"),
                            func.sum(func.coalesce(PGS.fga, 0)).label("tfga"),
                            func.sum(func.coalesce(PGS.fg3m, 0)).label("tfg3m"),
                            func.sum(func.coalesce(PGS.fg3a, 0)).label("tfg3a"),
                        )
                        .join(Game, PGS.game_id == Game.game_id)
                        .filter(
                            PGS.team_id == team_id,
                            Game.season == selected_games_season,
                        )
                        .group_by(PGS.player_id)
                        .all()
                    )
                    stats_by_pid = {}
                    for r in stat_rows:
                        gp = int(r.gp or 0)
                        if gp == 0:
                            continue
                        total_sec = (int(r.tmin or 0) * 60) + int(r.tsec or 0)
                        stats_by_pid[r.player_id] = {
                            "gp": gp,
                            "mpg": (total_sec / 60) / gp,
                            "ppg": float(r.tpts or 0) / gp,
                            "rpg": float(r.treb or 0) / gp,
                            "apg": float(r.tast or 0) / gp,
                            "fg_pct": (float(r.tfgm or 0) / float(r.tfga)) if r.tfga else None,
                            "fg3_pct": (float(r.tfg3m or 0) / float(r.tfg3a)) if r.tfg3a else None,
                        }
                    for p in roster_players:
                        s = stats_by_pid.get(p["player_id"])
                        if s:
                            p.update(s)

                coach_stint_rows = (
                    session.query(TeamCoachStint)
                    .filter(
                        TeamCoachStint.team_id == team_id,
                        TeamCoachStint.joined_at <= effective_end,
                        (TeamCoachStint.left_at.is_(None)) | (TeamCoachStint.left_at >= effective_end),
                    )
                    .order_by(
                        TeamCoachStint.is_assistant.asc(),
                        TeamCoachStint.coach_name.asc(),
                    )
                    .all()
                )
                for c in coach_stint_rows:
                    roster_coaches.append({
                        "coach_id": c.coach_id,
                        "coach_name": c.coach_name,
                        "coach_type": c.coach_type or ("Assistant Coach" if c.is_assistant else "Head Coach"),
                        "is_assistant": bool(c.is_assistant),
                        "joined_at": c.joined_at,
                        "left_at": c.left_at,
                    })

            # Sort roster: players with stats first by PPG desc, then those
            # without stats by jersey number, then by name.
            def _sort_key(p):
                ppg = p.get("ppg")
                has_stats = ppg is not None
                return (
                    0 if has_stats else 1,
                    -(ppg or 0.0),
                    p["full_name"],
                )
            roster_players.sort(key=_sort_key)

            # ── Contract / cap-hit lookup for current + future seasons ──
            # PlayerContract uses 5-digit-stripped season ints (2024 == 2024-25),
            # matching _sel_year.
            salary_players: list[dict] = []
            salary_seasons: list[int] = []  # current + future for the multi-year grid
            salary_team_totals_by_season: dict[int, int] = {}
            salary_by_position: list[dict] = []
            cap_thresholds: dict | None = None
            future_window = 5  # current + next 4 future seasons

            try:
                from db.models import PlayerContract, PlayerContractYear
                from db.league_salary_caps import get_thresholds as _get_caps
            except (ImportError, AttributeError):
                PlayerContract = PlayerContractYear = None
                _get_caps = None

            if (
                PlayerContractYear is not None
                and _sel_year is not None
                and roster_players
            ):
                roster_pids = [p["player_id"] for p in roster_players]
                salary_seasons = list(range(_sel_year, _sel_year + future_window))

                contract_rows = (
                    session.query(PlayerContractYear, PlayerContract)
                    .join(PlayerContract, PlayerContractYear.contract_id == PlayerContract.id)
                    .filter(
                        PlayerContractYear.player_id.in_(roster_pids),
                        PlayerContractYear.season.in_(salary_seasons),
                    )
                    .all()
                )

                # Index per (player, season). When a player has overlapping
                # contracts (rare — typically a superseding extension), prefer
                # the one signed with this team, else the larger cap hit.
                by_pid_season: dict[tuple[str, int], tuple] = {}
                for cy, c in contract_rows:
                    key = (cy.player_id, cy.season)
                    cur = by_pid_season.get(key)
                    if cur is None:
                        by_pid_season[key] = (cy, c)
                        continue
                    cur_cy, cur_c = cur
                    if c.signed_with_team_id == team_id and cur_c.signed_with_team_id != team_id:
                        by_pid_season[key] = (cy, c)
                    elif (cy.cap_hit_usd or cy.cash_annual_usd or 0) > (
                        cur_cy.cap_hit_usd or cur_cy.cash_annual_usd or 0
                    ):
                        by_pid_season[key] = (cy, c)

                def _effective(cy_) -> int | None:
                    if cy_ is None:
                        return None
                    return cy_.cap_hit_usd or cy_.cash_annual_usd or None

                for p in roster_players:
                    pid = p["player_id"]
                    cur_pair = by_pid_season.get((pid, _sel_year))
                    if cur_pair is None:
                        p["contract"] = None
                        p["future_caps"] = []
                        continue
                    cy, c = cur_pair
                    p["contract"] = {
                        "cap_hit_usd": cy.cap_hit_usd,
                        "cash_annual_usd": cy.cash_annual_usd,
                        "status": cy.status,
                        "contract_type": c.contract_type,
                        "years": c.years,
                        "start_season": c.start_season,
                        "end_season": c.end_season,
                        "total_value_usd": c.total_value_usd,
                        "aav_usd": c.aav_usd,
                        "guaranteed_usd": c.guaranteed_usd,
                        "signed_with_team_id": c.signed_with_team_id,
                        "signed_using": c.signed_using,
                    }
                    p["effective_cap_usd"] = _effective(cy)
                    # Per-future-season cap hits, parallel array to salary_seasons
                    p["future_caps"] = []
                    for s in salary_seasons:
                        pair_s = by_pid_season.get((pid, s))
                        cy_s = pair_s[0] if pair_s else None
                        p["future_caps"].append({
                            "season": s,
                            "cap_hit_usd": _effective(cy_s),
                            "status": cy_s.status if cy_s else None,
                        })

                salary_players = sorted(
                    [p for p in roster_players if p.get("contract")],
                    key=lambda p: -(p.get("effective_cap_usd") or 0),
                )

                # ── Per-season team totals (powers the multi-year grid totals) ─
                for s in salary_seasons:
                    salary_team_totals_by_season[s] = sum(
                        (yr["cap_hit_usd"] or 0)
                        for p in salary_players
                        for yr in p["future_caps"]
                        if yr["season"] == s
                    )

                # ── Bucket by position (G / F / C) for the position view ──
                def _bucket(pos_str: str | None) -> str:
                    if not pos_str:
                        return "Other"
                    s = pos_str.upper().strip()
                    # Split on hyphen; primary token wins. "G-F" -> G, "C-F" -> C
                    primary = s.split("-")[0].split("/")[0].strip()
                    if primary in {"G", "GUARD", "PG", "SG"}:
                        return "G"
                    if primary in {"F", "FORWARD", "SF", "PF"}:
                        return "F"
                    if primary in {"C", "CENTER"}:
                        return "C"
                    return "Other"

                bucket_totals: dict[str, dict] = {b: {"position": b, "total_usd": 0, "count": 0, "players": []}
                                                  for b in ("G", "F", "C", "Other")}
                for p in salary_players:
                    bucket = _bucket(p.get("position"))
                    bucket_totals[bucket]["total_usd"] += p.get("effective_cap_usd") or 0
                    bucket_totals[bucket]["count"] += 1
                    bucket_totals[bucket]["players"].append({
                        "player_id": p["player_id"],
                        "full_name": p["full_name"],
                        "full_name_zh": p.get("full_name_zh"),
                        "slug": p.get("slug"),
                        "cap_hit_usd": p.get("effective_cap_usd") or 0,
                        "position": p.get("position"),
                    })
                # Drop empty Other and order G, F, C, then Other if non-empty
                salary_by_position = [bucket_totals[b] for b in ("G", "F", "C", "Other")
                                      if bucket_totals[b]["count"] > 0]

                # ── League cap thresholds for this season ──
                if _get_caps is not None:
                    th = _get_caps(_sel_year)
                    if th is not None:
                        cap_thresholds = {
                            "season": th.season,
                            "cap": th.cap,
                            "tax": th.tax,
                            "apron1": th.apron1,
                            "apron2": th.apron2,
                            "minimum_floor": th.minimum_floor,
                        }

        # ── Era-aware header data ─────────────────────────────────────
        # Resolve the franchise's name, abbreviation, city and logo as they
        # were during the selected season. For current-era seasons this just
        # echoes team.full_name / team.abbr; for historical seasons it
        # swaps in the era variant (Seattle SuperSonics, St. Louis Hawks…).
        from web.historical_team_locations import (
            get_era_entry_any,
            get_logo_url_for_year,
            get_era_abbr_for_year,
            get_era_name_for_year,
        )
        era = None
        era_logo_url = None
        if _sel_year is not None:
            era = get_era_entry_any(team_id, _sel_year)
            era_logo_url = get_logo_url_for_year(team_id, _sel_year, static_prefix="/")
        if era_logo_url is None:
            # Fall back to the team's best current logo (get_logo_url_for_year
            # already does this when called with a valid year, but we also
            # need a sane value when _sel_year is None).
            era_logo_url = get_logo_url_for_year(team_id, 2025, static_prefix="/")
        era_view = {
            "name": (era or {}).get("era_name") or get_display_team_name()(team),
            "abbr": (era or {}).get("abbr") or team.abbr or "",
            "city": (era or {}).get("city") or team.city or "",
            "state": (era or {}).get("state") or team.state or "",
            "logo_url": era_logo_url,
            "year_start": (era or {}).get("year_start"),
            "year_end": (era or {}).get("year_end"),
        }

        # Selected-season record (pulled out of season_summary so we don't
        # need a separate query). season_summary is keyed by 5-digit season
        # code — match exact.
        season_record = {"wins": 0, "losses": 0, "games": 0}
        for row in season_summary:
            if row["season"] == selected_season:
                season_record = {
                    "wins": row["wins"],
                    "losses": row["losses"],
                    "games": row["games"],
                }
                break

        # Split season_options into regular/playoff variants so the picker
        # can offer an R/P tab (same 4-digit year, different leading digit).
        regular_seasons = sorted(
            [s for s in season_options if str(s).startswith("2")],
            key=lambda s: int(str(s)[1:]) if str(s)[1:].isdigit() else 0,
        )
        playoff_seasons = sorted(
            [s for s in season_options if str(s).startswith("4")],
            key=lambda s: int(str(s)[1:]) if str(s)[1:].isdigit() else 0,
        )
        playoff_year_lookup = {str(s)[1:]: s for s in playoff_seasons}
        regular_year_lookup = {str(s)[1:]: s for s in regular_seasons}

        # For the barcode/picker UI, ship a minimal row per season with
        # W/L so users can see on hover what that bar represents.
        season_bars = []
        for s in regular_seasons:
            for row in season_summary:
                if row["season"] == s:
                    y = str(s)[1:]
                    season_bars.append({
                        "season": s,
                        "year_start": int(y) if y.isdigit() else None,
                        "year_label": f"{y}-{(int(y) + 1) % 100:02d}" if y.isdigit() else s,
                        "wins": row["wins"],
                        "losses": row["losses"],
                        "has_playoffs": y in playoff_year_lookup,
                    })
                    break

        # What kind is the selected season? (regular / playoffs)
        _sel_prefix = str(selected_season)[:1] if selected_season else ""
        selected_season_kind = "playoffs" if _sel_prefix == "4" else "regular"
        selected_year_label = (
            f"{_sel_year}-{(_sel_year + 1) % 100:02d}" if _sel_year is not None else "—"
        )

        # ── Team picker: which franchises played in the selected season ──
        # The team-hero exposes a popover that lets you jump to another
        # franchise while staying on the same #section. Scope the list to
        # teams that actually played in the currently-selected season so a
        # 1995 page surfaces Vancouver Grizzlies / Seattle SuperSonics, not
        # OKC / NOP. Era-aware abbr + name so the chips read correctly for
        # the historical season.
        team_picker_options: list[dict] = []
        if selected_season is not None:
            picker_team_ids = (
                session.query(TeamGameStats.team_id)
                .join(Game, TeamGameStats.game_id == Game.game_id)
                .filter(Game.season == selected_season)
                .distinct()
                .all()
            )
            picker_team_ids = [tid for (tid,) in picker_team_ids if tid]
            if picker_team_ids:
                picker_teams = (
                    session.query(Team)
                    .filter(Team.team_id.in_(picker_team_ids))
                    .all()
                )
                for pt in picker_teams:
                    if not pt.slug:
                        continue
                    era_year = _sel_year if _sel_year is not None else 2025
                    era_abbr = (
                        get_era_abbr_for_year(pt.team_id, era_year)
                        or pt.abbr
                        or ""
                    )
                    era_name = (
                        get_era_name_for_year(pt.team_id, era_year)
                        or pt.full_name
                        or pt.abbr
                        or pt.slug
                    )
                    team_picker_options.append({
                        "team_id": pt.team_id,
                        "slug": pt.slug,
                        "abbr": era_abbr,
                        "name": era_name,
                        "logo_url": get_logo_url_for_year(
                            pt.team_id, era_year, static_prefix="/"
                        ),
                        "is_current": pt.team_id == team_id,
                    })
                team_picker_options.sort(key=lambda o: (o["abbr"] or "", o["name"]))

        return get_render_template()(
            "team.html",
            team=team,
            season_summary=season_summary_view,
            season_kind=season_kind,
            current_season=current_season,
            season_options=season_options,
            selected_games_season=selected_games_season,  # legacy alias
            selected_season=selected_season,
            selected_year_label=selected_year_label,
            selected_season_kind=selected_season_kind,
            sel_year=_sel_year,
            era=era_view,
            season_record=season_record,
            season_bars=season_bars,
            regular_year_lookup=regular_year_lookup,
            playoff_year_lookup=playoff_year_lookup,
            current_games=current_games,
            team_metrics=team_metrics,
            team_championships=team_championships,
            canonical_team=canonical_team,
            roster_players=roster_players,
            roster_coaches=roster_coaches,
            roster_season_label=roster_season_label,
            salary_players=salary_players,
            salary_seasons=salary_seasons,
            salary_team_totals_by_season=salary_team_totals_by_season,
            salary_by_position=salary_by_position,
            cap_thresholds=cap_thresholds,
            team_picker_options=team_picker_options,
        )

    def game_page(slug: str):
        SessionLocal = get_session_local()
        Game = get_game_model()
        TeamGameStats = get_team_game_stats_model()
        PlayerGameStats = get_player_game_stats_model()
        Player = get_player_model()
        GamePlayByPlay = get_game_pbp_model()
        ShotRecord = get_shot_record_model()

        with SessionLocal() as session:
            persisted_game = _lookup_by_slug_or_id(session, Game, slug, id_attr="game_id", prefix="game-")
            if persisted_game is not None and getattr(persisted_game, "slug", None) and persisted_game.slug != slug:
                redirect_params = request.args.to_dict(flat=True)
                return redirect(
                    get_localized_url_for()("game_page", slug=persisted_game.slug, **redirect_params),
                    code=302,
                )
            game = persisted_game
            game_id = game.game_id if game else slug

            def _game_analysis_issues():
                return [
                    {
                        "id": item.id,
                        "issue_id": item.issue_id,
                        "issue_identifier": item.issue_identifier,
                        "issue_url": get_paperclip_issue_url()(item.issue_identifier),
                        "issue_status": item.issue_status,
                        "title": item.title,
                        "trigger_source": item.trigger_source,
                        "source_date": item.source_date,
                        "created_at": item.created_at,
                        "updated_at": item.updated_at,
                        "created_at_label": item.created_at.replace("T", " ")[:19] if item.created_at else None,
                        "updated_at_label": item.updated_at.replace("T", " ")[:19] if item.updated_at else None,
                        "posts": [
                            {
                                "post_id": int(post["post_id"]),
                                "topic": str(post.get("topic") or ""),
                                "status": str(post.get("status") or ""),
                                "source_date": str(post.get("source_date") or ""),
                                "discovered_via": str(post.get("discovered_via") or ""),
                            }
                            for post in item.posts
                        ],
                    }
                    for item in get_game_analysis_issue_history()(game_id)
                ]

            if game is None:
                live_payload = fetch_live_game_detail(game_id)
                live_summary = (live_payload or {}).get("summary") or fetch_live_scoreboard_map().get(game_id)
                if live_summary is None or live_summary.get("status") not in {GAME_STATUS_LIVE, GAME_STATUS_UPCOMING}:
                    abort(404, description=f"Game {game_id} not found")
                game = build_live_game_stub(live_summary)
                if game is None:
                    abort(404, description=f"Game {game_id} not found")
            else:
                live_payload = None

            if persisted_game is not None and _request_line_score_backfill_enabled():
                try:
                    from db.backfill_nba_game_line_score import back_fill_game_line_score, has_game_line_score

                    if not has_game_line_score(session, game_id):
                        back_fill_game_line_score(session, game_id, commit=True)
                except Exception:
                    get_logger().exception("inline line-score fetch failed for game_id=%s", game_id)

            teams = get_team_map()(session)
            game_status = get_game_status(game)
            live_summary = (live_payload or {}).get("summary") or fetch_live_scoreboard_map().get(game_id)

            # If the persisted row still looks "upcoming" but the NBA live feed
            # reports the game has tipped off, serve the live view directly —
            # otherwise the first page load shows an upcoming preview until the
            # 60s client poll forces a reload.
            if (
                game_status == GAME_STATUS_UPCOMING
                and live_summary
                and live_summary.get("status") == GAME_STATUS_LIVE
            ):
                game_status = GAME_STATUS_LIVE

            # Derive the season start year from the Game row so we can render
            # era-appropriate team names and abbreviations (Seattle SuperSonics
            # in 2007, not Oklahoma City Thunder, etc.).
            from web.historical_team_locations import (
                get_era_name_for_year,
                get_era_abbr_for_year,
            )
            _season_str = str(game.season or "")
            _game_season_year = int(_season_str[1:]) if len(_season_str) == 5 and _season_str.isdigit() else None

            def _era_team_name(team_id):
                if _game_season_year is not None:
                    era_name = get_era_name_for_year(team_id, _game_season_year)
                    if era_name:
                        return era_name
                return get_team_name()(teams, team_id)

            def _era_team_abbr(team_id):
                if _game_season_year is not None:
                    era_abbr = get_era_abbr_for_year(team_id, _game_season_year)
                    if era_abbr:
                        return era_abbr
                return get_team_abbr()(teams, team_id)

            def _render_scoreboard_only_game(*, refresh_interval_ms, game_analysis_issues):
                return get_render_template()(
                    "game.html",
                    game=game,
                    game_status=game_status,
                    live_summary=live_summary,
                    live_refresh_interval_ms=refresh_interval_ms,
                    team_name=_era_team_name,
                    team_abbr=_era_team_abbr,
                    fmt_date=get_fmt_date(),
                    team_stats=[],
                    players_by_team={},
                    ordered_team_ids=[team_id for team_id in [game.road_team_id, game.home_team_id] if team_id],
                    pbp_rows=[],
                    shot_rows=[],
                    shot_rows_by_team={},
                    shot_chart_team_ids=[],
                    shot_made_count=0,
                    shot_miss_count=0,
                    shot_made_count_by_team={},
                    shot_miss_count_by_team={},
                    shot_backfill_status=request.args.get("shot_backfill"),
                    shot_backfill_count=request.args.get("shot_count"),
                    score_progression_json="[]",
                    road_abbr=_era_team_abbr(game.road_team_id),
                    home_abbr=_era_team_abbr(game.home_team_id),
                    quarter_scores=[],
                    home_team_id=game.home_team_id,
                    game_analysis_issues=game_analysis_issues,
                    upcoming_preview=None,
                    game_leaders=None,
                    live_quick_panel=(
                        _build_live_quick_panel(
                            game,
                            live_card=_safe_fetch_live_card(game.game_id),
                        )
                        if game_status == GAME_STATUS_LIVE
                        else None
                    ),
                )

            def _render_with_live_payload(payload, *, refresh_interval_ms):
                """Render game.html using nba_api live data instead of DB rows.

                Used for in-progress games AND for completed games that haven't
                been ingested yet (the 10-minute backfill window).
                """
                import json as _json_live
                summary = payload["summary"]
                live_score_progression = payload.get("score_progression") or []
                return get_render_template()(
                    "game.html",
                    game=game,
                    game_status=game_status,
                    live_summary=summary,
                    live_refresh_interval_ms=refresh_interval_ms,
                    team_name=_era_team_name,
                    team_abbr=_era_team_abbr,
                    fmt_date=get_fmt_date(),
                    team_stats=payload["team_stats"],
                    players_by_team=payload["players_by_team"],
                    ordered_team_ids=[team_id for team_id in payload["ordered_team_ids"] if team_id],
                    pbp_rows=payload["pbp_rows"],
                    shot_rows=[],
                    shot_rows_by_team={},
                    shot_chart_team_ids=[],
                    shot_made_count=0,
                    shot_miss_count=0,
                    shot_made_count_by_team={},
                    shot_miss_count_by_team={},
                    shot_backfill_status=request.args.get("shot_backfill"),
                    shot_backfill_count=request.args.get("shot_count"),
                    score_progression_json=_json_live.dumps(live_score_progression) if len(live_score_progression) > 1 else "[]",
                    road_abbr=_era_team_abbr(game.road_team_id),
                    home_abbr=_era_team_abbr(game.home_team_id),
                    quarter_scores=payload["quarter_scores"],
                    home_team_id=game.home_team_id,
                    game_analysis_issues=_game_analysis_issues(),
                    upcoming_preview=None,
                    game_leaders=None,
                    live_quick_panel=(
                        _build_live_quick_panel(
                            game,
                            live_card=_safe_fetch_live_card(game.game_id),
                        )
                        if game_status == GAME_STATUS_LIVE
                        else None
                    ),
                )

            def _localize_live_player_names(payload):
                display_name = get_display_player_name()
                pids = set()
                for rows in (payload.get("players_by_team") or {}).values():
                    for row in rows:
                        if row.get("player_id"):
                            pids.add(str(row["player_id"]))
                if not pids:
                    return
                db_players = session.query(Player).filter(Player.player_id.in_(pids)).all()
                name_map = {str(p.player_id): display_name(p) for p in db_players}
                for rows in (payload.get("players_by_team") or {}).values():
                    for row in rows:
                        zh = name_map.get(str(row.get("player_id") or ""))
                        if zh:
                            row["player_name"] = zh

            if game_status == GAME_STATUS_LIVE:
                if live_payload is None:
                    live_payload = fetch_live_game_detail(game_id)
                if live_payload is not None:
                    _localize_live_player_names(live_payload)
                    return _render_with_live_payload(live_payload, refresh_interval_ms=15000)
                return _render_scoreboard_only_game(
                    refresh_interval_ms=15000,
                    game_analysis_issues=_game_analysis_issues(),
                )

            if game_status == GAME_STATUS_UPCOMING:
                upcoming_preview = _build_upcoming_preview(
                    session,
                    game,
                    teams,
                    live_summary,
                    headshot_fn=get_player_headshot_url(),
                )
                return get_render_template()(
                    "game.html",
                    game=game,
                    game_status=game_status,
                    live_summary=live_summary,
                    live_refresh_interval_ms=60000,
                    team_name=_era_team_name,
                    team_abbr=_era_team_abbr,
                    fmt_date=get_fmt_date(),
                    team_stats=[],
                    players_by_team={},
                    ordered_team_ids=[team_id for team_id in [game.road_team_id, game.home_team_id] if team_id],
                    pbp_rows=[],
                    shot_rows=[],
                    shot_rows_by_team={},
                    shot_chart_team_ids=[],
                    shot_made_count=0,
                    shot_miss_count=0,
                    shot_made_count_by_team={},
                    shot_miss_count_by_team={},
                    shot_backfill_status=request.args.get("shot_backfill"),
                    shot_backfill_count=request.args.get("shot_count"),
                    score_progression_json="[]",
                    road_abbr=_era_team_abbr(game.road_team_id),
                    home_abbr=_era_team_abbr(game.home_team_id),
                    quarter_scores=[],
                    home_team_id=game.home_team_id,
                    game_analysis_issues=[],
                    upcoming_preview=upcoming_preview,
                    game_leaders=None,
                    live_quick_panel=None,
                )

            team_stats_rows = (
                session.query(TeamGameStats)
                .filter(TeamGameStats.game_id == game_id)
                .order_by(TeamGameStats.team_id.asc())
                .all()
            )
            team_stats = sorted(team_stats_rows, key=lambda row: 0 if row.team_id == game.home_team_id else 1)

            # If the game is marked completed but ingestion hasn't run yet
            # (the 10-min `ingest_recent_games` window), DB stats are empty
            # but nba_api still serves the final box score. Fall back to the
            # live endpoint and render with that payload — the page reloads
            # itself in 60s so the eventual DB-backed view replaces it.
            if (
                not team_stats_rows
                and game_status == GAME_STATUS_COMPLETED
            ):
                if live_payload is None:
                    live_payload = fetch_live_game_detail(game_id)
                if live_payload is not None and live_payload.get("team_stats"):
                    _localize_live_player_names(live_payload)
                    return _render_with_live_payload(live_payload, refresh_interval_ms=60000)

            player_rows = (
                session.query(PlayerGameStats, Player)
                .outerjoin(Player, Player.player_id == PlayerGameStats.player_id)
                .filter(PlayerGameStats.game_id == game_id)
                .order_by(PlayerGameStats.team_id.asc(), PlayerGameStats.player_id.asc())
                .all()
            )

            def _is_dnp(stat):
                return (stat.min is None and stat.sec is None) or (stat.comment or "").strip() != ""

            def _sort_key(stat):
                if stat.starter:
                    return (0, -(stat.pts or 0))
                if not _is_dnp(stat):
                    return (1, -(stat.pts or 0))
                return (2, 0)

            players_by_team: dict[str, list[dict[str, str | int]]] = defaultdict(list)
            team_rows: dict[str, list] = defaultdict(list)
            for stat, player in player_rows:
                team_rows[stat.team_id].append((stat, player))
            for team_id, rows in team_rows.items():
                rows.sort(key=lambda row: _sort_key(row[0]))
                for stat, player in rows:
                    player_name = get_display_player_name()(player) if player is not None else stat.player_id
                    players_by_team[team_id].append(
                        {
                            "player_id": stat.player_id,
                            "player_name": player_name,
                            "status": get_player_status()(stat),
                            "minutes": get_fmt_minutes()(stat.min, stat.sec),
                            "is_starter": bool(stat.starter),
                            "is_dnp": _is_dnp(stat),
                            "pts": stat.pts if stat.pts is not None else "-",
                            "reb": stat.reb if stat.reb is not None else "-",
                            "ast": stat.ast if stat.ast is not None else "-",
                            "stl": stat.stl if stat.stl is not None else "-",
                            "blk": stat.blk if stat.blk is not None else "-",
                            "tov": stat.tov if stat.tov is not None else "-",
                            "fgm": stat.fgm if stat.fgm is not None else "-",
                            "fga": stat.fga if stat.fga is not None else "-",
                            "fg3m": stat.fg3m if stat.fg3m is not None else "-",
                            "fg3a": stat.fg3a if stat.fg3a is not None else "-",
                            "ftm": stat.ftm if stat.ftm is not None else "-",
                            "fta": stat.fta if stat.fta is not None else "-",
                            "plus_minus": stat.plus if stat.plus is not None else "-",
                        }
                    )

            ordered_team_ids = [team_id for team_id in [game.road_team_id, game.home_team_id] if team_id]
            for team_id in players_by_team:
                if team_id not in ordered_team_ids:
                    ordered_team_ids.append(team_id)

            pbp_rows_raw = (
                session.query(GamePlayByPlay)
                .filter(GamePlayByPlay.game_id == game_id)
                .order_by(GamePlayByPlay.period.asc(), GamePlayByPlay.event_num.asc(), GamePlayByPlay.id.asc())
                .all()
            )
            pbp_rows = [
                {
                    "event_num": row.event_num if row.event_num is not None else "-",
                    "period": row.period if row.period is not None else "-",
                    "clock": row.pc_time or row.wc_time or "-",
                    "event_type": get_pbp_event_type_label()(row.event_msg_type),
                    "event_type_code": row.event_msg_type,
                    "description": get_pbp_text()(row) or "-",
                    "score": row.score or "-",
                    "margin": row.score_margin or "-",
                    "team_id": (
                        game.home_team_id if row.home_description
                        else game.road_team_id if row.visitor_description
                        else None
                    ),
                }
                for row in pbp_rows_raw
            ]

            player_name_map = {
                str(stat.player_id): (get_display_player_name()(player) if player else str(stat.player_id))
                for stat, player in player_rows
            }

            score_progression = [{"t": 0.0, "road": 0, "home": 0, "scorer": None, "desc": None}]
            prev_road = prev_home = 0
            for row in pbp_rows_raw:
                if not row.score or row.period is None:
                    continue
                try:
                    parts = row.score.split("-")
                    if len(parts) != 2:
                        continue
                    home_score, road_score = int(parts[0].strip()), int(parts[1].strip())
                except (AttributeError, ValueError):
                    continue
                if road_score == prev_road and home_score == prev_home:
                    continue
                period = int(row.period)
                clock = row.pc_time or "0:00"
                try:
                    mins, secs = clock.split(":")
                    remaining = int(mins) * 60 + int(secs)
                except Exception:
                    remaining = 0
                if period <= 4:
                    offset = (period - 1) * 12 * 60
                    dur = 12 * 60
                else:
                    offset = 48 * 60 + (period - 5) * 5 * 60
                    dur = 5 * 60
                elapsed = round((offset + dur - remaining) / 60, 3)
                raw_desc = (
                    row.home_description if home_score > prev_home
                    else row.visitor_description if road_score > prev_road
                    else None
                ) or ""
                if row.player1_id and str(row.player1_id) in player_name_map:
                    scorer = player_name_map[str(row.player1_id)]
                else:
                    scorer = raw_desc.split()[0] if raw_desc.strip() else None
                desc = raw_desc.split("(")[0].strip() or None
                score_progression.append({"t": elapsed, "road": road_score, "home": home_score, "scorer": scorer, "desc": desc})
                prev_road, prev_home = road_score, home_score

            from metrics.helpers import get_quarter_scores as _get_quarter_scores

            quarter_scores = [
                {"period": row["period"], "home": row["home_pts"], "road": row["road_pts"]}
                for row in _get_quarter_scores(session, game_id)
            ]

            shot_rows_raw = (
                session.query(ShotRecord)
                .filter(ShotRecord.game_id == game_id, ShotRecord.shot_attempted.is_(True))
                .order_by(ShotRecord.id.asc())
                .all()
            )
            shot_player_ids = sorted({str(row.player_id) for row in shot_rows_raw if row.player_id})
            shot_player_map = {}
            if shot_player_ids:
                shot_players = session.query(Player).filter(Player.player_id.in_(shot_player_ids)).all()
                shot_player_map = {
                    str(player.player_id): (get_display_player_name()(player) or str(player.player_id))
                    for player in shot_players
                }

            shot_rows = []
            shot_rows_by_team: dict[str, list[dict[str, str | int | bool]]] = defaultdict(list)
            shot_made_count_by_team: dict[str, int] = defaultdict(int)
            shot_miss_count_by_team: dict[str, int] = defaultdict(int)
            made_count = 0
            miss_count = 0
            for row in shot_rows_raw:
                if row.loc_x is None or row.loc_y is None:
                    continue
                is_made = bool(row.shot_made)
                period = int(row.period or 0)
                if period <= 4:
                    period_label = f"Q{period}" if period > 0 else "-"
                else:
                    period_label = f"OT{period - 4}"
                if is_made:
                    made_count += 1
                else:
                    miss_count += 1
                shot = {
                    "x": row.loc_x,
                    "y": row.loc_y,
                    "made": is_made,
                    "period": period,
                    "period_label": period_label,
                    "clock": get_fmt_minutes()(row.min, row.sec),
                    "team_id": row.team_id,
                    "team_name": _era_team_name(row.team_id),
                    "player_id": row.player_id,
                    "player_name": shot_player_map.get(str(row.player_id), str(row.player_id or "-")),
                    "shot_type": row.shot_type or "-",
                    "shot_distance": row.shot_distance if row.shot_distance is not None else "-",
                }
                shot_rows.append(shot)
                if row.team_id:
                    shot_rows_by_team[row.team_id].append(shot)
                    if is_made:
                        shot_made_count_by_team[row.team_id] += 1
                    else:
                        shot_miss_count_by_team[row.team_id] += 1

            shot_chart_team_ids = [team_id for team_id in [game.road_team_id, game.home_team_id] if team_id]
            for team_id in shot_rows_by_team:
                if team_id not in shot_chart_team_ids:
                    shot_chart_team_ids.append(team_id)

            import json as _json

            score_progression_json = _json.dumps(score_progression)
            road_abbr = _era_team_abbr(game.road_team_id)
            home_abbr = _era_team_abbr(game.home_team_id)
            home_team_id = game.home_team_id
            game_analysis_issues = _game_analysis_issues()
            game_leaders = (
                _build_game_leaders(session, game, headshot_fn=get_player_headshot_url())
                if game_status == GAME_STATUS_COMPLETED
                else None
            )
            live_quick_panel = (
                _build_live_quick_panel(game, live_card=_safe_fetch_live_card(game.game_id))
                if game_status == GAME_STATUS_LIVE
                else None
            )

        return get_render_template()(
            "game.html",
            game=game,
            game_status=game_status,
            live_summary=live_summary,
            live_refresh_interval_ms=None,
            team_name=_era_team_name,
            team_abbr=_era_team_abbr,
            fmt_date=get_fmt_date(),
            team_stats=team_stats,
            players_by_team=players_by_team,
            ordered_team_ids=ordered_team_ids,
            pbp_rows=pbp_rows,
            shot_rows=shot_rows,
            shot_rows_by_team=shot_rows_by_team,
            shot_chart_team_ids=shot_chart_team_ids,
            shot_made_count=made_count,
            shot_miss_count=miss_count,
            shot_made_count_by_team=shot_made_count_by_team,
            shot_miss_count_by_team=shot_miss_count_by_team,
            shot_backfill_status=request.args.get("shot_backfill"),
            shot_backfill_count=request.args.get("shot_count"),
            score_progression_json=score_progression_json,
            road_abbr=road_abbr,
            home_abbr=home_abbr,
            quarter_scores=quarter_scores,
            home_team_id=home_team_id,
            game_analysis_issues=game_analysis_issues,
            upcoming_preview=None,
            game_leaders=game_leaders,
            live_quick_panel=live_quick_panel,
        )

    def game_fragment_metrics(slug: str):
        SessionLocal = get_session_local()
        Game = get_game_model()

        with SessionLocal() as session:
            game = session.query(Game).filter(Game.slug == slug).first()
            if game is None:
                abort(404)
            payload = get_build_game_metrics_payload()(session, game.game_id, game.season)
        return get_render_template()(
            "_game_metrics.html",
            game_metrics=payload["game_metrics"],
            triggered_player_metrics=payload["triggered_player_metrics"],
            triggered_team_metrics=payload["triggered_team_metrics"],
        )

    def api_game_period_stats(game_id: str):
        """Return per-period player stats for a game. Fetch from NBA API if not in DB."""
        from db.models import PlayerGamePeriodStats

        SessionLocal = get_session_local()
        Game = get_game_model()

        with SessionLocal() as session:
            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game is None:
                return jsonify({"ok": False, "error": "game not found"}), 404

            rows = (
                session.query(PlayerGamePeriodStats)
                .filter(PlayerGamePeriodStats.game_id == game_id)
                .order_by(PlayerGamePeriodStats.period, PlayerGamePeriodStats.player_id)
                .all()
            )

            if not rows:
                # Try fetching from NBA API
                try:
                    from db.backfill_nba_game_detail import (
                        create_player_period_stats,
                        fetch_all_period_stats,
                    )

                    periods = fetch_all_period_stats(game_id)
                    if periods:
                        for period, period_rows in periods.items():
                            for ps in period_rows:
                                create_player_period_stats(session, game_id, period, ps)
                        session.commit()
                        rows = (
                            session.query(PlayerGamePeriodStats)
                            .filter(PlayerGamePeriodStats.game_id == game_id)
                            .order_by(PlayerGamePeriodStats.period, PlayerGamePeriodStats.player_id)
                            .all()
                        )
                except Exception:
                    get_logger().exception("Failed to fetch period stats for %s", game_id)

            result: dict[str, list[dict]] = {}
            for r in rows:
                result.setdefault(r.player_id, []).append({
                    "period": r.period,
                    "min": r.min or 0,
                    "sec": r.sec or 0,
                    "pts": r.pts or 0,
                    "reb": r.reb or 0,
                    "ast": r.ast or 0,
                    "stl": r.stl or 0,
                    "blk": r.blk or 0,
                    "tov": r.tov or 0,
                    "fgm": r.fgm or 0,
                    "fga": r.fga or 0,
                    "fg3m": r.fg3m or 0,
                    "fg3a": r.fg3a or 0,
                    "ftm": r.ftm or 0,
                    "fta": r.fta or 0,
                    "pf": r.pf or 0,
                    "plus_minus": r.plus_minus or 0,
                })

        return jsonify({"ok": True, "players": result})

    app.add_url_rule("/api/games/<game_id>/period-stats", endpoint="api_game_period_stats", view_func=api_game_period_stats)

    app.add_url_rule("/cn/players/<slug>", endpoint="player_page_zh", view_func=player_page)
    app.add_url_rule("/players/<slug>", endpoint="player_page", view_func=player_page)
    app.add_url_rule("/cn/teams/<slug>", endpoint="team_page_zh", view_func=team_page)
    app.add_url_rule("/teams/<slug>", endpoint="team_page", view_func=team_page)
    app.add_url_rule("/cn/games/<slug>", endpoint="game_page_zh", view_func=game_page)
    app.add_url_rule("/games/<slug>", endpoint="game_page", view_func=game_page)
    app.add_url_rule("/cn/games/<slug>/fragment/metrics", endpoint="game_fragment_metrics_zh", view_func=game_fragment_metrics)
    app.add_url_rule("/games/<slug>/fragment/metrics", endpoint="game_fragment_metrics", view_func=game_fragment_metrics)

    return SimpleNamespace(
        player_page=player_page,
        team_page=team_page,
        game_page=game_page,
        game_fragment_metrics=game_fragment_metrics,
    )
