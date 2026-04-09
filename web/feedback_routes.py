from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable

from flask import render_template, request


def register_feedback_routes(
    app,
    *,
    get_session_local: Callable[[], Any],
    get_current_user: Callable[[], Any],
    get_feedback_model: Callable[[], Any],
    get_user_model: Callable[[], Any],
    require_admin_page: Callable[[], Any],
    limiter,
):
    def submit_feedback():
        user = get_current_user()
        if not user:
            return {"error": "login_required"}, 401

        content = (request.json or {}).get("content", "").strip()
        if not content:
            return {"error": "empty"}, 400
        if len(content) > 2000:
            return {"error": "too_long"}, 400

        page_url = (request.json or {}).get("page_url", "")[:500] or None

        from datetime import datetime

        SessionLocal = get_session_local()
        Feedback = get_feedback_model()
        with SessionLocal() as db:
            fb = Feedback(
                user_id=user.id,
                content=content,
                page_url=page_url,
                created_at=datetime.utcnow(),
            )
            db.add(fb)
            db.commit()
        return {"ok": True}, 201

    def admin_feedback():
        denied = require_admin_page()
        if denied:
            return denied

        SessionLocal = get_session_local()
        Feedback = get_feedback_model()
        User = get_user_model()
        with SessionLocal() as db:
            rows = (
                db.query(Feedback, User)
                .join(User, Feedback.user_id == User.id)
                .order_by(Feedback.created_at.desc())
                .limit(200)
                .all()
            )

        items = [
            {
                "id": fb.id,
                "content": fb.content,
                "page_url": fb.page_url,
                "created_at": fb.created_at,
                "user_display_name": u.display_name,
                "user_email": u.email,
                "user_avatar": u.avatar_url,
            }
            for fb, u in rows
        ]
        return render_template("admin_feedback.html", items=items)

    app.add_url_rule(
        "/feedback",
        endpoint="submit_feedback",
        view_func=limiter.limit("10 per minute")(submit_feedback),
        methods=["POST"],
    )
    app.add_url_rule("/admin/feedback", endpoint="admin_feedback", view_func=admin_feedback)

    return SimpleNamespace(
        submit_feedback=submit_feedback,
        admin_feedback=admin_feedback,
    )
