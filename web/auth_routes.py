from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import urlparse

from flask import flash, redirect, render_template, request, session, url_for


def create_oauth(app):
    from authlib.integrations.flask_client import OAuth

    oauth = OAuth(app)
    oauth.register(
        name="google",
        client_id=os.environ.get("GOOGLE_CLIENT_ID"),
        client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )
    return oauth


def _safe_redirect_url(url: str | None) -> str:
    """Return a safe local redirect target; fall back to home.

    Accepts:
    - Local paths: /foo, /foo?q=1
    - Same-origin absolute URLs: http://localhost:5001/foo, normalized to /foo

    Rejects protocol-relative (//evil.com) and cross-origin URLs.
    """
    if not url:
        return url_for("home")

    parsed = urlparse(url)
    if not parsed.scheme and not parsed.netloc:
        if parsed.path.startswith("/") and not url.startswith("//"):
            return url
        return url_for("home")

    if parsed.scheme in ("http", "https") and parsed.netloc == request.host:
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        if parsed.fragment:
            path += "#" + parsed.fragment
        return path

    return url_for("home")


def register_auth_routes(
    app,
    *,
    get_session_local: Callable[[], Any],
    get_oauth: Callable[[], Any],
    get_logger: Callable[[], Any],
    get_user_model: Callable[[], Any],
    get_magic_token_model: Callable[[], Any],
    create_user_id: Callable[[], str],
    limiter,
):
    def auth_login():
        """Show login page with Google and email options."""
        next_url = _safe_redirect_url(request.args.get("next") or request.referrer)
        session["oauth_next"] = next_url
        return render_template("login.html", next_url=next_url)

    def auth_google():
        """Redirect to Google OAuth consent screen."""
        if not os.environ.get("GOOGLE_CLIENT_ID") or os.environ.get("GOOGLE_CLIENT_ID", "").startswith("REPLACE_"):
            flash("Google sign-in is not configured on this server.", "error")
            return redirect(url_for("home"))

        next_url = _safe_redirect_url(request.args.get("next") or request.referrer)
        session["oauth_next"] = next_url
        callback = url_for("auth_callback", _external=True)
        return get_oauth().google.authorize_redirect(callback)

    def auth_callback():
        """Handle OAuth callback: create/update User, set session."""
        from datetime import datetime

        try:
            oauth = get_oauth()
            token = oauth.google.authorize_access_token()
            userinfo = token.get("userinfo") or oauth.google.userinfo()
        except Exception:
            flash("Sign-in failed. Please try again.", "error")
            return redirect(url_for("home"))

        google_id = userinfo.get("sub")
        email = userinfo.get("email", "")
        display_name = userinfo.get("name", email)
        avatar_url = userinfo.get("picture")

        if not google_id:
            flash("Sign-in failed. Please try again.", "error")
            return redirect(url_for("home"))

        now = datetime.utcnow()
        try:
            SessionLocal = get_session_local()
            User = get_user_model()
            with SessionLocal() as db:
                user = db.query(User).filter(User.google_id == google_id).first()
                if user is None:
                    user = db.query(User).filter(User.email == email).first()
                if user is None:
                    user = User(
                        id=create_user_id(),
                        google_id=google_id,
                        email=email,
                        display_name=display_name,
                        avatar_url=avatar_url,
                        created_at=now,
                        last_login_at=now,
                    )
                    db.add(user)
                else:
                    user.google_id = google_id
                    user.email = email
                    user.display_name = display_name
                    user.avatar_url = avatar_url
                    user.last_login_at = now
                db.commit()
                db.refresh(user)
                session.permanent = True
                session["user_id"] = user.id
        except Exception:
            get_logger().exception("auth_callback: DB error")
            flash("Sign-in failed. Please try again.", "error")
            return redirect(url_for("home"))

        next_url = _safe_redirect_url(session.pop("oauth_next", None))
        return redirect(next_url)

    def auth_magic_send():
        """Send a magic login link to the provided email."""
        import secrets
        import resend
        from datetime import datetime, timedelta

        email = (request.form.get("email") or "").strip().lower()
        next_url = _safe_redirect_url(request.form.get("next"))

        if not email or "@" not in email:
            flash("Please enter a valid email address.", "error")
            return redirect(url_for("auth_login", next=next_url))

        resend_key = os.environ.get("RESEND_API_KEY")
        if not resend_key:
            flash("Email sign-in is not configured.", "error")
            return redirect(url_for("auth_login", next=next_url))

        now = datetime.utcnow()
        token_str = secrets.token_urlsafe(32)

        try:
            SessionLocal = get_session_local()
            MagicToken = get_magic_token_model()
            with SessionLocal() as db:
                mt = MagicToken(
                    token=token_str,
                    email=email,
                    expires_at=now + timedelta(minutes=15),
                    used=False,
                    next_url=next_url if next_url != url_for("home") else None,
                    created_at=now,
                )
                db.add(mt)
                db.commit()
        except Exception:
            get_logger().exception("auth_magic_send: DB error")
            flash("Something went wrong. Please try again.", "error")
            return redirect(url_for("auth_login", next=next_url))

        magic_url = url_for("auth_magic_verify", token=token_str, _external=True)
        try:
            resend.api_key = resend_key
            resend.Emails.send({
                "from": "Funba <noreply@funba.app>",
                "to": [email],
                "subject": "Your Funba login link",
                "html": (
                    f'<div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:40px 20px;">'
                    f'<h2 style="color:#f97316;margin-bottom:24px;">Funba</h2>'
                    f'<p>Click the button below to sign in. This link expires in 15 minutes.</p>'
                    f'<a href="{magic_url}" style="display:inline-block;margin:24px 0;padding:12px 32px;'
                    f'background:#f97316;color:#fff;text-decoration:none;border-radius:8px;font-weight:600;">'
                    f'Sign in to Funba</a>'
                    f'<p style="color:#888;font-size:13px;">If you didn\'t request this, you can safely ignore this email.</p>'
                    f'</div>'
                ),
            })
        except Exception:
            get_logger().exception("auth_magic_send: Resend error")
            flash("Failed to send login email. Please try again.", "error")
            return redirect(url_for("auth_login", next=next_url))

        return render_template("magic_sent.html", email=email)

    def auth_magic_verify():
        """Verify magic link token, create/update user, log in."""
        from datetime import datetime

        token_str = request.args.get("token", "")
        if not token_str:
            flash("Invalid login link.", "error")
            return redirect(url_for("auth_login"))

        now = datetime.utcnow()
        try:
            SessionLocal = get_session_local()
            MagicToken = get_magic_token_model()
            User = get_user_model()
            with SessionLocal() as db:
                mt = db.query(MagicToken).filter(MagicToken.token == token_str).first()
                if mt is None:
                    flash("Invalid login link.", "error")
                    return redirect(url_for("auth_login"))
                if mt.used:
                    flash("This login link has already been used.", "error")
                    return redirect(url_for("auth_login"))
                if now > mt.expires_at:
                    flash("This login link has expired. Please request a new one.", "error")
                    return redirect(url_for("auth_login"))

                mt.used = True
                email = mt.email
                next_url = mt.next_url

                user = db.query(User).filter(User.email == email).first()
                if user is None:
                    user = User(
                        id=create_user_id(),
                        google_id=None,
                        email=email,
                        display_name=email.split("@")[0],
                        created_at=now,
                        last_login_at=now,
                    )
                    db.add(user)
                else:
                    user.last_login_at = now
                db.commit()
                db.refresh(user)
                session.permanent = True
                session["user_id"] = user.id
        except Exception:
            get_logger().exception("auth_magic_verify: DB error")
            flash("Sign-in failed. Please try again.", "error")
            return redirect(url_for("auth_login"))

        return redirect(_safe_redirect_url(next_url))

    def auth_logout():
        """Clear session and redirect to home."""
        session.pop("user_id", None)
        return redirect(url_for("home"))

    app.add_url_rule("/auth/login", endpoint="auth_login", view_func=auth_login)
    app.add_url_rule("/auth/google", endpoint="auth_google", view_func=auth_google)
    app.add_url_rule("/auth/callback", endpoint="auth_callback", view_func=auth_callback)
    app.add_url_rule(
        "/auth/magic",
        endpoint="auth_magic_send",
        view_func=limiter.limit("5 per minute")(auth_magic_send),
        methods=["POST"],
    )
    app.add_url_rule("/auth/magic/verify", endpoint="auth_magic_verify", view_func=auth_magic_verify)
    app.add_url_rule("/auth/logout", endpoint="auth_logout", view_func=auth_logout, methods=["POST"])

    return SimpleNamespace(
        auth_login=auth_login,
        auth_google=auth_google,
        auth_callback=auth_callback,
        auth_magic_send=auth_magic_send,
        auth_magic_verify=auth_magic_verify,
        auth_logout=auth_logout,
    )
