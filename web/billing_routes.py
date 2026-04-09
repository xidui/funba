from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any, Callable

from flask import flash, redirect, render_template, request, url_for


_stripe_price_cache: dict = {}


def _get_stripe_price(get_logger: Callable[[], Any]) -> dict | None:
    """Fetch Pro price from Stripe, cached for 1 hour."""
    import time

    cached = _stripe_price_cache.get("data")
    if cached and time.time() - _stripe_price_cache.get("fetched_at", 0) < 3600:
        return cached

    price_id = os.environ.get("STRIPE_PRO_PRICE_ID", "")
    secret_key = os.environ.get("STRIPE_SECRET_KEY", "")
    if not price_id or not secret_key:
        return None

    try:
        import stripe

        stripe.api_key = secret_key
        price = stripe.Price.retrieve(price_id)
        data = {
            "amount": price.unit_amount,
            "currency": (price.currency or "usd").upper(),
            "interval": price.recurring.interval if price.recurring else "month",
        }
        _stripe_price_cache["data"] = data
        _stripe_price_cache["fetched_at"] = time.time()
        return data
    except Exception:
        get_logger().exception("Failed to fetch Stripe price")
        return cached


def register_billing_routes(
    app,
    *,
    get_session_local: Callable[[], Any],
    get_current_user: Callable[[], Any],
    get_localized_url_for: Callable[..., str],
    get_user_model: Callable[[], Any],
    get_logger: Callable[[], Any],
):
    def pricing():
        price_info = _get_stripe_price(get_logger)
        return render_template("pricing.html", price_info=price_info)

    def account_page():
        user = get_current_user()
        if not user:
            return redirect(url_for("auth_login", next=get_localized_url_for("account_page")))
        return render_template("account.html", user=user, checkout=request.args.get("checkout"))

    def subscribe_checkout():
        import stripe

        user = get_current_user()
        if not user:
            return redirect(url_for("auth_login", next=get_localized_url_for("pricing")))

        stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
        if not stripe.api_key:
            flash("Payment is not configured yet.", "error")
            return redirect(get_localized_url_for("pricing"))

        if not user.stripe_customer_id:
            customer = stripe.Customer.create(
                email=user.email,
                name=user.display_name,
                metadata={"funba_user_id": user.id},
            )
            SessionLocal = get_session_local()
            User = get_user_model()
            with SessionLocal() as db:
                db_user = db.get(User, user.id)
                db_user.stripe_customer_id = customer.id
                db.commit()
            customer_id = customer.id
        else:
            customer_id = user.stripe_customer_id

        price_id = os.environ.get("STRIPE_PRO_PRICE_ID", "")
        if not price_id:
            flash("Payment is not configured yet.", "error")
            return redirect(get_localized_url_for("pricing"))

        checkout_session = stripe.checkout.Session.create(
            customer=customer_id,
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=get_localized_url_for("account_page", checkout="success", _external=True),
            cancel_url=get_localized_url_for("pricing", _external=True),
        )
        return redirect(checkout_session.url, code=303)

    def subscribe_portal():
        import stripe

        user = get_current_user()
        if not user or not user.stripe_customer_id:
            return redirect(get_localized_url_for("pricing"))

        stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
        portal_session = stripe.billing_portal.Session.create(
            customer=user.stripe_customer_id,
            return_url=get_localized_url_for("account_page", _external=True),
        )
        return redirect(portal_session.url, code=303)

    def _on_checkout_completed(session_data):
        customer_id = session_data.get("customer")
        if not customer_id:
            return

        SessionLocal = get_session_local()
        User = get_user_model()
        with SessionLocal() as db:
            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
            if user:
                user.subscription_tier = "pro"
                user.subscription_status = "active"
                user.subscription_expires_at = None
                db.commit()

    def _on_subscription_changed(subscription):
        from datetime import datetime as _dt

        customer_id = subscription.get("customer")
        if not customer_id:
            return

        status = subscription.get("status", "")
        SessionLocal = get_session_local()
        User = get_user_model()
        with SessionLocal() as db:
            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
            if not user:
                return
            user.subscription_status = status
            if status == "active":
                user.subscription_tier = "pro"
                user.subscription_expires_at = None
            elif status == "canceled":
                period_end = subscription.get("current_period_end")
                if period_end:
                    user.subscription_expires_at = _dt.utcfromtimestamp(period_end)
                else:
                    user.subscription_tier = "free"
            elif status in ("unpaid", "incomplete_expired"):
                user.subscription_tier = "free"
                user.subscription_expires_at = None
            db.commit()

    def _on_payment_failed(invoice):
        customer_id = invoice.get("customer")
        if not customer_id:
            return

        SessionLocal = get_session_local()
        User = get_user_model()
        with SessionLocal() as db:
            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
            if user:
                user.subscription_status = "past_due"
                db.commit()

    def _handle_stripe_event(event):
        event_type = event["type"]
        data = event["data"]["object"]

        if event_type == "checkout.session.completed":
            _on_checkout_completed(data)
        elif event_type in ("customer.subscription.updated", "customer.subscription.deleted"):
            _on_subscription_changed(data)
        elif event_type == "invoice.payment_failed":
            _on_payment_failed(data)

    def stripe_webhook():
        import stripe

        payload = request.get_data()
        sig_header = request.headers.get("Stripe-Signature")
        webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

        try:
            event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
        except (ValueError, stripe.error.SignatureVerificationError):
            return "Invalid signature", 400

        _handle_stripe_event(event)
        return "", 200

    app.add_url_rule("/cn/pricing", endpoint="pricing_zh", view_func=pricing)
    app.add_url_rule("/pricing", endpoint="pricing", view_func=pricing)
    app.add_url_rule("/cn/account", endpoint="account_page_zh", view_func=account_page)
    app.add_url_rule("/account", endpoint="account_page", view_func=account_page)
    app.add_url_rule("/subscribe/checkout", endpoint="subscribe_checkout", view_func=subscribe_checkout, methods=["POST"])
    app.add_url_rule("/subscribe/portal", endpoint="subscribe_portal", view_func=subscribe_portal, methods=["POST"])
    app.add_url_rule("/stripe/webhook", endpoint="stripe_webhook", view_func=stripe_webhook, methods=["POST"])

    return SimpleNamespace(
        pricing=pricing,
        account_page=account_page,
        subscribe_checkout=subscribe_checkout,
        subscribe_portal=subscribe_portal,
        stripe_webhook=stripe_webhook,
        get_stripe_price=lambda: _get_stripe_price(get_logger),
        handle_stripe_event=_handle_stripe_event,
        on_checkout_completed=_on_checkout_completed,
        on_subscription_changed=_on_subscription_changed,
        on_payment_failed=_on_payment_failed,
    )
