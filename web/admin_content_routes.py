from __future__ import annotations

import json
import mimetypes
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from flask import abort, jsonify, make_response, request, url_for


def register_admin_content_routes(app, deps):
    def admin_pipeline():
        denied = deps.require_admin_page()()
        if denied:
            return denied
        return deps.render_template()(
            "admin.html",
            admin_page_url=deps.admin_page_url(),
            admin_fragment_url=deps.admin_fragment_url(),
        )

    def admin_settings():
        denied = deps.require_admin_page()()
        if denied:
            return denied
        return deps.render_template()(
            "admin_settings.html",
            llm_available_models=deps.available_llm_models()(),
        )

    def admin_content():
        denied = deps.require_admin_page()()
        if denied:
            return denied
        status_filter = request.args.get("status")
        page = max(1, request.args.get("page", 1, type=int))
        page_size = 30

        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            q = s.query(SocialPost).order_by(SocialPost.source_date.desc(), SocialPost.priority.asc())
            if status_filter and status_filter in ("draft", "ai_review", "in_review", "approved", "archived"):
                q = q.filter(SocialPost.status == status_filter)
            total = q.count()
            import math
            total_pages = max(1, math.ceil(total / page_size))
            page = min(page, total_pages)
            posts = q.offset((page - 1) * page_size).limit(page_size).all()
            post_rows = deps.build_social_post_rows()(s, posts)

        yesterday = (date.today() - timedelta(days=1)).isoformat()
        return deps.render_template()(
            "admin_content.html",
            posts=post_rows,
            page=page,
            total_pages=total_pages,
            total=total,
            status_filter=status_filter or "all",
            today=yesterday,
            single_post_view=False,
        )

    def admin_content_post(post_id: int):
        denied = deps.require_admin_page()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not post:
                abort(404)
            post_rows = deps.build_social_post_rows()(s, [post])

        yesterday = (date.today() - timedelta(days=1)).isoformat()
        return deps.render_template()(
            "admin_content.html",
            posts=post_rows,
            page=1,
            total_pages=1,
            total=1,
            status_filter="all",
            today=yesterday,
            single_post_view=True,
            focused_post_id=post_id,
        )

    def admin_content_card(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        expanded = request.args.get("expanded") in {"1", "true", "True"}
        active_variant_id = request.args.get("active_variant_id", type=int)
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not post:
                return jsonify({"error": "not_found"}), 404
            row = deps.build_social_post_rows()(s, [post])[0]
        html = deps.render_template()(
            "_admin_content_post_card.html",
            p=row,
            expanded=expanded,
            active_variant_id=active_variant_id,
        )
        return jsonify({"ok": True, "html": html, "post_status": row["status"]})

    def admin_content_detail(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        SocialPostVariant = deps.social_post_variant_model()
        SocialPostDelivery = deps.social_post_delivery_model()
        SocialPostImage = deps.social_post_image_model()
        with SessionLocal() as s:
            p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not p:
                return jsonify({"error": "not_found"}), 404
            variants = s.query(SocialPostVariant).filter(SocialPostVariant.post_id == post_id).order_by(SocialPostVariant.id).all()
            variant_ids = [v.id for v in variants]
            deliveries = s.query(SocialPostDelivery).filter(SocialPostDelivery.variant_id.in_(variant_ids)).all() if variant_ids else []
            d_by_variant: dict[int, list] = {}
            for d in deliveries:
                d_by_variant.setdefault(d.variant_id, []).append(d)
            images = s.query(SocialPostImage).filter(SocialPostImage.post_id == post_id).order_by(SocialPostImage.id).all()

            return jsonify(
                {
                    "id": p.id,
                    "topic": p.topic,
                    "source_date": p.source_date.isoformat() if p.source_date else None,
                    "source_metrics": json.loads(p.source_metrics) if p.source_metrics else [],
                    "source_game_ids": json.loads(p.source_game_ids) if p.source_game_ids else [],
                    "status": p.status,
                    "priority": p.priority,
                    "admin_comments": deps.social_post_comments()(p),
                    "llm_model": p.llm_model,
                    "created_at": p.created_at.isoformat() if p.created_at else None,
                    "workflow": deps.paperclip_workflow_view()(p),
                    "variants": [
                        {
                            "id": v.id,
                            "title": v.title,
                            "content_raw": v.content_raw,
                            "audience_hint": v.audience_hint,
                            "deliveries": [deps.social_post_delivery_view()(d) for d in d_by_variant.get(v.id, [])],
                        }
                        for v in variants
                    ],
                    "images": [deps.social_post_image_view()(post_id, img) for img in images],
                }
            )

    def admin_content_update(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        handoff_action = None
        handoff_comment_text = None
        handoff_comment_timestamp = None
        topic_changed = False
        priority_changed = False
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not p:
                return jsonify({"error": "not_found"}), 404
            previous_status = p.status
            comments = deps.social_post_comments()(p)
            if "topic" in data:
                new_topic = (data["topic"] or "").strip()
                if new_topic and new_topic != p.topic:
                    p.topic = new_topic
                    topic_changed = True
            if "status" in data and data["status"] in ("draft", "ai_review", "in_review", "approved", "archived"):
                p.status = data["status"]
            if "priority" in data:
                new_priority = int(data["priority"])
                if new_priority != p.priority:
                    p.priority = new_priority
                    priority_changed = True
            if previous_status == "ai_review" and p.status == "in_review":
                validation_errors = deps.post_ai_review_validation_errors()(s, post_id)
                if validation_errors:
                    return jsonify({"error": "ai_review_validation_failed", "details": validation_errors}), 400
            if p.status != previous_status:
                if p.status == "ai_review":
                    handoff_action = "send_to_ai_review"
                    handoff_comment_text = "Sent this post to AI review from Funba."
                elif p.status == "in_review":
                    handoff_action = "send_to_review"
                    handoff_comment_text = "Sent this post to review from Funba."
                elif p.status == "draft":
                    handoff_action = "request_revision"
                    handoff_comment_text = "Requested revision on this post from Funba."
                elif p.status == "approved":
                    handoff_action = "approve_and_queue_publish"
                    handoff_comment_text = "Approved this post and queued publishing from Funba."
                elif p.status == "archived":
                    handoff_action = "archive_post"
                    handoff_comment_text = "Archived this post from Funba."
                if handoff_comment_text:
                    handoff_comment_timestamp = deps.append_admin_comment()(
                        comments,
                        text=handoff_comment_text,
                        author=deps.paperclip_actor_name()(),
                        origin="system",
                        event_type="handoff",
                    )
                    deps.write_social_post_comments()(p, comments)
                else:
                    p.updated_at = datetime.utcnow()
            else:
                p.updated_at = datetime.utcnow()
            s.commit()
        if handoff_action and handoff_comment_timestamp and handoff_comment_text:
            deps.handoff_social_post()(
                post_id,
                action=handoff_action,
                local_comment_timestamp=handoff_comment_timestamp,
                local_comment_text=handoff_comment_text,
            )
        elif topic_changed or priority_changed:
            deps.ensure_paperclip_issue_for_post()(post_id)
        sync_result = deps.sync_social_post_from_paperclip()(post_id, ensure_issue=False)
        return jsonify({"ok": True, **(sync_result or {})})

    def admin_content_comment(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        text_val = (data.get("text") or "").strip()
        if not text_val:
            return jsonify({"error": "text required"}), 400
        comment_timestamp = None
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not p:
                return jsonify({"error": "not_found"}), 404
            comments = deps.social_post_comments()(p)
            user = deps.current_user()()
            comment_timestamp = deps.append_admin_comment()(
                comments,
                text=text_val,
                author=user.display_name if user and getattr(user, "display_name", None) else "admin",
                origin="funba_user",
                event_type="comment",
            )
            deps.write_social_post_comments()(p, comments)
            s.commit()
        if comment_timestamp:
            deps.mirror_paperclip_comment()(post_id, text=text_val, local_comment_timestamp=comment_timestamp)
        sync_result = deps.sync_social_post_from_paperclip()(post_id, ensure_issue=False)
        comments = None
        workflow = None
        if sync_result:
            comments = sync_result.get("comments")
            workflow = sync_result.get("workflow")
        if comments is None or workflow is None:
            with SessionLocal() as s:
                p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
                comments = deps.social_post_comments()(p) if p else []
                workflow = deps.paperclip_workflow_view()(p) if p else {}
        return jsonify({"ok": True, "comments": comments, "workflow": workflow})

    def admin_content_delete(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        issue_id = None
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        SocialPostVariant = deps.social_post_variant_model()
        SocialPostDelivery = deps.social_post_delivery_model()
        with SessionLocal() as s:
            p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not p:
                return jsonify({"error": "not_found"}), 404
            issue_id = p.paperclip_issue_id
            variant_ids = [v.id for v in s.query(SocialPostVariant.id).filter(SocialPostVariant.post_id == post_id).all()]
            if variant_ids:
                s.query(SocialPostDelivery).filter(SocialPostDelivery.variant_id.in_(variant_ids)).delete(synchronize_session=False)
            s.query(SocialPostVariant).filter(SocialPostVariant.post_id == post_id).delete(synchronize_session=False)
            s.query(SocialPost).filter(SocialPost.id == post_id).delete(synchronize_session=False)
            s.commit()
        if issue_id and deps.paperclip_bridge_enabled()():
            try:
                client, _cfg = deps.paperclip_client_or_raise()()
                client.update_issue(
                    issue_id,
                    {
                        "status": "cancelled",
                        "comment": "## Funba Workflow Update\n\nAction: delete_post\nTriggered from: Funba admin content\n\nThe linked SocialPost was deleted in Funba, so this workflow thread is now cancelled.",
                    },
                )
            except deps.paperclip_bridge_error_cls() as exc:
                deps.logger().warning("Failed to cancel Paperclip issue %s for deleted post %s: %s", issue_id, post_id, exc)
        return jsonify({"ok": True})

    def admin_content_sync_paperclip(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        result = deps.sync_social_post_from_paperclip()(post_id, ensure_issue=True)
        if result is None:
            return jsonify({"error": "not_found"}), 404
        return jsonify({"ok": True, **result})

    def _create_metric_deep_dive_placeholder_post(metric_key: str, metric_name: str, brief_text: str) -> tuple[int, str]:
        now = datetime.utcnow()
        comments: list[dict[str, object]] = []
        brief_timestamp = deps.append_admin_comment()(
            comments,
            text=brief_text,
            author=deps.paperclip_actor_name()(),
            origin="system",
            event_type=deps.social_post_event_metric_deep_dive_brief(),
        )
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            post = SocialPost(
                topic=f"{metric_name} 数据分析",
                source_date=date.today(),
                source_metrics=json.dumps([metric_key], ensure_ascii=False),
                source_game_ids=json.dumps([], ensure_ascii=False),
                status="draft",
                priority=35,
                llm_model=None,
                admin_comments=json.dumps(comments, ensure_ascii=False),
                created_at=now,
                updated_at=now,
            )
            s.add(post)
            s.commit()
            return post.id, brief_timestamp

    def admin_metric_trigger_deep_dive_post(metric_key: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        metric_page_url = (data.get("metric_page_url") or "").strip()
        if metric_page_url.startswith("/"):
            metric_page_url = request.url_root.rstrip("/") + metric_page_url
        if not metric_page_url:
            metric_page_url = url_for("metric_detail", metric_key=metric_key, _external=True)

        try:
            deps.paperclip_client_or_raise()()
        except deps.paperclip_bridge_error_cls() as exc:
            return jsonify({"error": str(exc)}), 400

        SessionLocal = deps.session_local()
        MetricDefinitionModel = deps.metric_definition_model()
        with SessionLocal() as s:
            from metrics.framework.runtime import get_metric as _get_metric

            runtime_metric = _get_metric(metric_key, session=s)
            db_metric = (
                s.query(MetricDefinitionModel)
                .filter(MetricDefinitionModel.key == metric_key, MetricDefinitionModel.status != "archived")
                .first()
            )
            if runtime_metric is None and db_metric is None:
                return jsonify({"error": "metric_not_found"}), 404

            metric_def = deps.metric_def_view()(
                runtime_metric or db_metric,
                source_type=getattr(db_metric, "source_type", None),
            )
            deep_dive_state = deps.metric_deep_dive_state()(s, metric_key)
            if not deep_dive_state["can_trigger"]:
                return jsonify({"error": "already_running", "metric_deep_dive": deep_dive_state}), 409

            brief_text = deps.build_metric_deep_dive_brief()(
                session=s,
                metric_name=metric_def.name_en,
                metric_name_zh=metric_def.name_zh,
                metric_key=metric_key,
                metric_description=metric_def.description_en,
                metric_scope=metric_def.scope,
                metric_page_url=metric_page_url,
            )

        create_placeholder = getattr(deps, "create_metric_deep_dive_placeholder_post", None)
        if create_placeholder is not None:
            post_id, brief_timestamp = create_placeholder()(metric_key, metric_def.name, brief_text)
        else:
            post_id, brief_timestamp = _create_metric_deep_dive_placeholder_post(metric_key, metric_def.name, brief_text)
        deps.ensure_paperclip_issue_for_post()(post_id)

        SocialPost = deps.social_post_model()
        SocialPostVariant = deps.social_post_variant_model()
        SocialPostDelivery = deps.social_post_delivery_model()
        with SessionLocal() as s:
            post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if post is None or not post.paperclip_issue_id:
                error_message = (post.paperclip_sync_error if post is not None else None) or "Failed to create Paperclip issue for this deep-dive post."
                variant_ids = [v.id for v in s.query(SocialPostVariant.id).filter(SocialPostVariant.post_id == post_id).all()]
                if variant_ids:
                    s.query(SocialPostDelivery).filter(SocialPostDelivery.variant_id.in_(variant_ids)).delete(synchronize_session=False)
                s.query(SocialPostVariant).filter(SocialPostVariant.post_id == post_id).delete(synchronize_session=False)
                s.query(SocialPost).filter(SocialPost.id == post_id).delete(synchronize_session=False)
                s.commit()
                return jsonify({"error": error_message}), 500

        deps.mirror_paperclip_comment()(post_id, text=brief_text, local_comment_timestamp=brief_timestamp)
        sync_result = deps.sync_social_post_from_paperclip()(post_id, ensure_issue=False)

        with SessionLocal() as s:
            deep_dive_state = deps.metric_deep_dive_state()(s, metric_key)

        response = {"ok": True, "post_id": post_id, "metric_deep_dive": deep_dive_state}
        if sync_result:
            response.update(sync_result)
        return jsonify(response)

    def admin_content_trigger_daily_analysis():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        source_date = (data.get("source_date") or "").strip()
        force = bool(data.get("force"))
        try:
            target_date = date.fromisoformat(source_date) if source_date else (date.today() - timedelta(days=1))
        except ValueError:
            return jsonify({"error": "invalid source_date"}), 400
        try:
            result = deps.ensure_game_content_analysis_issues()(target_date, force=force)
        except deps.paperclip_bridge_error_cls() as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("Failed to trigger game content analysis for %s", target_date.isoformat())
            return jsonify({"error": str(exc)}), 500
        if result.get("issue_identifier"):
            result["issue_url"] = deps.paperclip_issue_url()(result.get("issue_identifier"))
        return jsonify(result)

    def admin_game_trigger_content_analysis(game_id: str):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        force = bool(data.get("force"))
        try:
            result = deps.ensure_game_content_analysis_issue_for_game()(game_id, force=force, trigger_source="manual")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 404
        except deps.paperclip_bridge_error_cls() as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            deps.logger().exception("Failed to trigger game content analysis for game_id=%s", game_id)
            return jsonify({"error": str(exc)}), 500
        if result.get("status") == "waiting_for_pipeline":
            result["readiness_detail"] = deps.game_analysis_readiness_detail()(game_id)
        result["issues"] = [
            {
                "id": item.id,
                "issue_id": item.issue_id,
                "issue_identifier": item.issue_identifier,
                "issue_url": deps.paperclip_issue_url()(item.issue_identifier),
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
            for item in deps.game_analysis_issue_history()(game_id)
        ]
        return jsonify(result)

    def admin_content_variant_update(post_id: int, variant_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        SessionLocal = deps.session_local()
        SocialPostVariant = deps.social_post_variant_model()
        with SessionLocal() as s:
            v = s.query(SocialPostVariant).filter(SocialPostVariant.id == variant_id, SocialPostVariant.post_id == post_id).first()
            if not v:
                return jsonify({"error": "not_found"}), 404
            if "title" in data:
                v.title = data["title"]
            if "content_raw" in data:
                v.content_raw = data["content_raw"]
            if "audience_hint" in data:
                v.audience_hint = data["audience_hint"]
            v.updated_at = datetime.utcnow()
            s.commit()
        deps.ensure_paperclip_issue_for_post()(post_id)
        return jsonify({"ok": True})

    def admin_content_add_destination(post_id: int, variant_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        platform = (data.get("platform") or "").strip()
        forum = (data.get("forum") or "").strip() or None
        if platform.lower() == "hupu":
            forum = deps.normalize_hupu_forum()(forum)
        if platform.lower() == "reddit":
            forum = deps.normalize_reddit_forum()(forum)
        if not platform:
            return jsonify({"error": "platform required"}), 400
        now = datetime.utcnow()
        SessionLocal = deps.session_local()
        SocialPostVariant = deps.social_post_variant_model()
        SocialPostDelivery = deps.social_post_delivery_model()
        with SessionLocal() as s:
            v = s.query(SocialPostVariant).filter(SocialPostVariant.id == variant_id, SocialPostVariant.post_id == post_id).first()
            if not v:
                return jsonify({"error": "variant not_found"}), 404
            d = SocialPostDelivery(
                variant_id=variant_id,
                platform=platform,
                forum=forum,
                is_enabled=True,
                status="pending",
                created_at=now,
                updated_at=now,
            )
            s.add(d)
            if platform.lower() == "reddit":
                v.audience_hint = deps.reddit_english_audience_hint()(v.audience_hint, forum=forum)
                v.updated_at = now
            s.commit()
            delivery_id = d.id
        deps.ensure_paperclip_issue_for_post()(post_id)
        return jsonify({"ok": True, "delivery_id": delivery_id})

    def admin_content_toggle_delivery(post_id: int, delivery_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        if "is_enabled" not in data:
            return jsonify({"error": "is_enabled required"}), 400
        enabled = bool(data.get("is_enabled"))
        handoff_action = None
        handoff_comment_text = None
        handoff_comment_timestamp = None
        retry_issue_id = None
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        SocialPostDelivery = deps.social_post_delivery_model()
        SocialPostVariant = deps.social_post_variant_model()
        with SessionLocal() as s:
            post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            d = (
                s.query(SocialPostDelivery)
                .join(SocialPostVariant, SocialPostVariant.id == SocialPostDelivery.variant_id)
                .filter(SocialPostDelivery.id == delivery_id, SocialPostVariant.post_id == post_id)
                .first()
            )
            if not d:
                return jsonify({"error": "not_found"}), 404
            d.is_enabled = enabled
            d.updated_at = datetime.utcnow()
            if not enabled and d.status == "publishing":
                d.status = "failed"
                d.error_message = "Delivery disabled while publishing"
            elif enabled and d.status == "failed":
                d.status = "pending"
                d.error_message = None
                d.published_url = None
                d.published_at = None
            if enabled and post and post.status == "approved":
                comments = deps.social_post_comments()(post)
                handoff_action = "retry_enabled_delivery"
                handoff_comment_text = f"Re-enabled delivery {delivery_id} for retry from Funba."
                handoff_comment_timestamp = deps.append_admin_comment()(
                    comments,
                    text=handoff_comment_text,
                    author=deps.paperclip_actor_name()(),
                    origin="system",
                    event_type="handoff",
                )
                deps.write_social_post_comments()(post, comments)
                retry_issue_id = post.paperclip_issue_id
            s.commit()
        if handoff_action and handoff_comment_timestamp and handoff_comment_text:
            deps.handoff_social_post()(
                post_id,
                action=handoff_action,
                local_comment_timestamp=handoff_comment_timestamp,
                local_comment_text=handoff_comment_text,
            )
            try:
                client, cfg = deps.paperclip_client_or_raise()()
                if cfg.delivery_publisher_agent_id and retry_issue_id:
                    client.wake_agent(
                        cfg.delivery_publisher_agent_id,
                        reason="retry_enabled_delivery",
                        payload={"issueId": retry_issue_id},
                        force_fresh_session=True,
                    )
            except Exception as exc:
                deps.logger().warning("Failed to explicitly wake Delivery Publisher for post %s retry: %s", post_id, exc)
        else:
            deps.ensure_paperclip_issue_for_post()(post_id)
        return jsonify({"ok": True, "delivery_id": delivery_id, "is_enabled": enabled})

    def admin_content_toggle_image(post_id: int, image_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        if "is_enabled" not in data:
            return jsonify({"error": "is_enabled required"}), 400
        enabled = bool(data["is_enabled"])
        review_reason = (data.get("reason") or "").strip() or None
        review_source = deps.normalize_image_review_source()(data.get("review_source"))
        now = datetime.utcnow()
        SessionLocal = deps.session_local()
        SocialPostImage = deps.social_post_image_model()
        with SessionLocal() as s:
            img = s.query(SocialPostImage).filter(SocialPostImage.id == image_id, SocialPostImage.post_id == post_id).first()
            if not img:
                return jsonify({"error": "not_found"}), 404
            img.is_enabled = enabled
            if review_reason or review_source:
                deps.apply_image_review_metadata()(
                    img,
                    decision="enable" if enabled else "disable",
                    reason=review_reason,
                    source=review_source or "manual_toggle",
                    reviewed_at=now,
                )
            s.commit()
        return jsonify({"ok": True, "image_id": image_id, "is_enabled": enabled})

    def admin_content_add_image(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        try:
            prepared = deps.validate_prepared_image_specs()([data])[0]
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except FileNotFoundError as exc:
            return jsonify({"error": str(exc)}), 400

        now = datetime.utcnow()
        stored_path = None
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        SocialPostImage = deps.social_post_image_model()
        with SessionLocal() as s:
            post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not post:
                return jsonify({"error": "not_found"}), 404
            existing = s.query(SocialPostImage).filter(SocialPostImage.post_id == post_id, SocialPostImage.slot == prepared["slot"]).first()
            if existing:
                return jsonify({"error": "slot_exists", "slot": prepared["slot"]}), 400
            try:
                stored_path = deps.store_prepared_image()(prepared["source_path"], post_id=post_id, slot=prepared["slot"])
                img = SocialPostImage(
                    post_id=post_id,
                    slot=prepared["slot"],
                    image_type=prepared["image_type"],
                    spec=prepared["spec_json"],
                    note=prepared["note"],
                    file_path=stored_path,
                    is_enabled=bool(prepared["is_enabled"]),
                    error_message=None,
                    created_at=now,
                )
                s.add(img)
                s.commit()
                image_id = img.id
            except Exception as exc:
                s.rollback()
                deps.remove_managed_post_image_file()(stored_path, post_id=post_id)
                return jsonify({"error": str(exc)}), 400

        deps.ensure_paperclip_issue_for_post()(post_id)
        return jsonify({"ok": True, "image_id": image_id})

    def admin_content_replace_image(post_id: int, image_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        try:
            prepared = deps.validate_prepared_image_specs()([data])[0]
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except FileNotFoundError as exc:
            return jsonify({"error": str(exc)}), 400

        stored_path = None
        old_path = None
        SessionLocal = deps.session_local()
        SocialPostImage = deps.social_post_image_model()
        with SessionLocal() as s:
            img = s.query(SocialPostImage).filter(SocialPostImage.id == image_id, SocialPostImage.post_id == post_id).first()
            if not img:
                return jsonify({"error": "not_found"}), 404
            if prepared["slot"] != img.slot:
                existing = (
                    s.query(SocialPostImage)
                    .filter(SocialPostImage.post_id == post_id, SocialPostImage.slot == prepared["slot"], SocialPostImage.id != image_id)
                    .first()
                )
                if existing:
                    return jsonify({"error": "slot_exists", "slot": prepared["slot"]}), 400
            old_path = img.file_path
            try:
                stored_path = deps.store_prepared_image()(prepared["source_path"], post_id=post_id, slot=prepared["slot"])
                img.slot = prepared["slot"]
                img.image_type = prepared["image_type"]
                img.spec = prepared["spec_json"]
                img.note = prepared["note"]
                img.file_path = stored_path
                img.is_enabled = bool(prepared["is_enabled"])
                img.error_message = None
                img.review_decision = None
                img.review_reason = None
                img.review_source = None
                img.reviewed_at = None
                s.commit()
            except Exception as exc:
                s.rollback()
                deps.remove_managed_post_image_file()(stored_path, post_id=post_id)
                return jsonify({"error": str(exc)}), 400
        if old_path and old_path != stored_path:
            deps.remove_managed_post_image_file()(old_path, post_id=post_id)
        deps.ensure_paperclip_issue_for_post()(post_id)
        return jsonify({"ok": True, "image_id": image_id})

    def admin_content_image_review_payload(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        include_disabled = request.args.get("include_disabled") in {"1", "true", "True"}
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        SocialPostVariant = deps.social_post_variant_model()
        SocialPostImage = deps.social_post_image_model()
        with SessionLocal() as s:
            post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not post:
                return jsonify({"error": "not_found"}), 404
            variants = s.query(SocialPostVariant).filter(SocialPostVariant.post_id == post_id).order_by(SocialPostVariant.id).all()
            image_query = s.query(SocialPostImage).filter(SocialPostImage.post_id == post_id).order_by(SocialPostImage.id)
            if not include_disabled:
                image_query = image_query.filter(SocialPostImage.is_enabled == True)
            images = image_query.all()
            return jsonify(
                {
                    "ok": True,
                    "post_id": post.id,
                    "topic": post.topic,
                    "status": post.status,
                    "source_date": post.source_date.isoformat() if post.source_date else None,
                    "variants": [
                        {
                            "id": v.id,
                            "title": v.title,
                            "audience_hint": v.audience_hint,
                            "content_raw": v.content_raw,
                            "referenced_slots": deps.extract_image_slots_from_content()(v.content_raw),
                        }
                        for v in variants
                    ],
                    "images": [deps.social_post_image_view()(post_id, img) for img in images],
                }
            )

    def admin_content_apply_image_review(post_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        decisions = data.get("image_decisions") or []
        if not isinstance(decisions, list) or not decisions:
            return jsonify({"error": "image_decisions required"}), 400
        review_source = deps.normalize_image_review_source()(data.get("review_source")) or "content_reviewer_agent"
        review_summary = (data.get("summary") or "").strip() or None
        now = datetime.utcnow()
        updated_images: list[dict[str, object]] = []
        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        SocialPostImage = deps.social_post_image_model()
        with SessionLocal() as s:
            post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not post:
                return jsonify({"error": "not_found"}), 404
            comments = deps.social_post_comments()(post)
            for decision in decisions:
                image_id = int(decision.get("image_id") or 0)
                action = str(decision.get("action") or "").strip().lower()
                reason = str(decision.get("reason") or "").strip() or None
                if action not in {"keep", "disable", "enable"}:
                    return jsonify({"error": "invalid_action", "image_id": image_id}), 400
                img = s.query(SocialPostImage).filter(SocialPostImage.id == image_id, SocialPostImage.post_id == post_id).first()
                if not img:
                    return jsonify({"error": "image_not_found", "image_id": image_id}), 404
                if action == "disable":
                    img.is_enabled = False
                elif action == "enable":
                    img.is_enabled = True
                deps.apply_image_review_metadata()(
                    img,
                    decision=action,
                    reason=reason,
                    source=review_source,
                    reviewed_at=now,
                )
                updated_images.append({"image_id": image_id, "action": action, "is_enabled": bool(img.is_enabled), "reason": reason})
            if review_summary:
                deps.append_admin_comment()(
                    comments,
                    text=f"Image review ({review_source}): {review_summary}",
                    author=review_source,
                    origin="system",
                    event_type="image_review",
                    timestamp=now.isoformat() + "Z",
                )
                deps.write_social_post_comments()(post, comments)
            s.commit()
        return jsonify({"ok": True, "post_id": post_id, "review_source": review_source, "updated_images": updated_images})

    def serve_social_post_image(post_id: int, filename: str):
        media_dir = Path(__file__).resolve().parent.parent / "media" / "social_posts" / str(post_id)
        file_path = media_dir / filename
        if not file_path.exists() or not file_path.is_file():
            abort(404)
        try:
            file_path.resolve().relative_to(media_dir.resolve())
        except ValueError:
            abort(403)
        mime_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        return make_response(open(file_path, "rb").read(), 200, {"Content-Type": mime_type, "Cache-Control": "public, max-age=3600"})

    def api_content_create_post():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        topic = (data.get("topic") or "").strip()
        if not topic:
            return jsonify({"error": "topic required"}), 400
        source_date_str = data.get("source_date")
        if not source_date_str:
            return jsonify({"error": "source_date required"}), 400
        analysis_issue_id = (data.get("analysis_issue_id") or "").strip() or None
        analysis_issue_identifier = (data.get("analysis_issue_identifier") or "").strip() or None
        if analysis_issue_id or analysis_issue_identifier:
            try:
                resolved_issue = deps.resolve_game_analysis_issue_record()(
                    analysis_issue_id=analysis_issue_id,
                    analysis_issue_identifier=analysis_issue_identifier,
                )
            except deps.paperclip_bridge_error_cls() as exc:
                return jsonify({"error": str(exc)}), 400
            except Exception as exc:
                return jsonify({"error": str(exc)}), 400
            if resolved_issue is None:
                return jsonify({"error": "analysis issue not found"}), 400
            SessionLocal = deps.session_local()
            GameContentAnalysisIssuePost = deps.game_content_analysis_issue_post_model()
            with SessionLocal() as s:
                existing_link = s.query(GameContentAnalysisIssuePost).filter(GameContentAnalysisIssuePost.issue_record_id == resolved_issue.id).first()
                if existing_link:
                    return jsonify({"error": "this game-analysis issue already has a linked post", "existing_post_id": existing_link.post_id}), 409
        try:
            prepared_images = deps.validate_prepared_image_specs()(data.get("images", []))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except FileNotFoundError as exc:
            return jsonify({"error": str(exc)}), 400

        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            existing = (
                s.query(SocialPost)
                .filter(SocialPost.source_date == date.fromisoformat(source_date_str), SocialPost.topic == topic, SocialPost.status != "archived")
                .first()
            )
            if existing:
                return jsonify({"ok": True, "post_id": existing.id, "status": "duplicate_skipped"}), 200

        now = datetime.utcnow()
        staged_files: list[str] = []
        variant_ids: list[int] = []
        issue_link_requested = bool(analysis_issue_id or analysis_issue_identifier)
        SocialPostVariant = deps.social_post_variant_model()
        SocialPostDelivery = deps.social_post_delivery_model()
        SocialPostImage = deps.social_post_image_model()
        try:
            with SessionLocal() as s:
                sp = SocialPost(
                    topic=topic,
                    source_date=date.fromisoformat(source_date_str),
                    source_metrics=json.dumps(data.get("source_metrics", []), ensure_ascii=False),
                    source_game_ids=json.dumps(data.get("source_game_ids", []), ensure_ascii=False),
                    status=data.get("status", "draft"),
                    priority=int(data.get("priority", 50)),
                    llm_model=data.get("llm_model"),
                    admin_comments=None,
                    created_at=now,
                    updated_at=now,
                )
                s.add(sp)
                s.flush()

                for vd in data.get("variants", []):
                    vtitle = (vd.get("title") or "").strip()
                    vcontent = (vd.get("content_raw") or "").strip()
                    if not vtitle or not vcontent:
                        continue
                    destinations = vd.get("destinations", [])
                    audience_hint = (vd.get("audience_hint") or "").strip() or None
                    for dest in destinations:
                        if str(dest.get("platform") or "").strip().lower() == "reddit":
                            audience_hint = deps.reddit_english_audience_hint()(audience_hint, forum=dest.get("forum"))
                    sv = SocialPostVariant(
                        post_id=sp.id,
                        title=vtitle,
                        content_raw=vcontent,
                        audience_hint=audience_hint,
                        created_at=now,
                        updated_at=now,
                    )
                    s.add(sv)
                    s.flush()
                    variant_ids.append(sv.id)
                    for dest in destinations:
                        platform = (dest.get("platform") or "").strip()
                        forum = (dest.get("forum") or "").strip() or None
                        if platform.lower() == "hupu":
                            forum = deps.normalize_hupu_forum()(forum)
                        if platform.lower() == "reddit":
                            forum = deps.normalize_reddit_forum()(forum)
                        if platform:
                            s.add(
                                SocialPostDelivery(
                                    variant_id=sv.id,
                                    platform=platform,
                                    forum=forum,
                                    is_enabled=True,
                                    status="pending",
                                    created_at=now,
                                    updated_at=now,
                                )
                            )

                post_id = sp.id
                image_results = []
                for img in prepared_images:
                    stored_path = deps.store_prepared_image()(img["source_path"], post_id=post_id, slot=img["slot"])
                    staged_files.append(stored_path)
                    image_results.append((img["slot"], img["image_type"], img["spec_json"], img["note"], stored_path, None, img["is_enabled"]))

                for slot, image_type, spec_json, note, file_path, error_msg, is_enabled in image_results:
                    s.add(
                        SocialPostImage(
                            post_id=post_id,
                            slot=slot,
                            image_type=image_type,
                            spec=spec_json,
                            note=note,
                            file_path=file_path,
                            is_enabled=bool(is_enabled and file_path is not None),
                            error_message=error_msg,
                            created_at=now,
                        )
                    )

                s.commit()
                if issue_link_requested:
                    deps.link_post_to_game_analysis_issue()(
                        post_id,
                        analysis_issue_id=analysis_issue_id,
                        analysis_issue_identifier=analysis_issue_identifier,
                        discovered_via="api_create",
                    )
        except Exception as exc:
            for staged_path in staged_files:
                try:
                    Path(staged_path).unlink(missing_ok=True)
                except Exception:
                    pass
            deps.logger().warning("Prepared image ingest failed for post topic %s: %s", topic, exc)
            return jsonify({"error": str(exc)}), 400

        deps.ensure_paperclip_issue_for_post()(post_id)
        sync_result = deps.sync_social_post_from_paperclip()(post_id, ensure_issue=False)
        response = {"ok": True, "post_id": post_id, "variant_ids": variant_ids}
        if prepared_images:
            response["images"] = [{"slot": slot, "ok": True, "error": None} for slot, _, _, _, _, _, _ in image_results]
        if sync_result:
            response.update(sync_result)
        return jsonify(response)

    def api_content_list_posts():
        denied = deps.require_admin_json()()
        if denied:
            return denied
        status_filter = request.args.get("status")
        date_filter = request.args.get("date")
        limit = min(int(request.args.get("limit", 50)), 200)
        offset = int(request.args.get("offset", 0))

        SessionLocal = deps.session_local()
        SocialPost = deps.social_post_model()
        with SessionLocal() as s:
            q = s.query(SocialPost).order_by(SocialPost.source_date.desc(), SocialPost.priority.asc())
            if status_filter:
                q = q.filter(SocialPost.status == status_filter)
            if date_filter:
                q = q.filter(SocialPost.source_date == date_filter)
            total = q.count()
            posts = q.offset(offset).limit(limit).all()
            return jsonify(
                {
                    "total": total,
                    "posts": [
                        {
                            "id": p.id,
                            "topic": p.topic,
                            "source_date": p.source_date.isoformat() if p.source_date else None,
                            "status": p.status,
                            "priority": p.priority,
                            "created_at": p.created_at.isoformat() if p.created_at else None,
                            "source_metrics": json.loads(p.source_metrics) if p.source_metrics else [],
                            "source_game_ids": json.loads(p.source_game_ids) if p.source_game_ids else [],
                        }
                        for p in posts
                    ],
                }
            )

    def api_content_delivery_status(delivery_id: int):
        denied = deps.require_admin_json()()
        if denied:
            return denied
        data = request.get_json(force=True) or {}
        new_status = (data.get("status") or "").strip()
        if new_status not in ("pending", "publishing", "published", "failed"):
            return jsonify({"error": "invalid status"}), 400

        SessionLocal = deps.session_local()
        SocialPostDelivery = deps.social_post_delivery_model()
        with SessionLocal() as s:
            d = s.query(SocialPostDelivery).filter(SocialPostDelivery.id == delivery_id).first()
            if not d:
                return jsonify({"error": "not_found"}), 404
            if "published_url" in data:
                d.published_url = data["published_url"]
            if "content_final" in data:
                d.content_final = data["content_final"]
            if "error_message" in data:
                d.error_message = data["error_message"]
            if new_status == "published" and d.platform == "hupu" and not deps.is_valid_hupu_thread_url()(d.published_url):
                bad_url = d.published_url or "<missing>"
                d.status = "failed"
                d.published_url = None
                d.error_message = f"Invalid Hupu published_url reported: {bad_url}"
                d.published_at = None
            else:
                d.status = new_status
                if new_status == "published":
                    d.published_at = datetime.utcnow()
            response_status = d.status
            d.updated_at = datetime.utcnow()
            s.commit()
        return jsonify({"ok": True, "status": response_status})

    app.add_url_rule("/admin", endpoint="admin_pipeline", view_func=admin_pipeline)
    app.add_url_rule("/admin/settings", endpoint="admin_settings", view_func=admin_settings)
    app.add_url_rule("/admin/content", endpoint="admin_content", view_func=admin_content)
    app.add_url_rule("/admin/content/<int:post_id>", endpoint="admin_content_post", view_func=admin_content_post)
    app.add_url_rule("/api/admin/content/<int:post_id>/card", endpoint="admin_content_card", view_func=admin_content_card)
    app.add_url_rule("/api/admin/content/<int:post_id>", endpoint="admin_content_detail", view_func=admin_content_detail)
    app.add_url_rule("/api/admin/content/<int:post_id>/update", endpoint="admin_content_update", view_func=admin_content_update, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/comment", endpoint="admin_content_comment", view_func=admin_content_comment, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/delete", endpoint="admin_content_delete", view_func=admin_content_delete, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/paperclip/sync", endpoint="admin_content_sync_paperclip", view_func=admin_content_sync_paperclip, methods=["POST"])
    app.add_url_rule("/api/admin/metrics/<metric_key>/deep-dive-post", endpoint="admin_metric_trigger_deep_dive_post", view_func=admin_metric_trigger_deep_dive_post, methods=["POST"])
    app.add_url_rule("/api/admin/content/daily-analysis/trigger", endpoint="admin_content_trigger_daily_analysis", view_func=admin_content_trigger_daily_analysis, methods=["POST"])
    app.add_url_rule("/api/admin/games/<game_id>/content-analysis/trigger", endpoint="admin_game_trigger_content_analysis", view_func=admin_game_trigger_content_analysis, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/variants/<int:variant_id>/update", endpoint="admin_content_variant_update", view_func=admin_content_variant_update, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/variants/<int:variant_id>/destinations", endpoint="admin_content_add_destination", view_func=admin_content_add_destination, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/deliveries/<int:delivery_id>/toggle", endpoint="admin_content_toggle_delivery", view_func=admin_content_toggle_delivery, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/images/<int:image_id>/toggle", endpoint="admin_content_toggle_image", view_func=admin_content_toggle_image, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/images", endpoint="admin_content_add_image", view_func=admin_content_add_image, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/images/<int:image_id>/replace", endpoint="admin_content_replace_image", view_func=admin_content_replace_image, methods=["POST"])
    app.add_url_rule("/api/admin/content/<int:post_id>/image-review-payload", endpoint="admin_content_image_review_payload", view_func=admin_content_image_review_payload)
    app.add_url_rule("/api/admin/content/<int:post_id>/image-review/apply", endpoint="admin_content_apply_image_review", view_func=admin_content_apply_image_review, methods=["POST"])
    app.add_url_rule("/media/social_posts/<int:post_id>/<filename>", endpoint="serve_social_post_image", view_func=serve_social_post_image)
    app.add_url_rule("/api/content/posts", endpoint="api_content_create_post", view_func=api_content_create_post, methods=["POST"])
    app.add_url_rule("/api/content/posts", endpoint="api_content_list_posts", view_func=api_content_list_posts)
    app.add_url_rule("/api/content/deliveries/<int:delivery_id>/status", endpoint="api_content_delivery_status", view_func=api_content_delivery_status, methods=["POST"])

    return SimpleNamespace(
        admin_pipeline=admin_pipeline,
        admin_settings=admin_settings,
        admin_content=admin_content,
        admin_content_post=admin_content_post,
        admin_content_card=admin_content_card,
        admin_content_detail=admin_content_detail,
        admin_content_update=admin_content_update,
        admin_content_comment=admin_content_comment,
        admin_content_delete=admin_content_delete,
        admin_content_sync_paperclip=admin_content_sync_paperclip,
        admin_metric_trigger_deep_dive_post=admin_metric_trigger_deep_dive_post,
        admin_content_trigger_daily_analysis=admin_content_trigger_daily_analysis,
        admin_game_trigger_content_analysis=admin_game_trigger_content_analysis,
        admin_content_variant_update=admin_content_variant_update,
        admin_content_add_destination=admin_content_add_destination,
        admin_content_toggle_delivery=admin_content_toggle_delivery,
        admin_content_toggle_image=admin_content_toggle_image,
        admin_content_add_image=admin_content_add_image,
        admin_content_replace_image=admin_content_replace_image,
        admin_content_image_review_payload=admin_content_image_review_payload,
        admin_content_apply_image_review=admin_content_apply_image_review,
        serve_social_post_image=serve_social_post_image,
        api_content_create_post=api_content_create_post,
        api_content_list_posts=api_content_list_posts,
        api_content_delivery_status=api_content_delivery_status,
    )
