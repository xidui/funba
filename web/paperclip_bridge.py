from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PaperclipBridgeConfig:
    api_url: str
    api_key: str | None
    company_id: str | None
    project_id: str | None
    content_analyst_agent_id: str | None
    content_reviewer_agent_id: str | None
    delivery_publisher_agent_id: str | None
    review_user_id: str | None
    content_analyst_name: str
    content_reviewer_name: str
    delivery_publisher_name: str
    review_user_name: str
    company_name: str
    timeout_seconds: float


@dataclass(frozen=True)
class DesiredIssueState:
    status: str
    assignee_agent_id: str | None
    assignee_user_id: str | None
    owner_label: str
    why_owner: str
    warnings: tuple[str, ...] = ()


class PaperclipBridgeError(RuntimeError):
    pass


REVIEW_PROFILE_PATHS = {
    "default": "agents/content-reviewer/profiles/default.md",
    "game_analysis": "agents/content-reviewer/profiles/game_analysis.md",
    "metric_deep_dive": "agents/content-reviewer/profiles/metric_deep_dive.md",
    "hero_highlight": "agents/content-reviewer/profiles/hero_highlight.md",
}


def review_profile_for_post(post: Mapping[str, Any]) -> str:
    explicit = str(post.get("review_profile") or "").strip()
    if explicit in REVIEW_PROFILE_PATHS:
        return explicit
    pipeline_type = str(post.get("pipeline_type") or "").strip()
    if pipeline_type in REVIEW_PROFILE_PATHS:
        return pipeline_type
    topic = str(post.get("topic") or "").strip()
    if topic.startswith("Hero Highlight"):
        return "hero_highlight"
    return "default"


def load_paperclip_bridge_config(environ: Mapping[str, str] | None = None) -> PaperclipBridgeConfig | None:
    env = environ or os.environ
    api_url = (env.get("PAPERCLIP_API_URL") or "http://127.0.0.1:3100").strip()
    api_key = (env.get("PAPERCLIP_API_KEY") or "").strip() or None
    return PaperclipBridgeConfig(
        api_url=api_url.rstrip("/"),
        api_key=api_key,
        company_id=(env.get("PAPERCLIP_COMPANY_ID") or "").strip() or None,
        project_id=(env.get("PAPERCLIP_FUNBA_PROJECT_ID") or "").strip() or None,
        content_analyst_agent_id=(env.get("PAPERCLIP_CONTENT_ANALYST_AGENT_ID") or "").strip() or None,
        content_reviewer_agent_id=(env.get("PAPERCLIP_CONTENT_REVIEWER_AGENT_ID") or "").strip() or None,
        delivery_publisher_agent_id=(env.get("PAPERCLIP_DELIVERY_PUBLISHER_AGENT_ID") or "").strip() or None,
        review_user_id=(env.get("PAPERCLIP_CONTENT_REVIEW_USER_ID") or "").strip() or None,
        content_analyst_name=(env.get("PAPERCLIP_CONTENT_ANALYST_NAME") or "Content Analyst").strip(),
        content_reviewer_name=(env.get("PAPERCLIP_CONTENT_REVIEWER_NAME") or "Content Reviewer").strip(),
        delivery_publisher_name=(env.get("PAPERCLIP_DELIVERY_PUBLISHER_NAME") or "Delivery Publisher").strip(),
        review_user_name=(env.get("PAPERCLIP_CONTENT_REVIEW_USER_NAME") or "Reviewer").strip(),
        company_name=(env.get("PAPERCLIP_COMPANY_NAME") or "FUNBA").strip(),
        timeout_seconds=float((env.get("PAPERCLIP_TIMEOUT_SECONDS") or "10").strip()),
    )


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_admin_comments(raw_comments: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_comments, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw_comments:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        normalized.append(
            {
                "text": text,
                "timestamp": str(item.get("timestamp") or ""),
                "from": str(item.get("from") or "unknown"),
                "origin": str(item.get("origin") or "funba_user"),
                "paperclip_comment_id": item.get("paperclip_comment_id"),
                "event_type": str(item.get("event_type") or "comment"),
            }
        )
    return normalized


def append_admin_comment(
    comments: list[dict[str, Any]],
    *,
    text: str,
    author: str,
    origin: str,
    event_type: str = "comment",
    timestamp: str | None = None,
    paperclip_comment_id: str | None = None,
) -> str:
    ts = timestamp or iso_utc_now()
    comments.append(
        {
            "text": text,
            "timestamp": ts,
            "from": author,
            "origin": origin,
            "paperclip_comment_id": paperclip_comment_id,
            "event_type": event_type,
        }
    )
    return ts


def desired_issue_state_for_post(post: Mapping[str, Any], cfg: PaperclipBridgeConfig) -> DesiredIssueState:
    local_status = str(post.get("status") or "draft").strip() or "draft"
    warnings: list[str] = []
    if local_status == "draft":
        if not cfg.content_analyst_agent_id:
            warnings.append("PAPERCLIP_CONTENT_ANALYST_AGENT_ID is not configured; issue left unassigned.")
        return DesiredIssueState(
            status="todo",
            assignee_agent_id=cfg.content_analyst_agent_id,
            assignee_user_id=None,
            owner_label=cfg.content_analyst_name,
            why_owner="revision was requested from the Funba content review UI",
            warnings=tuple(warnings),
        )
    if local_status == "ai_review":
        if not cfg.content_reviewer_agent_id:
            warnings.append("PAPERCLIP_CONTENT_REVIEWER_AGENT_ID is not configured; AI review issue left unassigned.")
        return DesiredIssueState(
            status="todo",
            assignee_agent_id=cfg.content_reviewer_agent_id,
            assignee_user_id=None,
            owner_label=cfg.content_reviewer_name,
            why_owner="the post is waiting for AI content review before human review",
            warnings=tuple(warnings),
        )
    if local_status == "in_review":
        if not cfg.review_user_id:
            warnings.append("PAPERCLIP_CONTENT_REVIEW_USER_ID is not configured; review issue left unassigned.")
        return DesiredIssueState(
            status="in_review",
            assignee_agent_id=None,
            assignee_user_id=cfg.review_user_id,
            owner_label=cfg.review_user_name,
            why_owner="the post is waiting for human review in Funba",
            warnings=tuple(warnings),
        )
    if local_status == "approved":
        if not cfg.delivery_publisher_agent_id:
            warnings.append("PAPERCLIP_DELIVERY_PUBLISHER_AGENT_ID is not configured; publish issue left unassigned.")
        return DesiredIssueState(
            status="todo",
            assignee_agent_id=cfg.delivery_publisher_agent_id,
            assignee_user_id=None,
            owner_label=cfg.delivery_publisher_name,
            why_owner="the post was approved in Funba and pending deliveries should now be published",
            warnings=tuple(warnings),
        )
    return DesiredIssueState(
        status="cancelled",
        assignee_agent_id=None,
        assignee_user_id=None,
        owner_label="None",
        why_owner="the post was archived in Funba",
        warnings=tuple(warnings),
    )


def build_post_issue_title(post: Mapping[str, Any]) -> str:
    topic = str(post.get("topic") or "Untitled post").strip() or "Untitled post"
    source_date = str(post.get("source_date") or "").strip()
    if source_date:
        return f"Funba content — {source_date} — {topic}"[:240]
    return f"Funba content — {topic}"[:240]


def _build_twitter_engagement_post_issue_description(
    *,
    post: Mapping[str, Any],
    variants: list[Mapping[str, Any]],
    variant_block: str,
    twitter_context: Mapping[str, Any],
    payload: dict[str, Any],
) -> str:
    conversation = twitter_context.get("conversation") or {}
    current_message = twitter_context.get("current_message") or {}
    metric_contexts = twitter_context.get("metric_contexts") or []
    messages = twitter_context.get("messages") or []
    post_id = post.get("id") or "<ID>"
    endpoint_lines = []
    for variant in variants:
        variant_id = variant.get("id") or "<VARIANT_ID>"
        title = str(variant.get("title") or "Untitled variant").strip()
        endpoint_lines.append(f"- {title}: `/api/admin/content/{post_id}/variants/{variant_id}/update`")
    endpoint_text = "\n".join(endpoint_lines) if endpoint_lines else f"- `/api/admin/content/{post_id}/variants/<VARIANT_ID>/update`"
    payload["twitter_context"] = twitter_context

    desc = (
        "Funba is the source of truth for this X/Twitter reply work item.\n\n"
        "## Objective\n\n"
        "Rewrite the existing seed variant into a natural, high-signal reply to the target X post.\n\n"
        "## Reply Voice\n\n"
        "Write as an NBA data analysis expert: concise, specific, and grounded in the supplied Funba facts. "
        "Use hero/notable metrics selectively; prefer one sharp supported stat when it directly answers the target post, and omit metrics when they would feel forced.\n\n"
        "## Hard Boundaries\n\n"
        "- Do not publish, submit, or send anything to X/Twitter.\n"
        "- Do not enable the disabled `twitter_reply` delivery until Yue manually confirms sending.\n"
        "- Do not create a new SocialPost, add destinations, or use normal platform-specific post-generation playbooks for this reply.\n"
        "- Keep the SocialPost and variant in `in_review` for manual confirmation.\n"
        "- Keep the reply concise, usually under 240 characters.\n"
        "- Include at most one Funba URL, and only use facts supported by the context below.\n\n"
        "## Required Funba Write\n\n"
        "Update only the existing variant through the matching endpoint:\n"
        f"{endpoint_text}\n\n"
        "Variants:\n"
        f"{variant_block}\n\n"
        "## X/Twitter Engagement Context\n\n"
        f"Conversation DB ID: {conversation.get('id') or 'unknown'}\n"
        f"X conversation ID: {conversation.get('x_conversation_id') or 'unknown'}\n"
        f"Current inbound message DB ID: {current_message.get('id') or 'unknown'}\n"
        f"Current target URL: {current_message.get('tweet_url') or 'unknown'}\n\n"
        "Hero/notable metric context:\n"
    )
    if isinstance(metric_contexts, list) and metric_contexts:
        desc += (
            "```json\n"
            f"{json.dumps(metric_contexts, ensure_ascii=False, indent=2)}\n"
            "```\n\n"
        )
    else:
        desc += "- none stored\n\n"

    desc += "Recent conversation messages:\n"
    if isinstance(messages, list) and messages:
        for message in messages[-12:]:
            if not isinstance(message, Mapping):
                continue
            author = str(message.get("author_username") or "unknown")
            direction = str(message.get("direction") or "unknown")
            status = str(message.get("status") or "unknown")
            text = " ".join(str(message.get("text") or "").split())[:500]
            desc += f"- {direction}/{status} @{author}: {text}\n"
    else:
        desc += "- none stored\n"

    desc += (
        "\n<funba_post>\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
        "</funba_post>"
    )
    return desc


def build_post_issue_description(post: Mapping[str, Any]) -> str:
    variants = post.get("variants") or []
    images = post.get("images") or []
    review_profile = review_profile_for_post(post)
    review_profile_path = REVIEW_PROFILE_PATHS[review_profile]
    pipeline_type = str(post.get("pipeline_type") or "").strip() or "unknown"
    enabled_images = [img for img in images if img.get("is_enabled")]
    enabled_slots = [str(img.get("slot") or "").strip() for img in enabled_images if str(img.get("slot") or "").strip()]
    destination_platforms = {
        str(destination.get("platform") or "").strip().lower()
        for variant in variants
        for destination in (variant.get("destinations") or [])
    }
    variant_lines = []
    placeholder_warnings = []
    for variant in variants:
        audience = str(variant.get("audience_hint") or "").strip() or "unspecified audience"
        destinations = variant.get("destinations") or []
        destination_labels = [f"{d.get('platform')}/{d.get('forum') or '?'}" for d in destinations]
        dest_text = ", ".join(destination_labels) if destination_labels else "none"
        variant_status = str(variant.get("status") or "in_review").strip() or "in_review"
        variant_lines.append(
            f"- {variant.get('title') or 'Untitled variant'} [{audience}] (status: {variant_status}) -> {dest_text}"
        )
        content_raw = str(variant.get("content_raw") or "")
        has_any_placeholder = "[[IMAGE:" in content_raw
        if enabled_slots and not has_any_placeholder:
            placeholder_warnings.append(
                f"- Variant '{variant.get('title') or 'Untitled variant'}' has enabled image pool assets but no `[[IMAGE:slot=...]]` placeholder in `content_raw`."
            )

    payload = {
        "post_id": post.get("id"),
        "source_date": post.get("source_date"),
        "topic": post.get("topic"),
        "status": post.get("status"),
        "priority": post.get("priority"),
        "pipeline_type": pipeline_type,
        "review_profile": review_profile,
        "review_profile_path": review_profile_path,
        "source_metrics": post.get("source_metrics") or [],
        "source_game_ids": post.get("source_game_ids") or [],
    }
    image_count = len(images)
    enabled_count = len(enabled_images)

    variant_block = "\n".join(variant_lines) if variant_lines else "- none yet"
    twitter_context = post.get("twitter_context") or {}
    if pipeline_type == "twitter_engagement" and isinstance(twitter_context, Mapping):
        return _build_twitter_engagement_post_issue_description(
            post=post,
            variants=[variant for variant in variants if isinstance(variant, Mapping)],
            variant_block=variant_block,
            twitter_context=twitter_context,
            payload=payload,
        )

    desc = (
        "Funba is the source of truth for this post. Review and publishing signals come from the Funba admin content UI.\n\n"
        f"Review profile: `{review_profile}`\n"
        f"Reviewer playbook: `{review_profile_path}`\n"
        f"Pipeline type: `{pipeline_type}`\n\n"
        "Variants:\n"
        f"{variant_block}\n\n"
    )
    if review_profile == "hero_highlight":
        expected_slots = ["poster"]
        if "instagram" in destination_platforms or "ig" in destination_platforms:
            expected_slots.append("poster_ig")
        missing_expected_slots = [slot for slot in expected_slots if slot not in set(enabled_slots)]
        desc += (
            "## Hero Highlight Review Rules\n\n"
            "This is a short deterministic hero-card post, not a long-form article. Review the poster and copy against the encoded game, trigger entity, metric value, and rank context before moving it forward.\n"
            f"Use `GET /api/admin/content/{post.get('id') or '<ID>'}/image-review-payload?include_disabled=1` to inspect `poster` and, when Instagram is enabled, `poster_ig`.\n"
            f"Expected hero image slots: {', '.join(expected_slots)}\n"
        )
        if image_count > 0:
            desc += f"Image pool: {enabled_count}/{image_count} enabled\n"
        if enabled_slots:
            desc += f"Enabled slots: {', '.join(enabled_slots)}\n"
        if missing_expected_slots or placeholder_warnings:
            desc += "\nCurrent warnings:\n"
            for slot in missing_expected_slots:
                desc += f"- Expected hero image slot `{slot}` is not enabled.\n"
            if placeholder_warnings:
                desc += "\n".join(placeholder_warnings) + "\n"
        desc += "\n"
    if image_count > 0:
        if review_profile != "hero_highlight":
            desc += (
                f"Image pool: {enabled_count}/{image_count} enabled\n\n"
            )
            desc += (
                "## Image Placeholder Rules\n\n"
                "Target image pool size for normal social posts is 10+ images so reviewers and publishers can choose the strongest set.\n"
                "Content Analyst must place slot-based placeholders like `[[IMAGE:slot=img1]]` directly inside `content_raw` where the image should appear.\n"
                "Do not assume the image pool alone is enough. If placeholders are missing, the images can be unused and the published post may go out as text-only.\n"
            )
            if enabled_slots:
                desc += f"Enabled slots: {', '.join(enabled_slots)}\n\n"
            if image_count < 10:
                desc += f"Current warnings:\n- Image pool is only {image_count} item(s); target is at least 10.\n"
                if placeholder_warnings:
                    desc += "\n".join(placeholder_warnings) + "\n\n"
                else:
                    desc += "\n"
            if placeholder_warnings:
                if image_count >= 10:
                    desc += "Current warnings:\n"
                    desc += "\n".join(placeholder_warnings) + "\n\n"
    desc += (
        "## Publishing with images\n\n"
        "When publishing to external platforms, pass `--post-id <post_id>` to the platform-specific CLI or publisher so it can read enabled images from the DB pool.\n"
        "Slot-based placeholders like `[[IMAGE:slot=img1]]` in `content_raw` are then resolved automatically.\n\n"
        "```\n"
        f"python -m social_media.<platform>.post ... --post-id {post.get('id') or '<ID>'}\n"
        "```\n\n"
        "<funba_post>\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n"
        "</funba_post>"
    )
    return desc


def build_status_handoff_comment(
    *,
    post: Mapping[str, Any],
    action: str,
    actor_name: str,
    desired_state: DesiredIssueState,
) -> str:
    lines = [
        "## Funba Workflow Update",
        "",
        f"Action: {action}",
        f"Post: {post.get('id')}",
        f"Topic: {post.get('topic')}",
        f"Triggered by: {actor_name}",
        "Triggered from: Funba admin content",
        "",
        f"Next owner: {desired_state.owner_label}",
        f"Why this owner: {desired_state.why_owner}.",
    ]
    if desired_state.warnings:
        lines.extend(["", "Warnings:"])
        lines.extend([f"- {warning}" for warning in desired_state.warnings])
    return "\n".join(lines)


def actor_label_for_issue(
    *,
    assignee_agent_id: str | None,
    assignee_user_id: str | None,
    cfg: PaperclipBridgeConfig | None,
) -> str:
    if cfg and assignee_agent_id and assignee_agent_id == cfg.content_analyst_agent_id:
        return cfg.content_analyst_name
    if cfg and assignee_agent_id and assignee_agent_id == cfg.content_reviewer_agent_id:
        return cfg.content_reviewer_name
    if cfg and assignee_agent_id and assignee_agent_id == cfg.delivery_publisher_agent_id:
        return cfg.delivery_publisher_name
    if cfg and assignee_user_id and assignee_user_id == cfg.review_user_id:
        return cfg.review_user_name
    if assignee_user_id:
        return f"user:{assignee_user_id}"
    if assignee_agent_id:
        return f"agent:{assignee_agent_id}"
    return "Unassigned"


def author_label_for_comment(comment: Mapping[str, Any], cfg: PaperclipBridgeConfig | None) -> str:
    author_agent_id = comment.get("authorAgentId")
    author_user_id = comment.get("authorUserId")
    if cfg and author_agent_id and author_agent_id == cfg.content_analyst_agent_id:
        return cfg.content_analyst_name
    if cfg and author_agent_id and author_agent_id == cfg.content_reviewer_agent_id:
        return cfg.content_reviewer_name
    if cfg and author_agent_id and author_agent_id == cfg.delivery_publisher_agent_id:
        return cfg.delivery_publisher_name
    if cfg and author_user_id and author_user_id == cfg.review_user_id:
        return cfg.review_user_name
    if author_agent_id:
        return f"agent:{author_agent_id}"
    if author_user_id:
        return f"user:{author_user_id}"
    return "paperclip"


def merge_paperclip_comments(
    local_comments: list[dict[str, Any]],
    remote_comments: list[Mapping[str, Any]],
    *,
    cfg: PaperclipBridgeConfig | None,
) -> bool:
    existing_ids = {c.get("paperclip_comment_id") for c in local_comments if c.get("paperclip_comment_id")}
    changed = False
    for remote in remote_comments:
        remote_id = remote.get("id")
        if not remote_id or remote_id in existing_ids:
            continue
        append_admin_comment(
            local_comments,
            text=str(remote.get("body") or "").strip() or "(empty comment)",
            author=author_label_for_comment(remote, cfg),
            origin="paperclip_agent" if remote.get("authorAgentId") else "paperclip_user",
            event_type="comment",
            timestamp=str(remote.get("createdAt") or iso_utc_now()),
            paperclip_comment_id=str(remote_id),
        )
        existing_ids.add(remote_id)
        changed = True
    return changed


class PaperclipClient:
    def __init__(self, cfg: PaperclipBridgeConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        if cfg.api_key:
            self.session.headers["Authorization"] = f"Bearer {cfg.api_key}"

    def discover_defaults(self) -> PaperclipBridgeConfig:
        cfg = self.cfg
        company_id = cfg.company_id
        if not company_id:
            companies = self._request("GET", "/api/companies")
            if isinstance(companies, list):
                for company in companies:
                    if str(company.get("name") or "").strip().lower() == cfg.company_name.lower():
                        company_id = company.get("id")
                        break
                if not company_id and len(companies) == 1:
                    company_id = companies[0].get("id")

        project_id = cfg.project_id
        content_analyst_agent_id = cfg.content_analyst_agent_id
        content_reviewer_agent_id = cfg.content_reviewer_agent_id
        delivery_publisher_agent_id = cfg.delivery_publisher_agent_id
        review_user_id = cfg.review_user_id

        if company_id:
            if not project_id:
                projects = self._request("GET", f"/api/companies/{company_id}/projects")
                repo_root = str(Path(__file__).resolve().parents[1])
                if isinstance(projects, list):
                    for project in projects:
                        primary_workspace = project.get("primaryWorkspace") or {}
                        codebase = project.get("codebase") or {}
                        paths = [
                            str(primary_workspace.get("cwd") or ""),
                            str(codebase.get("effectiveLocalFolder") or ""),
                            str(codebase.get("localFolder") or ""),
                        ]
                        if repo_root in paths:
                            project_id = project.get("id")
                            break
                    if not project_id:
                        for project in projects:
                            if str(project.get("name") or "").strip().lower() == "funba":
                                project_id = project.get("id")
                                break

            need_agents = not content_analyst_agent_id or not content_reviewer_agent_id or not delivery_publisher_agent_id
            if need_agents:
                agents = self._request("GET", f"/api/companies/{company_id}/agents")
                if isinstance(agents, list):
                    for agent in agents:
                        name = str(agent.get("name") or "").strip()
                        if not content_analyst_agent_id and name == cfg.content_analyst_name:
                            content_analyst_agent_id = agent.get("id")
                        if not content_reviewer_agent_id and name == cfg.content_reviewer_name:
                            content_reviewer_agent_id = agent.get("id")
                        if not delivery_publisher_agent_id and name == cfg.delivery_publisher_name:
                            delivery_publisher_agent_id = agent.get("id")

            if not review_user_id:
                members = self._request("GET", f"/api/companies/{company_id}/members")
                if isinstance(members, list):
                    for member in members:
                        if member.get("principalType") == "user" and member.get("membershipRole") == "owner":
                            review_user_id = member.get("principalId")
                            break

        resolved = PaperclipBridgeConfig(
            api_url=cfg.api_url,
            api_key=cfg.api_key,
            company_id=company_id,
            project_id=project_id,
            content_analyst_agent_id=content_analyst_agent_id,
            content_reviewer_agent_id=content_reviewer_agent_id,
            delivery_publisher_agent_id=delivery_publisher_agent_id,
            review_user_id=review_user_id,
            content_analyst_name=cfg.content_analyst_name,
            content_reviewer_name=cfg.content_reviewer_name,
            delivery_publisher_name=cfg.delivery_publisher_name,
            review_user_name=cfg.review_user_name,
            company_name=cfg.company_name,
            timeout_seconds=cfg.timeout_seconds,
        )
        self.cfg = resolved
        return resolved

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        url = f"{self.cfg.api_url}{path}"
        try:
            response = self.session.request(
                method=method,
                url=url,
                json=json_body,
                params=params,
                timeout=self.cfg.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise PaperclipBridgeError(f"Paperclip request failed: {exc}") from exc
        if response.status_code >= 400:
            body = response.text.strip()
            if len(body) > 300:
                body = body[:297] + "..."
            raise PaperclipBridgeError(f"Paperclip {method} {path} -> HTTP {response.status_code}: {body}")
        if not response.content:
            return None
        return response.json()

    def create_issue(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not self.cfg.company_id:
            raise PaperclipBridgeError("Paperclip company_id is not configured.")
        return self._request("POST", f"/api/companies/{self.cfg.company_id}/issues", json_body=payload)

    def update_issue(self, issue_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._request("PATCH", f"/api/issues/{issue_id}", json_body=payload)

    def add_comment(self, issue_id: str, body: str) -> dict[str, Any]:
        return self._request("POST", f"/api/issues/{issue_id}/comments", json_body={"body": body})

    def wake_agent(
        self,
        agent_id: str,
        *,
        reason: str,
        payload: Mapping[str, Any] | None = None,
        force_fresh_session: bool = False,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/agents/{agent_id}/wakeup",
            json_body={
                "source": "on_demand",
                "triggerDetail": "manual",
                "reason": reason,
                "payload": payload or None,
                "forceFreshSession": force_fresh_session,
            },
        )

    def get_issue(self, issue_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/issues/{issue_id}")

    def list_comments(self, issue_id: str, *, after_comment_id: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"order": "asc"}
        if after_comment_id:
            params["after"] = after_comment_id
        try:
            result = self._request("GET", f"/api/issues/{issue_id}/comments", params=params)
        except PaperclipBridgeError:
            if not after_comment_id:
                raise
            # Paperclip's incremental comment query can fail on some issues when
            # `after` is present even though the full comments endpoint succeeds.
            # Fall back to fetching the full ascending comment list and trim it
            # client-side so Funba syncs stay usable.
            result = self._request("GET", f"/api/issues/{issue_id}/comments", params={"order": "asc"})
            if not isinstance(result, list):
                return []
            anchor_index = next((i for i, c in enumerate(result) if c.get("id") == after_comment_id), None)
            if anchor_index is None:
                return []
            result = result[anchor_index + 1 :]
        return result if isinstance(result, list) else []

    def list_issues(self, *, q: str | None = None, project_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        if not self.cfg.company_id:
            raise PaperclipBridgeError("Paperclip company_id is not configured.")
        params: dict[str, Any] = {}
        if q:
            params["q"] = q
        if project_id:
            params["projectId"] = project_id
        if status:
            params["status"] = status
        result = self._request("GET", f"/api/companies/{self.cfg.company_id}/issues", params=params)
        return result if isinstance(result, list) else []
