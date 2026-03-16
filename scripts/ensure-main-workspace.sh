#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"
TARGET_BRANCH="main"
REMOTE_REF="origin/main"

git -C "$REPO_ROOT" fetch origin main --quiet 2>/dev/null || true

if ! git -C "$REPO_ROOT" rev-parse "$TARGET_BRANCH" >/dev/null 2>&1; then
  if git -C "$REPO_ROOT" rev-parse "$REMOTE_REF" >/dev/null 2>&1; then
    git -C "$REPO_ROOT" branch --track "$TARGET_BRANCH" "$REMOTE_REF" >/dev/null 2>&1 || true
  else
    echo "Unable to find local or remote main branch." >&2
    exit 1
  fi
fi

CURRENT_BRANCH="$(git -C "$REPO_ROOT" symbolic-ref --quiet --short HEAD 2>/dev/null || echo DETACHED)"
STATUS_OUTPUT="$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal)"

if [[ "$CURRENT_BRANCH" != "$TARGET_BRANCH" ]]; then
  if [[ -n "$STATUS_OUTPUT" ]]; then
    echo "Workspace root is on '$CURRENT_BRANCH' with local changes; refusing to switch to main." >&2
    printf '%s\n' "$STATUS_OUTPUT" >&2
    exit 1
  fi
  git -C "$REPO_ROOT" switch "$TARGET_BRANCH" >/dev/null
fi

if git -C "$REPO_ROOT" rev-parse "$REMOTE_REF" >/dev/null 2>&1; then
  read -r LEFT RIGHT <<EOF
$(git -C "$REPO_ROOT" rev-list --left-right --count "$TARGET_BRANCH...$REMOTE_REF")
EOF
  if [[ "$LEFT" -eq 0 && "$RIGHT" -gt 0 ]]; then
    git -C "$REPO_ROOT" merge --ff-only "$REMOTE_REF" >/dev/null
  fi
fi

echo "BRANCH=$TARGET_BRANCH"
