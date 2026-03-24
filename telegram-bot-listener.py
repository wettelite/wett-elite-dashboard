#!/usr/bin/env python3
"""
Wett Elite Telegram Bot Listener
==================================
Runs on Hetzner as a systemd service.
Polls for Telegram callback_query events (inline button presses) and:
  1. Updates manual-overrides.json in the GitHub repo via GitHub API
  2. Triggers a new GitHub Actions workflow run
  3. Edits the original Telegram message to show the resolution

Environment variables (set in systemd service or .env):
  TG_BOT_TOKEN   — Telegram bot token
  GITHUB_TOKEN   — GitHub PAT with contents:write + actions:write
  GITHUB_REPO    — e.g. wettelite/wett-elite-dashboard
"""

import os, json, time, base64, requests, logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

TG_TOKEN    = os.environ.get("TG_BOT_TOKEN",  "8637002260:AAHmr8VNjus3TTVY_TcKueSNJHSEIWFQ_ug")
GH_TOKEN    = os.environ.get("GITHUB_TOKEN",  "")
GH_REPO     = os.environ.get("GITHUB_REPO",   "wettelite/wett-elite-dashboard")
POLL_SLEEP  = 2   # seconds between polls

TG_API      = f"https://api.telegram.org/bot{TG_TOKEN}"
GH_API      = "https://api.github.com"
GH_HEADERS  = {
    "Authorization": f"token {GH_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

ACTION_MAP = {
    "app_B": ("Approved",           "Betlabel", "✅ Approved — Betlabel"),
    "app_W": ("Approved",           "Winnerz",  "✅ Approved — Winnerz"),
    "app_R": ("Approved",           "Winrolla", "✅ Approved — Winrolla"),
    "exc_O": ("Excluded (Oddify)",  "Unknown",  "❌ Excluded — Oddify"),
    "exc_P": ("Excluded (Promo)",   "Unknown",  "❌ Excluded — Promo"),
    "exc_F": ("Excluded (No FTD)",  "Unknown",  "❌ No successful FTD"),
}

# ---------------------------------------------------------------------------
# GitHub helpers
# ---------------------------------------------------------------------------

def gh_get_file(path: str) -> tuple[dict, str]:
    """Returns (parsed_json, sha) for a file in the repo."""
    resp = requests.get(f"{GH_API}/repos/{GH_REPO}/contents/{path}", headers=GH_HEADERS)
    resp.raise_for_status()
    data = resp.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content), data["sha"]


def gh_update_file(path: str, sha: str, content: dict, message: str) -> None:
    """Commits updated JSON back to the repo."""
    encoded = base64.b64encode(
        json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")
    ).decode("utf-8")
    requests.put(
        f"{GH_API}/repos/{GH_REPO}/contents/{path}",
        headers=GH_HEADERS,
        json={"message": message, "content": encoded, "sha": sha},
    ).raise_for_status()


def gh_trigger_workflow() -> None:
    """Triggers a dashboard-only refresh (no Telegram spam)."""
    requests.post(
        f"{GH_API}/repos/{GH_REPO}/actions/workflows/daily-update.yml/dispatches",
        headers=GH_HEADERS,
        json={"ref": "main", "inputs": {"skip_telegram": "true"}},
    ).raise_for_status()


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------

def tg_answer_callback(callback_id: str, text: str) -> None:
    requests.post(f"{TG_API}/answerCallbackQuery",
                  json={"callback_query_id": callback_id, "text": text}, timeout=5)


def tg_edit_message(chat_id: int, message_id: int, new_text: str) -> None:
    requests.post(f"{TG_API}/editMessageText",
                  json={"chat_id": chat_id, "message_id": message_id,
                        "text": new_text, "parse_mode": "HTML"}, timeout=5)


# ---------------------------------------------------------------------------
# Core handler
# ---------------------------------------------------------------------------

def handle_callback(callback: dict) -> None:
    cid      = callback["id"]
    data     = callback.get("data", "")
    user     = callback["from"].get("username") or callback["from"].get("first_name", "?")
    chat_id  = callback["message"]["chat"]["id"]
    msg_id   = callback["message"]["message_id"]
    orig_txt = callback["message"].get("text", "")

    # Parse callback data: "action:ticket"
    parts = data.split(":", 1)
    if len(parts) != 2:
        tg_answer_callback(cid, "⚠️ Unknown action")
        return

    action, ticket = parts
    if action not in ACTION_MAP:
        tg_answer_callback(cid, "⚠️ Unknown action")
        return

    new_status, new_campaign, label = ACTION_MAP[action]
    log.info(f"Button press: {action} on {ticket} by @{user}")

    # 1 — Update manual-overrides.json in GitHub
    try:
        overrides, sha = gh_get_file("manual-overrides.json")
        if ticket not in overrides:
            overrides[ticket] = {}
        overrides[ticket]["status"]   = new_status
        overrides[ticket]["campaign"] = new_campaign
        overrides[ticket]["reviewed_by"]  = f"@{user}"
        overrides[ticket]["reviewed_at"]  = datetime.now(timezone.utc).isoformat()
        gh_update_file(
            "manual-overrides.json", sha, overrides,
            f"review: {label} — {ticket} (@{user})"
        )
        log.info(f"GitHub updated: {ticket} → {new_status} / {new_campaign}")
    except Exception as e:
        log.error(f"GitHub update failed: {e}")
        tg_answer_callback(cid, "❌ GitHub update failed — try again")
        return

    # 2 — Answer the callback (removes the loading spinner in Telegram)
    tg_answer_callback(cid, f"{label} saved!")

    # 3 — Edit the original message to show resolution
    resolved_text = (
        orig_txt
        + f"\n\n{label}\n👮 Reviewed by @{user}"
    )
    tg_edit_message(chat_id, msg_id, resolved_text)

    # 4 — Trigger a fresh dashboard run (non-blocking best-effort)
    try:
        gh_trigger_workflow()
        log.info("GitHub Actions workflow triggered")
    except Exception as e:
        log.warning(f"Could not trigger workflow: {e}")


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def main():
    if not GH_TOKEN:
        log.error("GITHUB_TOKEN not set — bot cannot update overrides. Exiting.")
        return

    log.info(f"🤖 Wett Elite bot listener started (repo: {GH_REPO})")
    offset = 0

    while True:
        try:
            resp = requests.get(
                f"{TG_API}/getUpdates",
                params={"offset": offset, "timeout": 30, "allowed_updates": ["callback_query"]},
                timeout=35,
            )
            updates = resp.json().get("result", [])
            for update in updates:
                offset = update["update_id"] + 1
                if "callback_query" in update:
                    try:
                        handle_callback(update["callback_query"])
                    except Exception as e:
                        log.error(f"handle_callback error: {e}")
        except Exception as e:
            log.error(f"Poll error: {e}")
            time.sleep(5)

        time.sleep(POLL_SLEEP)


if __name__ == "__main__":
    main()
