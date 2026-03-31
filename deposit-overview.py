#!/usr/bin/env python3
"""
Deposit Registration Overview
==============================
Scans all HTML transcripts in Google Drive, detects which affiliate campaign
(Winrolla / Betlabel / Winnerz) each ticket belongs to, checks whether the user
submitted a deposit screenshot, and whether an admin approved them.

Writes results to a new Google Sheet with two tabs:
  - Details  : one row per ticket
  - Summary  : aggregated counts per campaign

Usage:
  source venv/bin/activate
  python deposit-overview.py

NOTE: First run will trigger a browser OAuth re-auth (Sheets scope added).
"""

import base64
import datetime
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import requests as http_requests
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    import io
except ImportError:
    print("ERROR: Run: pip install google-auth-oauthlib google-api-python-client requests")
    sys.exit(1)

try:
    import anthropic as anthropic_sdk
except ImportError:
    print("ERROR: Run: pip install anthropic")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent

# Old bulk-exported folder (historical, no longer updated)
GDRIVE_FOLDER_OLD = "1npYHrpWLiq234qP1Ix0ECbU2VC9iDUEq"
# New folder where TicketTool now auto-uploads transcripts
GDRIVE_FOLDER_NEW = "1UuKbmwxmEgYqNzM2V3QzquukW4N6ZV5A"
GDRIVE_TRANSCRIPTS_FOLDER_IDS = [GDRIVE_FOLDER_OLD, GDRIVE_FOLDER_NEW]

OAUTH_CLIENT_FILE    = SCRIPT_DIR / "oauth-client.json"
OAUTH_TOKEN_FILE     = SCRIPT_DIR / "oauth-token.json"
ANTHROPIC_KEY_FILE   = SCRIPT_DIR / "anthropic-api-key.txt"
# Fallback: check shared scripts folder
_ANTHROPIC_KEY_FALLBACK = Path("/Users/francisco/Claude/scripts/anthropic-api-key.txt")

# Both Drive (read) and Sheets (write) scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Admin/mod usernames — messages from these users are admin messages
# Partial substrings are fine — checked via "any a.lower() in name_lower"
ADMIN_USERS = {
    "Dominik / Mod", "adonis.39", "adonis39", "adonis",
    "Jerry / Mod", "wettelitemod", "Wett Elite", "wettelite",
    "Dom / Mod", "dominik", "jerry",
}

# Discord user IDs of known mods (fallback if username not in ADMIN_USERS)
ADMIN_USER_IDS = {
    "1477503779690516633",
    "1479116852255920241",
    "1382483704084955158",
}

# ---------------------------------------------------------------------------
# Campaign detection patterns
# ---------------------------------------------------------------------------
CAMPAIGNS = {
    "Winrolla": [
        r"wnrl\.fynkelto\.com",
        r"winrolla",
        r"mid=284740",
    ],
    "Betlabel": [
        r"moy\.auraodin\.com",
        r"betlabel",
        r"pid=168318",
        r"bid=1650",
    ],
    "Winnerz": [
        r"go\.spinwise\.partners",
        r"winnerz",
        r"bta=982878",
    ],
}

# ---------------------------------------------------------------------------
# Approval detection
# ---------------------------------------------------------------------------
APPROVAL_KEYWORDS = [
    # Core approval phrases
    r"freigeschaltet",
    r"freigeschalten",
    r"schalte.*frei",       # "schalte dich frei", "schalte ich dich frei"
    r"frei\s*geschaltet",
    r"bist registriert",
    r"bist eingetragen",
    r"hab dich",
    r"habe dich",
    r"alles klar",
    r"passt",
    r"best[äa]tigt",
    r"approved",
    r"erledigt",
    r"geht klar",
    r"bist drin",
    r"bist durch",
    r"\bdone\b",
    r"perfekt",
    r"\btop\b",             # catches "Top!!", "top", "top,"
    r"willkommen",          # "Willkommen!" / "Willkommen mein br" = they're in
    r"welcome",
    r"viel spa[sß]",        # "viel spaß" = have fun, sent after approval
    r"herzlichen gl[üu]ckwunsch",
    r"gl[üu]ckwunsch",
    r"congrat",
    r"\bok\b",              # simple "ok" as standalone reply
    r"\bokay\b",
    r"\bsuper\b",
    r"\bgut\b",
    r"nice",
    # Emoji in message content
    r"👍", r"✅", r"✔", r"🎉", r"🔥",
]

REJECTION_KEYWORDS = [
    r"\bfake\b",
    r"ungültig",
    r"ung.ltig",
    r"invalid",
    r"abgelehnt",
    r"nicht akzeptiert",
    r"nicht g.ltig",
]

# Emoji that count as approval reactions (unicode)
APPROVAL_EMOJIS = {"👍", "✅", "✔️", "🎉", "✔", "☑️"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ---------------------------------------------------------------------------
# Google Auth — Drive + Sheets
# ---------------------------------------------------------------------------
def get_services():
    creds = None
    if OAUTH_TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(OAUTH_TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                log("Google token refreshed")
            except Exception:
                creds = None

        if not creds:
            log("Opening browser for Google OAuth (one-time)...")
            flow = InstalledAppFlow.from_client_secrets_file(str(OAUTH_CLIENT_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
            log("OAuth authorized")

        OAUTH_TOKEN_FILE.write_text(creds.to_json())

    drive   = build("drive",   "v3", credentials=creds, cache_discovery=False)
    sheets  = build("sheets",  "v4", credentials=creds, cache_discovery=False)
    return drive, sheets, creds

# ---------------------------------------------------------------------------
# Drive helpers
# ---------------------------------------------------------------------------
def drive_list_files(service, folder_id: str) -> list[dict]:
    files, page_token = [], None
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name)",
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def drive_download_threadsafe(token: str, file_id: str) -> bytes:
    """Download a Drive file using plain requests — safe to call from multiple threads."""
    resp = http_requests.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.content

# ---------------------------------------------------------------------------
# Transcript parsing
# ---------------------------------------------------------------------------
def parse_messages(html_bytes: bytes) -> list[dict] | None:
    """Decode the TicketTool base64 JSON blob from an HTML transcript."""
    html = html_bytes.decode("utf-8", errors="replace")
    m = re.search(r'let messages\s*=\s*"([A-Za-z0-9+/=]+)"', html)
    if not m:
        return None
    try:
        raw = base64.b64decode(m.group(1)).decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None

def get_author(msg: dict) -> tuple[str, str]:
    """Return (username, user_id) from a message regardless of structure."""
    username = msg.get("username") or msg.get("nick") or (msg.get("author") or {}).get("name") or ""
    user_id  = msg.get("user_id") or (msg.get("author") or {}).get("id") or ""
    return username, user_id

def is_admin(msg: dict) -> bool:
    username, user_id = get_author(msg)
    if user_id in ADMIN_USER_IDS:
        return True
    name_lower = username.lower()
    return any(a.lower() in name_lower for a in ADMIN_USERS)

# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------
def detect_campaign(messages: list[dict]) -> str:
    """Search all message content for affiliate link patterns."""
    all_text = " ".join(
        (msg.get("content") or "") for msg in messages
    ).lower()

    for campaign, patterns in CAMPAIGNS.items():
        for pat in patterns:
            if re.search(pat, all_text, re.IGNORECASE):
                return campaign
    return "Unknown"

def detect_screenshot(messages: list[dict]) -> tuple[bool, str]:
    """
    Returns (has_screenshot, user_who_sent_it).
    Looks for image attachments from non-admin users.
    """
    for msg in messages:
        if is_admin(msg):
            continue
        username, _ = get_author(msg)
        for att in msg.get("attachments", []):
            b64 = att.get("base64", "")
            if b64.startswith("data:image/"):
                return True, username
            # Some transcripts store URL instead of base64
            url = att.get("url", "") or att.get("proxy_url", "")
            if url and re.search(r"\.(jpg|jpeg|png|gif|webp)", url, re.IGNORECASE):
                return True, username
    return False, ""

def extract_ticket_date(messages: list[dict]) -> str:
    """Return ISO-8601 UTC datetime of the first message, or '' if unavailable."""
    for msg in messages:
        created_ms = msg.get("created")
        if created_ms and isinstance(created_ms, (int, float)) and created_ms > 1_000_000_000_000:
            dt = datetime.datetime.fromtimestamp(created_ms / 1000, tz=datetime.timezone.utc)
            return dt.isoformat()
    return ""

def detect_approval(messages: list[dict], screenshot_present: bool) -> tuple[str, str, str]:
    """
    Returns (status, signal, approving_admin).
    status: "Approved" | "To be checked" | "Not Approved"
    signal: description of what triggered approval
    approving_admin: name of admin who approved

    Only checks admin messages that come AFTER the first user screenshot,
    so the admin's welcome/instruction message is never counted as approval.
    """
    if not screenshot_present:
        return "Not Approved", "", ""

    # Find index of first user screenshot
    screenshot_idx = None
    for i, msg in enumerate(messages):
        if is_admin(msg):
            continue
        for att in msg.get("attachments", []):
            if att.get("base64", "").startswith("data:image/"):
                screenshot_idx = i
                break
            url = att.get("url", "") or att.get("proxy_url", "")
            if url and re.search(r"\.(jpg|jpeg|png|gif|webp)", url, re.IGNORECASE):
                screenshot_idx = i
                break
        if screenshot_idx is not None:
            break

    if screenshot_idx is None:
        return "To be checked", "", ""

    # Check emoji reactions ON the user's screenshot message itself
    # (admin may have reacted with ✅ without typing)
    ss_msg = messages[screenshot_idx]
    for reaction in ss_msg.get("reactions", []):
        emoji_name = reaction.get("emoji", {}).get("name", "")
        if emoji_name in APPROVAL_EMOJIS:
            return "Approved", f"emoji reaction on screenshot: {emoji_name}", "admin"

    # Only check admin messages AFTER the screenshot
    for msg in messages[screenshot_idx + 1:]:
        if not is_admin(msg):
            continue

        username, _ = get_author(msg)
        content = (msg.get("content") or "").lower()

        # Check emoji reactions on this admin message
        for reaction in msg.get("reactions", []):
            emoji_name = reaction.get("emoji", {}).get("name", "")
            if emoji_name in APPROVAL_EMOJIS:
                return "Approved", f"emoji reaction: {emoji_name}", username

        # Check emoji in message content directly
        for emoji_char in APPROVAL_EMOJIS:
            if emoji_char in content:
                return "Approved", f"emoji in message: {emoji_char}", username

        if not content:
            continue

        # Check for rejection first
        is_rejection = any(re.search(p, content, re.IGNORECASE) for p in REJECTION_KEYWORDS)
        if is_rejection:
            continue  # skip this message, don't count as approval

        # Check for approval keyword
        for kw in APPROVAL_KEYWORDS:
            if re.search(kw, content, re.IGNORECASE):
                snippet = content[:80].replace("\n", " ")
                return "Approved", f'"{snippet}"', username

    return "To be checked", "", ""

# ---------------------------------------------------------------------------
# Claude Vision — brand classification for Unknown tickets
# ---------------------------------------------------------------------------
def extract_all_user_images(messages: list[dict]) -> list[tuple[str, str]]:
    """Return list of (base64_data, media_type) for ALL non-admin images in ticket."""
    images = []
    for msg in messages:
        if is_admin(msg):
            continue
        for att in msg.get("attachments", []):
            b64_uri = att.get("base64", "")
            m = re.match(r"data:(image/\w+);base64,(.+)", b64_uri, re.DOTALL)
            if m:
                media_type = m.group(1)
                images.append((m.group(2).strip(), media_type))
    return images

def _build_image_content(images: list[tuple[str, str]]) -> list[dict]:
    """Build the list of image content blocks (up to 4 images)."""
    content = []
    for img_b64, media_type in images[:4]:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": img_b64,
            },
        })
    return content


def _parse_amount(text: str) -> float | None:
    """Extract a deposit amount from a Vision response line like 'AMOUNT: 50.00'."""
    m = re.search(r'AMOUNT:\s*([\d]+(?:[.,][\d]+)?)', text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except ValueError:
            pass
    return None


def classify_brand_and_amount_by_vision(
    anthropic_client, images: list[tuple[str, str]]
) -> tuple[str, float | None, str]:
    """Ask Claude to identify the brand, deposit amount, AND verdict from ticket screenshots.
    Returns (brand, amount, verdict) where:
      brand is one of the known brands or 'Unknown',
      amount is a float (€) or None if not visible,
      verdict is 'Approved', 'No FTD', or 'Promo'.
    """
    if not images:
        return "Unknown", None, "No FTD"
    try:
        content = _build_image_content(images)
        content.append({
            "type": "text",
            "text": (
                "These screenshots are from a Discord ticket where a user submitted proof of deposit "
                "at an online casino. The three possible casino brands are:\n"
                "- Winrolla (winrolla.com / wnrl)\n"
                "- Betlabel (betlabel.de)\n"
                "- Winnerz (winnerz.com)\n\n"
                "Look at ALL the images carefully — some may be bank/transaction history pages, "
                "but at least one should show the casino interface or logo.\n\n"
                "Reply with EXACTLY three lines and nothing else:\n"
                "Line 1: VERDICT: [Approved|No FTD|Promo]\n"
                "  - Approved = clear deposit screenshot visible showing a successful transaction\n"
                "  - No FTD = no deposit proof shown, or screenshot doesn't show a completed deposit\n"
                "  - Promo = this is a promotional/bonus/Verlosung deposit, not a first-time deposit\n"
                "Line 2: BRAND: [Winrolla|Betlabel|Winnerz|Unknown]\n"
                "Line 3: AMOUNT: [deposit amount as a number using dot notation, e.g. 50.00 — "
                "strip any currency symbol; use the deposit confirmation amount, not an account balance; "
                "reply Unknown if the amount is not clearly visible]\n\n"
                "Example response:\nVERDICT: Approved\nBRAND: Winnerz\nAMOUNT: 25.00"
            ),
        })

        resp = anthropic_client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=80,
            messages=[{"role": "user", "content": content}],
        )
        answer = resp.content[0].text.strip()

        # Parse verdict
        verdict = "No FTD"
        if re.search(r'VERDICT:\s*Approved', answer, re.IGNORECASE):
            verdict = "Approved"
        elif re.search(r'VERDICT:\s*Promo', answer, re.IGNORECASE):
            verdict = "Promo"

        # Parse brand
        brand = "Unknown"
        for b in ("Winrolla", "Betlabel", "Winnerz"):
            if b.lower() in answer.lower():
                brand = b
                break

        # Parse amount
        amount = _parse_amount(answer)
        return brand, amount, verdict
    except Exception:
        return "Unknown", None, "No FTD"


def extract_amount_by_vision(
    anthropic_client, images: list[tuple[str, str]]
) -> float | None:
    """Ask Claude to extract only the deposit amount from ticket screenshots.
    Used for tickets where the brand is already known.
    Returns a float (€) or None if not visible.
    """
    if not images:
        return None
    try:
        content = _build_image_content(images)
        content.append({
            "type": "text",
            "text": (
                "These screenshots are from a Discord ticket where a user submitted proof of a casino deposit. "
                "Look at ALL the images carefully.\n\n"
                "Reply with EXACTLY one line and nothing else:\n"
                "AMOUNT: [deposit amount as a number using dot notation, e.g. 50.00 — "
                "strip any currency symbol; use the deposit confirmation amount, not an account balance; "
                "reply Unknown if the amount is not clearly visible]\n\n"
                "Example: AMOUNT: 100.00"
            ),
        })

        resp = anthropic_client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=30,
            messages=[{"role": "user", "content": content}],
        )
        answer = resp.content[0].text.strip()
        return _parse_amount(answer)
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Manual overrides — load saved human corrections from previous runs
# ---------------------------------------------------------------------------
MANUAL_OVERRIDES_FILE = SCRIPT_DIR / "manual-overrides.json"

def load_manual_overrides() -> dict:
    if MANUAL_OVERRIDES_FILE.exists():
        try:
            return json.loads(MANUAL_OVERRIDES_FILE.read_text())
        except Exception:
            pass
    return {}

def save_manual_overrides(overrides: dict):
    MANUAL_OVERRIDES_FILE.write_text(json.dumps(overrides, indent=2, ensure_ascii=False))

# ---------------------------------------------------------------------------
# Exclusion & deduplication helpers
# ---------------------------------------------------------------------------
def extract_user_id(ticket_name: str) -> str:
    """Extract Discord user_id from ticket filename (part after last underscore)."""
    m = re.search(r'_(\d{15,20})$', ticket_name)
    return m.group(1) if m else ""

def detect_oddify(messages: list[dict]) -> bool:
    """Return True if any message content mentions 'oddify'."""
    for msg in messages:
        content = (msg.get("content") or "").lower()
        if "oddify" in content:
            return True
    return False

def apply_exclusions_and_overrides(result: dict, messages: list[dict] | None,
                                   manual_overrides: dict):
    """
    Mutate result in-place applying business rules in priority order:
      1. Manual override with definitive status → ALWAYS wins (admin decision is final)
      2. einzahlung-promo ticket name → Excluded (Promo)
      3. oddify- ticket name OR message content → Excluded (Oddify)
      4. Promo text in message content → Excluded (Promo)
      5. Otherwise keep auto-detected status
    Admin overrides take priority so a manually approved ticket is never
    re-excluded by auto-detection on subsequent runs.
    """
    ticket_lower = result["ticket"].lower()

    # 1. Manual overrides — ALWAYS checked first so admin decisions are never overridden
    override = manual_overrides.get(result["ticket"])
    if override:
        saved_status = override.get("status", "")

        # If override has a definitive status, apply it and skip all auto-detection
        if saved_status and saved_status != "To be checked":
            result["approval_status"] = saved_status
            result["approval_signal"] = override.get("signal") or saved_status
            if override.get("reviewed_by"):
                result["approving_admin"] = override["reviewed_by"]
            elif override.get("approving_admin"):
                result["approving_admin"] = override["approving_admin"]
            # Fall through to apply campaign/date/amount from override below

        # If human OR Vision saved a campaign → honour it
        if override.get("campaign") and override["campaign"] != "Unknown":
            if result["campaign"] == "Unknown":
                result["campaign"] = override["campaign"]
                if override.get("campaign_source"):
                    result["campaign_source"] = override["campaign_source"]

        # Restore saved ticket_date if analysis couldn't extract it
        if not result.get("ticket_date") and override.get("ticket_date"):
            result["ticket_date"] = override["ticket_date"]

        # Always carry first_seen_at forward from overrides
        if override.get("first_seen_at"):
            result["first_seen_at"] = override["first_seen_at"]

        # Restore cached deposit amount (key presence check — None/null is a valid cached value)
        if "deposit_amount" in override:
            result["deposit_amount"] = override["deposit_amount"]
            result["deposit_amount_source"] = override.get("deposit_amount_source", "")

        # If we applied a definitive status, we're done — skip auto-detection
        if saved_status and saved_status != "To be checked":
            return

    # 2. Promo tickets by ticket name (no override saved yet)
    PROMO_NAME_KEYWORDS = ["einzahlung-promo", "discord-promo", "weekend-promo"]
    if any(kw in ticket_lower for kw in PROMO_NAME_KEYWORDS):
        result["approval_status"] = "Excluded (Promo)"
        result["approval_signal"] = "Ticket name indicates promo deposit (already registered)"
        return

    # 3. Oddify by ticket name
    if ticket_lower.startswith("oddify"):
        result["approval_status"] = "Excluded (Oddify)"
        result["approval_signal"] = "Ticket name indicates Oddify source"
        return

    # 4. Oddify by message content
    if messages and detect_oddify(messages):
        result["approval_status"] = "Excluded (Oddify)"
        result["approval_signal"] = "Message content mentions Oddify"
        return

    # 5. Promo by message content
    PROMO_TEXT_KEYWORDS = ["verlosung", "gewinnspiel", "promotion deposit", "promo einzahlung"]
    if messages:
        all_text = " ".join((m.get("content") or "") for m in messages).lower()
        if any(kw in all_text for kw in PROMO_TEXT_KEYWORDS):
            result["approval_status"] = "Excluded (Promo)"
            result["approval_signal"] = "Message content indicates promo deposit"
            return


def analyze_transcript(html_bytes: bytes, filename: str,
                       manual_overrides: dict | None = None) -> dict:
    """Full analysis of one transcript. Returns a result dict."""
    messages = parse_messages(html_bytes)
    ticket_name = filename.replace(".html", "")

    result = {
        "ticket":                ticket_name,
        "user_id":               extract_user_id(ticket_name),
        "user":                  "",
        "campaign":              "Unknown",
        "has_screenshot":        False,
        "approval_status":       "Not Approved",
        "approval_signal":       "",
        "approving_admin":       "",
        "ticket_date":           "",
        "parse_error":           False,
        "deposit_amount":        None,   # float or None if not extractable
        "deposit_amount_source": "",     # "vision" | "manual" | ""
        "drive_file_id":         "",     # Google Drive file ID for linking
    }

    if messages is None:
        result["parse_error"] = True
        return result

    result["campaign"]     = detect_campaign(messages)
    result["ticket_date"]  = extract_ticket_date(messages)

    has_screenshot, user = detect_screenshot(messages)
    result["has_screenshot"] = has_screenshot
    result["user"] = user

    # Fallback: if no screenshot sender, use the first non-admin message author
    if not user:
        for msg in messages:
            if not is_admin(msg):
                uname, _ = get_author(msg)
                # Skip bot / system usernames
                if uname and uname.lower() not in ("ticket tool", "tickettool", "ticket-tool"):
                    result["user"] = uname
                    break

    status, signal, admin = detect_approval(messages, has_screenshot)
    result["approval_status"] = status
    result["approval_signal"] = signal
    result["approving_admin"] = admin

    # Apply exclusion rules and manual overrides (mutates result in-place)
    if manual_overrides is not None:
        apply_exclusions_and_overrides(result, messages, manual_overrides)

    return result

# ---------------------------------------------------------------------------
# Google Sheets output
# ---------------------------------------------------------------------------
SUMMARY_CAMPAIGNS = ["Winrolla", "Betlabel", "Winnerz", "Unknown"]

EXCLUDED_STATUSES = {"Excluded (Oddify)", "Excluded (Promo)", "Excluded (Internal)", "Excluded (No FTD)"}

# Discord cached member data (roles)
DISCORD_MEMBERS_FILE = SCRIPT_DIR / "discord-ftd-members.json"

# Role ID → display name mapping
DISCORD_ROLE_NAMES = {
    "1480118877722513499": "Koenisch Elite",
    "1485926360907124816": "WR",
    "1485926449255940147": "Winnerz",
    "1485926628273164348": "BL",
}


def load_discord_members() -> tuple[dict[str, list[str]], dict[str, dict]]:
    """Load cached Discord member data.

    Returns:
        roles:   {user_id: [role_name, ...]}
        profiles: {user_id: {"username": ..., "display_name": ...}}
    """
    if not DISCORD_MEMBERS_FILE.exists():
        return {}, {}
    try:
        members = json.loads(DISCORD_MEMBERS_FILE.read_text())
        roles = {}
        profiles = {}
        for m in members:
            u = m.get("user", {})
            uid = u.get("id", "")
            if not uid:
                continue
            role_names = []
            for rid in m.get("roles", []):
                name = DISCORD_ROLE_NAMES.get(str(rid))
                if name:
                    role_names.append(name)
            roles[uid] = role_names
            profiles[uid] = {
                "username": u.get("username", ""),
                "display_name": u.get("global_name") or m.get("nick") or "",
            }
        return roles, profiles
    except Exception:
        return {}, {}


def build_user_lookup_data(results: list[dict], discord_roles: dict[str, list[str]],
                           discord_profiles: dict[str, dict] | None = None) -> list[dict]:
    """Build per-user aggregated data for the User Lookup feature.

    Deduplicates users: same user_id → merged. If no user_id, same username → merged.
    """
    ftd_keys = get_ftd_ticket_keys(results)
    discord_profiles = discord_profiles or {}

    status_rank = {
        "Approved": 0, "To be checked": 1,
        "Not Approved": 2, "Excluded (Oddify)": 3,
        "Excluded (Promo)": 4, "Excluded (Internal)": 5,
        "Excluded (No FTD)": 6,
    }

    user_map: dict[str, dict] = {}
    # Secondary index: username (lowered) → key, for dedup when user_id differs
    username_to_key: dict[str, str] = {}

    for r in results:
        uid = r.get("user_id", "").strip()
        user = r.get("user", "").strip()
        # Primary key: user_id, fallback to username, last resort ticket name
        key = uid if uid else (user if user else r["ticket"])

        # Dedup: if we have a username match already under a different key, merge
        if user and key not in user_map:
            existing_key = username_to_key.get(user.lower())
            if existing_key and existing_key in user_map:
                key = existing_key

        if key not in user_map:
            user_map[key] = {
                "user_id": uid,
                "username": user,
                "brand": "",
                "ftd_approved": False,
                "status": "Not Approved",
                "status_detail": "",
                "ftd_amount": None,
                "total_deposits": 0.0,
                "deposit_count": 0,
                "promo_count": 0,
                "approval_signal": "",
                "approving_admin": "",
                "first_deposit_date": "",
                "tickets": [],
                "display_name": "",
                "discord_roles": [],
                "_best_rank": 99,
            }

        entry = user_map[key]

        # Update username if we have a better one
        if user and not entry["username"]:
            entry["username"] = user
        # Update user_id if we have one
        if uid and not entry["user_id"]:
            entry["user_id"] = uid

        # Track username → key for dedup
        if user:
            username_to_key.setdefault(user.lower(), key)

        # Determine status for this ticket
        t_status = r.get("approval_status", "Not Approved")
        rank = status_rank.get(t_status, 99)

        ticket_info = {
            "ticket": r["ticket"],
            "campaign": r.get("campaign", "Unknown"),
            "status": t_status,
            "date": r.get("ticket_date", ""),
            "amount": r.get("deposit_amount"),
            "signal": r.get("approval_signal", ""),
            "admin": r.get("approving_admin", ""),
            "drive_file_id": r.get("drive_file_id", ""),
        }
        entry["tickets"].append(ticket_info)

        # Track promo tickets
        if t_status == "Excluded (Promo)":
            entry["promo_count"] += 1

        # Track approved deposits
        if t_status == "Approved":
            entry["ftd_approved"] = True
            entry["deposit_count"] += 1
            amt = r.get("deposit_amount")
            if amt is not None:
                entry["total_deposits"] += amt

            # Track first (earliest) approved ticket for FTD info
            is_ftd = r["ticket"] in ftd_keys
            if is_ftd:
                entry["ftd_amount"] = amt
                entry["first_deposit_date"] = r.get("ticket_date", "")
                entry["brand"] = r.get("campaign", "Unknown")
                entry["approval_signal"] = r.get("approval_signal", "")
                entry["approving_admin"] = r.get("approving_admin", "")

        # Track best status
        if rank < entry["_best_rank"]:
            entry["_best_rank"] = rank
            entry["status"] = t_status

    # Second pass: compute derived fields + Discord roles
    user_list = []
    for key, entry in user_map.items():
        # Clean up
        del entry["_best_rank"]
        entry["total_deposits"] = round(entry["total_deposits"], 2) if entry["total_deposits"] else None

        # Status detail for excluded entries
        status = entry["status"]
        if "Internal" in status or "No FTD" in status:
            sig = entry.get("approval_signal", "")
            if "fake" in sig.lower() or "internal" in sig.lower():
                entry["status_detail"] = "Internal / Fake"
            elif "no_ftd" in sig.lower():
                entry["status_detail"] = "No FTD"
            else:
                entry["status_detail"] = status.replace("Excluded (", "").replace(")", "")

        # Brand fallback: use campaign from best ticket if not set from FTD
        if not entry["brand"] and entry["tickets"]:
            for t in entry["tickets"]:
                if t["campaign"] != "Unknown":
                    entry["brand"] = t["campaign"]
                    break
            if not entry["brand"]:
                entry["brand"] = "Unknown"

        # Discord roles + display name
        uid = entry["user_id"]
        if uid and uid in discord_roles:
            entry["discord_roles"] = discord_roles[uid]
        if uid and uid in discord_profiles:
            prof = discord_profiles[uid]
            dn = prof.get("display_name", "")
            if dn and dn.lower() != entry["username"].lower():
                entry["display_name"] = dn
            # Also pick up Discord username if we only had a ticket-derived one
            disc_uname = prof.get("username", "")
            if disc_uname and not entry["username"]:
                entry["username"] = disc_uname

        # Sort tickets by date
        entry["tickets"].sort(key=lambda t: t.get("date") or "")

        user_list.append(entry)

    # Sort: approved first, then by display_name/username (entries without name go last)
    def _sort_key(u):
        primary = u["display_name"] or u["username"]
        has_name = 0 if primary else 1
        is_approved = 0 if u["ftd_approved"] else 1
        return (has_name, is_approved, (primary or u["user_id"] or "").lower())
    user_list.sort(key=_sort_key)
    return user_list

def count_unique_registrations(subset: list[dict]) -> int:
    """Count distinct users with at least one Approved ticket in this subset."""
    seen: set[str] = set()
    for r in subset:
        if r["approval_status"] == "Approved":
            uid = r.get("user_id", "")
            # Fall back to username if no user_id
            key = uid if uid else r.get("user", r["ticket"])
            seen.add(key)
    return len(seen)

def sum_amounts(subset: list[dict]) -> float | None:
    """Sum deposit_amount for approved tickets with a known amount. Returns None if no data."""
    vals = [
        r["deposit_amount"] for r in subset
        if r.get("deposit_amount") is not None and r["approval_status"] == "Approved"
    ]
    return round(sum(vals), 2) if vals else None


def get_ftd_ticket_keys(results: list[dict]) -> set[str]:
    """Return the set of ticket names that represent each user's FIRST approved deposit.
    For users with multiple approved tickets, picks the earliest by ticket_date.
    """
    user_tickets: dict[str, list[dict]] = {}
    for r in results:
        if r["approval_status"] != "Approved":
            continue
        uid  = r.get("user_id", "").strip()
        user = r.get("user", "").strip()
        key  = uid if uid else (user if user else r["ticket"])
        user_tickets.setdefault(key, []).append(r)

    ftd_keys: set[str] = set()
    for tickets in user_tickets.values():
        earliest = min(tickets, key=lambda x: x.get("ticket_date") or "")
        ftd_keys.add(earliest["ticket"])
    return ftd_keys


def group_by_day(results: list[dict], days: int = 7) -> list[dict]:
    """Return one entry per calendar day (UTC) for the last N days, newest first.
    Each entry has:
      count          — all approved deposits
      ftd_count      — first-time deposits (unique users, earliest ticket)
      amount         — total volume (all approved, known amounts)
      ftd_amount     — FTD-only volume (first ticket per user, known amounts)
      amount_known   — tickets with a known amount (all)
      ftd_amount_known — FTD tickets with a known amount
      by_brand       — per-brand {count, ftd_count, amount, ftd_amount}
    """
    ftd_keys = get_ftd_ticket_keys(results)
    now = datetime.datetime.now(datetime.timezone.utc)
    day_data: dict[str, dict] = {}
    for i in range(days):
        d = (now - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        day_data[d] = {
            "date": d,
            "count": 0,
            "ftd_count": 0,
            "amount": 0.0,
            "ftd_amount": 0.0,
            "amount_known": 0,
            "ftd_amount_known": 0,
            "by_brand": {
                b: {"count": 0, "ftd_count": 0, "amount": 0.0, "ftd_amount": 0.0}
                for b in SUMMARY_CAMPAIGNS
            },
        }
    VOLUME_STATUSES = {"Approved", "Excluded (Promo)"}
    for r in results:
        status = r["approval_status"]
        is_approved = status == "Approved"
        counts_for_volume = status in VOLUME_STATUSES
        if not counts_for_volume:
            continue
        date_str = (r.get("ticket_date") or "")[:10]
        if date_str not in day_data:
            continue
        is_ftd = r["ticket"] in ftd_keys
        brand  = r.get("campaign", "Unknown")
        entry  = day_data[date_str]

        # FTD counts: Approved only
        if is_approved:
            entry["count"] += 1
            if is_ftd:
                entry["ftd_count"] += 1

        bb = entry["by_brand"].get(brand)
        if bb is not None and is_approved:
            bb["count"] += 1
            if is_ftd:
                bb["ftd_count"] += 1

        # Deposit amounts: Approved + Excluded (Promo) — real money deposited
        amt = r.get("deposit_amount")
        if amt is not None:
            entry["amount"] += amt
            entry["amount_known"] += 1
            if bb is not None:
                bb["amount"] += amt
            if is_ftd:
                entry["ftd_amount"] += amt
                entry["ftd_amount_known"] += 1
                if bb is not None:
                    bb["ftd_amount"] += amt

    for entry in day_data.values():
        entry["amount"]     = round(entry["amount"], 2)
        entry["ftd_amount"] = round(entry["ftd_amount"], 2)
        for bb in entry["by_brand"].values():
            bb["amount"]     = round(bb["amount"], 2)
            bb["ftd_amount"] = round(bb["ftd_amount"], 2)

    return sorted(day_data.values(), key=lambda x: x["date"], reverse=True)


def build_summary(results: list[dict]) -> list[list]:
    ftd_keys = get_ftd_ticket_keys(results)
    header = [
        "Campaign", "Total Tickets",
        "✅ Unique Registrations (FTDs)", "All Approved Deposits",
        "To be checked",
        "Excl. Oddify", "Excl. Promo",
        "No Screenshot",
        "FTD Volume (€)", "Total Volume (€)", "Avg Deposit (€)",
    ]
    rows = [header]
    totals = [0] * 8  # indices 0-7 for numeric cols before amounts

    for campaign in SUMMARY_CAMPAIGNS:
        subset    = [r for r in results if r["campaign"] == campaign]
        total     = len(subset)
        approved  = sum(1 for r in subset if r["approval_status"] == "Approved")
        unique    = count_unique_registrations(subset)
        to_check  = sum(1 for r in subset if r["approval_status"] == "To be checked")
        ex_odd    = sum(1 for r in subset if r["approval_status"] == "Excluded (Oddify)")
        ex_promo  = sum(1 for r in subset if r["approval_status"] == "Excluded (Promo)")
        no_ss     = sum(1 for r in subset if r["approval_status"] == "Not Approved")

        total_dep = sum_amounts(subset)
        ftd_subset = [r for r in subset if r["ticket"] in ftd_keys]
        ftd_dep   = sum_amounts(ftd_subset)

        amounts_known = sum(
            1 for r in subset
            if r["approval_status"] == "Approved" and r.get("deposit_amount") is not None
        )
        avg_dep       = round(total_dep / amounts_known, 2) if total_dep and amounts_known else ""
        total_dep_val = total_dep if total_dep is not None else ""
        ftd_dep_val   = ftd_dep   if ftd_dep   is not None else ""

        rows.append([campaign, total, unique, approved, to_check, ex_odd, ex_promo, no_ss,
                     ftd_dep_val, total_dep_val, avg_dep])
        for i, v in enumerate([total, unique, approved, to_check, ex_odd, ex_promo, no_ss]):
            totals[i] += v
        if ftd_dep   is not None:
            totals[7] = round((totals[7] or 0) + ftd_dep, 2)

    all_dep   = sum_amounts(results)
    all_ftd   = sum_amounts([r for r in results if r["ticket"] in ftd_keys])
    all_known = sum(
        1 for r in results
        if r["approval_status"] == "Approved" and r.get("deposit_amount") is not None
    )
    all_avg   = round(all_dep / all_known, 2) if all_dep and all_known else ""
    rows.append(["TOTAL"] + totals[:7] + [totals[7] or "", all_dep or "", all_avg])
    return rows

def build_details(results: list[dict]) -> list[list]:
    header = ["Ticket", "User", "User ID", "Campaign", "Screenshot?", "Approval Status", "Approval Signal", "Approving Admin", "Deposit Amount (€)"]
    rows = [header]
    for r in sorted(results, key=lambda x: x["ticket"]):
        amount = r.get("deposit_amount")
        rows.append([
            r["ticket"],
            r["user"],
            r.get("user_id", ""),
            r["campaign"],
            "Yes" if r["has_screenshot"] else "No",
            r["approval_status"],
            r["approval_signal"],
            r["approving_admin"],
            f"{amount:.2f}" if amount is not None else "",
        ])
    return rows


def build_daily_volumes(results: list[dict], days: int = 14) -> list[list]:
    """Build daily volumes sheet data for the last N days.
    FTDs = unique first-time depositors. All Deps = every approved ticket.
    FTD Volume = sum of deposit amounts for FTD tickets only.
    Total Volume = sum of ALL approved deposit amounts (includes repeat depositors).
    """
    header = [
        "Date",
        "New FTDs (unique)",
        "Betlabel", "Winnerz", "Winrolla", "Unknown",
        "FTD Volume (€)", "Promo Volume (€)", "Total Volume (€)",
        "Betlabel (€)", "Winnerz (€)", "Winrolla (€)", "Unknown (€)",
    ]
    rows = [header]

    def fa(a: float) -> str:
        return f"{a:.2f}" if a > 0 else ""

    empty = {"count": 0, "ftd_count": 0, "amount": 0.0, "ftd_amount": 0.0}
    for entry in group_by_day(results, days=days):
        bb = entry["by_brand"]
        promo_vol = round(entry["amount"] - entry["ftd_amount"], 2)
        rows.append([
            entry["date"],
            entry["ftd_count"],
            bb.get("Betlabel", empty)["count"],
            bb.get("Winnerz",  empty)["count"],
            bb.get("Winrolla", empty)["count"],
            bb.get("Unknown",  empty)["count"],
            fa(entry["ftd_amount"]),
            fa(promo_vol),
            fa(entry["amount"]),
            fa(bb.get("Betlabel", empty)["amount"]),
            fa(bb.get("Winnerz",  empty)["amount"]),
            fa(bb.get("Winrolla", empty)["amount"]),
            fa(bb.get("Unknown",  empty)["amount"]),
        ])
    return rows

def build_user_overview(results: list[dict]) -> list[list]:
    """One row per unique user (deduplicated by user_id or username)."""
    header = ["User", "User ID", "Campaign", "Screenshot?", "Approval Status", "Approving Admin"]
    seen: dict[str, dict] = {}  # key → best result for this user

    status_rank = {
        "Approved": 0, "To be checked": 1,
        "Excluded (Oddify)": 2, "Excluded (Promo)": 3, "Excluded (Internal)": 4, "Not Approved": 5,
    }

    for r in results:
        uid  = r.get("user_id", "").strip()
        user = r.get("user", "").strip()
        key  = uid if uid else (user if user else r["ticket"])
        rank = status_rank.get(r["approval_status"], 5)

        if key not in seen or rank < status_rank.get(seen[key]["approval_status"], 5):
            seen[key] = r

    rows = [header]
    for r in sorted(seen.values(), key=lambda x: x.get("user", x["ticket"]).lower()):
        rows.append([
            r.get("user", ""),
            r.get("user_id", ""),
            r.get("campaign", ""),
            "Yes" if r.get("has_screenshot") else "No",
            r.get("approval_status", ""),
            r.get("approving_admin", ""),
        ])
    return rows


def write_to_sheets(sheets_service, results: list[dict]) -> str:
    """Create a new Google Sheet and write Details + Summary tabs. Returns URL."""
    log("Creating Google Sheet...")

    spreadsheet = sheets_service.spreadsheets().create(body={
        "properties": {"title": f"Wett Elite Deposit Overview — {time.strftime('%Y-%m-%d %H:%M')}"},
        "sheets": [
            {"properties": {"title": "Summary",       "index": 0}},
            {"properties": {"title": "User Overview",  "index": 1}},
            {"properties": {"title": "Daily Volumes",  "index": 2}},
            {"properties": {"title": "Details",        "index": 3}},
        ],
    }).execute()

    sheet_id  = spreadsheet["spreadsheetId"]
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    summary_data  = build_summary(results)
    details_data  = build_details(results)
    overview_data = build_user_overview(results)
    daily_data    = build_daily_volumes(results)

    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": "Summary!A1",       "values": summary_data},
                {"range": "User Overview!A1", "values": overview_data},
                {"range": "Daily Volumes!A1", "values": daily_data},
                {"range": "Details!A1",       "values": details_data},
            ],
        },
    ).execute()

    # Basic formatting: bold headers, freeze row 1
    fmt_requests = []
    for tab_index, tab_title in enumerate(["Summary", "User Overview", "Daily Volumes", "Details"]):
        tab_info = next(
            s for s in spreadsheet["sheets"]
            if s["properties"]["title"] == tab_title
        )
        tab_id = tab_info["properties"]["sheetId"]
        if tab_title == "Summary":
            col_count = len(summary_data[0])
        elif tab_title == "User Overview":
            col_count = len(overview_data[0])
        elif tab_title == "Daily Volumes":
            col_count = len(daily_data[0])
        else:
            col_count = len(details_data[0])

        fmt_requests += [
            # Bold header row
            {
                "repeatCell": {
                    "range": {"sheetId": tab_id, "startRowIndex": 0, "endRowIndex": 1,
                               "startColumnIndex": 0, "endColumnIndex": col_count},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            },
            # Freeze row 1
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": tab_id, "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            # Auto-resize columns
            {
                "autoResizeDimensions": {
                    "dimensions": {"sheetId": tab_id, "dimension": "COLUMNS",
                                   "startIndex": 0, "endIndex": col_count}
                }
            },
        ]

    summary_sheet = next(s for s in spreadsheet["sheets"] if s["properties"]["title"] == "Summary")
    summary_id    = summary_sheet["properties"]["sheetId"]
    n_summary_cols = len(summary_data[0])

    # Green header row
    fmt_requests.append({
        "repeatCell": {
            "range": {"sheetId": summary_id, "startRowIndex": 0, "endRowIndex": 1,
                       "startColumnIndex": 0, "endColumnIndex": n_summary_cols},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.2, "green": 0.65, "blue": 0.32},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }
    })

    # Highlight "Unique Registrations" column (col index 2) in light blue
    fmt_requests.append({
        "repeatCell": {
            "range": {"sheetId": summary_id, "startRowIndex": 1,
                       "endRowIndex": len(summary_data),
                       "startColumnIndex": 2, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.72, "green": 0.88, "blue": 1.0},
                "textFormat": {"bold": True},
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }
    })

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": fmt_requests},
    ).execute()

    return sheet_url

# ---------------------------------------------------------------------------
# Telegram notification
# ---------------------------------------------------------------------------
TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8637002260:AAHmr8VNjus3TTVY_TcKueSNJHSEIWFQ_ug")
TG_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "-1003760626133")

def send_telegram(text: str) -> None:
    try:
        http_requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log(f"⚠️  Telegram error: {e}")

# ---------------------------------------------------------------------------
# HTML Dashboard
# ---------------------------------------------------------------------------
def generate_html_dashboard(results: list[dict], output_path: Path, user_lookup: list[dict] | None = None) -> None:
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cutoff_24h = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)).isoformat()

    # Colours — defined early so 24h pills can reference them
    STATUS_COLOURS = {
        "Approved":          ("#d1fae5", "#065f46"),
        "To be checked":     ("#fef3c7", "#92400e"),
        "Not Approved":      ("#fee2e2", "#991b1b"),
        "Excluded (Oddify)":    ("#e0e7ff", "#3730a3"),
        "Excluded (Promo)":     ("#f3e8ff", "#6b21a8"),
        "Excluded (Internal)":  ("#f1f5f9", "#475569"),
    }
    CAMP_COLOURS = {"Betlabel": "#3b82f6", "Winnerz": "#10b981", "Winrolla": "#f59e0b", "Unknown": "#6b7280"}

    # Build summary per campaign
    campaigns_data = []
    for camp in SUMMARY_CAMPAIGNS:
        subset   = [r for r in results if r["campaign"] == camp]
        unique   = count_unique_registrations(subset)
        approved = sum(1 for r in subset if r["approval_status"] == "Approved")
        to_check = sum(1 for r in subset if r["approval_status"] == "To be checked")
        campaigns_data.append({"name": camp, "unique": unique, "approved": approved, "to_check": to_check, "total": len(subset)})

    total_unique   = count_unique_registrations(results)
    total_approved = sum(1 for r in results if r["approval_status"] == "Approved")
    total_check    = sum(1 for r in results if r["approval_status"] == "To be checked")

    # Last-24h stats — use first_seen_at (when we discovered it) with ticket_date as fallback
    def is_new(r):
        d = r.get("first_seen_at") or r.get("ticket_date", "")
        return bool(d and d >= cutoff_24h)

    new_approved    = [r for r in results if r["approval_status"] == "Approved" and is_new(r)]
    new_to_check    = [r for r in results if r["approval_status"] == "To be checked" and is_new(r)]
    new_total       = len(new_approved)
    new_per_brand   = {}
    for r in new_approved:
        c = r.get("campaign", "Unknown")
        new_per_brand[c] = new_per_brand.get(c, 0) + 1

    # FTD keys for the full result set
    ftd_keys_html = get_ftd_ticket_keys(results)

    # 24h deposit volume — split FTD vs Total
    new_ftd_approved = [r for r in new_approved if r["ticket"] in ftd_keys_html]
    vol_24h_total    = sum_amounts(new_approved)
    vol_24h_ftd      = sum_amounts(new_ftd_approved)
    vol_24h_known    = sum(1 for r in new_approved if r.get("deposit_amount") is not None)
    vol_24h_ftd_str  = f"€{vol_24h_ftd:.0f}"   if vol_24h_ftd   is not None else "—"
    vol_24h_total_str= f"€{vol_24h_total:.0f}"  if vol_24h_total is not None else "—"

    # 7-day deposit volumes — use group_by_day which now carries ftd_amount
    daily_rows = group_by_day(results, days=7)
    # Aggregate 7-day totals per brand from daily_rows entries
    empty_bb = {"count": 0, "ftd_count": 0, "amount": 0.0, "ftd_amount": 0.0}
    vol_brands: dict[str, dict] = {b: {"total": 0.0, "ftd": 0.0, "ftd_count": 0, "approved": 0, "known": 0} for b in SUMMARY_CAMPAIGNS}
    overall_7d_ftd   = 0.0
    overall_7d_total = 0.0
    overall_7d_ftd_count   = 0
    overall_7d_approved    = 0
    overall_7d_known       = 0
    for entry in daily_rows:
        overall_7d_ftd       += entry["ftd_amount"]
        overall_7d_total     += entry["amount"]
        overall_7d_ftd_count += entry["ftd_count"]
        overall_7d_approved  += entry["count"]
        overall_7d_known     += entry["amount_known"]
        for brand in SUMMARY_CAMPAIGNS:
            bb = entry["by_brand"].get(brand, empty_bb)
            vol_brands[brand]["total"]     += bb["amount"]
            vol_brands[brand]["ftd"]       += bb["ftd_amount"]
            vol_brands[brand]["ftd_count"] += bb["ftd_count"]
            vol_brands[brand]["approved"]  += bb["count"]
            vol_brands[brand]["known"]     += (1 if bb["amount"] > 0 else 0)
    overall_7d_ftd   = round(overall_7d_ftd,   2)
    overall_7d_total = round(overall_7d_total,  2)

    # Pre-build 24h brand pills (avoids escaped-quote issues inside f-string)
    h24_brands_html = "".join(
        f'<div class="h24-brand"><span style="color:{CAMP_COLOURS.get(b, "#6b7280")}">{b}</span>'
        f' <strong>{n}</strong></div>'
        for b, n in sorted(new_per_brand.items())
    )
    h24_volume_html = (
        f'<div class="h24-vol">'
        f'💰 FTD: <strong>{vol_24h_ftd_str}</strong>'
        f'<span class="h24-vol-sep"> · </span>'
        f'Total: <strong style="color:#94a3b8">{vol_24h_total_str}</strong>'
        f'<span class="h24-vol-sub"> ({vol_24h_known}/{new_total} with amount)</span>'
        f'</div>'
        if new_total > 0 else ""
    )

    # Volume brand cards for 7-day section
    def _fmt_amt(v: float) -> str:
        return f"€{v:.0f}" if v > 0 else "—"

    vol_brand_cards_html = ""
    for brand in SUMMARY_CAMPAIGNS:
        colour = CAMP_COLOURS.get(brand, "#6b7280")
        vb = vol_brands[brand]
        vol_brand_cards_html += (
            f'<div class="vol-card" style="border-top:3px solid {colour}">'
            f'<div class="vol-card-label" style="color:{colour}">{brand}</div>'
            f'<div class="vol-card-amount">{_fmt_amt(vb["ftd"])}</div>'
            f'<div class="vol-card-detail">Total: {_fmt_amt(vb["total"])}</div>'
            f'<div class="vol-card-sub">{vb["ftd_count"]} FTDs · {vb["approved"]} all deposits</div>'
            f'</div>'
        )

    # Daily table rows
    daily_table_rows_html = ""
    for entry in daily_rows:
        bb  = entry["by_brand"]
        eb  = empty_bb
        bet = _fmt_amt(bb.get("Betlabel", eb)["amount"])
        win = _fmt_amt(bb.get("Winnerz",  eb)["amount"])
        rol = _fmt_amt(bb.get("Winrolla", eb)["amount"])
        unk = _fmt_amt(bb.get("Unknown",  eb)["amount"])
        ftd_vol   = _fmt_amt(entry["ftd_amount"])
        promo_vol = _fmt_amt(round(entry["amount"] - entry["ftd_amount"], 2))
        all_vol   = _fmt_amt(entry["amount"])
        daily_table_rows_html += (
            f'<tr>'
            f'<td class="mono">{entry["date"]}</td>'
            f'<td style="text-align:center">{entry["ftd_count"]}</td>'
            f'<td style="text-align:right;color:#34d399;font-weight:700">{ftd_vol}</td>'
            f'<td style="text-align:right;color:#a78bfa">{promo_vol}</td>'
            f'<td style="text-align:right;color:#e2e8f0;font-weight:800">{all_vol}</td>'
            f'<td style="text-align:right">{bet}</td>'
            f'<td style="text-align:right">{win}</td>'
            f'<td style="text-align:right">{rol}</td>'
            f'<td style="text-align:right;color:#6b7280">{unk}</td>'
            f'</tr>'
        )
    overall_7d_ftd_str   = _fmt_amt(overall_7d_ftd)
    overall_7d_total_str = _fmt_amt(overall_7d_total)
    h24_review_html = (
        f'<div class="h24-warn">⚠️ {len(new_to_check)} new ticket(s) need review</div>'
        if new_to_check else
        '<div class="h24-ok">✅ No new tickets pending review</div>'
    )

    # (STATUS_COLOURS and CAMP_COLOURS defined above)

    # Rows for the detail table (sorted: approved first, then to_check, then rest)
    def sort_key(r):
        order = {"Approved": 0, "To be checked": 1, "Not Approved": 2}
        return (order.get(r["approval_status"], 3), r.get("campaign",""), r.get("user",""))
    sorted_results = sorted(results, key=sort_key)

    rows_html = ""
    for r in sorted_results:
        status = r["approval_status"]
        bg, fg = STATUS_COLOURS.get(status, ("#f9fafb", "#111"))
        camp   = r.get("campaign", "Unknown")
        cc     = CAMP_COLOURS.get(camp, "#6b7280")
        ss     = "✅" if r.get("has_screenshot") else "—"
        admin  = r.get("approving_admin", "") or "—"
        amt    = r.get("deposit_amount")
        amt_html = (
            f'<span style="color:#34d399;font-weight:700">€{amt:.2f}</span>'
            if amt is not None else
            '<span style="color:#334155">—</span>'
        )
        rows_html += f"""
        <tr>
          <td class="mono">{r['ticket']}</td>
          <td>{r.get('user','')}</td>
          <td><span class="badge" style="background:{cc}20;color:{cc};border:1px solid {cc}40">{camp}</span></td>
          <td style="text-align:center">{ss}</td>
          <td><span class="badge" style="background:{bg};color:{fg}">{status}</span></td>
          <td>{admin}</td>
          <td style="text-align:right">{amt_html}</td>
        </tr>"""

    # Summary cards HTML
    cards_html = ""
    for c in campaigns_data:
        colour = CAMP_COLOURS.get(c["name"], "#6b7280")
        vb = vol_brands.get(c["name"], {})
        all_time_dep = sum_amounts([r for r in results if r["campaign"] == c["name"]])
        dep_str = f"€{all_time_dep:.0f}" if all_time_dep is not None else ""
        cards_html += f"""
        <div class="card" style="border-top:4px solid {colour}">
          <div class="card-label">{c['name']}</div>
          <div class="card-number" style="color:{colour}">{c['unique']}</div>
          <div class="card-sub">unique registrations</div>
          <div class="card-detail">{c['approved']} approved tickets</div>
          {f'<div class="card-detail" style="color:#34d399">{dep_str} total deposits</div>' if dep_str else ""}
          {f'<div class="card-warn">⚠️ {c["to_check"]} to review</div>' if c["to_check"] else ""}
        </div>"""

    PASSWORD_HASH = "0caa2ea1f7b59cd2995a1a43e8ddeb224c2fd317bf238692a35fddbae7a5ac58"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Wett Elite — Deposit Dashboard</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }}
  .header {{ background: #1e293b; border-bottom: 1px solid #334155; padding: 20px 32px; display: flex; align-items: center; justify-content: space-between; }}
  .header h1 {{ font-size: 1.4rem; font-weight: 700; color: #f1f5f9; }}
  .header .updated {{ font-size: 0.8rem; color: #64748b; }}
  .main {{ padding: 32px; max-width: 1600px; margin: 0 auto; }}
  .total-banner {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 24px 32px; margin-bottom: 28px; display: flex; align-items: center; gap: 40px; }}
  .total-banner .big {{ font-size: 3rem; font-weight: 800; color: #6366f1; }}
  .total-banner .label {{ font-size: 0.9rem; color: #94a3b8; margin-top: 4px; }}
  .total-banner .sub {{ font-size: 0.85rem; color: #64748b; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 32px; }}
  .card {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 20px; }}
  .card-label {{ font-size: 0.8rem; text-transform: uppercase; letter-spacing: .05em; color: #64748b; margin-bottom: 8px; }}
  .card-number {{ font-size: 2.4rem; font-weight: 800; line-height: 1; }}
  .card-sub {{ font-size: 0.75rem; color: #64748b; margin-top: 4px; }}
  .card-detail {{ font-size: 0.8rem; color: #94a3b8; margin-top: 8px; }}
  .card-warn {{ font-size: 0.8rem; color: #f59e0b; margin-top: 4px; }}
  .table-wrap {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; overflow: hidden; }}
  .table-header {{ padding: 16px 20px; border-bottom: 1px solid #334155; display: flex; align-items: center; justify-content: space-between; gap: 12px; }}
  .table-header h2 {{ font-size: 1rem; font-weight: 600; color: #f1f5f9; }}
  input[type=search] {{ background: #0f172a; border: 1px solid #334155; border-radius: 8px; color: #e2e8f0; padding: 8px 14px; font-size: 0.85rem; width: 260px; outline: none; }}
  input[type=search]:focus {{ border-color: #6366f1; }}
  select {{ background: #0f172a; border: 1px solid #334155; border-radius: 8px; color: #e2e8f0; padding: 8px 12px; font-size: 0.85rem; outline: none; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.83rem; }}
  th {{ background: #0f172a; color: #64748b; text-transform: uppercase; letter-spacing: .04em; font-size: 0.72rem; padding: 10px 14px; text-align: left; border-bottom: 1px solid #334155; position: sticky; top: 0; }}
  td {{ padding: 9px 14px; border-bottom: 1px solid #1e293b; vertical-align: middle; }}
  tr:hover td {{ background: #263548; }}
  .mono {{ font-family: monospace; font-size: 0.78rem; color: #94a3b8; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }}
  .tbl-container {{ max-height: 600px; overflow-y: auto; }}
  .stat-pill {{ display: inline-flex; align-items: center; gap: 6px; background: #0f172a; border: 1px solid #334155; border-radius: 20px; padding: 4px 14px; font-size: 0.8rem; color: #94a3b8; }}
  .stat-pill strong {{ color: #f1f5f9; }}
  .section-title {{ font-size: 0.75rem; text-transform: uppercase; letter-spacing: .08em; color: #64748b; font-weight: 600; margin: 28px 0 12px; }}
  .h24-banner {{ background: #1e293b; border: 1px solid #334155; border-left: 4px solid #6366f1; border-radius: 12px; padding: 20px 28px; margin-bottom: 12px; display: flex; align-items: center; gap: 40px; flex-wrap: wrap; }}
  .h24-num {{ font-size: 2.8rem; font-weight: 800; color: #6366f1; line-height: 1; }}
  .h24-label {{ font-size: 0.82rem; color: #94a3b8; margin-top: 4px; }}
  .h24-brands {{ display: flex; gap: 20px; flex-wrap: wrap; }}
  .h24-brand {{ font-size: 0.9rem; color: #94a3b8; }}
  .h24-brand strong {{ color: #f1f5f9; }}
  .h24-warn {{ font-size: 0.85rem; color: #f59e0b; margin-left: auto; }}
  .h24-ok {{ font-size: 0.85rem; color: #34d399; margin-left: auto; }}
  .h24-vol {{ font-size: 0.88rem; color: #94a3b8; }}
  .h24-vol strong {{ color: #34d399; font-size: 1.05rem; }}
  .h24-vol-sep {{ color: #334155; }}
  .h24-vol-sub {{ font-size: 0.78rem; color: #64748b; }}
  .vol-card-detail {{ font-size: 0.78rem; color: #64748b; margin-top: 2px; }}
  .vol-section {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 20px 24px; margin-bottom: 28px; }}
  .vol-brand-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }}
  .vol-card {{ background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 14px 16px; }}
  .vol-card-label {{ font-size: 0.75rem; text-transform: uppercase; letter-spacing: .05em; font-weight: 600; margin-bottom: 6px; }}
  .vol-card-amount {{ font-size: 1.6rem; font-weight: 800; color: #34d399; line-height: 1; }}
  .vol-card-sub {{ font-size: 0.72rem; color: #64748b; margin-top: 4px; }}
  .vol-table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
  .vol-table th {{ background: #0f172a; color: #64748b; text-transform: uppercase; letter-spacing: .04em; font-size: 0.7rem; padding: 8px 12px; text-align: left; border-bottom: 1px solid #334155; }}
  .vol-table td {{ padding: 7px 12px; border-bottom: 1px solid #1e293b; }}
  .vol-table tr:hover td {{ background: #263548; }}
  .vol-coverage {{ font-size: 0.75rem; color: #64748b; margin-top: 10px; }}
  #gate {{ display:flex;align-items:center;justify-content:center;min-height:100vh;background:#0f172a; }}
  .gate-box {{ background:#1e293b;border:1px solid #334155;border-radius:16px;padding:40px;text-align:center;width:320px; }}
  .gate-box h2 {{ color:#f1f5f9;margin-bottom:8px;font-size:1.3rem; }}
  .gate-box p {{ color:#64748b;font-size:0.85rem;margin-bottom:24px; }}
  .gate-box input {{ width:100%;background:#0f172a;border:1px solid #334155;border-radius:8px;color:#e2e8f0;padding:10px 14px;font-size:1rem;outline:none;margin-bottom:12px; }}
  .gate-box input:focus {{ border-color:#6366f1; }}
  .gate-box button {{ width:100%;background:#6366f1;color:#fff;border:none;border-radius:8px;padding:10px;font-size:1rem;font-weight:600;cursor:pointer; }}
  .gate-box button:hover {{ background:#4f46e5; }}
  .gate-error {{ color:#f87171;font-size:0.82rem;margin-top:8px;display:none; }}
  /* Tab navigation */
  .tab-bar {{ display:flex;gap:0;background:#1e293b;border-bottom:1px solid #334155;padding:0 32px; }}
  .tab-btn {{ padding:12px 24px;font-size:0.9rem;font-weight:600;color:#64748b;cursor:pointer;border:none;background:none;border-bottom:3px solid transparent;transition:all 0.2s; }}
  .tab-btn:hover {{ color:#94a3b8; }}
  .tab-btn.active {{ color:#6366f1;border-bottom-color:#6366f1; }}
  .tab-content {{ display:none; }}
  .tab-content.active {{ display:block; }}
  /* User Lookup */
  .ul-search-bar {{ display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:20px; }}
  .ul-search-bar input {{ flex:1;min-width:200px;background:#0f172a;border:1px solid #334155;border-radius:8px;color:#e2e8f0;padding:10px 16px;font-size:0.9rem;outline:none; }}
  .ul-search-bar input:focus {{ border-color:#6366f1; }}
  .ul-search-bar select {{ background:#0f172a;border:1px solid #334155;border-radius:8px;color:#e2e8f0;padding:10px 14px;font-size:0.85rem;outline:none; }}
  .ul-count {{ font-size:0.82rem;color:#64748b; }}
  .ul-table {{ width:100%;border-collapse:collapse;font-size:0.85rem; }}
  .ul-table th {{ background:#0f172a;color:#64748b;text-transform:uppercase;letter-spacing:.04em;font-size:0.72rem;padding:10px 14px;text-align:left;border-bottom:1px solid #334155;position:sticky;top:0;cursor:pointer; }}
  .ul-table th:hover {{ color:#94a3b8; }}
  .ul-table td {{ padding:9px 14px;border-bottom:1px solid #1e293b;vertical-align:middle; }}
  .ul-table tr:hover td {{ background:#263548; }}
  .ul-table tr {{ cursor:pointer; }}
  .ul-detail {{ background:#1e293b;border:1px solid #334155;border-radius:12px;padding:24px 28px;margin-bottom:20px;display:none; }}
  .ul-detail.open {{ display:block; }}
  .ul-detail-header {{ display:flex;align-items:center;justify-content:space-between;margin-bottom:16px; }}
  .ul-detail-header h3 {{ font-size:1.2rem;font-weight:700;color:#f1f5f9; }}
  .ul-detail-close {{ cursor:pointer;color:#64748b;font-size:1.2rem;padding:4px 8px;border-radius:4px; }}
  .ul-detail-close:hover {{ color:#f1f5f9;background:#334155; }}
  .ul-detail-grid {{ display:grid;grid-template-columns:repeat(auto-fit, minmax(200px, 1fr));gap:16px;margin-bottom:20px; }}
  .ul-detail-field {{ }}
  .ul-detail-field .lbl {{ font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;color:#64748b;margin-bottom:4px; }}
  .ul-detail-field .val {{ font-size:0.95rem;color:#e2e8f0;font-weight:600; }}
  .ul-detail-field .val.green {{ color:#34d399; }}
  .ul-detail-field .val.red {{ color:#f87171; }}
  .ul-detail-field .val.yellow {{ color:#fbbf24; }}
  .ul-tickets-table {{ width:100%;border-collapse:collapse;font-size:0.82rem;margin-top:12px; }}
  .ul-tickets-table th {{ background:#0f172a;color:#64748b;text-transform:uppercase;letter-spacing:.04em;font-size:0.7rem;padding:8px 12px;text-align:left;border-bottom:1px solid #334155; }}
  .ul-tickets-table td {{ padding:7px 12px;border-bottom:1px solid #1e293b; }}
  .ul-tickets-table tr:hover td {{ background:#263548; }}
  .ul-tickets-table a {{ color:#818cf8;text-decoration:none; }}
  .ul-tickets-table a:hover {{ text-decoration:underline; }}
  .role-badge {{ display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600;margin-right:4px;background:#334155;color:#94a3b8; }}
  .role-badge.ke {{ background:#6366f120;color:#818cf8;border:1px solid #6366f140; }}
</style>
</head>
<body>
<div id="gate">
  <div class="gate-box">
    <h2>🔒 Wett Elite Dashboard</h2>
    <p>Enter the password to continue</p>
    <input type="password" id="pw" placeholder="Password" onkeydown="if(event.key==='Enter')unlock()">
    <button onclick="unlock()">Enter</button>
    <div class="gate-error" id="gate-err">Wrong password — try again.</div>
  </div>
</div>
<div id="dashboard" style="display:none">
<div class="header">
  <h1>🎰 Wett Elite — Deposit Dashboard</h1>
  <div class="updated">Last updated: {now}</div>
</div>
<div class="tab-bar">
  <button class="tab-btn active" onclick="switchTab('overview', this)">Overview</button>
  <button class="tab-btn" onclick="switchTab('userlookup', this)">User Lookup</button>
</div>
<div class="main">
<div id="tab-overview" class="tab-content active">

  <div class="total-banner">
    <div>
      <div class="big">{total_unique}</div>
      <div class="label">Total Unique Registrations</div>
    </div>
    <div class="sub">
      {total_approved} approved tickets &nbsp;·&nbsp;
      {total_check} need review &nbsp;·&nbsp;
      {len(results)} total tickets processed
    </div>
  </div>

  <div class="section-title">Last 24 Hours</div>
  <div class="h24-banner">
    <div class="h24-main">
      <div class="h24-num">{new_total}</div>
      <div class="h24-label">New First Deposits</div>
    </div>
    <div class="h24-brands">
      {h24_brands_html}
    </div>
    {h24_volume_html}
    {h24_review_html}
  </div>

  <div class="section-title">Deposit Volumes — Last 7 Days</div>
  <div class="vol-section">
    <div class="vol-brand-row">
      {vol_brand_cards_html}
    </div>
    <div class="table-wrap" style="border:none;border-radius:0;overflow:visible">
      <table class="vol-table">
        <thead>
          <tr>
            <th rowspan="2">Date</th>
            <th style="text-align:center">New FTDs</th>
            <th style="text-align:right;color:#34d399">FTD Volume (€)</th>
            <th style="text-align:right;color:#94a3b8">+ Promo Vol (€)</th>
            <th style="text-align:right;color:#e2e8f0;font-weight:800">= Total Vol (€)</th>
            <th style="text-align:right">Betlabel</th>
            <th style="text-align:right">Winnerz</th>
            <th style="text-align:right">Winrolla</th>
            <th style="text-align:right;color:#6b7280">Unknown</th>
          </tr>
          <tr style="font-size:0.65rem;color:#64748b">
            <th style="text-align:center">unique first depositors</th>
            <th style="text-align:right">from new users only</th>
            <th style="text-align:right">promo participants</th>
            <th style="text-align:right">FTD + Promo combined</th>
            <th colspan="4" style="text-align:center">total volume per brand (FTD + Promo)</th>
          </tr>
        </thead>
        <tbody>{daily_table_rows_html}</tbody>
      </table>
    </div>
    <div class="vol-coverage">
      7-day FTD volume: <strong style="color:#34d399">{overall_7d_ftd_str}</strong>
      &nbsp;·&nbsp; Total volume incl. promo: <strong>{overall_7d_total_str}</strong>
      &nbsp;·&nbsp; <span style="color:#94a3b8">{overall_7d_approved} FTDs · {overall_7d_known} deposits have a confirmed amount</span>
    </div>
  </div>

  <div class="section-title">All-Time Totals</div>
  <div class="cards">{cards_html}</div>

  <div class="table-wrap">
    <div class="table-header">
      <h2>All Tickets</h2>
      <div style="display:flex;gap:8px;align-items:center">
        <select id="filterCamp" onchange="filterTable()">
          <option value="">All Campaigns</option>
          <option>Betlabel</option><option>Winnerz</option>
          <option>Winrolla</option><option>Unknown</option>
        </select>
        <select id="filterStatus" onchange="filterTable()">
          <option value="">All Statuses</option>
          <option>Approved</option><option>To be checked</option>
          <option>Not Approved</option><option>Excluded (Oddify)</option>
          <option>Excluded (Promo)</option><option>Excluded (Internal)</option>
        </select>
        <input type="search" id="searchBox" placeholder="Search user / ticket…" oninput="filterTable()">
      </div>
    </div>
    <div class="tbl-container">
    <table id="mainTable">
      <thead><tr>
        <th>Ticket</th><th>User</th><th>Campaign</th>
        <th style="text-align:center">Screenshot</th>
        <th>Status</th><th>Approving Admin</th>
        <th style="text-align:right">Amount (€)</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    </div>
  </div>
</div><!-- end tab-overview -->

<div id="tab-userlookup" class="tab-content">
  <div class="ul-search-bar">
    <input type="search" id="ulSearch" placeholder="Search by username, user ID, or ticket..." oninput="filterUsers()">
    <select id="ulBrand" onchange="filterUsers()">
      <option value="">All Brands</option>
      <option>Betlabel</option><option>Winnerz</option><option>Winrolla</option><option>Unknown</option>
    </select>
    <select id="ulStatus" onchange="filterUsers()">
      <option value="">All Statuses</option>
      <option value="Approved">Approved</option>
      <option value="To be checked">To be checked</option>
      <option value="Not Approved">Not Approved</option>
      <option value="Excluded">Excluded</option>
    </select>
    <span class="ul-count" id="ulCount"></span>
  </div>
  <div id="ulDetail" class="ul-detail"></div>
  <div class="table-wrap">
    <div class="tbl-container" style="max-height:700px">
      <table class="ul-table" id="ulTable">
        <thead><tr>
          <th onclick="sortUsers('username')">Username</th>
          <th onclick="sortUsers('brand')">Brand</th>
          <th onclick="sortUsers('status')">Status</th>
          <th onclick="sortUsers('discord_roles')" style="min-width:120px">Discord Roles</th>
          <th onclick="sortUsers('ftd_amount')" style="text-align:right">FTD Amount</th>
          <th onclick="sortUsers('total_deposits')" style="text-align:right">Total Deposits</th>
          <th onclick="sortUsers('deposit_count')" style="text-align:center">Deposits</th>
          <th onclick="sortUsers('promo_count')" style="text-align:center">Promos</th>
        </tr></thead>
        <tbody id="ulBody"></tbody>
      </table>
    </div>
  </div>
</div><!-- end tab-userlookup -->

</div>
</div>
<script>
const HASH = "{PASSWORD_HASH}";
async function sha256(msg) {{
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(msg));
  return Array.from(new Uint8Array(buf)).map(b=>b.toString(16).padStart(2,'0')).join('');
}}
async function unlock() {{
  const pw = document.getElementById('pw').value;
  const h  = await sha256(pw);
  if (h === HASH) {{
    sessionStorage.setItem('we_auth','1');
    document.getElementById('gate').style.display='none';
    document.getElementById('dashboard').style.display='block';
  }} else {{
    document.getElementById('gate-err').style.display='block';
  }}
}}
// Auto-unlock if already authenticated this session
if (sessionStorage.getItem('we_auth')==='1') {{
  document.getElementById('gate').style.display='none';
  document.getElementById('dashboard').style.display='block';
}}
function filterTable() {{
  const camp   = document.getElementById('filterCamp').value.toLowerCase();
  const status = document.getElementById('filterStatus').value.toLowerCase();
  const search = document.getElementById('searchBox').value.toLowerCase();
  document.querySelectorAll('#mainTable tbody tr').forEach(row => {{
    const text = row.textContent.toLowerCase();
    const campMatch   = !camp   || text.includes(camp);
    const statusMatch = !status || text.includes(status);
    const searchMatch = !search || text.includes(search);
    row.style.display = (campMatch && statusMatch && searchMatch) ? '' : 'none';
  }});
}}

// === Tab switching ===
function switchTab(tab, btn) {{
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  if (btn) btn.classList.add('active');
  if (tab === 'userlookup' && !usersRendered) {{
    usersRendered = true;
    document.getElementById('ulSearch').focus();
    applyFilters();
  }}
}}

// === User Lookup ===
const userData = __USER_LOOKUP_DATA__;
let usersRendered = false;
let filteredUsers = [];
let currentSort = {{ col: 'username', asc: true }};
const PAGE_SIZE = 100;
let currentPage = 0;
const campColours = {{"Betlabel":"#3b82f6","Winnerz":"#10b981","Winrolla":"#f59e0b","Unknown":"#6b7280"}};
const statusStyles = {{
  "Approved": ["#d1fae5","#065f46"],
  "To be checked": ["#fef3c7","#92400e"],
  "Not Approved": ["#fee2e2","#991b1b"],
}};

function getStatusStyle(s) {{
  if (s.startsWith("Excluded")) return ["#f1f5f9","#475569"];
  return statusStyles[s] || ["#f9fafb","#111"];
}}

function buildUserRow(u, dataIdx) {{
  const [bg, fg] = getStatusStyle(u.status);
  const cc = campColours[u.brand] || '#6b7280';
  const roles = (u.discord_roles || []).map(r =>
    '<span class="role-badge' + (r === 'Koenisch Elite' ? ' ke' : '') + '">' + r + '</span>'
  ).join('');
  const amt = u.ftd_amount != null ? '<span style="color:#34d399;font-weight:700">\u20ac' + u.ftd_amount.toFixed(2) + '</span>' : '<span style="color:#334155">\u2014</span>';
  const total = u.total_deposits != null && u.total_deposits > 0 ? '<span style="color:#34d399">\u20ac' + u.total_deposits.toFixed(2) + '</span>' : '<span style="color:#334155">\u2014</span>';
  const statusLabel = u.status_detail ? u.status + ' (' + u.status_detail + ')' : u.status;
  return '<tr data-idx="' + dataIdx + '" onclick="showUserDetail(' + dataIdx + ')">' +
    '<td style="font-weight:600">' + (u.display_name || u.username || u.user_id || '\u2014') + (u.display_name && u.username && u.display_name !== u.username ? '<br><span style="font-size:0.75em;color:#64748b;font-weight:400">@' + u.username + '</span>' : '') + '</td>' +
    '<td><span class="badge" style="background:' + cc + '20;color:' + cc + ';border:1px solid ' + cc + '40">' + u.brand + '</span></td>' +
    '<td><span class="badge" style="background:' + bg + ';color:' + fg + '">' + statusLabel + '</span></td>' +
    '<td>' + (roles || '<span style="color:#334155">\u2014</span>') + '</td>' +
    '<td style="text-align:right">' + amt + '</td>' +
    '<td style="text-align:right">' + total + '</td>' +
    '<td style="text-align:center">' + u.deposit_count + '</td>' +
    '<td style="text-align:center">' + (u.promo_count || '\u2014') + '</td>' +
    '</tr>';
}}

function renderUserTable(users) {{
  const body = document.getElementById('ulBody');
  const end = Math.min((currentPage + 1) * PAGE_SIZE, users.length);
  const rows = [];
  for (let i = 0; i < end; i++) {{
    rows.push(buildUserRow(users[i], i));
  }}
  body.innerHTML = rows.join('');
  const countEl = document.getElementById('ulCount');
  if (users.length > end) {{
    countEl.innerHTML = 'Showing ' + end + ' of ' + users.length + ' users &nbsp;<button onclick="loadMore()" style="background:#6366f1;color:#fff;border:none;border-radius:6px;padding:4px 12px;font-size:0.8rem;cursor:pointer">Load more</button>';
  }} else {{
    countEl.textContent = users.length + ' users';
  }}
}}

function loadMore() {{
  currentPage++;
  renderUserTable(filteredUsers);
}}

function applyFilters() {{
  const search = document.getElementById('ulSearch').value.toLowerCase();
  const brand = document.getElementById('ulBrand').value;
  const status = document.getElementById('ulStatus').value;
  filteredUsers = userData.filter(u => {{
    const matchSearch = !search ||
      (u.display_name || '').toLowerCase().includes(search) ||
      (u.username || '').toLowerCase().includes(search) ||
      (u.user_id || '').includes(search) ||
      (u.tickets || []).some(t => t.ticket.toLowerCase().includes(search));
    const matchBrand = !brand || u.brand === brand;
    const matchStatus = !status || (status === 'Excluded' ? u.status.startsWith('Excluded') : u.status === status);
    return matchSearch && matchBrand && matchStatus;
  }});
  currentPage = 0;
  renderUserTable(filteredUsers);
}}

// Debounced filter — waits 200ms after last keystroke
let _filterTimer = null;
function filterUsers() {{
  clearTimeout(_filterTimer);
  _filterTimer = setTimeout(applyFilters, 200);
}}

function sortUsers(col) {{
  if (currentSort.col === col) currentSort.asc = !currentSort.asc;
  else {{ currentSort.col = col; currentSort.asc = true; }}
  userData.sort((a, b) => {{
    let va = a[col], vb = b[col];
    if (col === 'discord_roles') {{ va = (va||[]).join(','); vb = (vb||[]).join(','); }}
    if (va == null) va = col === 'ftd_amount' || col === 'total_deposits' ? -1 : '';
    if (vb == null) vb = col === 'ftd_amount' || col === 'total_deposits' ? -1 : '';
    if (typeof va === 'string') {{ va = va.toLowerCase(); vb = (vb||'').toLowerCase(); }}
    if (va < vb) return currentSort.asc ? -1 : 1;
    if (va > vb) return currentSort.asc ? 1 : -1;
    return 0;
  }});
  filterUsers();
}}

function showUserDetail(idx) {{
  const u = filteredUsers[idx];
  if (!u) return;
  const panel = document.getElementById('ulDetail');
  const [bg, fg] = getStatusStyle(u.status);
  const cc = campColours[u.brand] || '#6b7280';
  const roles = (u.discord_roles || []).map(r =>
    '<span class="role-badge' + (r === 'Koenisch Elite' ? ' ke' : '') + '">' + r + '</span>'
  ).join('') || '<span style="color:#64748b">No roles data</span>';

  let ticketsHtml = '';
  (u.tickets || []).forEach(t => {{
    const [tbg, tfg] = getStatusStyle(t.status);
    const tcc = campColours[t.campaign] || '#6b7280';
    const link = t.drive_file_id
      ? '<a href="https://drive.google.com/file/d/' + t.drive_file_id + '/preview" target="_blank" style="color:#60a5fa;text-decoration:underline">' + t.ticket + '</a>'
      : t.ticket;
    const tAmt = t.amount != null ? '€' + t.amount.toFixed(2) : '—';
    ticketsHtml += '<tr>' +
      '<td>' + link + '</td>' +
      '<td><span class="badge" style="background:' + tcc + '20;color:' + tcc + '">' + t.campaign + '</span></td>' +
      '<td><span class="badge" style="background:' + tbg + ';color:' + tfg + '">' + t.status + '</span></td>' +
      '<td>' + (t.date ? t.date.substring(0, 10) : '—') + '</td>' +
      '<td style="text-align:right">' + tAmt + '</td>' +
      '<td style="font-size:0.78rem;color:#94a3b8;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + (t.signal||'').replace(/"/g,'&quot;') + '">' + (t.signal || '—') + '</td>' +
      '<td>' + (t.admin || '—') + '</td>' +
      '</tr>';
  }});

  panel.innerHTML =
    '<div class="ul-detail-header">' +
      '<h3>' + (u.display_name || u.username || u.user_id || '—') + (u.display_name && u.username && u.display_name !== u.username ? ' <span style="font-size:0.7em;color:#64748b;font-weight:400">@' + u.username + '</span>' : '') + '</h3>' +
      '<span class="ul-detail-close" onclick="document.getElementById(&quot;ulDetail&quot;).classList.remove(&quot;open&quot;)">✕</span>' +
    '</div>' +
    '<div class="ul-detail-grid">' +
      '<div class="ul-detail-field"><div class="lbl">Status</div><div class="val"><span class="badge" style="background:' + bg + ';color:' + fg + '">' + u.status + '</span>' +
        (u.status_detail ? ' <span style="color:#64748b;font-size:0.8rem">(' + u.status_detail + ')</span>' : '') + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">Brand</div><div class="val" style="color:' + cc + '">' + u.brand + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">FTD Amount</div><div class="val' + (u.ftd_amount != null ? ' green' : '') + '">' + (u.ftd_amount != null ? '€' + u.ftd_amount.toFixed(2) : '—') + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">Total Deposits</div><div class="val' + (u.total_deposits ? ' green' : '') + '">' + (u.total_deposits ? '€' + u.total_deposits.toFixed(2) : '—') + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">Deposit Count</div><div class="val">' + u.deposit_count + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">Promo Participations</div><div class="val">' + (u.promo_count || 0) + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">First Deposit Date</div><div class="val">' + (u.first_deposit_date ? u.first_deposit_date.substring(0, 10) : '—') + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">User ID</div><div class="val mono" style="font-size:0.82rem">' + (u.user_id || '—') + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">Discord Roles</div><div class="val">' + roles + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">Approval Signal</div><div class="val" style="font-size:0.82rem;color:#94a3b8;max-width:400px;word-break:break-word">' + (u.approval_signal || '—') + '</div></div>' +
      '<div class="ul-detail-field"><div class="lbl">Approving Admin</div><div class="val">' + (u.approving_admin || '—') + '</div></div>' +
    '</div>' +
    '<div class="section-title" style="margin-top:8px">Tickets (' + (u.tickets||[]).length + ')</div>' +
    '<table class="ul-tickets-table"><thead><tr>' +
      '<th>Ticket</th><th>Brand</th><th>Status</th><th>Date</th><th style="text-align:right">Amount</th><th>Approval Signal</th><th>Admin</th>' +
    '</tr></thead><tbody>' + ticketsHtml + '</tbody></table>';

  panel.classList.add('open');
  panel.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
}}
</script>
</body>
</html>"""

    # Inject user lookup JSON data (can't go in f-string — curly braces conflict)
    user_json = json.dumps(user_lookup or [], ensure_ascii=False)
    html = html.replace("__USER_LOOKUP_DATA__", user_json)

    output_path.write_text(html, encoding="utf-8")
    log(f"✅ Dashboard written to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not OAUTH_CLIENT_FILE.exists():
        print(f"ERROR: {OAUTH_CLIENT_FILE} not found")
        sys.exit(1)

    log("=== Wett Elite Deposit Overview ===")
    drive_svc, sheets_svc, creds = get_services()
    token = creds.token

    log("Listing transcripts folders (old + new)...")
    all_files = []
    seen_ids = set()
    for folder_id in GDRIVE_TRANSCRIPTS_FOLDER_IDS:
        folder_files = drive_list_files(drive_svc, folder_id)
        for f in folder_files:
            if f["id"] not in seen_ids:
                seen_ids.add(f["id"])
                all_files.append(f)
        log(f"  Folder {folder_id}: {len(folder_files)} files")
    html_files = [f for f in all_files if f["name"].lower().endswith(".html")]
    log(f"Found {len(html_files)} HTML transcripts total across both folders (before dedup)")

    # Deduplicate across both old and new folder naming conventions:
    #   Old format: "closed-0042_1001174280966525060.html"   → (ticket_num, user_id)
    #   New format: "serverID:channelID:closed-0062.html"    → (ticket_num, channel_id)
    # Prefer "closed-" prefix over "support-" or others. Also handle TicketTool
    # duplicates where the exact same file appears twice in the new folder.
    dedup: dict[str, dict] = {}
    for f in html_files:
        name = f["name"]

        # New format: serverID:channelID:ticketname.html
        m_new = re.match(r'^\d+:(\d+):([a-zA-Z\-]+)-(\d+)\.html$', name)
        if m_new:
            channel_id, prefix, ticket_num = m_new.group(1), m_new.group(2), m_new.group(3)
            # Normalise filename so the rest of the pipeline sees "closed-XXXX_channelID.html"
            normalised = f"{prefix}-{ticket_num}_{channel_id}.html"
            f = dict(f, name=normalised)  # shallow copy with normalised name
            key = f"{ticket_num}_{channel_id}"
            if key not in dedup or prefix == "closed":
                dedup[key] = f
            continue

        # Old format: prefix-XXXX_userID.html
        m_old = re.match(r'^([a-zA-Z\-]+)-(\d+)_(\d+)\.html$', name)
        if m_old:
            prefix, ticket_num, user_id = m_old.group(1), m_old.group(2), m_old.group(3)
            key = f"{ticket_num}_{user_id}"
            if key not in dedup or prefix == "closed":
                dedup[key] = f
            continue

        # Fallback: keep as-is but avoid exact duplicates
        if name not in dedup:
            dedup[name] = f

    html_files = list(dedup.values())
    log(f"After dedup: {len(html_files)} unique tickets")

    # Load manual overrides from previous human corrections
    manual_overrides = load_manual_overrides()
    log(f"Loaded {len(manual_overrides)} manual override entries")

    results = []
    errors  = 0
    counter = {"done": 0}
    lock    = threading.Lock()
    total   = len(html_files)

    # Statuses that are fully decided — no need to re-download the transcript
    # Uses startswith("Excluded") to catch all variants (Oddify, Promo, Internal, No FTD, etc.)
    def _is_final(status: str) -> bool:
        return status in ("Approved", "Not Approved") or status.startswith("Excluded")

    def result_from_cache(ticket_name: str, override: dict) -> dict:
        """Reconstruct a result dict from a cached override entry — no Drive download."""
        return {
            "ticket":                ticket_name,
            "user_id":               extract_user_id(ticket_name),
            "user":                  override.get("user", ""),
            "campaign":              override.get("campaign", "Unknown"),
            "has_screenshot":        override.get("has_screenshot", False),
            "approval_status":       override.get("status", "Not Approved"),
            "approval_signal":       override.get("signal", ""),
            "approving_admin":       override.get("approving_admin", ""),
            "ticket_date":           override.get("ticket_date", ""),
            "parse_error":           False,
            "deposit_amount":        override.get("deposit_amount"),
            "deposit_amount_source": override.get("deposit_amount_source", ""),
            "first_seen_at":         override.get("first_seen_at", ""),
            "campaign_source":       override.get("campaign_source", ""),
            "drive_file_id":         override.get("drive_file_id", ""),
        }

    def process_file(f):
        fname = f["name"]
        ticket_name = fname.replace(".html", "")
        override = manual_overrides.get(ticket_name, {})

        # --- Fast path: ticket fully processed and amount either extracted or confirmed no Vision needed ---
        # Only skip if amount source is set (Vision ran) OR ticket is non-Approved (no amount needed)
        _amount_settled = (
            bool(override.get("deposit_amount_source"))       # Vision ran → settled
            or override.get("status") not in ("Approved",)    # Not approved → no amount needed
        )
        if (
            override.get("first_seen_at")
            and _is_final(override.get("status", ""))
            and "deposit_amount" in override
            and _amount_settled
        ):
            cached = result_from_cache(ticket_name, override)
            cached["drive_file_id"] = override.get("drive_file_id") or f["id"]
            return cached, None, fname, None

        # --- Slow path: new ticket or still pending review — download & analyse ---
        try:
            html_bytes = drive_download_threadsafe(token, f["id"])
        except Exception as e:
            return None, None, fname, str(e)
        result = analyze_transcript(html_bytes, fname, manual_overrides=manual_overrides)
        result["drive_file_id"] = f["id"]  # Capture Drive file ID for linking
        # Stash images for the Vision pass if needed:
        #   mode "brand_and_amount" — Unknown campaign, no cached Vision brand
        #   mode "amount_only"      — brand known, Approved, no cached deposit_amount
        img_data = None
        # "deposit_amount" key present AND source is set means Vision actually ran.
        # Key present with empty source means it was set by migration (not Vision) — re-queue.
        already_has_amount = (
            "deposit_amount" in override
            and bool(override.get("deposit_amount_source"))
        )
        already_vision_classified = (
            override.get("campaign_source") == "vision"
            and override.get("campaign", "Unknown") != "Unknown"
        )
        # Vision needed if: Unknown brand, OR ticket is unresolved (To be checked / Not Approved)
        needs_brand = (
            result["has_screenshot"]
            and not already_vision_classified
            and (result["campaign"] == "Unknown"
                 or result["approval_status"] in ("To be checked", "Not Approved"))
        )
        needs_amount = (
            result["approval_status"] == "Approved"
            and result["has_screenshot"]
            and not already_has_amount
            and not needs_brand
        )
        if needs_brand or needs_amount:
            messages = parse_messages(html_bytes)
            if messages:
                imgs = extract_all_user_images(messages)
                if imgs:
                    mode = "brand_and_amount" if needs_brand else "amount_only"
                    img_data = (imgs, mode)
        return result, img_data, fname, None

    vision_queue: list[tuple[dict, list, str]] = []  # (result, images_list, mode)

    log(f"Downloading and analysing with 10 parallel workers...")
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(process_file, f): f for f in html_files}
        for future in as_completed(futures):
            try:
                result, img_data, fname, err = future.result()
            except Exception as e:
                log(f"  ❌ Unexpected error: {e}")
                errors += 1
                continue
            with lock:
                counter["done"] += 1
                done = counter["done"]
            if done % 100 == 0 or done == total:
                log(f"  [{done}/{total}] done...")
            if err:
                errors += 1
                continue
            results.append(result)
            if result["parse_error"]:
                errors += 1
            if img_data:
                imgs, mode = img_data
                vision_queue.append((result, imgs, mode))

    # ---------------------------------------------------------------------------
    # Claude Vision pass — classify Unknown tickets by screenshot
    # ---------------------------------------------------------------------------
    if vision_queue:
        api_key = None
        if ANTHROPIC_KEY_FILE.exists():
            api_key = ANTHROPIC_KEY_FILE.read_text().strip()
        if not api_key and _ANTHROPIC_KEY_FALLBACK.exists():
            api_key = _ANTHROPIC_KEY_FALLBACK.read_text().strip()
        if not api_key:
            api_key = os.environ.get("ANTHROPIC_API_KEY")

        if api_key:
            anthropic_client = anthropic_sdk.Anthropic(api_key=api_key)
            brand_jobs   = sum(1 for _, _, m in vision_queue if m == "brand_and_amount")
            amount_jobs  = sum(1 for _, _, m in vision_queue if m == "amount_only")
            log(f"\nRunning Claude Vision on {len(vision_queue)} tickets "
                f"({brand_jobs} brand+amount, {amount_jobs} amount-only)...")
            reclassified = 0
            amounts_found = 0
            auto_approved = 0
            auto_excluded = 0
            for i, (result, images, mode) in enumerate(vision_queue, 1):
                t = result["ticket"]
                if mode == "brand_and_amount":
                    brand, amount, verdict = classify_brand_and_amount_by_vision(anthropic_client, images)
                    if brand != "Unknown":
                        result["campaign"] = brand
                        result["campaign_source"] = "vision"
                        reclassified += 1
                    result["deposit_amount"] = amount
                    result["deposit_amount_source"] = "vision"

                    # Auto-approve/exclude based on Vision verdict
                    if verdict == "Approved" and brand != "Unknown":
                        result["approval_status"] = "Approved"
                        result["approval_signal"] = "vision_auto_approved"
                        result["approving_admin"] = "vision"
                        auto_approved += 1
                    elif verdict == "Promo":
                        result["approval_status"] = "Excluded (Promo)"
                        result["approval_signal"] = "vision_promo"
                        auto_excluded += 1
                    elif verdict == "No FTD":
                        result["approval_status"] = "Excluded (No FTD)"
                        result["approval_signal"] = "vision_no_ftd"
                        auto_excluded += 1
                else:  # amount_only
                    amount = extract_amount_by_vision(anthropic_client, images)
                    result["deposit_amount"] = amount
                    result["deposit_amount_source"] = "vision"

                if result.get("deposit_amount") is not None:
                    amounts_found += 1

                # Cache immediately so future runs skip this ticket
                manual_overrides.setdefault(t, {})
                if mode == "brand_and_amount":
                    manual_overrides[t]["campaign"] = result["campaign"]
                    manual_overrides[t]["campaign_source"] = "vision"
                    manual_overrides[t]["status"] = result["approval_status"]
                    manual_overrides[t]["signal"] = result["approval_signal"]
                manual_overrides[t]["deposit_amount"] = result["deposit_amount"]
                manual_overrides[t]["deposit_amount_source"] = "vision"

                if i % 10 == 0 or i == len(vision_queue):
                    log(f"  [{i}/{len(vision_queue)}] vision done "
                        f"(reclassified {reclassified}, amounts found {amounts_found})...")
            log(f"Vision pass complete: {reclassified} reclassified, {auto_approved} auto-approved, "
                f"{auto_excluded} auto-excluded, {amounts_found} amounts extracted")
        else:
            log("⚠️  No Anthropic API key found — skipping vision classification")

    # Inject DM-approved synthetic entries from manual_overrides into results
    for key, v in manual_overrides.items():
        if key.startswith("dm-approved-"):
            results.append({
                "ticket":                key,
                "user_id":               v.get("user_id", ""),
                "user":                  v.get("user", ""),
                "campaign":              v.get("campaign", "Unknown"),
                "has_screenshot":        v.get("has_screenshot", False),
                "approval_status":       v.get("status", "Approved"),
                "approval_signal":       v.get("signal", ""),
                "approving_admin":       v.get("approving_admin", "DM"),
                "parse_error":           False,
                "campaign_source":       v.get("campaign_source", "manual"),
                "ticket_date":           v.get("ticket_date", ""),
                "first_seen_at":         v.get("first_seen_at", ""),
                "deposit_amount":        v.get("deposit_amount"),
                "deposit_amount_source": v.get("deposit_amount_source", ""),
                "drive_file_id":         v.get("drive_file_id", ""),
            })

    # Inject orphaned override entries: tickets in overrides with a definitive status
    # but not matched from any Drive file (e.g. manually reviewed tickets whose Drive
    # filename has since changed, or entries added via API with slightly different keys).
    result_keys = {r["ticket"] for r in results}
    orphan_count = 0
    for key, v in manual_overrides.items():
        if key in result_keys or key.startswith("dm-approved-"):
            continue
        status = v.get("status", "")
        if status in ("Approved", "Excluded (Promo)") and v.get("ticket_date"):
            log(f"  ⚠️  Orphaned override injected into results: {key} (status={status})")
            results.append(result_from_cache(key, v))
            orphan_count += 1
    if orphan_count:
        log(f"Injected {orphan_count} orphaned override entries into results")

    # After analysis, save updated overrides (adds any new tickets to the file)
    updated_overrides = dict(manual_overrides)
    for r in results:
        t = r["ticket"]
        if t not in updated_overrides:
            updated_overrides[t] = {
                "status": r["approval_status"],
                "signal": r["approval_signal"],
                "user": r["user"],
                "campaign": r["campaign"],
                "has_screenshot": r["has_screenshot"],
                "approving_admin": r["approving_admin"],
                "campaign_source": r.get("campaign_source", ""),
                "ticket_date": r.get("ticket_date", ""),
                "first_seen_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "deposit_amount": r.get("deposit_amount"),
                "deposit_amount_source": r.get("deposit_amount_source", ""),
                "drive_file_id": r.get("drive_file_id", ""),
            }
        else:
            # Backfill user field if missing
            if r.get("user") and not updated_overrides[t].get("user"):
                updated_overrides[t]["user"] = r["user"]
            if r.get("ticket_date") and not updated_overrides[t].get("ticket_date"):
                updated_overrides[t]["ticket_date"] = r["ticket_date"]
            if r.get("campaign_source") == "vision":
                # Always update campaign_source for vision-classified tickets
                updated_overrides[t]["campaign"] = r["campaign"]
                updated_overrides[t]["campaign_source"] = "vision"

        # Persist drive_file_id if available
        if r.get("drive_file_id") and not updated_overrides.get(t, {}).get("drive_file_id"):
            updated_overrides.setdefault(t, {})["drive_file_id"] = r["drive_file_id"]

        # Persist deposit_amount if newly extracted (never overwrite an existing value)
        if (
            r.get("deposit_amount_source") == "vision"
            and "deposit_amount" not in updated_overrides.get(t, {})
        ):
            updated_overrides[t]["deposit_amount"] = r.get("deposit_amount")
            updated_overrides[t]["deposit_amount_source"] = "vision"
    save_manual_overrides(updated_overrides)
    log(f"Saved {len(updated_overrides)} entries to manual-overrides.json")

    # Print terminal summary
    log(f"\n=== Results ===")
    log(f"  {'Campaign':12s}  {'Total':>5}  {'UniqueReg':>9}  {'Approved':>8}  {'ToCheck':>7}  {'Oddify':>6}  {'Promo':>5}  {'NoSS':>5}")
    for campaign in SUMMARY_CAMPAIGNS:
        subset   = [r for r in results if r["campaign"] == campaign]
        approved = sum(1 for r in subset if r["approval_status"] == "Approved")
        unique   = count_unique_registrations(subset)
        to_check = sum(1 for r in subset if r["approval_status"] == "To be checked")
        ex_odd   = sum(1 for r in subset if r["approval_status"] == "Excluded (Oddify)")
        ex_promo = sum(1 for r in subset if r["approval_status"] == "Excluded (Promo)")
        no_ss    = sum(1 for r in subset if r["approval_status"] == "Not Approved")
        log(f"  {campaign:12s}  {len(subset):5d}  {unique:9d}  {approved:8d}  {to_check:7d}  {ex_odd:6d}  {ex_promo:5d}  {no_ss:5d}")

    total    = len(results)
    approved = sum(1 for r in results if r["approval_status"] == "Approved")
    unique   = count_unique_registrations(results)
    to_check = sum(1 for r in results if r["approval_status"] == "To be checked")
    ex_odd   = sum(1 for r in results if r["approval_status"] == "Excluded (Oddify)")
    ex_promo = sum(1 for r in results if r["approval_status"] == "Excluded (Promo)")
    log(f"  {'TOTAL':12s}  {total:5d}  {unique:9d}  {approved:8d}  {to_check:7d}  {ex_odd:6d}  {ex_promo:5d}")
    log(f"  Parse errors: {errors}")

    # Build user lookup data
    discord_roles, discord_profiles = load_discord_members()
    log(f"Loaded Discord roles for {len(discord_roles)} members")
    user_lookup = build_user_lookup_data(results, discord_roles, discord_profiles)
    log(f"Built user lookup data: {len(user_lookup)} unique users")

    # Generate HTML dashboard
    html_path = SCRIPT_DIR / "dashboard.html"
    generate_html_dashboard(results, html_path, user_lookup=user_lookup)

    # Write to Google Sheets
    sheet_url = write_to_sheets(sheets_svc, results)
    log(f"\n✅ Sheet ready: {sheet_url}")
    print(f"\nOpen your sheet: {sheet_url}")

    # Send Telegram notification
    # SKIP_TELEGRAM=true when triggered by bot button presses (dashboard refresh only)
    # Also enforces once-per-day limit: only one Telegram blast per calendar day (UTC)
    skip_telegram = os.environ.get("SKIP_TELEGRAM", "false").lower() == "true"
    to_check_items = [r for r in results if r["approval_status"] == "To be checked"]

    # Once-per-day guard: check if we already sent today
    today_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    _state_path = Path("manual-overrides.json")
    try:
        _state_data = json.loads(_state_path.read_text()) if _state_path.exists() else {}
    except Exception:
        _state_data = {}
    last_sent_date = _state_data.get("_state", {}).get("last_telegram_date", "")
    if last_sent_date == today_utc and not skip_telegram:
        log(f"📱 Telegram: already sent today ({today_utc}) — skipping to avoid flood")
        skip_telegram = True

    if TG_BOT_TOKEN and TG_CHAT_ID and not skip_telegram:
        cutoff_24h = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)).isoformat()
        def is_new(r):
            d = r.get("first_seen_at") or r.get("ticket_date", "")
            return bool(d and d >= cutoff_24h)

        u_reg   = count_unique_registrations(results)
        u_bet   = count_unique_registrations([r for r in results if r["campaign"] == "Betlabel"])
        u_win   = count_unique_registrations([r for r in results if r["campaign"] == "Winnerz"])
        u_rol   = count_unique_registrations([r for r in results if r["campaign"] == "Winrolla"])
        new_24h_list = [r for r in results if r["approval_status"] == "Approved" and is_new(r)]
        new_24h = len(new_24h_list)
        new_bet = sum(1 for r in new_24h_list if r["campaign"] == "Betlabel")
        new_win = sum(1 for r in new_24h_list if r["campaign"] == "Winnerz")
        new_rol = sum(1 for r in new_24h_list if r["campaign"] == "Winrolla")
        vol_24h_tg      = sum_amounts(new_24h_list)
        vol_24h_known_tg = sum(1 for r in new_24h_list if r.get("deposit_amount") is not None)
        vol_24h_tg_str  = f"€{vol_24h_tg:.0f}" if vol_24h_tg is not None else "—"

        dashboard_url = "https://wettelite.github.io/wett-elite-dashboard/dashboard.html"

        summary = (
            f"🎰 <b>Wett Elite Dashboard Updated</b>\n"
            f"🕐 {time.strftime('%d.%m.%Y %H:%M')} UTC\n\n"
            f"📅 <b>Last 24 Hours:</b> +{new_24h} new deposits\n"
            f"  • Betlabel: +{new_bet}\n"
            f"  • Winnerz:  +{new_win}\n"
            f"  • Winrolla: +{new_rol}\n"
            f"💰 Volume: {vol_24h_tg_str} ({vol_24h_known_tg} of {new_24h} with amount)\n\n"
            f"📊 <b>All-Time Unique Registrations:</b> {u_reg}\n"
            f"  • Betlabel: {u_bet}\n"
            f"  • Winnerz:  {u_win}\n"
            f"  • Winrolla: {u_rol}\n\n"
            f"🔗 <a href='{dashboard_url}'>Open Dashboard</a>"
        )
        if to_check_items:
            summary += f"\n\n⚠️ <b>{len(to_check_items)} ticket(s) need review</b> — see below 👇"
        else:
            summary += "\n\n✅ No tickets need review."
        send_telegram(summary)

        # Send one message per "to be checked" ticket with inline approve buttons
        # Skip tickets already sent to Telegram (prevent re-blast on every workflow trigger)
        overrides_path = Path("manual-overrides.json")
        try:
            current_overrides = json.loads(overrides_path.read_text()) if overrides_path.exists() else {}
        except Exception:
            current_overrides = {}

        newly_sent = 0
        for r in to_check_items:
            ticket   = r["ticket"]
            # Skip if already sent to Telegram
            if current_overrides.get(ticket, {}).get("telegram_sent_at"):
                continue

            user     = r.get("user", "?") or "?"
            campaign = r.get("campaign", "Unknown")
            ss       = "✅ Yes" if r.get("has_screenshot") else "❌ No"
            text = (
                f"⚠️ <b>Ticket needs review</b>\n\n"
                f"👤 <b>User:</b> {user}\n"
                f"🎯 <b>Detected campaign:</b> {campaign}\n"
                f"📸 <b>Screenshot:</b> {ss}\n"
                f"🎫 <b>Ticket:</b> <code>{ticket}</code>"
            )
            # Callback data format: action:ticket  (max 64 bytes — kept short)
            # Actions: app_B=approve Betlabel, app_W=approve Winnerz,
            #          app_R=approve Winrolla,  exc_O=exclude Oddify,
            #          exc_P=exclude Promo,     exc_F=no successful FTD
            t = ticket  # shorthand
            buttons = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Betlabel", "callback_data": f"app_B:{t}"},
                        {"text": "✅ Winnerz",  "callback_data": f"app_W:{t}"},
                        {"text": "✅ Winrolla", "callback_data": f"app_R:{t}"},
                    ],
                    [
                        {"text": "❌ Oddify",   "callback_data": f"exc_O:{t}"},
                        {"text": "❌ Promo",    "callback_data": f"exc_P:{t}"},
                        {"text": "❌ No FTD",   "callback_data": f"exc_F:{t}"},
                    ],
                ]
            }
            try:
                http_requests.post(
                    f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                    json={"chat_id": TG_CHAT_ID, "text": text,
                          "parse_mode": "HTML", "reply_markup": buttons},
                    timeout=10,
                )
                # Mark as sent so we don't resend on the next workflow run
                if ticket not in current_overrides:
                    current_overrides[ticket] = {}
                current_overrides[ticket]["telegram_sent_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                newly_sent += 1
            except Exception as e:
                log(f"⚠️  Telegram inline msg error: {e}")

        # Save updated telegram_sent_at flags + daily state back to manual-overrides.json
        try:
            if "_state" not in current_overrides:
                current_overrides["_state"] = {}
            current_overrides["_state"]["last_telegram_date"] = today_utc
            overrides_path.write_text(json.dumps(current_overrides, indent=2, ensure_ascii=False))
        except Exception as e:
            log(f"⚠️  Could not save telegram state flags: {e}")

        log(f"📱 Telegram: summary + {newly_sent} new inline ticket(s) sent ({len(to_check_items) - newly_sent} skipped, already sent)")

if __name__ == "__main__":
    main()
