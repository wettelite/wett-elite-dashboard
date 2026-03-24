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

GDRIVE_TRANSCRIPTS_FOLDER_ID = "1npYHrpWLiq234qP1Ix0ECbU2VC9iDUEq"

OAUTH_CLIENT_FILE    = SCRIPT_DIR / "oauth-client.json"
OAUTH_TOKEN_FILE     = SCRIPT_DIR / "oauth-token.json"
ANTHROPIC_KEY_FILE   = SCRIPT_DIR / "anthropic-api-key.txt"

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

def classify_brand_by_vision(anthropic_client, images: list[tuple[str, str]]) -> str:
    """Ask Claude to identify the casino brand from ALL user deposit screenshots in a ticket.
    Sends up to 4 images at once so even if the first is a bank page, later ones may show the brand.
    """
    if not images:
        return "Unknown"
    try:
        # Build content with up to 4 images + one question
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
                "Reply with ONLY one word: Winrolla, Betlabel, Winnerz, or Unknown."
            ),
        })

        resp = anthropic_client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=20,
            messages=[{"role": "user", "content": content}],
        )
        answer = resp.content[0].text.strip()
        for brand in ("Winrolla", "Betlabel", "Winnerz"):
            if brand.lower() in answer.lower():
                return brand
        return "Unknown"
    except Exception:
        return "Unknown"

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
      1. einzahlung-promo ticket name → Excluded (Promo)
      2. oddify- ticket name OR message content → Excluded (Oddify)
      3. Manual override says Excluded → honour it
      4. Manual override says Approved → upgrade to Approved
      5. Otherwise keep auto-detected status
    """
    ticket_lower = result["ticket"].lower()

    # 1. Promo tickets (existing registered users depositing for a promotion)
    if "einzahlung-promo" in ticket_lower:
        result["approval_status"] = "Excluded (Promo)"
        result["approval_signal"] = "Ticket name indicates promo deposit (already registered)"
        return

    # 2. Oddify by ticket name
    if ticket_lower.startswith("oddify"):
        result["approval_status"] = "Excluded (Oddify)"
        result["approval_signal"] = "Ticket name indicates Oddify source"
        return

    # 3. Oddify by message content (for tickets where admin noted it or user mentioned it)
    if messages and detect_oddify(messages):
        result["approval_status"] = "Excluded (Oddify)"
        result["approval_signal"] = "Message content mentions Oddify"
        return

    # 4. Check manual overrides
    override = manual_overrides.get(result["ticket"])
    if override:
        saved_signal = (override.get("signal") or "").lower()
        saved_status = override.get("status", "")

        # Honour manual Excluded flags recorded via signal text
        if "oddify" in saved_signal:
            result["approval_status"] = "Excluded (Oddify)"
            result["approval_signal"] = override.get("signal", "Oddify — does not count")
            return
        if "promo" in saved_signal or "promotion" in saved_signal:
            result["approval_status"] = "Excluded (Promo)"
            result["approval_signal"] = override.get("signal", "Promotion deposit")
            return

        # If human manually approved a ticket our auto-detect missed → keep Approved
        if saved_status == "Approved" and result["approval_status"] != "Approved":
            result["approval_status"] = "Approved"
            result["approval_signal"] = override.get("signal") or "Manually approved"
            result["approving_admin"] = override.get("approving_admin") or "manual"

        # If human OR Vision saved a campaign → honour it
        if override.get("campaign") and override["campaign"] != "Unknown":
            if result["campaign"] == "Unknown":
                result["campaign"] = override["campaign"]
                if override.get("campaign_source"):
                    result["campaign_source"] = override["campaign_source"]


def analyze_transcript(html_bytes: bytes, filename: str,
                       manual_overrides: dict | None = None) -> dict:
    """Full analysis of one transcript. Returns a result dict."""
    messages = parse_messages(html_bytes)
    ticket_name = filename.replace(".html", "")

    result = {
        "ticket":          ticket_name,
        "user_id":         extract_user_id(ticket_name),
        "user":            "",
        "campaign":        "Unknown",
        "has_screenshot":  False,
        "approval_status": "Not Approved",
        "approval_signal": "",
        "approving_admin": "",
        "parse_error":     False,
    }

    if messages is None:
        result["parse_error"] = True
        return result

    result["campaign"] = detect_campaign(messages)

    has_screenshot, user = detect_screenshot(messages)
    result["has_screenshot"] = has_screenshot
    result["user"] = user

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

EXCLUDED_STATUSES = {"Excluded (Oddify)", "Excluded (Promo)"}

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

def build_summary(results: list[dict]) -> list[list]:
    header = [
        "Campaign", "Total Tickets",
        "✅ Unique Registrations", "Approved Tickets",
        "To be checked",
        "Excl. Oddify", "Excl. Promo",
        "No Screenshot",
    ]
    rows = [header]
    col_count = len(header)
    totals = [0] * (col_count - 1)

    for campaign in SUMMARY_CAMPAIGNS:
        subset   = [r for r in results if r["campaign"] == campaign]
        total    = len(subset)
        approved = sum(1 for r in subset if r["approval_status"] == "Approved")
        unique   = count_unique_registrations(subset)
        to_check = sum(1 for r in subset if r["approval_status"] == "To be checked")
        ex_odd   = sum(1 for r in subset if r["approval_status"] == "Excluded (Oddify)")
        ex_promo = sum(1 for r in subset if r["approval_status"] == "Excluded (Promo)")
        no_ss    = sum(1 for r in subset if r["approval_status"] == "Not Approved")

        rows.append([campaign, total, unique, approved, to_check, ex_odd, ex_promo, no_ss])
        for i, v in enumerate([total, unique, approved, to_check, ex_odd, ex_promo, no_ss]):
            totals[i] += v

    rows.append(["TOTAL"] + totals)
    return rows

def build_details(results: list[dict]) -> list[list]:
    header = ["Ticket", "User", "User ID", "Campaign", "Screenshot?", "Approval Status", "Approval Signal", "Approving Admin"]
    rows = [header]
    for r in sorted(results, key=lambda x: x["ticket"]):
        rows.append([
            r["ticket"],
            r["user"],
            r.get("user_id", ""),
            r["campaign"],
            "Yes" if r["has_screenshot"] else "No",
            r["approval_status"],
            r["approval_signal"],
            r["approving_admin"],
        ])
    return rows

def build_user_overview(results: list[dict]) -> list[list]:
    """One row per unique user (deduplicated by user_id or username)."""
    header = ["User", "User ID", "Campaign", "Screenshot?", "Approval Status", "Approving Admin"]
    seen: dict[str, dict] = {}  # key → best result for this user

    status_rank = {
        "Approved": 0, "To be checked": 1,
        "Excluded (Oddify)": 2, "Excluded (Promo)": 3, "Not Approved": 4,
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
            {"properties": {"title": "Summary",      "index": 0}},
            {"properties": {"title": "User Overview", "index": 1}},
            {"properties": {"title": "Details",       "index": 2}},
        ],
    }).execute()

    sheet_id  = spreadsheet["spreadsheetId"]
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    summary_data  = build_summary(results)
    details_data  = build_details(results)
    overview_data = build_user_overview(results)

    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": "Summary!A1",       "values": summary_data},
                {"range": "User Overview!A1", "values": overview_data},
                {"range": "Details!A1",       "values": details_data},
            ],
        },
    ).execute()

    # Basic formatting: bold headers, freeze row 1
    fmt_requests = []
    for tab_index, tab_title in enumerate(["Summary", "User Overview", "Details"]):
        tab_info = next(
            s for s in spreadsheet["sheets"]
            if s["properties"]["title"] == tab_title
        )
        tab_id = tab_info["properties"]["sheetId"]
        if tab_title == "Summary":
            col_count = len(summary_data[0])
        elif tab_title == "User Overview":
            col_count = len(overview_data[0])
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
TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8482071827:AAFR5_5LqBOefGQbJhCU2j09KNY7Rk7pjhk")
TG_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "657334852")

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
def generate_html_dashboard(results: list[dict], output_path: Path) -> None:
    import datetime
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

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

    # Status badge colours
    STATUS_COLOURS = {
        "Approved":         ("#d1fae5", "#065f46"),
        "To be checked":    ("#fef3c7", "#92400e"),
        "Not Approved":     ("#fee2e2", "#991b1b"),
        "Excluded (Oddify)":("#e0e7ff", "#3730a3"),
        "Excluded (Promo)": ("#f3e8ff", "#6b21a8"),
    }
    CAMP_COLOURS = {"Betlabel": "#3b82f6", "Winnerz": "#10b981", "Winrolla": "#f59e0b", "Unknown": "#6b7280"}

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
        rows_html += f"""
        <tr>
          <td class="mono">{r['ticket']}</td>
          <td>{r.get('user','')}</td>
          <td><span class="badge" style="background:{cc}20;color:{cc};border:1px solid {cc}40">{camp}</span></td>
          <td style="text-align:center">{ss}</td>
          <td><span class="badge" style="background:{bg};color:{fg}">{status}</span></td>
          <td>{admin}</td>
        </tr>"""

    # Summary cards HTML
    cards_html = ""
    for c in campaigns_data:
        colour = CAMP_COLOURS.get(c["name"], "#6b7280")
        cards_html += f"""
        <div class="card" style="border-top:4px solid {colour}">
          <div class="card-label">{c['name']}</div>
          <div class="card-number" style="color:{colour}">{c['unique']}</div>
          <div class="card-sub">unique registrations</div>
          <div class="card-detail">{c['approved']} approved tickets</div>
          {f'<div class="card-warn">⚠️ {c["to_check"]} to review</div>' if c["to_check"] else ""}
        </div>"""

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
</style>
</head>
<body>
<div class="header">
  <h1>🎰 Wett Elite — Deposit Dashboard</h1>
  <div class="updated">Last updated: {now}</div>
</div>
<div class="main">

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
          <option>Excluded (Promo)</option>
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
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    </div>
  </div>

</div>
<script>
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
</script>
</body>
</html>"""

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

    log("Listing transcripts folder...")
    all_files = drive_list_files(drive_svc, GDRIVE_TRANSCRIPTS_FOLDER_ID)
    html_files = [f for f in all_files if f["name"].lower().endswith(".html")]
    log(f"Found {len(html_files)} HTML transcripts (before dedup)")

    # Deduplicate: each ticket exists as both "closed-XXXX_UID" and "prefix-XXXX_UID"
    # Group by (ticket_number, user_id), prefer the "closed-" version (most complete)
    dedup: dict[str, dict] = {}
    for f in html_files:
        m = re.match(r'^([a-zA-Z\-]+)-(\d+)_(\d+)\.html$', f["name"])
        if not m:
            key = f["name"]  # fallback: keep as-is
            if key not in dedup:
                dedup[key] = f
            continue
        prefix, ticket_num, user_id = m.group(1), m.group(2), m.group(3)
        key = f"{ticket_num}_{user_id}"
        if key not in dedup or prefix == "closed":
            dedup[key] = f
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

    def process_file(f):
        fname = f["name"]
        try:
            html_bytes = drive_download_threadsafe(token, f["id"])
        except Exception as e:
            return None, None, fname, str(e)
        result = analyze_transcript(html_bytes, fname, manual_overrides=manual_overrides)
        # For Unknown tickets with screenshots, stash ALL images for vision pass
        # Skip if campaign was already resolved by a previous Vision run (saved in overrides)
        img_data = None
        override = manual_overrides.get(result["ticket"], {})
        already_vision_classified = (
            override.get("campaign_source") == "vision"
            and override.get("campaign", "Unknown") != "Unknown"
        )
        if result["campaign"] == "Unknown" and result["has_screenshot"] and not already_vision_classified:
            messages = parse_messages(html_bytes)
            if messages:
                imgs = extract_all_user_images(messages)
                if imgs:
                    img_data = imgs  # list of (b64, media_type)
        return result, img_data, fname, None

    vision_queue: list[tuple[dict, list]] = []  # (result, images_list)

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
                vision_queue.append((result, img_data))

    # ---------------------------------------------------------------------------
    # Claude Vision pass — classify Unknown tickets by screenshot
    # ---------------------------------------------------------------------------
    if vision_queue:
        api_key = None
        if ANTHROPIC_KEY_FILE.exists():
            api_key = ANTHROPIC_KEY_FILE.read_text().strip()
        if not api_key:
            api_key = os.environ.get("ANTHROPIC_API_KEY")

        if api_key:
            anthropic_client = anthropic_sdk.Anthropic(api_key=api_key)
            log(f"\nRunning Claude Vision on {len(vision_queue)} Unknown tickets (all images, sonnet)...")
            reclassified = 0
            for i, (result, images) in enumerate(vision_queue, 1):
                brand = classify_brand_by_vision(anthropic_client, images)
                if brand != "Unknown":
                    result["campaign"] = brand
                    result["campaign_source"] = "vision"
                    reclassified += 1
                    # Save Vision result immediately so future runs skip this ticket
                    t = result["ticket"]
                    if t in manual_overrides:
                        manual_overrides[t]["campaign"] = brand
                        manual_overrides[t]["campaign_source"] = "vision"
                if i % 10 == 0 or i == len(vision_queue):
                    log(f"  [{i}/{len(vision_queue)}] vision done (reclassified {reclassified} so far)...")
            log(f"Vision pass complete: {reclassified}/{len(vision_queue)} reclassified")
        else:
            log("⚠️  No Anthropic API key found — skipping vision classification")

    # Inject DM-approved synthetic entries from manual_overrides into results
    for key, v in manual_overrides.items():
        if key.startswith("dm-approved-"):
            results.append({
                "ticket":          key,
                "user_id":         v.get("user_id", ""),
                "user":            v.get("user", ""),
                "campaign":        v.get("campaign", "Unknown"),
                "has_screenshot":  v.get("has_screenshot", False),
                "approval_status": v.get("status", "Approved"),
                "approval_signal": v.get("signal", ""),
                "approving_admin": v.get("approving_admin", "DM"),
                "parse_error":     False,
                "campaign_source": v.get("campaign_source", "manual"),
            })

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
            }
        elif r.get("campaign_source") == "vision":
            # Always update campaign_source for vision-classified tickets
            updated_overrides[t]["campaign"] = r["campaign"]
            updated_overrides[t]["campaign_source"] = "vision"
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

    # Generate HTML dashboard
    html_path = SCRIPT_DIR / "dashboard.html"
    generate_html_dashboard(results, html_path)

    # Write to Google Sheets
    sheet_url = write_to_sheets(sheets_svc, results)
    log(f"\n✅ Sheet ready: {sheet_url}")
    print(f"\nOpen your sheet: {sheet_url}")

    # Send Telegram notification for unclear cases
    to_check_items = [r for r in results if r["approval_status"] == "To be checked"]
    if TG_BOT_TOKEN and TG_CHAT_ID:
        u_reg   = count_unique_registrations(results)
        u_bet   = count_unique_registrations([r for r in results if r["campaign"] == "Betlabel"])
        u_win   = count_unique_registrations([r for r in results if r["campaign"] == "Winnerz"])
        u_rol   = count_unique_registrations([r for r in results if r["campaign"] == "Winrolla"])
        msg = (
            f"🎰 <b>Wett Elite Deposit Dashboard</b> — updated\n\n"
            f"📊 <b>Unique Registrations:</b> {u_reg}\n"
            f"  • Betlabel: {u_bet}\n"
            f"  • Winnerz:  {u_win}\n"
            f"  • Winrolla: {u_rol}\n"
        )
        if to_check_items:
            msg += f"\n⚠️ <b>{len(to_check_items)} tickets need your review:</b>\n"
            for r in to_check_items[:10]:
                msg += f"  — {r.get('user','?')} ({r.get('campaign','?')})\n"
            if len(to_check_items) > 10:
                msg += f"  … and {len(to_check_items)-10} more\n"
            msg += f"\n🔗 <a href='{sheet_url}'>Open Sheet</a>"
        else:
            msg += "\n✅ No tickets need review."
        send_telegram(msg)
        log("📱 Telegram notification sent")

if __name__ == "__main__":
    main()
