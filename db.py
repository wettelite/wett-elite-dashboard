"""
Thin SQLite wrapper for the deposit dashboard.

Replaces manual-overrides.json with atomic, conflict-free storage.
No ORM, no classes — just functions.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tickets (
    ticket_key          TEXT PRIMARY KEY,
    user_name           TEXT,
    user_id             TEXT,
    campaign            TEXT DEFAULT 'Unknown',
    campaign_source     TEXT DEFAULT '',
    status              TEXT DEFAULT 'Not Approved',
    signal              TEXT DEFAULT '',
    approving_admin     TEXT DEFAULT '',
    has_screenshot      BOOLEAN DEFAULT 0,
    ticket_date         TEXT,
    first_seen_at       TEXT,
    deposit_amount      REAL,
    deposit_amount_source TEXT DEFAULT '',
    drive_file_id       TEXT DEFAULT '',
    vision_amount_retries INTEGER DEFAULT 0,
    text_amount_tried   BOOLEAN DEFAULT 0,
    reviewed_by         TEXT DEFAULT '',
    telegram_sent_at    TEXT,
    updated_at          TEXT
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_key    TEXT NOT NULL,
    author        TEXT,
    content       TEXT,
    is_admin      BOOLEAN DEFAULT 0,
    has_attachment BOOLEAN DEFAULT 0,
    FOREIGN KEY (ticket_key) REFERENCES tickets(ticket_key)
);

CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

# Column names in tickets table (excluding ticket_key)
_TICKET_COLS = [
    "user_name", "user_id", "campaign", "campaign_source", "status",
    "signal", "approving_admin", "has_screenshot", "ticket_date",
    "first_seen_at", "deposit_amount", "deposit_amount_source",
    "drive_file_id", "vision_amount_retries", "text_amount_tried",
    "reviewed_by", "telegram_sent_at", "updated_at",
]

# Mapping from JSON keys to DB columns (where they differ)
_JSON_TO_DB = {
    "user": "user_name",
}

# Mapping from DB columns to JSON keys (where they differ)
_DB_TO_JSON = {v: k for k, v in _JSON_TO_DB.items()}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


def _bool_to_int(val) -> int | None:
    """Convert Python bool / truthy to SQLite integer. Preserves None."""
    if val is None:
        return None
    return 1 if val else 0


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def init_db(path: str = "dashboard.db") -> sqlite3.Connection:
    """Open (or create) the database and ensure schema exists."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Tickets
# ---------------------------------------------------------------------------

def get_ticket(conn: sqlite3.Connection, ticket_key: str) -> dict | None:
    """Return a single ticket as a dict, or None if not found."""
    row = conn.execute(
        "SELECT * FROM tickets WHERE ticket_key = ?", (ticket_key,)
    ).fetchone()
    return _row_to_dict(row) if row else None


def get_all_tickets(conn: sqlite3.Connection) -> dict:
    """
    Return all tickets as {ticket_key: {fields...}}.

    Output format mirrors manual-overrides.json so the rest of the
    pipeline can swap in without changes. Chat data is included under
    the "chat" key for each ticket that has messages.
    """
    rows = conn.execute("SELECT * FROM tickets").fetchall()
    result = {}
    for row in rows:
        d = _row_to_dict(row)
        key = d.pop("ticket_key")

        # Map DB column names back to JSON names
        for db_col, json_key in _DB_TO_JSON.items():
            if db_col in d:
                d[json_key] = d.pop(db_col)

        # Convert int bools back to Python bools (preserve None)
        for bool_col in ("has_screenshot", "text_amount_tried"):
            json_col = _DB_TO_JSON.get(bool_col, bool_col)
            if json_col in d:
                d[json_col] = bool(d[json_col]) if d[json_col] is not None else None

        # Strip internal-only fields that aren't in the original JSON
        d.pop("updated_at", None)

        # Attach chat if any
        chat = get_chat(conn, key)
        if chat:
            d["chat"] = chat

        result[key] = d
    return result


def upsert_ticket(conn: sqlite3.Connection, ticket_key: str, data: dict):
    """Insert or replace a ticket row. Sets updated_at automatically."""
    data = dict(data)  # don't mutate caller's dict
    data["updated_at"] = _utcnow()

    # Map JSON keys to DB columns
    for json_key, db_col in _JSON_TO_DB.items():
        if json_key in data:
            data[db_col] = data.pop(json_key)

    # Convert booleans
    for bool_col in ("has_screenshot", "text_amount_tried"):
        if bool_col in data:
            data[bool_col] = _bool_to_int(data[bool_col])

    # Build column list from data keys that match known columns
    cols = ["ticket_key"]
    vals = [ticket_key]
    for col in _TICKET_COLS:
        if col in data:
            cols.append(col)
            vals.append(data[col])

    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    conn.execute(
        f"INSERT OR REPLACE INTO tickets ({col_names}) VALUES ({placeholders})",
        vals,
    )
    conn.commit()


def upsert_tickets(conn: sqlite3.Connection, tickets: dict):
    """Bulk upsert from a {ticket_key: {fields...}} dict."""
    for key, data in tickets.items():
        # Separate chat data — store it in chat_messages, not tickets
        data = dict(data)
        chat = data.pop("chat", None)
        upsert_ticket(conn, key, data)
        if chat:
            save_chat(conn, key, chat)


# ---------------------------------------------------------------------------
# Chat messages
# ---------------------------------------------------------------------------

def get_chat(conn: sqlite3.Connection, ticket_key: str) -> list[dict]:
    """Return chat messages for a ticket in JSON-compatible format."""
    rows = conn.execute(
        "SELECT author, content, is_admin, has_attachment "
        "FROM chat_messages WHERE ticket_key = ? ORDER BY id",
        (ticket_key,),
    ).fetchall()
    result = []
    for row in rows:
        msg = {
            "a": row["author"] or "",
            "t": row["content"] or "",
        }
        if row["is_admin"]:
            msg["admin"] = True
        if row["has_attachment"]:
            msg["img"] = True
        result.append(msg)
    return result


def save_chat(conn: sqlite3.Connection, ticket_key: str, messages: list[dict]):
    """
    Save chat messages for a ticket. Replaces existing messages.

    Accepts JSON format: [{a: author, t: text, admin: bool, img: bool}]
    """
    conn.execute(
        "DELETE FROM chat_messages WHERE ticket_key = ?", (ticket_key,)
    )
    for msg in messages:
        conn.execute(
            "INSERT INTO chat_messages (ticket_key, author, content, is_admin, has_attachment) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                ticket_key,
                msg.get("a", ""),
                msg.get("t", ""),
                _bool_to_int(msg.get("admin")),
                _bool_to_int(msg.get("img")),
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Key/value state
# ---------------------------------------------------------------------------

def get_state(conn: sqlite3.Connection, key: str) -> str | None:
    """Retrieve a state value by key."""
    row = conn.execute(
        "SELECT value FROM state WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else None


def set_state(conn: sqlite3.Connection, key: str, value: str):
    """Set a state value (insert or replace)."""
    conn.execute(
        "INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Migration: JSON <-> SQLite
# ---------------------------------------------------------------------------

def migrate_from_json(conn: sqlite3.Connection, json_path: str) -> int:
    """
    Import manual-overrides.json into the database.

    Returns the number of tickets migrated.
    """
    path = Path(json_path)
    if not path.exists():
        return 0

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        return 0

    # Handle the special _state key (stored in state table, not tickets)
    if "_state" in data:
        state_entry = data.pop("_state")
        for k, v in state_entry.items():
            set_state(conn, k, str(v) if v is not None else "")

    count = 0
    for key, entry in data.items():
        entry = dict(entry)
        chat = entry.pop("chat", None)

        upsert_ticket(conn, key, entry)
        if chat:
            save_chat(conn, key, chat)
        count += 1

    return count


def export_to_json(conn: sqlite3.Connection, json_path: str) -> int:
    """
    Export all tickets (with chat) to JSON in manual-overrides format.

    Returns the number of tickets exported.
    """
    tickets = get_all_tickets(conn)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(tickets, f, indent=2, ensure_ascii=False)

    return len(tickets)


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python db.py <migrate|export|stats> [args...]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "migrate":
        json_file = sys.argv[2] if len(sys.argv) > 2 else "manual-overrides.json"
        db = init_db()
        n = migrate_from_json(db, json_file)
        print(f"Migrated {n} tickets from {json_file}")
        db.close()

    elif cmd == "export":
        json_file = sys.argv[2] if len(sys.argv) > 2 else "manual-overrides-export.json"
        db = init_db()
        n = export_to_json(db, json_file)
        print(f"Exported {n} tickets to {json_file}")
        db.close()

    elif cmd == "stats":
        db = init_db()
        tickets = db.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]
        chats = db.execute(
            "SELECT COUNT(DISTINCT ticket_key) FROM chat_messages"
        ).fetchone()[0]
        msgs = db.execute("SELECT COUNT(*) FROM chat_messages").fetchone()[0]
        print(f"Tickets: {tickets}")
        print(f"Tickets with chat: {chats}")
        print(f"Total chat messages: {msgs}")
        db.close()

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
