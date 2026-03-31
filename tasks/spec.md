# Task: Step 1 — Stabilize Dashboard with SQLite

## Goal
Replace the fragile `manual-overrides.json` with SQLite as the primary data store. Eliminate git merge conflicts on data files, get atomic writes, and make the pipeline more resilient.

## Current State

**Data flow today:**
```
Google Drive (HTML transcripts)
    ↓ download & parse
manual-overrides.json (1230 entries, 588KB)
    ↓ read/write every run
deposit-overview.py (2500 lines, does everything)
    ↓ generates
dashboard.html (1.3MB, pushed to GitHub Pages)
```

**Pain points:**
1. `manual-overrides.json` causes git merge conflicts on every CI run vs local run
2. OAuth token expires hourly, refresh token gets revoked — CI breaks
3. ~1000 files re-downloaded from Drive every run (only ~30 are new)
4. Partial saves: if script crashes mid-run, JSON may be half-written
5. Chat data embedded in overrides bloats the file over time

## Proposed Solution

### 1. SQLite Database (`dashboard.db`)

**Schema:**
```sql
CREATE TABLE tickets (
    ticket_key TEXT PRIMARY KEY,     -- e.g. "closed-0187_1488188450468265997"
    user_name TEXT,
    user_id TEXT,
    campaign TEXT DEFAULT 'Unknown',
    campaign_source TEXT DEFAULT '',
    status TEXT DEFAULT 'Not Approved',
    signal TEXT DEFAULT '',
    approving_admin TEXT DEFAULT '',
    has_screenshot BOOLEAN DEFAULT 0,
    ticket_date TEXT,
    first_seen_at TEXT,
    deposit_amount REAL,
    deposit_amount_source TEXT DEFAULT '',
    drive_file_id TEXT DEFAULT '',
    vision_amount_retries INTEGER DEFAULT 0,
    text_amount_tried BOOLEAN DEFAULT 0,
    reviewed_by TEXT DEFAULT '',
    updated_at TEXT
);

CREATE TABLE chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_key TEXT NOT NULL,
    author TEXT,
    content TEXT,
    is_admin BOOLEAN DEFAULT 0,
    has_attachment BOOLEAN DEFAULT 0,
    FOREIGN KEY (ticket_key) REFERENCES tickets(ticket_key)
);

CREATE TABLE state (
    key TEXT PRIMARY KEY,
    value TEXT
);
-- Stores: last_telegram_date, last_run_at, etc.
```

**Why separate `chat_messages`:** Chat data is write-once (parsed from transcript), never updated. Keeping it separate means the `tickets` table stays small and fast for the hot path.

### 2. Migration Path

1. Add `db.py` — thin wrapper: `get_ticket()`, `upsert_ticket()`, `get_chat()`, `save_chat()`, `export_json()`
2. Add migration script: reads `manual-overrides.json` → populates `dashboard.db`
3. Update `deposit-overview.py`:
   - Replace `load_manual_overrides()` → `db.load_all_tickets()`
   - Replace `save_manual_overrides()` → `db.upsert_tickets()`
   - Remove JSON read/write in hot path
4. Keep JSON export as a post-run step (backup, human-readable)
5. `.gitignore` the database file (it's generated, not source)

### 3. Git / CI Cleanup

- `.gitignore`: add `dashboard.db`, `manual-overrides.json`, `oauth-token.json`
- `dashboard.html` stays in git (it's the GitHub Pages output)
- CI workflow: download `dashboard.db` as artifact from previous run (or rebuild from Drive if missing)
- Alternative: store `dashboard.db` in a GitHub Release asset (persistent across runs)

### 4. OAuth Token Handling

- Store refresh token (not access token) in GitHub secret `GOOGLE_REFRESH_TOKEN`
- Script refreshes access token on startup using refresh token + client credentials
- No more manual token rotation needed (refresh tokens are long-lived unless revoked)

## Files Affected

| File | Change |
|------|--------|
| `db.py` (NEW) | SQLite wrapper — schema, CRUD, migration |
| `deposit-overview.py` | Replace JSON calls with db calls, ~100 lines changed |
| `.gitignore` | Add `dashboard.db`, `manual-overrides.json`, `oauth-token.json` |
| `.github/workflows/daily-update.yml` | Add db artifact upload/download step |
| `manual-overrides.json` | Becomes backup export only, gitignored |

## Risks & Trade-offs

| Risk | Mitigation |
|------|------------|
| SQLite file not persisted across CI runs | Use GitHub Actions artifacts or Release assets |
| Migration bug loses data | Run migration with validation: compare JSON vs DB counts |
| SQLite concurrent writes | Not an issue — only one process writes at a time |
| Larger code change | `db.py` is isolated; `deposit-overview.py` changes are mechanical (swap function calls) |

**What we're NOT doing in Step 1:**
- No web server / API (that's Step 2)
- No Postgres / hosted DB (overkill for now)
- No refactoring the 2500-line script into modules (separate task)

## Success Criteria

1. ✅ `dashboard.db` is the primary data store
2. ✅ `manual-overrides.json` is exported after each run (backup)
3. ✅ No more git merge conflicts on data files
4. ✅ CI runs without manual token intervention
5. ✅ All existing data preserved (migration validates counts)
6. ✅ Dashboard output identical before/after migration
