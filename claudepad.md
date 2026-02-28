# Session Summaries

## 2026-02-27T19:30Z - Format compatibility and polish session
- Fixed NDJSON parser to support real SMS IE format (`__sender_address`/`__recipient_addresses`) alongside legacy `__addresses`
- Fixed attachment `_data` path handling for real Android filesystem paths (extract basename, search in data/ dir)
- Added RCS detection and counting (`_looks_like_rcs`, `count_messages` returns `rcs` key)
- Added 16 new tests: real SMS IE format (parser + pipeline), RCS handling, Android data path resolution
- Updated README with feature list, GitHub URL, "How It Works" section
- Updated ARCHITECTURE.md with dual format docs
- Lint clean (ruff), 184 tests passing
- Code review agents launched for correctness and refactoring

# Key Findings

## iOS sms.db injection critical notes
- SQLite DDL auto-commits in Python's sqlite3 module - can't put trigger DROP in same transaction as DML
- Trigger management uses try/finally pattern for guaranteed restoration
- Chat GUID: 1:1 = `SMS;-;+phone`, group = `SMS;-;chat{sha256(sorted)[:16]}`
- Content-hash dedup: `sha256(handle_id|date|text)`
- Message GUID prefix: `green2blue:<uuid>`

## SMS Import/Export format
- Real format: `__sender_address` (object) + `__recipient_addresses` (array of objects)
- Legacy/test format: `__addresses` (array of objects) - both supported
- `_data` field contains FULL Android paths (e.g., `/data/user/0/.../PART_xxx.jpg`)
- ZIP stores attachments as `data/{basename}` (just the filename under data/)
- RCS has no explicit type marker - detected via `rcs_*` prefixed fields
- MMS `date` is in seconds; SMS `date` is in milliseconds

## Test architecture
- 184 tests across 12 test files
- conftest.py has both legacy and real-format fixtures
- Pipeline tests create full synthetic iPhone backups with sms.db, Manifest.db, plists
