# Session Summaries

## 2026-02-28T12:00Z - Smart backup selection + restore checkpoint rename
- `find_backup()` now auto-selects most recent uninjected backup when multiple exist (no more MultipleBackupsError)
- `list_backups()` filters out `.restore_checkpoint_` directories
- Safety copies renamed from `.g2b_backup_` to `.restore_checkpoint_`
- Added `has_restore_checkpoint()` helper
- Interactive confirmation prompt in CLI inject (skip with `-y`, `--backup`, or non-TTY)
- 7 new/updated tests in test_backup.py, 2 assertion updates in test_pipeline.py

## 2026-02-28T00:00Z - Encrypted backup support
- Implemented full encrypted backup pipeline: decrypt → inject → re-encrypt
- Added `generate_file_key()` and `encrypt_new_file()` to `EncryptedBackup` class
- Added `get_file_encryption_info()` to `ManifestDB` with UID dereference for real iOS blobs
- Added encryption-aware attachment copying and Manifest.db entry creation
- Added `_run_encrypted_pipeline()` in pipeline.py with temp-file approach
- Extracted `SMSDatabase.update_attachment_sizes()` to eliminate duplication
- Fixed fd leak: close `mkstemp` fd immediately, don't hold for pipeline duration
- 18 new tests (22 crypto + 7 pipeline encrypted), 207 total, lint clean
- Updated ARCHITECTURE.md with encrypted flow, key hierarchy, pipeline steps

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
- 207 tests across 12 test files
- conftest.py has both legacy and real-format fixtures
- Pipeline tests create full synthetic iPhone backups with sms.db, Manifest.db, plists
- Encrypted tests build synthetic keybags with low iteration counts for fast PBKDF2

## Encrypted backup notes
- Key hierarchy: password → PBKDF2 → derived key → unwrap class keys → unwrap per-file keys
- ManifestKey prefix and EncryptionKey prefix are little-endian; keybag TLVs are big-endian
- Real iOS NSKeyedArchiver blobs use `plistlib.UID` references for EncryptionKey (not inline bytes)
- iOS stores plaintext file size in MBFile blobs even for encrypted files
- Protection class 3 (`NSFileProtectionCompleteUntilFirstUserAuthentication`) is standard for SMS data
