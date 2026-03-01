# Session Summaries

## 2026-03-01T00:00Z - Real backup injection + comparison testing
- Injected test messages into real iOS 26.2 backup copy, compared field-by-field
- Fixed dynamic attachment schema detection (sr_ck_sync_state, preview_generation_state, original_guid)
- Fixed pipeline digest ordering: attachment size update BEFORE Manifest.db digest computation
- Fixed group_title: None instead of '' (15,877/15,884 SMS use NULL)
- Fixed date_recovered: set to 0 (all 27,054 real messages have 0)
- Fixed has_dd_results: set to 0 (iOS populates after data detection, majority have 0)
- Fixed import ordering in sms_db.py (E402 lint)
- Final comparison: 83/94 columns match, 7 inherently different, 4 iOS-generated blobs
- 258 tests pass, lint clean

## 2026-02-28T22:30Z - Field-identical iOS data matching from real backup comparison
- Compared real iOS 26.2 sms.db field-by-field against green2blue output
- Fixed 15+ field mismatches to match real iOS behavior:
  - message: version=10, account/account_guid=NULL, ck_record_id/tag='', was_data_detected=1, is_delivered=True always
  - chat: account_login='E:', server_change_token='', group_id=UUID, account_id auto-detected from existing chats
  - attachment: created_date in seconds (not ns), start_date=0, original_guid=guid, preview_generation_state
- Updated prepare_sync.py: CK field resets use '' not NULL, server_change_token '' not NULL
- Updated ARCHITECTURE.md with field matching documentation
- 258 tests pass, lint clean

## 2026-02-28T20:00Z - CloudKit metadata for iCloud Messages sync survival
- Added CKStrategy enum (none/fake-synced/pending-upload) to models.py
- Added ck_sync_state, ck_record_id, ck_record_change_tag fields to iOSMessage
- Added ck_sync_state, cloudkit_record_id fields to iOSChat
- Updated sms_db.py INSERT SQL for message and chat tables to include CK columns
- Added _apply_ck_strategy() using dataclasses.replace() in message_converter.py
- Added --ck-strategy flag to CLI inject, wired through pipeline
- Added `diagnose` subcommand (CK sync state distribution, --injected-only filter)
- Created scripts/wet_test_sync.py: 6-strategy A/B test matrix for real device testing
- 19 new tests (8 sms_db + 11 converter), 244 total, lint clean
- Code review: refactored manual dataclass reconstruction to dataclasses.replace()
- Updated ARCHITECTURE.md with CloudKit sync metadata section

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

## Real backup injection comparison results (94 message columns)
- 83 columns match perfectly between injected and real iOS SMS
- 7 inherently different (ROWID, guid, text, handle_id, date, date_read, date_delivered)
- 3 remaining are iOS-generated blobs we don't create:
  - attributedBody (NSMutableAttributedString)
  - destination_caller_id (user phone), ck_chat_id (chat GUID)
- message_summary_info is now generated (see below)
- Real iOS `has_dd_results`: mostly 0 (iOS populates after data detection runs)
- Real iOS `was_data_detected`: mostly 1 (flag that detection should run)
- Real iOS `group_title`: NULL for 1:1, subject string for MMS groups
- Real iOS `date_recovered`: always 0
- Pipeline MUST update attachment sizes BEFORE computing sms.db digest for Manifest.db

## Test architecture
- 272 tests across 13 test files
- conftest.py has both legacy and real-format fixtures
- Pipeline tests create full synthetic iPhone backups with sms.db, Manifest.db, plists
- Encrypted tests build synthetic keybags with low iteration counts for fast PBKDF2

## Encrypted backup notes
- Key hierarchy: password → PBKDF2 → derived key → unwrap class keys → unwrap per-file keys
- ManifestKey prefix and EncryptionKey prefix are little-endian; keybag TLVs are big-endian
- Real iOS NSKeyedArchiver blobs use `plistlib.UID` references for EncryptionKey (not inline bytes)
- iOS stores plaintext file size in MBFile blobs even for encrypted files
- Protection class 3 (`NSFileProtectionCompleteUntilFirstUserAuthentication`) is standard for SMS data

## Backup management gotchas
- Real sms.db has 22 triggers calling iOS internal functions (verify_chat, etc.) - MUST drop before direct SQL injection
- Finder caches backup list via a system service - renaming directories with `.hidden` suffix is NOT enough to hide backups
- Must physically move backup directories out of MobileSync/Backup/ for Finder to stop showing them
- Unplugging and replugging iPhone may be needed to refresh Finder's backup list
- Restore checkpoint + secondary backups all show same timestamp in Finder (from shared Info.plist Date field), making it impossible for users to distinguish — hide extras before restore

## message_summary_info plist structure (reverse-engineered from real iOS 26.2)
- Binary plist dict on every message with text (27,033/27,050 messages in real backup)
- Only 17 messages with NULL blob — all system events (item_type != 0) with no text
- Canonical SMS blob: `{'cmmS\x10': 0, 'cmmAO': 0}` — covers 80%+ of SMS messages
- Key meanings decoded from 27K+ message corpus:
  - `cmmS\x10`, `cmmAO` = always 0, universal metadata keys
  - `ust` = "uses shared transport" (True for iMessage, rare on SMS)
  - `amc` = associated message count (0=normal, 1=tapback-target)
  - `oui` = "originating user identifier" (SMS received, sender handle)
  - `ams` = associated message summary (truncated original text in tapbacks)
  - `ampt` = associated message part typed (NSAttributedString blob)
  - `enc` = encrypted (bool, some iMessage threads)
  - `osn` = original service name (e.g., 'iMessage' for SMS-fallback)
  - `ec` = edit corrections, `ep` = edit parts, `otr` = original text range
  - `smm` = spam ML metadata (iOS filter results on shortcodes)
  - `swybid`/`swyan` = Shared With You bundle ID / app name
  - `raa` = RCS authentication assessment
  - `uat` = unknown attachment type? (True for RCS attachment-only)
  - `rfgs` = reply-from GUIDs, `rp` = reply parts
  - `eogcd` = edit or generation count/delta
  - `amsa` = associated message Siri author, `amab` = associated message attributed body
  - `hbr` = has been replied
- plistlib key order differs from iOS but both are valid binary plists
- Schema detection (`message_summary_info in self._msg_schema`) ensures older iOS versions unaffected
- 272 tests (14 new for message_summary), lint clean

## CloudKit sync test in progress
- 6 test messages injected into main backup (00008101-000E60C43C40001E) with password `glorious1`
- Restore checkpoint moved to /tmp/ during restore (must move back after)
- Secondary backup (031457) also moved to /tmp/
- Test phones: +15550000001 through +15550000006 (Tests A-F)
- After restore+sync, run: `python scripts/wet_test_sync.py --diagnose <backup_path> --password glorious1`
