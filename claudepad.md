# Proven Runbook

## 2026-03-11 proven wet-test path (secondary iPhone 12, iOS 26.3.1)
- This path is now confirmed end-to-end on-device.
- Preconditions:
  - Phone is at the normal home screen, paired, and unlocked.
  - Do not sign into iCloud or enable Messages in iCloud before the restore test.
  - Local backup encryption must be ON. Unencrypted restores can succeed but Messages content will not appear.
  - Keep the phone connected and do not cancel a restore because the UI looks quiet.
- Proven sequence:
  1. Start from a clean device state and complete minimal local setup to the home screen.
  2. Enable local backup encryption and keep using the same backup password for the whole run.
  3. Create a fresh encrypted baseline backup from the phone.
  4. Copy that backup to a new restore-working directory. Never modify the baseline in place.
  5. Run `green2blue inject ... --password <backup_password>` against the copied backup.
  6. Verify the modified backup before restore:
     - encrypted `Manifest.db` digest for `sms.db` must match SHA1 of the encrypted on-disk file bytes
     - injected chats must have `chat_service` rows
     - `green2blue diagnose --injected-only` should show the expected messages
  7. Restore with `idevicebackup2 restore --system --settings --remove --password <backup_password> <restore_root>`.
  8. If the phone asks for the device passcode during backup/restore authorization, enter the iPhone passcode, not the backup password.
  9. After reboot/setup, open Messages and search for the test marker string (for this session: `CLAUDEUS`).
- Known-good artifact from the successful run:
  - restore root: `.live_restore_roots/20260311_043505/`
  - injected export: `.live_test_exports/20260311_041359/claudeus_encrypted_round2.zip`
  - commit with the fix set: `f03a4018f`

## 2026-03-11 root causes fixed
- Encrypted backup manifest digests must use the SHA1 of the encrypted file bytes, not plaintext bytes. Plaintext digesting caused restore failure `MBErrorDomain/205`.
- Chats also need auxiliary indexing in `chat_service`. Without that, Messages can show unread-badge counts while hiding the actual threads from the conversation list.
- `green2blue` device handling now surfaces real backup/restore readiness errors better, but `idevicebackup2` remains the proven live restore tool for this device/iOS combination.

# Session Summaries

## 2026-03-11T04:45Z - Encrypted restore path proven on-device
- Confirmed the full encrypted backup workflow on the secondary iPhone 12 running iOS 26.3.1
- Root cause of `MBErrorDomain/205` was wrong digest basis in encrypted `Manifest.db`: SHA1 had to be computed over ciphertext, not plaintext
- Root cause of "badge but no visible threads" was missing `chat_service` rows for injected chats
- Added `chat_service` backfill in `sms_db.py` and verification coverage so this fails offline instead of on-device
- `idevicebackup2` clean control restore succeeded first; modified encrypted restore then succeeded with visible `CLAUDEUS` messages
- Device doctor / backup-state handling improved, but live restore proof came from `idevicebackup2`
- 503 tests pass, plus wet-test success on the repaired restore image

## 2026-03-03T19:00Z - CLONE injection mode (Hack Patrol approach)
- Added `InjectionMode.CLONE` — third mode alongside INSERT and OVERWRITE
- Faithfully reproduces Hack Patrol (2022) single-SMS injection technique
- Clones last existing incoming SMS message, inheriting ALL columns including CK metadata
- Key Hack Patrol choices flagged with `# HACK_PATROL_NOTE:` comments:
  - No trigger dropping (will fail on real sms.db with iOS triggers)
  - CK metadata duplicated (same ck_record_id on all cloned messages)
  - message_summary_info inherited, not generated per-message
  - Chat GUID "SMS;-;" prefix (not "any;-;"), is_filtered=1
  - Plain UUID message GUID (no "green2blue:" prefix)
  - Binary template attributedBody (128 UTF-16 limit, fallback for longer)
- New files: `tests/test_clone.py` (48 tests across 12 classes)
- New classes: `CloneStats`, `CloneSourceError`, `_build_hackpatrol_attributed_body()`
- Pipeline + CLI fully integrated (both encrypted/unencrypted paths)
- 479 tests (48 new), lint clean

## 2026-03-01T14:00Z - iCloud sign-in kills restored messages; overwrite POC built
- Root cause confirmed: CK reconciliation during Apple ID sign-in wipes all local messages
- Without Apple ID: 104K messages load fine from pristine backup sms.db
- Unencrypted backups don't restore messages (Apple gates SMS behind encryption flag)
- Built `scripts/wet_test_overwrite.py` — UPDATE existing messages instead of INSERT
- CK metadata analysis: all messages have unique ck_record_id (64-char hex), base-36 change tags
- `CloudKitSyncingEnabled` flag found in `com.apple.madrid.plist` — can disable in backup
- Organized pristine backups in `.pristine_backups/` by date, cleaned up ~160GB of old copies
- IsFullBackup finding: Apple sets False on all backups; True = full wipe (NOT what we want)
- Next: test if `CloudKitSyncingEnabled=False` in backup survives Apple ID sign-in

## 2026-03-01T08:40Z - Fixed Manifest.db digest corruption + missing directory entries
- Two critical bugs found during backup restore investigation:
  1. Digest corruption: _try_raw_patch blind byte search matched Size/LastModified inside SHA1 digest data
  2. Missing flags=2 directory entries for injected attachment parent paths
- Fix: digest patching now always uses plistlib roundtrip; raw patching only for Size/LastModified
- Fix: _ensure_directory_entries creates flags=2 entries for all parent paths
- Extracted shared extract_mbfile_digest() to plist_utils.py (eliminated 3x duplication)
- Removed dead code (_replace_data_value, unused new_digest param in _try_raw_patch)
- Fixed non-atomic commit in _ensure_directory_entries
- 330 tests (11 new), lint clean

## 2026-03-01T08:00Z - Closed all iOS-generated field gaps
- Implemented destination_caller_id, ck_chat_id, account, account_guid auto-detection
- All auto-detect from most frequent values in existing messages (parallels _detect_account_id pattern)
- ck_chat_id derived from chat GUID: replace `any;-;` prefix with `{service};-;`
- Added `ck_chat_id TEXT` column to test schemas
- Final comparison: 85/97 match, 7 inherently different, 3 CK sync (by design), 2 correct-as-is
- 319 tests (8 new), lint clean
- All iOS-generated fields now closed: attributedBody, message_summary_info, destination_caller_id, ck_chat_id, account, account_guid

## 2026-03-01T06:00Z - Reverse-engineered attributedBody typedstream format
- Analyzed 26,891 real iOS 26.2 attributedBody blobs from sms.db backup
- Format is Apple typedstream (NSArchiver), NOT NSKeyedArchiver binary plist
- Two variants: NSAttributedString (36%) and NSMutableAttributedString (64%) - both accepted by iOS
- Built generator using compact NSAttributedString variant - 100% byte-identical match on 7,499 simple messages
- Only 2 mismatches in 7,501 tested (game invite + multi-part attachment - not generated by green2blue)
- Integrated into sms_db.py _insert_message with dynamic schema detection
- 39 new tests (7 int encoding + 26 blob generation + 6 injection), 311 total, lint clean
- Updated ARCHITECTURE.md with attributedBody documentation

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
- 85 columns match perfectly between injected and real iOS SMS
- 7 inherently different (ROWID, guid, text, handle_id, date, date_read, date_delivered)
- 3 intentionally different: ck_sync_state/ck_record_id/ck_record_change_tag (unsynced by design)
- 2 correct-as-is: ck_chat_id (different conversation, same format), fallback_hash (89.7% NULL)
- All iOS-generated fields now populated: attributedBody, message_summary_info, destination_caller_id, ck_chat_id, account, account_guid
- Real iOS `has_dd_results`: mostly 0 (iOS populates after data detection runs)
- Real iOS `was_data_detected`: mostly 1 (flag that detection should run)
- Real iOS `group_title`: NULL for 1:1, subject string for MMS groups
- Real iOS `date_recovered`: always 0
- Pipeline MUST update attachment sizes BEFORE computing sms.db digest for Manifest.db

## Test architecture
- 311 tests across 14 test files
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
- **IsFullBackup: Apple/Finder sets `False` on ALL backups for this device.** `True` = full wipe-and-restore; `False` = partial overlay (no wipe). Previous sessions manually set `True` which likely caused restore failures. Codebase does NOT set this flag. NEVER set to `True` without explicit reason.

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

## attributedBody typedstream format (reverse-engineered from real iOS 26.2)
- Apple typedstream v4 format (header: `04 0b 'streamtyped' 81 e8 03`), system version 1000
- NOT NSKeyedArchiver — this is the older NSArchiver binary serialization
- Class hierarchy: NSAttributedString > NSObject, with NSString storing text as UTF-8
- Simple messages have single attribute run: `__kIMMessagePartAttributeName = NSNumber(0)`
- Typedstream integer encoding: 0-127 = single byte, 128-32767 = `0x81` + u16 LE, 32768+ = `0x82` + u32 LE
- String length field encodes UTF-8 byte count; iI fields encode (run_count, UTF-16_code_units)
- NSString.length = UTF-16 code units (BMP chars = 1, astral/emoji = 2 per surrogate pair)
- Two variants in real data: NSAttributedString (compact, 177+ bytes) and NSMutableAttributedString (+48 bytes overhead)
- Complex messages (URLs, dates, money, phone numbers) have extra attribute runs with DDScannerResult NSData blobs — iOS regenerates these via data detection after restore, so simple form is correct
- Cache indices shift between variants: non-mutable uses 0x94/0x96/0x99 for NSObject/'+'/i refs; mutable uses 0x95/0x98/0x9b
- Schema detection: `attributedBody in self._msg_schema` ensures older iOS versions unaffected
- The 868686 trailer = three nested end-of-object markers (NSDictionary, attribute run, NSAttributedString)

## CloudKit sync test in progress
- 6 test messages injected into main backup (00008101-000E60C43C40001E) with password `glorious1`
- Restore checkpoint moved to /tmp/ during restore (must move back after)
- Secondary backup (031457) also moved to /tmp/
- Test phones: +15550000001 through +15550000006 (Tests A-F)
- After restore+sync, run: `python scripts/wet_test_sync.py --diagnose <backup_path> --password glorious1`
