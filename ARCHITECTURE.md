# Architecture

## System Overview

green2blue converts Android SMS/MMS exports into iOS Messages database format and injects them into iPhone backups. The tool operates on local backup files — it never touches a live iPhone.

```
┌──────────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────────┐
│  Android ZIP │────>│    Parser    │────>│   Converter   │────>│  iOS Backup  │
│  (NDJSON)    │     │              │     │               │     │  Injector    │
└──────────────┘     └──────────────┘     └───────────────┘     └──────────────┘
                      AndroidSMS/MMS       iOSMessage/Chat       sms.db writes
                      dataclasses          dataclasses           Manifest.db
                                                                 attachment files
```

## Module Responsibilities

### `parser/`
- **zip_reader.py** — Extract and validate SMS Import/Export ZIP archives. Ensures `messages.ndjson` exists.
- **ndjson_parser.py** — Stream-parse NDJSON line by line. Classifies records as SMS (has `body`+`address`) or MMS (has `__parts`+`__sender_address`/`__recipient_addresses` or legacy `__addresses`). Detects RCS messages heuristically. Yields frozen dataclasses.

### `converter/`
- **phone.py** — E.164 phone normalization without external dependencies. Country calling code table for 40+ countries. Handles parentheses, dashes, spaces, dots, short codes.
- **timestamp.py** — Android epoch milliseconds ↔ iOS CoreData nanoseconds (since 2001-01-01). Formula: `ios_ns = (unix_ms / 1000 - 978307200) * 1_000_000_000`.
- **message_converter.py** — Android models → iOS models. Groups messages into conversations by normalized phone. Generates UUIDs. Maps Android type codes to iOS booleans. MIME→UTI mapping for attachments.

### `ios/`
- **backup.py** — Discover iPhone backups at platform-specific paths. Read metadata from Info.plist/Manifest.plist/Status.plist. Smart auto-selection: picks the most recent uninjected backup when multiple exist. Creates `.restore_checkpoint_` safety copies before modification. Filters out checkpoint directories from backup listings. Validate backup structure.
- **sms_db.py** — Core injection into sms.db. Handle/chat creation with dedup (composite key `(id, service)` allows same phone as both SMS and iMessage). Message insertion with ~35 columns. Overwrite mode: UPDATE existing sacrifice messages preserving CK metadata. Attachment insertion. Join table management. Trigger drop/restore. Single-transaction safety. Generates `message_summary_info` and `attributedBody` blobs. Detection queries (`_detect_account_id`, etc.) are parameterized by service.
- **attributed_body.py** — Generate `attributedBody` typedstream blobs (Apple NSArchiver format, NOT NSKeyedArchiver). Every iOS message with text has this blob; it contains an NSAttributedString with the text and `__kIMMessagePartAttributeName = 0` attribute. Uses the compact NSAttributedString (non-mutable) variant. Verified byte-identical against real iOS 26.2 sms.db (100% match on 7,499+ simple messages). Schema dynamically detected.
- **message_summary.py** — Generate `message_summary_info` binary plist blobs. Every iOS message with text has this blob; it contains metadata keys (`cmmS\x10`, `cmmAO`, etc.). For SMS messages, the minimal blob `{'cmmS\x10': 0, 'cmmAO': 0}` matches 80%+ of real iOS messages. The schema is dynamically detected so older iOS versions without this column are unaffected.
- **manifest.py** — Update Manifest.db with new file sizes (sms.db) and new entries (attachments). Computes fileID as `SHA1('{domain}-{relativePath}')`. Creates `flags=2` directory entries for all parent paths of injected attachments (required by iOS restore).
- **plist_utils.py** — NSKeyedArchiver binary plist construction for MBFile objects. Uses plistlib roundtrip for digest patching (avoiding raw byte search corruption). Raw patching used only for Size/LastModified when no digest change needed.
- **attachment.py** — Copy MMS binary files from ZIP into backup directory structure. Path: `{backup}/{hash[:2]}/{hash}`.
- **prepare_sync.py** — Post-injection CK metadata reset for the iCloud sync reset workflow. Drops triggers, resets injected message/attachment/chat CK state, restores triggers.
- **crypto.py** — Encrypted backup support (optional). Keybag parsing, PBKDF2 key derivation, AES key unwrap (RFC3394), AES-256-CBC file decrypt/re-encrypt.
- **device.py** — Direct device communication via pymobiledevice3 (optional). USB backup creation, restore with correct flags, synthetic backup push. Lazy imports for optional dependency.
- **mbdb.py** — Manifest.mbdb binary format (version 2.4) for synthetic/partial backups. MbdbRecord serialization, SyntheticBackup builder with auto-generated plists and keybag.

### Top-level
- **pipeline.py** — Orchestrates full flow: find backup → safety copy → parse → convert → inject → copy attachments → update manifest → verify.
- **verify.py** — Post-injection checks: SQLite integrity, foreign key consistency, join table consistency, attachment files exist, Manifest.db entry present.
- **cli.py** — argparse CLI with subcommands: `inject`, `list-backups`, `inspect`, `verify`, `diagnose`, `prepare-sync`, `device`, `wizard`, `quickstart`. Smart no-args behavior: launches wizard on TTY, suggests `inject` for bare .zip args. Inject args split into "Common options" and "Advanced options" groups. Interactive backup confirmation on inject (skip with `--yes`/`-y`, `--backup`, or non-TTY stdin).
- **wizard.py** — Interactive guided flow for non-technical users. Steps: welcome → ZIP drag-and-drop → inspect → country detection → backup selection → encryption handling → confirm → inject → next steps. Auto-detects country from phone number pre-scan.
- **models.py** — All dataclasses: Android (`AndroidSMS`, `AndroidMMS`, `MMSPart`, `MMSAddress`) and iOS (`iOSMessage`, `iOSHandle`, `iOSChat`, `iOSAttachment`). Also `InjectionMode` enum (insert, overwrite), `CKStrategy` enum (none, fake-synced, pending-upload, icloud-reset), and `generate_ck_record_id()` helper.
- **exceptions.py** — Hierarchy with user-friendly `hint` attributes.

## Data Flow

### SMS Import/Export NDJSON Format
```json
{"address":"+12025551234","body":"Hello!","date":"1700000000000","type":"1","read":"1"}
```

MMS records have `__parts` (text/binary) and address fields. The real SMS IE format uses `__sender_address` (object) + `__recipient_addresses` (array):
```json
{"date":"1700000000","msg_box":"1","__parts":[...],"__sender_address":{...},"__recipient_addresses":[...]}
```

Legacy format uses `__addresses` (array) — both are supported:
```json
{"date":"1700000000","msg_box":"1","__parts":[...],"__addresses":[...]}
```

RCS messages appear as regular SMS or MMS records with no special type marker. They can be detected via `rcs_*` prefixed fields (vendor extension).

### iOS sms.db Schema (key tables)
- **handle** — Contact identifiers (E.164 phone numbers)
- **chat** — Conversations. `style=45` for 1:1, `style=43` for group.
- **message** — Individual messages with ~80 columns
- **attachment** — Binary file metadata
- **chat_handle_join** — Links chats to participants
- **chat_message_join** — Links messages to conversations
- **message_attachment_join** — Links attachments to messages

### Chat GUID Format
- 1:1: `any;-;+12025551234`
- Group: `any;-;chat{sha256(sorted_phones)[:16]}`

Real iOS 26.2+ uses the `any;-;` prefix for all SMS chats (confirmed from 3,151 chats in a real backup).

### Field Matching (Real iOS 26.2 Comparison)
Injection output is validated against real iOS backup data. Key field mappings:
- **message.version** = 10 (not 1)
- **message.account** = `P:+{owner_phone}` (auto-detected from most frequent value in existing messages; 81% of real SMS). Falls back to NULL if no existing messages.
- **message.account_guid** = device UUID (auto-detected from existing messages; consistent across all messages on a device). Falls back to NULL if no existing messages.
- **message.ck_record_id / ck_record_change_tag** = `''` (empty string, not NULL) for unsynced messages
- **message.was_data_detected** = 1, **has_dd_results** = 0 (iOS populates after data detection runs)
- **message.is_delivered** = 1 for both incoming and outgoing
- **message.group_title** = NULL for 1:1 (not empty string)
- **message.attributedBody** = typedstream (NSArchiver) blob containing NSAttributedString with `__kIMMessagePartAttributeName = 0`; NULL for attachment-only messages. Uses non-mutable variant. iOS regenerates detected-data attributes (URLs, dates, money) after restore.
- **message.message_summary_info** = binary plist `{'cmmS\x10': 0, 'cmmAO': 0}` for messages with text; NULL for attachment-only or system messages
- **message.destination_caller_id** = device owner's E.164 phone number (auto-detected from most frequent value in existing messages; NULL if no existing messages)
- **message.ck_chat_id** = `{service};-;{chat_identifier}` — derived from chat GUID by replacing `any;-;` prefix with the message's service (e.g., `SMS;-;+12025551234` for 1:1, `SMS;-;chat{hash}` for group)
- **message.date_recovered** = 0
- **chat.account_login** = `'E:'` (SMS) or `'e:'` (iMessage)
- **chat.account_id** = device UUID (auto-detected from existing chats)
- **chat.server_change_token** = `''` (empty string, not NULL)
- **chat.group_id** = generated UUID per chat
- **attachment.created_date** = Apple epoch **seconds** (NOT nanoseconds like message.date)
- **attachment.start_date** = 0
- **attachment.original_guid** = same as guid
- **attachment.preview_generation_state** behaves like a processing-state field, not a media-type enum. For imported local attachments, green2blue prefers real same-service attachment templates and preserves their preview state when possible; the fallback for new file-backed media is `0`, which matches the dominant real SMS/MMS shape in the reference backup.
- **attachment.user_info** is attachment transport metadata when present (commonly MMCS/iCloud or RCS binary plist data), not rendering metadata. For imported local attachments, green2blue clears inherited `user_info` instead of cloning stale transport records from template rows.
- Attachments stored in **MediaDomain** (not HomeDomain)
- Attachment paths: two-level hex subdirs `{hash[:2]}/{hash[2:4]}/{UUID}/{filename}`

### Backup File Layout
Files are stored as `{backup_dir}/{SHA1[:2]}/{SHA1}` where SHA1 is computed from `{domain}-{relativePath}`.

## Encrypted Backup Flow

When a backup is encrypted (`IsEncrypted=True` in Manifest.plist), green2blue uses a decrypt-modify-re-encrypt strategy:

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Unlock Keybag  │────>│  Decrypt to Temp │────>│   Inject (same   │
│  (PBKDF2 + AES  │     │  Manifest.db +   │     │   as unencrypted │
│   key unwrap)   │     │  sms.db          │     │   path)          │
└─────────────────┘     └──────────────────┘     └────────┬─────────┘
                                                          │
┌─────────────────┐     ┌──────────────────┐              │
│  Write back to  │<────│  Re-encrypt      │<─────────────┘
│  backup dir     │     │  modified files  │
└─────────────────┘     └──────────────────┘
```

### Key Hierarchy

```
User password
  └─ PBKDF2-SHA256 (dpsl/dpic, iOS 10.2+)
      └─ PBKDF2-SHA1 (salt/iterations)
          └─ 32-byte derived key
              └─ AES key unwrap (RFC 3394)
                  └─ Class keys (per protection class)
                      └─ AES key unwrap
                          └─ Per-file keys (AES-256-CBC, zero IV)
```

- **Keybag**: Binary TLV blob in `Manifest.plist > BackupKeyBag`. Contains wrapped class keys.
- **Class keys**: Indexed by protection class (1-11). Class 3 (`NSFileProtectionCompleteUntilFirstUserAuthentication`) is standard for SMS data.
- **Per-file keys**: Each file has a unique AES-256 key, wrapped with its class key. Stored as `EncryptionKey` in the MBFile blob in Manifest.db.
- **ManifestKey**: Special per-file key for Manifest.db itself, stored in `Manifest.plist`.

### Encrypted Pipeline Steps

1. Parse keybag from `Manifest.plist`
2. Derive encryption key from password (PBKDF2, two rounds)
3. Unwrap class keys using derived key
4. Decrypt Manifest.db → temp file (using ManifestKey)
5. Read sms.db's EncryptionKey + ProtectionClass from Manifest.db
6. Decrypt sms.db → temp file
7. Create safety copy of entire backup
8. Inject messages into temp sms.db (identical to unencrypted path)
9. Copy+encrypt attachments (each gets a fresh random per-file key)
10. Update Manifest.db entries (sms.db size, attachment entries with encryption keys)
11. Verify integrity on decrypted temp files
12. Re-encrypt sms.db → backup
13. Re-encrypt Manifest.db → backup
14. Clean up temp files

### New Attachment Encryption

Each new attachment file gets a fresh random AES-256 key:
1. Generate 32 random bytes
2. Wrap with the class key via AES key wrap
3. Prepend 4-byte little-endian protection class prefix
4. Store wrapped key blob as `EncryptionKey` in the Manifest.db MBFile entry
5. Encrypt attachment data with the unwrapped key (AES-256-CBC, zero IV, PKCS7)
6. Store the **plaintext** file size in the MBFile blob (matching iOS convention)

## CloudKit Sync Metadata

iCloud Messages sync uses CloudKit metadata columns in sms.db to track which messages have been synced to the cloud. When a backup is restored with iCloud Messages enabled, iOS reconciles local messages against cloud state — messages with `ck_sync_state=0` and no CloudKit record ID may be deleted during reconciliation.

### Service Type (`--service`)

The `--service` flag controls whether injected messages appear as green SMS bubbles or blue iMessage bubbles. Default is `SMS`.

| Field | SMS | iMessage |
|-------|-----|----------|
| `message.service` | `"SMS"` | `"iMessage"` |
| `handle.service` | `"SMS"` | `"iMessage"` |
| `chat.service_name` | `"SMS"` | `"iMessage"` |
| `chat.account_login` | `"E:"` | `"e:"` |
| `message.ck_chat_id` | `"SMS;-;+phone"` | `"iMessage;-;+phone"` |
| `message_summary_info` | `{cmmS\x10: 0, cmmAO: 0}` | `{cmmS\x10: 0, cmmAO: 0, ust: True}` |

Chat GUIDs are unchanged — all chats use the `any;-;` prefix regardless of service.

Handles are deduped by composite key `(id, service)`, so the same phone number can exist as both an SMS and iMessage handle in sms.db. This matches real iOS behavior.

### Overwrite Mode (`--mode overwrite`)

Instead of INSERTing new message rows, overwrite mode UPDATEs existing "sacrifice" messages. This preserves the original ROWIDs, GUIDs, and CloudKit metadata (`ck_sync_state`, `ck_record_id`, `ck_record_change_tag`), making injected messages indistinguishable from real iOS messages to CloudKit reconciliation.

**Usage:** `green2blue inject export.zip --mode overwrite --sacrifice-chat 86753 --sacrifice-chat 53849`

**Flow:**
1. User specifies sacrifice chat ROWIDs (conversations to repurpose)
2. Messages from those chats form a "sacrifice pool" (oldest first)
3. If pool < messages to inject, `InsufficientSacrificeError` is raised
4. For each Android message, a sacrifice message is popped and:
   - Content columns are UPDATEd: `text`, `attributedBody`, `message_summary_info`, `date`, `handle_id`, direction flags
   - CK columns are **preserved**: `ck_sync_state`, `ck_record_id`, `ck_record_change_tag`
   - ROWID and original GUID are preserved (no `green2blue:` prefix)
   - `chat_message_join` is updated to move the message to the target chat
   - Old attachment joins are removed, new ones inserted
5. Handles and chats for Android contacts are created as needed (same as INSERT mode)

### iCloud Sync Disable (`--disable-icloud-sync`)

Flips `CloudKitSyncingEnabled = False` in `com.apple.madrid.plist` inside the backup. After restore, iCloud Messages is disabled, preventing CloudKit from wiping local messages during Apple ID sign-in.

**Implementation:** Computes `fileID = SHA1("HomeDomain-Library/Preferences/com.apple.madrid.plist")`, reads the plist, sets the flag, re-serializes, updates the Manifest.db entry with new size/digest. Works for both encrypted and unencrypted backups.

### CK Strategy Options (`--ck-strategy`)

- **none** (default) — No CK metadata. Messages get `ck_sync_state=0`, no record IDs. Current behavior; at risk with iCloud Messages.
- **fake-synced** — Pretend already synced. Sets `ck_sync_state=1`, generates deterministic 64-char hex record IDs (`sha256(guid:salt)`), sets `ck_record_change_tag="1"`. Applied to both messages and chats.
- **pending-upload** — Signal needs upload. Sets `ck_sync_state=0` with record IDs but no change tags. May trigger iOS to upload messages to iCloud rather than delete them.

### Test Matrix Script (`scripts/wet_test_sync.py`)

Injects 6 test messages with different CK strategies (A through F) into a single backup for A/B testing on a real device. After restore + iCloud sync, the `--diagnose` flag checks which messages survived to determine the winning strategy.

### iCloud Reset Strategy (`--ck-strategy icloud-reset`)

The most reliable approach for iCloud Messages survival. Instead of trying to trick CloudKit reconciliation, this strategy works *with* iOS's merge behavior:

1. Inject messages with clean CK state (`ck_sync_state=0`, no record IDs)
2. After injection, `prepare_sync()` clears `server_change_token` on affected chats to force full reconciliation
3. User disables iCloud Messages on device before restoring
4. Restore backup via Finder
5. Re-enable iCloud Messages → iOS performs **bidirectional merge**: uploads local messages to iCloud, downloads cloud messages to device

Messages with `ck_sync_state=0` and no CK record IDs look like "new local messages" and get uploaded rather than deleted.

### Prepare-Sync Subcommand

`green2blue prepare-sync` post-processes an already-injected backup for the iCloud reset workflow. It:
- Resets CK metadata on injected messages (`green2blue:` GUID prefix) → `ck_sync_state=0`, clears record IDs
- Resets CK metadata on injected attachments (`green2blue-att:` prefix)
- Clears `server_change_token` on chats containing injected messages (forces full reconciliation)
- Resets CK state on pure-injected chats (only green2blue messages — safe, no cloud counterpart)
- Preserves CK state on mixed chats (pre-existing + injected — prevents duplicate conversations)

This is useful for re-preparing a backup that was injected with a different CK strategy (e.g., `fake-synced`).

### Diagnose Subcommand

`green2blue diagnose` inspects a backup's CK sync state distribution, showing which messages are at risk. Supports `--injected-only` to filter for green2blue messages and `--password` for encrypted backups.

## Direct Device Communication

### Overview

The `device` module (`ios/device.py`) enables direct USB backup/restore operations via pymobiledevice3, eliminating the need for Finder/iTunes. This is an optional dependency (`pip install green2blue[device]`).

### Synthetic Backup Format (Manifest.mbdb)

The `ios/mbdb.py` module implements the version 2.4 binary manifest format used by TrollRestore/Nugget for partial/overlay restores:

```
Manifest.mbdb binary layout:
  Header: b"mbdb\x05\x00"
  Records: [domain, filename, link, hash, key, mode, inode, uid, gid, mtime, atime, ctime, size, flags, properties...]
  Strings: 2-byte BE length prefix, 0xFFFF = NULL
```

`SyntheticBackup` class generates a complete backup directory: `Manifest.mbdb` + `Status.plist` (Version=2.4) + `Info.plist` + `Manifest.plist` (with static BackupKeyBag).

### Critical Restore Flags

The key discovery from idevicebackup2 issue #1504: `RemoveItemsNotRestored=True` is required for iOS to run post-restore data migration that makes sms.db usable.

```
Full restore (SMS works):     system=True, settings=True, remove=True, reboot=True
Partial/overlay (experimental): system=True, remove=False, reboot=True
```

`remove=True` triggers iOS data migration — without it, restored sms.db entries are invisible in Messages.app.

### Two Restore Modes

1. **Full restore** (`device inject` / `device restore`): Creates a full backup, injects, restores with `remove=True`. SMS data migration runs. Reliable but slow.
2. **Synthetic push** (`push_synthetic_backup`): Overlay files without deleting existing data. Uses `remove=False`. Experimental — SMS may not work due to missing data migration.

### CLI Subcommands

- `green2blue device list` — Show connected devices
- `green2blue device backup` — Create backup from device
- `green2blue device inject <zip>` — Full automated pipeline (backup → inject → restore)
- `green2blue device restore <path>` — Restore an already-modified backup

### Lazy Imports

pymobiledevice3 is imported inside functions, not at module level. This keeps the core tool working without the dependency installed. `check_pymobiledevice3()` raises `DeviceDependencyError` with an install hint.

## Design Decisions

1. **Zero runtime dependencies** for the core path. Only `cryptography` is needed for encrypted backups, `pymobiledevice3` for direct device operations.
2. **Frozen dataclasses** for all models — immutability prevents accidental mutation.
3. **Clone-and-patch for MBFile plists** — Safer than building from scratch; reuses existing format.
4. **Single SQLite transaction** — All writes are atomic. Failure = full rollback.
5. **Triggers dropped during injection** — iOS triggers call internal functions that would fail.
6. **Content-hash deduplication** — `sha256(phone + timestamp + body)` prevents re-injection.
7. **Safety copy before modification** — Full backup copy (`.restore_checkpoint_` suffix) is the ultimate escape hatch.
8. **Temp file approach for encryption** — Decrypt to temp files, modify, re-encrypt. Reuses all existing SQLite-based logic unchanged.
9. **Verify before re-encrypt** — Run integrity checks on decrypted data where SQLite queries are meaningful.
10. **Interactive wizard for non-technical users** — No-args entry point guides through the full workflow with drag-and-drop ZIP input, automatic country detection, and platform-aware next-step instructions.

## Distribution

### Standalone Binaries
PyInstaller spec (`green2blue.spec`) produces single-file executables for macOS (arm64/x86_64) and Windows (x86_64). Bundled with `cryptography` for encrypted backup support. GitHub Actions `release.yml` builds on tag push and attaches binaries to GitHub Releases.

### Platform Installers
- `scripts/install.command` (macOS) — Double-clickable Finder script. Installs Python via Homebrew if needed, creates venv, installs green2blue, creates Desktop launcher, launches wizard.
- `scripts/install.bat` (Windows) — Double-clickable batch script. Checks for Python, creates venv, installs, launches wizard.
