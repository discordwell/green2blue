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
- **sms_db.py** — Core injection into sms.db. Handle/chat creation with dedup. Message insertion with ~35 columns. Attachment insertion. Join table management. Trigger drop/restore. Single-transaction safety.
- **manifest.py** — Update Manifest.db with new file sizes (sms.db) and new entries (attachments). Computes fileID as `SHA1('{domain}-{relativePath}')`.
- **plist_utils.py** — NSKeyedArchiver binary plist construction for MBFile objects. Clone-and-patch strategy preferred; build-from-scratch fallback.
- **attachment.py** — Copy MMS binary files from ZIP into backup directory structure. Path: `{backup}/{hash[:2]}/{hash}`.
- **prepare_sync.py** — Post-injection CK metadata reset for the iCloud sync reset workflow. Drops triggers, resets injected message/attachment/chat CK state, restores triggers.
- **crypto.py** — Encrypted backup support (optional). Keybag parsing, PBKDF2 key derivation, AES key unwrap (RFC3394), AES-256-CBC file decrypt/re-encrypt.

### Top-level
- **pipeline.py** — Orchestrates full flow: find backup → safety copy → parse → convert → inject → copy attachments → update manifest → verify.
- **verify.py** — Post-injection checks: SQLite integrity, foreign key consistency, join table consistency, attachment files exist, Manifest.db entry present.
- **cli.py** — argparse CLI with subcommands: `inject`, `list-backups`, `inspect`, `verify`, `diagnose`, `prepare-sync`. Interactive backup confirmation prompt on inject (skip with `--yes`/`-y`, `--backup`, or non-TTY stdin).
- **models.py** — All dataclasses: Android (`AndroidSMS`, `AndroidMMS`, `MMSPart`, `MMSAddress`) and iOS (`iOSMessage`, `iOSHandle`, `iOSChat`, `iOSAttachment`). Also `CKStrategy` enum (none, fake-synced, pending-upload, icloud-reset) and `generate_ck_record_id()` helper.
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
- **message.account / account_guid** = NULL for SMS (not 'p:0')
- **message.ck_record_id / ck_record_change_tag** = `''` (empty string, not NULL) for unsynced messages
- **message.was_data_detected** = 1, **has_dd_results** = 0 (iOS populates after data detection runs)
- **message.is_delivered** = 1 for both incoming and outgoing
- **message.group_title** = NULL for 1:1 (not empty string)
- **message.date_recovered** = 0
- **chat.account_login** = `'E:'` (constant for SMS accounts)
- **chat.account_id** = device UUID (auto-detected from existing chats)
- **chat.server_change_token** = `''` (empty string, not NULL)
- **chat.group_id** = generated UUID per chat
- **attachment.created_date** = Apple epoch **seconds** (NOT nanoseconds like message.date)
- **attachment.start_date** = 0
- **attachment.original_guid** = same as guid
- **attachment.preview_generation_state** = 1 (image), 2 (video), 0 (other)
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

## Design Decisions

1. **Zero runtime dependencies** for the core path. Only `cryptography` is needed for encrypted backups.
2. **Frozen dataclasses** for all models — immutability prevents accidental mutation.
3. **Clone-and-patch for MBFile plists** — Safer than building from scratch; reuses existing format.
4. **Single SQLite transaction** — All writes are atomic. Failure = full rollback.
5. **Triggers dropped during injection** — iOS triggers call internal functions that would fail.
6. **Content-hash deduplication** — `sha256(phone + timestamp + body)` prevents re-injection.
7. **Safety copy before modification** — Full backup copy (`.restore_checkpoint_` suffix) is the ultimate escape hatch.
8. **Temp file approach for encryption** — Decrypt to temp files, modify, re-encrypt. Reuses all existing SQLite-based logic unchanged.
9. **Verify before re-encrypt** — Run integrity checks on decrypted data where SQLite queries are meaningful.
