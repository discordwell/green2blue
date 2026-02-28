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
- **backup.py** — Discover iPhone backups at platform-specific paths. Read metadata from Info.plist/Manifest.plist/Status.plist. Create safety copies. Validate backup structure.
- **sms_db.py** — Core injection into sms.db. Handle/chat creation with dedup. Message insertion with ~35 columns. Attachment insertion. Join table management. Trigger drop/restore. Single-transaction safety.
- **manifest.py** — Update Manifest.db with new file sizes (sms.db) and new entries (attachments). Computes fileID as `SHA1('{domain}-{relativePath}')`.
- **plist_utils.py** — NSKeyedArchiver binary plist construction for MBFile objects. Clone-and-patch strategy preferred; build-from-scratch fallback.
- **attachment.py** — Copy MMS binary files from ZIP into backup directory structure. Path: `{backup}/{hash[:2]}/{hash}`.
- **crypto.py** — Encrypted backup support (optional). Keybag parsing, PBKDF2 key derivation, AES key unwrap (RFC3394), AES-256-CBC file decrypt/re-encrypt.

### Top-level
- **pipeline.py** — Orchestrates full flow: find backup → safety copy → parse → convert → inject → copy attachments → update manifest → verify.
- **verify.py** — Post-injection checks: SQLite integrity, foreign key consistency, join table consistency, attachment files exist, Manifest.db entry present.
- **cli.py** — argparse CLI with subcommands: `inject`, `list-backups`, `inspect`, `verify`.
- **models.py** — All dataclasses: Android (`AndroidSMS`, `AndroidMMS`, `MMSPart`, `MMSAddress`) and iOS (`iOSMessage`, `iOSHandle`, `iOSChat`, `iOSAttachment`).
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
- 1:1: `SMS;-;+12025551234`
- Group: `SMS;-;chat{sha256(sorted_phones)[:16]}`

### Backup File Layout
Files are stored as `{backup_dir}/{SHA1[:2]}/{SHA1}` where SHA1 is computed from `{domain}-{relativePath}`.

## Design Decisions

1. **Zero runtime dependencies** for the core path. Only `cryptography` is needed for encrypted backups.
2. **Frozen dataclasses** for all models — immutability prevents accidental mutation.
3. **Clone-and-patch for MBFile plists** — Safer than building from scratch; reuses existing format.
4. **Single SQLite transaction** — All writes are atomic. Failure = full rollback.
5. **Triggers dropped during injection** — iOS triggers call internal functions that would fail.
6. **Content-hash deduplication** — `sha256(phone + timestamp + body)` prevents re-injection.
7. **Safety copy before modification** — Full backup copy is the ultimate escape hatch.
