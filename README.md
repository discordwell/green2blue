# green2blue

Transfer SMS/MMS/RCS messages from Android to iPhone using exports from the [SMS Import/Export](https://github.com/tmo1/sms-ie) Android app.

Move to iOS is unreliable and proprietary tools charge money for something that should be free. This tool injects your Android messages directly into an iPhone backup's `sms.db`, so when you restore the backup, all your messages appear in the Messages app.

## Requirements

- Python 3.10+
- An Android export ZIP from **SMS Import/Export** (NDJSON format)
- A **local** (unencrypted) iPhone backup created via Finder (macOS) or iTunes (Windows)
- macOS or Windows

## Quick Start

```bash
# Install
pip install green2blue

# Or install with encrypted backup support
pip install green2blue[encrypted]

# See what's in your export
green2blue inspect export.zip

# List available iPhone backups
green2blue list-backups

# Inject messages (auto-detects backup if only one exists)
green2blue inject export.zip

# Inject into a specific backup
green2blue inject export.zip --backup <UDID-or-path>

# Dry run — parse and convert without modifying anything
green2blue inject export.zip --dry-run

# Verify a backup after injection
green2blue verify /path/to/backup
```

## Step-by-Step Guide

### 1. Export from Android

Install [SMS Import/Export](https://github.com/tmo1/sms-ie) on your Android phone and export all messages. Choose **NDJSON** format and include attachments. This creates a ZIP file.

### 2. Create an iPhone Backup

Connect your iPhone to your computer. In Finder (macOS) or iTunes (Windows), create a **local backup**. For simplicity, leave "Encrypt local backup" unchecked. If your backup is encrypted, install `green2blue[encrypted]` and use `--password`.

### 3. Transfer the Export

Get the ZIP file from your Android to your computer (email, cloud, USB, etc.).

### 4. Run green2blue

```bash
green2blue inject export.zip
```

The tool will:
- Find your iPhone backup automatically
- Create a safety copy (`.g2b_backup_*` directory)
- Parse your Android messages
- Inject them into the backup's sms.db
- Copy MMS attachments
- Update Manifest.db
- Verify integrity

### 5. Restore the Backup

In Finder or iTunes, restore the modified backup to your iPhone. Your Android messages will appear in the Messages app.

## CLI Reference

```
green2blue inject <export.zip> [options]
    --backup <path|udid>     Specify backup (auto-detect if omitted)
    --country <code>         Country code for phone normalization (default: US)
    --skip-duplicates        Skip duplicate messages (default: on)
    --no-attachments         Skip copying MMS attachment files
    --dry-run                Parse and convert without modifying the backup
    --password <pw>          Backup encryption password
    -v, --verbose            Verbose output
    -q, --quiet              Minimal output

green2blue list-backups      List available iPhone backups
green2blue inspect <zip>     Show export contents without modifying anything
green2blue verify <path>     Verify a backup's integrity
```

## Safety

- **Safety copy**: Before any modification, a full copy of your backup is created with a `.g2b_backup_*` suffix. If anything goes wrong, just delete the modified backup and rename the safety copy.
- **Single transaction**: All database writes happen in one SQLite transaction. Any failure rolls back everything.
- **Trigger management**: iOS database triggers are dropped before injection and restored after, preventing internal function call failures.
- **Verification**: After injection, integrity checks run automatically on the database and file structure.

## Encrypted Backups

For encrypted iPhone backups, install the optional `cryptography` dependency:

```bash
pip install green2blue[encrypted]
green2blue inject export.zip --password "your backup password"
```

## Development

```bash
git clone https://github.com/user/green2blue.git
cd green2blue
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,encrypted]"
pytest
```

## License

MIT
