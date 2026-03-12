# CLI Reference

Full reference for all green2blue commands and options.

## Commands

### `green2blue` (no arguments)

Launches the interactive wizard when run from a terminal. Prints help when piped.

### `green2blue wizard`

Explicitly launches the interactive wizard — same as running with no arguments.

### `green2blue quickstart`

Prints a numbered step-by-step guide for the full Android-to-iPhone workflow.

### `green2blue inject <export.zip>`

Inject Android messages into an iPhone backup.

**Common options:**

| Flag | Description |
|------|-------------|
| `--backup <path\|udid>` | Specify backup (auto-detect if omitted) |
| `--password <pw>` | Backup encryption password |
| `--dry-run` | Parse and convert without modifying the backup |
| `-y, --yes` | Skip confirmation prompt |
| `-v, --verbose` | Verbose output |
| `-q, --quiet` | Minimal output |

**Advanced options:**

| Flag | Description |
|------|-------------|
| `--country <code>` | Country code for phone normalization (default: US) |
| `--ck-strategy <strategy>` | CloudKit metadata strategy: `none`, `fake-synced`, `pending-upload`, `icloud-reset` (default: none) |
| `--mode <mode>` | Injection mode: `insert`, `overwrite`, `clone` (default: insert) |
| `--sacrifice-chat <ROWID>` | Chat ROWID to sacrifice (repeatable, required for `--mode overwrite`) |
| `--service <type>` | Message service: `SMS` or `iMessage` (default: SMS) |
| `--disable-icloud-sync` | Set CloudKitSyncingEnabled=False in backup |
| `--backup-root <path>` | Override default backup directory |
| `--no-attachments` | Skip copying MMS attachment files |
| `--skip-duplicates` | Skip duplicate messages (default: on) |
| `--no-skip-duplicates` | Do not skip duplicate messages |

### `green2blue list-backups`

List all available iPhone backups.

| Flag | Description |
|------|-------------|
| `--backup-root <path>` | Override default backup directory |

### `green2blue inspect <export.zip>`

Show export contents (message counts, attachment info) without modifying anything.

### `green2blue archive <subcommand>`

Canonical archive workflows for future merge and re-render support.

| Subcommand | Description |
|------------|-------------|
| `archive import-android <zip> <archive.sqlite>` | Import an Android export into a canonical archive |
| `archive import-ios <backup> <archive.sqlite>` | Import an iPhone backup into a canonical archive |
| `archive inspect <archive.sqlite>` | Inspect a canonical archive |
| `archive merge <archive.sqlite>` | Materialize a merged cross-source view inside the archive |
| `archive report <archive.sqlite>` | Generate a migration-oriented archive report |
| `archive export-android <archive.sqlite> <output.zip>` | Export the merged archive view as an Android-style ZIP |
| `archive inject-ios <archive.sqlite>` | Export the merged view and inject it into an iPhone backup |

#### `green2blue archive import-ios <backup> <archive.sqlite>`

| Flag | Description |
|------|-------------|
| `--backup-root <path>` | Override the default backup directory when resolving a UDID |
| `--password <pw>` | Backup encryption password for encrypted iPhone backups |
| `--no-resume` | Force a new import run even if the exact same backup state was already imported |

#### `green2blue archive import-android <zip> <archive.sqlite>`

| Flag | Description |
|------|-------------|
| `--no-resume` | Force a new import run even if the exact same export ZIP was already imported |

### `green2blue corpus <subcommand>`

Privacy-safe representative Android sample capture.

| Subcommand | Description |
|------------|-------------|
| `corpus capture <zip> <output.zip>` | Build a redacted representative Android corpus ZIP |

#### `green2blue corpus capture <zip> <output.zip>`

| Flag | Description |
|------|-------------|
| `--max-per-bucket <n>` | Maximum kept messages per representative bucket |
| `--preserve-text` | Keep original message text instead of redacting it |
| `--preserve-media` | Keep original attachment bytes instead of generic replacement media |

### `green2blue verify <backup_path>`

Verify an iPhone backup's integrity after injection.

### `green2blue diagnose`

Diagnose CloudKit sync state of messages in a backup.

| Flag | Description |
|------|-------------|
| `--backup <path\|udid>` | Specify backup (auto-detect if omitted) |
| `--backup-root <path>` | Override default backup directory |
| `--password <pw>` | Backup encryption password |
| `--injected-only` | Only show green2blue-injected messages |

### `green2blue prepare-sync`

Prepare an injected backup for iCloud sync reset workflow.

| Flag | Description |
|------|-------------|
| `--backup <path\|udid>` | Specify backup |
| `--backup-root <path>` | Override default backup directory |
| `--password <pw>` | Backup encryption password |

### `green2blue device <subcommand>`

Direct device operations via USB (requires `pymobiledevice3`).

| Subcommand | Description |
|------------|-------------|
| `device list` | List connected iOS devices |
| `device doctor` | Check device readiness for backup/restore |
| `device backup` | Create a backup from a connected device |
| `device inject <zip>` | Full pipeline: backup, inject, restore |
| `device restore <path>` | Restore a modified backup to a device |

## Environment

green2blue looks for iPhone backups in the platform-specific default location:

- **macOS**: `~/Library/Application Support/MobileSync/Backup/`
- **Windows**: `%APPDATA%\Apple Computer\MobileSync\Backup\` or `%USERPROFILE%\Apple\MobileSync\Backup\`

Use `--backup-root` to override this on any command that accesses backups.

## Canonical Archive Workflow

The canonical archive is the new target-neutral storage layer for the future
merge product. Today it already supports collecting both sides of a migration
into one archive:

```bash
green2blue archive import-android android-export.zip merged.g2b.sqlite
green2blue archive import-ios /path/to/iphone-backup merged.g2b.sqlite
green2blue archive merge merged.g2b.sqlite
green2blue archive inspect merged.g2b.sqlite
green2blue archive report merged.g2b.sqlite
green2blue archive export-android merged.g2b.sqlite merged-export.zip
green2blue archive inject-ios merged.g2b.sqlite --backup <UDID>
```

You can also pass a backup UDID to `archive import-ios` instead of a full path
and combine it with `--backup-root`.

Exact-source archive imports are resumable by default. If the same Android ZIP
or same iPhone backup state is imported again, green2blue reuses the existing
completed import run instead of creating a second empty/deduped run. Use
`--no-resume` to force a fresh run.

`archive report` now includes:
- per-import-run summaries
- winner-source counts from the latest merge
- unsupported/downgraded feature markers
- metadata-only attachment counts
