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
