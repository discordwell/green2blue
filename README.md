# green2blue

Move your texts from Android to iPhone. SMS, MMS, RCS, group chats, photos, videos — all of it.

green2blue takes an export from the free [SMS Import/Export](https://github.com/tmo1/sms-ie) app and injects it into an iPhone backup. Restore the backup, and your messages show up in Messages like they were always there.

## Quickstart

### 1. Export from Android

Install [SMS Import/Export](https://github.com/tmo1/sms-ie) on your Android phone. Export all messages as **NDJSON** with attachments. You'll get a ZIP file.

### 2. Get the ZIP to your computer

Email it, AirDrop it, USB it, cloud it — whatever works.

### 3. Make an encrypted iPhone backup

Connect your iPhone. Open **Finder** (macOS) or **iTunes** (Windows). Turn on **"Encrypt local backup"**, then click **"Back Up Now"**.

> Encrypted backups are required — iOS won't restore messages from unencrypted ones.

### 4. Install green2blue

**Easiest — download a binary** from the [latest release](https://github.com/discordwell/green2blue/releases/latest). No Python needed.

Or install with pip:
```bash
pip install green2blue
```

### 5. Run it

```bash
green2blue
```

The wizard walks you through everything. Or go direct:
```bash
green2blue inject export.zip
```

### 6. Restore the backup

Back in Finder/iTunes, click **"Restore Backup"** and pick your backup. Done.

---

## Features

**Review before import** — Let the Android owner browse and filter their export in-browser before you touch anything. Deselect conversations, search by contact or content, download a trimmed ZIP.

```bash
green2blue review export.zip
```

**Privacy-safe corpus** — Generate a representative sample ZIP with redacted text, anonymized contacts, and placeholder media. Safe to share for testing or bug reports.

```bash
green2blue corpus capture export.zip sample.zip
```

**Canonical archive** — Merge Android and iPhone message histories into a single SQLite archive. Deduplicates across sources, content-addresses attachments, and can re-export to either platform.

```bash
green2blue archive import-android export.zip archive.sqlite
green2blue archive import-ios ~/path/to/backup archive.sqlite
green2blue archive merge archive.sqlite
```

**iCloud sync protection** — iCloud Messages can wipe injected messages on sign-in. The `--disable-icloud-sync` flag prevents that.

**Dry run** — See exactly what would be injected without modifying anything:
```bash
green2blue inject export.zip --dry-run
```

## Troubleshooting

**"macOS can't verify this app"** — Right-click the file, click Open, then Open again. Or: `xattr -d com.apple.quarantine green2blue-macos-*`

**"Windows Defender SmartScreen"** — Click "More info", then "Run anyway".

**"No iPhone backups found"** — Create a backup first. Finder on macOS, iTunes on Windows.

**"Wrong password"** — This is your *local backup password* from Finder/iTunes, not your Apple ID.

**Messages disappear after restore** — iCloud Messages is wiping them. Re-run with `--disable-icloud-sync`, or don't sign into iCloud Messages right after restore.

**Non-US phone numbers** — Use `--country GB` (or whatever). The wizard auto-detects this.

## CLI Reference

```
green2blue                     Interactive wizard
green2blue inject <zip>        Inject messages into a backup
green2blue review <zip>        Browse/filter export in browser
green2blue corpus capture      Privacy-safe sample generation
green2blue archive <cmd>       Canonical archive operations
green2blue list-backups        Show available iPhone backups
green2blue inspect <zip>       Preview export contents
green2blue verify <path>       Check backup integrity
green2blue diagnose <path>     Show CloudKit sync state
```

Full CLI docs: [docs/CLI.md](docs/CLI.md)

## Development

```bash
git clone https://github.com/discordwell/green2blue.git
cd green2blue
pip install -e ".[dev]"
pytest
```

## License

MIT
