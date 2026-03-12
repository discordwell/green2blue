# green2blue

Transfer your Android text messages to iPhone.

green2blue takes an export from the free [SMS Import/Export](https://github.com/tmo1/sms-ie) Android app and injects the messages into an iPhone backup. When you restore the backup, all your messages appear in the Messages app — SMS, MMS, group chats, photos, videos, and RCS.

## The Easy Way

Download the standalone binary for your platform from the [latest release](https://github.com/discordwell/green2blue/releases/latest). No Python required.

**macOS (Apple Silicon):**
```bash
chmod +x green2blue-macos-arm64
./green2blue-macos-arm64
```

**macOS (Intel):**
```bash
chmod +x green2blue-macos-x86_64
./green2blue-macos-x86_64
```

**Windows:**
Double-click `green2blue-windows-x86_64.exe`, or run from Command Prompt.

Running with no arguments launches an interactive wizard that guides you through the entire process.

## Step-by-Step Guide

### 1. Export from Android

Install [SMS Import/Export](https://github.com/tmo1/sms-ie) on your Android phone and export all messages. Choose **NDJSON** format and include attachments. This creates a ZIP file.

### 2. Transfer the ZIP

Get the ZIP file from your Android to your computer (email, cloud, USB, etc.).

### 3. Create an iPhone Backup

Connect your iPhone to your computer.
- **macOS**: Open Finder, select your iPhone, click "Back Up Now"
- **Windows**: Open iTunes, click the phone icon, click "Back Up Now"

For simplicity, leave "Encrypt local backup" unchecked. Encrypted backups are supported too — see below.

### 4. Run green2blue

```bash
green2blue
```

Follow the prompts: drag in your ZIP file, confirm your backup, and green2blue handles the rest. It will:
- Create a safety copy of your backup
- Parse and convert your Android messages
- Inject them into the backup
- Copy MMS attachments
- Verify integrity

Or use the CLI directly:
```bash
green2blue inject export.zip
```

### 5. Restore the Backup

- **macOS**: In Finder, click "Restore Backup" and select this backup
- **Windows**: In iTunes, click "Restore Backup" and select your backup

Your Android messages will appear in Messages.

## Installing from Source

For technical users who prefer pip:

```bash
# Basic install
pip install green2blue

# With encrypted backup support
pip install green2blue[encrypted]
```

Requires Python 3.10+. Zero runtime dependencies for the core path — only `cryptography` is needed for encrypted backup support.

### Alternative: One-Click Installer

If you have the repo but don't want to deal with Python setup:
- **macOS**: Double-click `scripts/install.command` in Finder
- **Windows**: Double-click `scripts/install.bat`

These scripts install Python if needed, create a virtual environment, install green2blue, and launch the wizard.

## Safety

- **Safety copy**: Before any modification, a full copy of your backup is created with a `.restore_checkpoint_*` suffix. If anything goes wrong, delete the modified backup and rename the safety copy.
- **Single transaction**: All database writes happen in one SQLite transaction. Any failure rolls back everything.
- **Trigger management**: iOS database triggers are dropped before injection and restored after, preventing internal function call failures.
- **Verification**: After injection, integrity checks run automatically on the database and file structure.

## Encrypted Backups

```bash
pip install green2blue[encrypted]
green2blue inject export.zip --password "your backup password"
```

Or just run `green2blue` — the wizard will prompt for your password when it detects an encrypted backup.

## Troubleshooting

**"macOS can't verify this app" / "unidentified developer"**
Right-click the file, click Open, then click Open again. Or run: `xattr -d com.apple.quarantine green2blue-macos-*`

**"Windows Defender SmartScreen prevented an unrecognized app"**
Click "More info" then "Run anyway".

**"No iPhone backups found"**
Connect your iPhone and create a backup first. On macOS use Finder; on Windows use iTunes.

**"Wrong password" on encrypted backup**
This is the local backup password you set in Finder/iTunes, not your Apple ID password.

**Messages disappear after restore**
If you sign in with an Apple ID that has iCloud Messages enabled, iCloud may remove messages it doesn't recognize. Use `--disable-icloud-sync` when injecting, or avoid signing into iCloud Messages immediately after restore.

**Non-US phone numbers**
Use `--country <code>` (e.g., `--country GB` for UK numbers). The wizard auto-detects this.

## What It Handles

- **SMS** — Standard text messages
- **MMS** — Picture/video messages with attachments, including group chats
- **RCS** — Rich Communication Services messages (treated as SMS/MMS internally)
- **Attachments** — Photos, videos, audio, VCards, and other MMS file attachments
- **Group chats** — Multi-participant conversations with proper chat grouping
- **Duplicate prevention** — Won't re-inject messages that already exist

## How It Works

1. Parses the NDJSON export from SMS Import/Export (streaming, handles large exports)
2. Normalizes phone numbers to E.164 format (supports 40+ countries)
3. Converts Android message format to iOS format (timestamps, type flags, chat grouping)
4. Creates a safety copy of your iPhone backup
5. Injects messages into `sms.db` within a single SQLite transaction
6. Copies MMS attachment files into the backup directory structure
7. Updates `Manifest.db` so iOS recognizes the new/changed files
8. Runs integrity verification on the modified backup

## CLI Reference

See [docs/CLI.md](docs/CLI.md) for the full command and flag reference.

Quick reference:
```
green2blue                     Interactive wizard
green2blue quickstart          Step-by-step guide
green2blue inject <zip>        Inject messages into a backup
green2blue list-backups        List available iPhone backups
green2blue inspect <zip>       Show export contents
green2blue verify <path>       Verify backup integrity
```

## Development

```bash
git clone https://github.com/discordwell/green2blue.git
cd green2blue
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,encrypted]"
pytest
```

### Synthetic Android Exports

Generate a test export ZIP without a physical Android device:

```bash
python scripts/generate_android_export_fixture.py /tmp/android_media.zip
python scripts/generate_android_export_fixture.py --list-scenarios
python scripts/generate_android_export_fixture.py /tmp/android_happy_path.zip --all
python scripts/generate_android_export_fixture.py /tmp/android_negative.zip \
  --all --include-negative-controls
green2blue inspect /tmp/android_media.zip
```

`--all` includes the happy-path scenarios only. Negative controls such as the
missing-attachment case are opt-in via `--include-negative-controls`.

The bundled media scenarios use real static photo/video assets, not tiny
placeholder blobs, so wet tests can verify actual attachment rendering in
Messages.

## License

MIT
