"""Command-line interface for green2blue."""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from green2blue import __version__
from green2blue.exceptions import Green2BlueError

if TYPE_CHECKING:
    from green2blue.ios.backup import BackupInfo


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _DeviceRunArtifacts:
    run_dir: Path
    log_path: Path
    metadata_path: Path
    mobiledevice_log_path: Path


def _default_device_run_root() -> Path:
    """Directory for timestamped live device operation bundles."""
    return Path.cwd() / ".live_device_runs"


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _capture_mobiledevice_logs(output_path: Path, started_at: datetime) -> None:
    """Persist host-side MobileDevice/usbmux logs for the live run."""
    start_text = started_at.strftime("%Y-%m-%d %H:%M:%S")
    predicate = (
        'process == "usbmuxd" || process CONTAINS "AMPDevice" || '
        'subsystem CONTAINS "MobileDevice"'
    )

    try:
        result = subprocess.run(
            [
                "log",
                "show",
                "--style",
                "compact",
                "--start",
                start_text,
                "--predicate",
                predicate,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        output_path.write_text("The macOS 'log' command is not available on this host.\n")
        return

    content = result.stdout
    if result.stderr:
        content = f"{content}\n[stderr]\n{result.stderr}"
    if result.returncode != 0 and not content.strip():
        content = f"log show exited with status {result.returncode}\n"
    output_path.write_text(content)


@contextmanager
def _device_run_session(command: str, metadata: dict[str, object]):
    """Capture logs and metadata for a live device operation."""
    started_at = datetime.now().astimezone()
    run_root = _default_device_run_root()
    run_root.mkdir(parents=True, exist_ok=True)
    run_dir = run_root / f"{started_at.strftime('%Y%m%d_%H%M%S')}_{command}"
    run_dir.mkdir(parents=True, exist_ok=True)

    artifacts = _DeviceRunArtifacts(
        run_dir=run_dir,
        log_path=run_dir / "green2blue.log",
        metadata_path=run_dir / "metadata.json",
        mobiledevice_log_path=run_dir / "mobiledevice.log",
    )

    file_handler = logging.FileHandler(artifacts.log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)

    status = "completed"
    error_text = ""

    logger.debug("Starting device run bundle at %s", run_dir)

    try:
        yield artifacts
    except Exception as exc:
        status = "failed"
        error_text = f"{type(exc).__name__}: {exc}"
        logger.exception("Device run failed")
        raise
    finally:
        root_logger.removeHandler(file_handler)
        file_handler.close()

        final_metadata = dict(metadata)
        final_metadata.update({
            "command": command,
            "cwd": str(Path.cwd()),
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now().astimezone().isoformat(),
            "status": status,
        })
        if error_text:
            final_metadata["error"] = error_text

        _write_json(artifacts.metadata_path, final_metadata)
        _capture_mobiledevice_logs(artifacts.mobiledevice_log_path, started_at)
        print(f"\nRun artifacts: {run_dir}")


def _format_progress_update(label: str, pct: float) -> str:
    return f"  {label} progress: {pct:.1f}%"


def _format_progress_heartbeat(
    label: str,
    pct: float | None,
    last_update_age: float | None,
    total_age: float,
) -> str:
    if pct is None or last_update_age is None:
        return f"  {label} heartbeat: waiting for progress callbacks ({total_age:.0f}s elapsed)"
    return f"  {label} heartbeat: {pct:.1f}% (last update {last_update_age:.0f}s ago)"


class _ProgressReporter:
    """Print live progress and heartbeat updates for long-running device steps."""

    def __init__(self, label: str, heartbeat_seconds: float = 15.0):
        self.label = label
        self.heartbeat_seconds = heartbeat_seconds
        self._start_time = time.monotonic()
        self._last_update = None
        self._last_progress = None
        self._last_printed = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def callback(self, pct: float) -> None:
        with self._lock:
            self._last_update = time.monotonic()
            self._last_progress = pct
            should_print = self._last_printed is None or abs(pct - self._last_printed) >= 0.1
            if should_print:
                self._last_printed = pct

        logger.debug("%s progress callback: %.1f%%", self.label, pct)
        if should_print:
            print(_format_progress_update(self.label, pct), flush=True)

    def finish(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=0.1)

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(self.heartbeat_seconds):
            with self._lock:
                now = time.monotonic()
                last_progress = self._last_progress
                last_update_age = None if self._last_update is None else now - self._last_update
                total_age = now - self._start_time

            message = _format_progress_heartbeat(
                self.label,
                last_progress,
                last_update_age,
                total_age,
            )
            logger.debug(message.strip())
            print(message, flush=True)


def _print_device_health_report(report) -> None:
    """Render a device doctor report for a human operator."""
    ready = "yes" if report.ready_for_backup_restore else "no"
    print(f"Device doctor: {report.name} ({report.udid})")
    print(f"  iOS: {report.ios_version}  Product: {report.product_type}")
    print(f"  State: {report.state}")
    print(f"  Ready for backup/restore: {ready}")
    for check in report.checks:
        status = "OK" if check.ok else "FAIL"
        print(f"  [{status}] {check.name}: {check.detail}")
    if report.hint:
        print(f"  Hint: {report.hint}")


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the CLI."""
    effective_argv = argv if argv is not None else sys.argv[1:]

    # Smart pre-parse checks (before argparse can error on unknown subcommands)
    if not effective_argv and getattr(sys.stdin, "isatty", lambda: False)():
        from green2blue.wizard import run_wizard

        return run_wizard()

    if len(effective_argv) == 1 and effective_argv[0].lower().endswith(".zip"):
        zip_arg = effective_argv[0]
        print(f"Did you mean: green2blue inject {zip_arg}", file=sys.stderr)
        print(f"\nRun:  green2blue inject {zip_arg}", file=sys.stderr)
        return 1

    parser = _build_parser()
    args = parser.parse_args(effective_argv)

    # Configure logging
    if hasattr(args, "verbose") and args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    elif hasattr(args, "quiet") and args.quiet:
        logging.basicConfig(level=logging.WARNING, format="%(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except Green2BlueError as e:
        print(f"Error: {e}", file=sys.stderr)
        if e.hint:
            print(f"Hint: {e.hint}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="green2blue",
        description="Transfer SMS/MMS/RCS messages from Android to iPhone.",
    )
    parser.add_argument("--version", action="version", version=f"green2blue {__version__}")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- inject ---
    inject_parser = subparsers.add_parser(
        "inject",
        help="Inject Android messages into an iPhone backup",
    )
    inject_parser.add_argument(
        "export_zip",
        type=Path,
        help="Path to the SMS Import/Export ZIP file",
    )

    # Common options (shown first in --help)
    common = inject_parser.add_argument_group("Common options")
    common.add_argument(
        "--backup",
        type=str,
        default=None,
        help="iPhone backup path or UDID (auto-detect if omitted)",
    )
    common.add_argument(
        "--password",
        type=str,
        default=None,
        help="Backup encryption password",
    )
    common.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Parse and convert without modifying the backup",
    )
    common.add_argument(
        "-y", "--yes",
        action="store_true",
        default=False,
        help="Skip confirmation prompt",
    )
    common.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    common.add_argument("-q", "--quiet", action="store_true", help="Minimal output")

    # Advanced options (shown after common)
    advanced = inject_parser.add_argument_group("Advanced options")
    advanced.add_argument(
        "--country",
        type=str,
        default="US",
        help="Default country code for phone normalization (default: US)",
    )
    advanced.add_argument(
        "--ck-strategy",
        type=str,
        choices=["none", "fake-synced", "pending-upload", "icloud-reset"],
        default="none",
        help="CloudKit metadata strategy for iCloud Messages sync survival (default: none)",
    )
    advanced.add_argument(
        "--mode",
        type=str,
        choices=["insert", "overwrite", "clone"],
        default="insert",
        help="Injection mode: insert new rows, overwrite sacrifice messages, "
             "or clone existing (Hack Patrol) (default: insert)",
    )
    advanced.add_argument(
        "--sacrifice-chat",
        type=int,
        action="append",
        default=None,
        dest="sacrifice_chats",
        help="Chat ROWID to sacrifice (repeatable, required for --mode overwrite)",
    )
    advanced.add_argument(
        "--service",
        type=str,
        choices=["SMS", "iMessage"],
        default="SMS",
        help="Message service type: SMS (green bubbles) or iMessage (blue bubbles) (default: SMS)",
    )
    advanced.add_argument(
        "--disable-icloud-sync",
        action="store_true",
        default=False,
        help="Set CloudKitSyncingEnabled=False in backup to prevent iCloud Messages wipe",
    )
    advanced.add_argument(
        "--backup-root",
        type=Path,
        default=None,
        help="Override the default backup directory",
    )
    advanced.add_argument(
        "--no-attachments",
        action="store_true",
        default=False,
        help="Skip copying MMS attachment files",
    )
    advanced.add_argument(
        "--skip-duplicates",
        action="store_true",
        default=True,
        help="Skip duplicate messages (default: on)",
    )
    advanced.add_argument(
        "--no-skip-duplicates",
        action="store_false",
        dest="skip_duplicates",
        help="Do not skip duplicate messages",
    )

    inject_parser.set_defaults(func=_cmd_inject)

    # --- list-backups ---
    list_parser = subparsers.add_parser(
        "list-backups",
        help="List available iPhone backups",
    )
    list_parser.add_argument(
        "--backup-root",
        type=Path,
        default=None,
        help="Override the default backup directory",
    )
    list_parser.add_argument("-v", "--verbose", action="store_true")
    list_parser.add_argument("-q", "--quiet", action="store_true")
    list_parser.set_defaults(func=_cmd_list_backups)

    # --- inspect ---
    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Inspect an Android export ZIP without modifying anything",
    )
    inspect_parser.add_argument(
        "export_zip",
        type=Path,
        help="Path to the SMS Import/Export ZIP file",
    )
    inspect_parser.add_argument("-v", "--verbose", action="store_true")
    inspect_parser.add_argument("-q", "--quiet", action="store_true")
    inspect_parser.set_defaults(func=_cmd_inspect)

    # --- verify ---
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify an iPhone backup's integrity",
    )
    verify_parser.add_argument(
        "backup_path",
        type=Path,
        help="Path to the iPhone backup directory",
    )
    verify_parser.add_argument("-v", "--verbose", action="store_true")
    verify_parser.add_argument("-q", "--quiet", action="store_true")
    verify_parser.set_defaults(func=_cmd_verify)

    # --- diagnose ---
    diagnose_parser = subparsers.add_parser(
        "diagnose",
        help="Diagnose CloudKit sync state of messages in a backup",
    )
    diagnose_parser.add_argument(
        "--backup",
        type=str,
        default=None,
        help="iPhone backup path or UDID (auto-detect if omitted)",
    )
    diagnose_parser.add_argument(
        "--backup-root",
        type=Path,
        default=None,
        help="Override the default backup directory",
    )
    diagnose_parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Backup encryption password",
    )
    diagnose_parser.add_argument(
        "--injected-only",
        action="store_true",
        default=False,
        help="Only show green2blue-injected messages",
    )
    diagnose_parser.add_argument("-v", "--verbose", action="store_true")
    diagnose_parser.add_argument("-q", "--quiet", action="store_true")
    diagnose_parser.set_defaults(func=_cmd_diagnose)

    # --- prepare-sync ---
    prepare_sync_parser = subparsers.add_parser(
        "prepare-sync",
        help="Prepare an injected backup for iCloud sync reset workflow",
    )
    prepare_sync_parser.add_argument(
        "--backup",
        type=str,
        default=None,
        help="iPhone backup path or UDID (auto-detect if omitted)",
    )
    prepare_sync_parser.add_argument(
        "--backup-root",
        type=Path,
        default=None,
        help="Override the default backup directory",
    )
    prepare_sync_parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Backup encryption password",
    )
    prepare_sync_parser.add_argument("-v", "--verbose", action="store_true")
    prepare_sync_parser.add_argument("-q", "--quiet", action="store_true")
    prepare_sync_parser.set_defaults(func=_cmd_prepare_sync)

    # --- device (subcommand group) ---
    device_parser = subparsers.add_parser(
        "device",
        help="Direct device operations via USB (requires pymobiledevice3)",
    )
    device_subs = device_parser.add_subparsers(dest="device_command", help="Device commands")

    # device list
    dev_list_parser = device_subs.add_parser(
        "list",
        help="List connected iOS devices",
    )
    dev_list_parser.add_argument("-v", "--verbose", action="store_true")
    dev_list_parser.add_argument("-q", "--quiet", action="store_true")
    dev_list_parser.set_defaults(func=_cmd_device_list)

    # device doctor
    dev_doctor_parser = device_subs.add_parser(
        "doctor",
        help="Check whether a connected device is actually ready for backup/restore",
    )
    dev_doctor_parser.add_argument(
        "--udid",
        type=str,
        default=None,
        help="Target device UDID (auto-select if only one device)",
    )
    dev_doctor_parser.add_argument("-v", "--verbose", action="store_true")
    dev_doctor_parser.add_argument("-q", "--quiet", action="store_true")
    dev_doctor_parser.set_defaults(func=_cmd_device_doctor)

    # device backup
    dev_backup_parser = device_subs.add_parser(
        "backup",
        help="Create a backup from a connected device",
    )
    dev_backup_parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output directory for backup (default: temp directory)",
    )
    dev_backup_parser.add_argument(
        "--udid",
        type=str,
        default=None,
        help="Target device UDID (auto-select if only one device)",
    )
    dev_backup_parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Backup encryption password",
    )
    dev_backup_parser.add_argument("-v", "--verbose", action="store_true")
    dev_backup_parser.add_argument("-q", "--quiet", action="store_true")
    dev_backup_parser.set_defaults(func=_cmd_device_backup)

    # device inject
    dev_inject_parser = device_subs.add_parser(
        "inject",
        help="Full pipeline: backup device, inject messages, restore",
    )
    dev_inject_parser.add_argument(
        "export_zip",
        type=Path,
        help="Path to the SMS Import/Export ZIP file",
    )
    dev_inject_parser.add_argument(
        "--udid",
        type=str,
        default=None,
        help="Target device UDID (auto-select if only one device)",
    )
    dev_inject_parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Backup encryption password",
    )
    dev_inject_parser.add_argument(
        "--country",
        type=str,
        default="US",
        help="Default country code for phone normalization (default: US)",
    )
    dev_inject_parser.add_argument(
        "--ck-strategy",
        type=str,
        choices=["none", "fake-synced", "pending-upload", "icloud-reset"],
        default="none",
        help="CloudKit metadata strategy (default: none)",
    )
    dev_inject_parser.add_argument(
        "--service",
        type=str,
        choices=["SMS", "iMessage"],
        default="SMS",
        help="Message service type: SMS (green) or iMessage (blue) (default: SMS)",
    )
    dev_inject_parser.add_argument(
        "--keep-backup",
        action="store_true",
        default=False,
        help="Keep the temporary backup after restore (default: delete)",
    )
    dev_inject_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        default=False,
        help="Skip confirmation prompt",
    )
    dev_inject_parser.add_argument("-v", "--verbose", action="store_true")
    dev_inject_parser.add_argument("-q", "--quiet", action="store_true")
    dev_inject_parser.set_defaults(func=_cmd_device_inject)

    # device restore
    dev_restore_parser = device_subs.add_parser(
        "restore",
        help="Restore an already-modified backup to a device",
    )
    dev_restore_parser.add_argument(
        "backup_path",
        type=Path,
        help="Path to the backup directory",
    )
    dev_restore_parser.add_argument(
        "--udid",
        type=str,
        default=None,
        help="Target device UDID (auto-select if only one device)",
    )
    dev_restore_parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Backup encryption password",
    )
    dev_restore_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        default=False,
        help="Skip confirmation prompt",
    )
    dev_restore_parser.add_argument("-v", "--verbose", action="store_true")
    dev_restore_parser.add_argument("-q", "--quiet", action="store_true")
    dev_restore_parser.set_defaults(func=_cmd_device_restore)

    # --- wizard ---
    wizard_parser = subparsers.add_parser(
        "wizard",
        help="Interactive guided setup (same as running green2blue with no arguments)",
    )
    wizard_parser.set_defaults(func=_cmd_wizard)

    # --- quickstart ---
    quickstart_parser = subparsers.add_parser(
        "quickstart",
        help="Print a step-by-step guide for getting started",
    )
    quickstart_parser.set_defaults(func=_cmd_quickstart)

    return parser


def _cmd_wizard(args: argparse.Namespace) -> int:
    """Execute the wizard command."""
    from green2blue.wizard import run_wizard

    return run_wizard()


def _cmd_quickstart(args: argparse.Namespace) -> int:
    """Print a step-by-step quickstart guide."""
    print("""
green2blue — Quick Start Guide
===============================

1. EXPORT FROM ANDROID
   Install "SMS Import/Export" from the Play Store on your Android phone.
   Open it and export all messages. Choose NDJSON format and include
   attachments. This creates a ZIP file.

2. TRANSFER THE ZIP
   Get the ZIP file from your Android to your computer — email it to
   yourself, upload to Google Drive, or transfer via USB cable.

3. CREATE AN iPHONE BACKUP
   Connect your iPhone to your computer with a cable.
   - macOS: Open Finder, select your iPhone, click "Back Up Now"
   - Windows: Open iTunes, click the phone icon, click "Back Up Now"

4. RUN GREEN2BLUE
   Open Terminal (macOS) or Command Prompt (Windows) and run:

       green2blue

   Follow the prompts — drag your ZIP file in, confirm your backup,
   and green2blue will inject the messages.

5. RESTORE THE BACKUP
   - macOS: In Finder, click "Restore Backup" and select your backup
   - Windows: In iTunes, click "Restore Backup" and select your backup

6. DONE!
   After the restore completes, open Messages on your iPhone.
   Your Android messages will be there.

Need more help? Visit: https://github.com/discordwell/green2blue
""")
    return 0


def _cmd_inject(args: argparse.Namespace) -> int:
    """Execute the inject command."""
    from green2blue.ios.backup import find_backup
    from green2blue.models import CKStrategy, InjectionMode
    from green2blue.pipeline import run_pipeline

    # Validate overwrite mode requirements
    injection_mode = InjectionMode(args.mode)
    if injection_mode == InjectionMode.OVERWRITE and not args.sacrifice_chats:
        print("Error: --mode overwrite requires at least one --sacrifice-chat", file=sys.stderr)
        return 1

    # Resolve backup with interactive confirmation
    backup_info = find_backup(args.backup, args.backup_root)

    # Show confirmation prompt unless skipped
    skip_prompt = args.backup or args.yes or not sys.stdin.isatty()
    if not skip_prompt:
        confirmed_path = _confirm_backup(backup_info, args.backup_root)
        if confirmed_path is None:
            print("Aborted.", file=sys.stderr)
            return 1
        # User may have picked a different backup from the list
        if confirmed_path != backup_info.path:
            backup_info = find_backup(str(confirmed_path), args.backup_root)

    ck_strategy = CKStrategy(args.ck_strategy)

    result = run_pipeline(
        export_path=args.export_zip,
        backup_path_or_udid=str(backup_info.path),
        backup_root=args.backup_root,
        country=args.country,
        skip_duplicates=args.skip_duplicates,
        include_attachments=not args.no_attachments,
        dry_run=args.dry_run,
        password=args.password,
        ck_strategy=ck_strategy,
        service=args.service,
        injection_mode=injection_mode,
        sacrifice_chats=args.sacrifice_chats,
        disable_icloud_sync=args.disable_icloud_sync,
    )

    # Print summary
    cl_stats = result.clone_stats
    ow_stats = result.overwrite_stats
    stats = result.injection_stats
    if cl_stats:
        print("\n--- Clone Summary (Hack Patrol) ---")
        print(f"Messages parsed:      {result.total_messages_parsed}")
        print(f"Messages cloned:      {cl_stats.messages_cloned}")
        print(f"Clone source ROWID:   {cl_stats.clone_source_rowid}")
        print(f"CK metadata duped:    {'yes' if cl_stats.ck_metadata_duplicated else 'no'}")
        reused_h = cl_stats.handles_existing
        print(f"Handles created:      {cl_stats.handles_inserted} (reused: {reused_h})")
        reused_c = cl_stats.chats_existing
        print(f"Chats created:        {cl_stats.chats_inserted} (reused: {reused_c})")
    elif ow_stats:
        print("\n--- Overwrite Summary ---")
        print(f"Messages parsed:      {result.total_messages_parsed}")
        print(f"Sacrifice pool:       {ow_stats.sacrifice_pool_size}")
        print(f"Messages overwritten: {ow_stats.messages_overwritten}")
        print(f"Messages skipped:     {ow_stats.messages_skipped + result.skipped_count}")
        reused_h = ow_stats.handles_existing
        print(f"Handles created:      {ow_stats.handles_inserted} (reused: {reused_h})")
        reused_c = ow_stats.chats_existing
        print(f"Chats created:        {ow_stats.chats_inserted} (reused: {reused_c})")
        print(f"Attachments:          {ow_stats.attachments_inserted}")
    elif stats:
        print("\n--- Injection Summary ---")
        print(f"Messages parsed:   {result.total_messages_parsed}")
        print(f"Messages injected: {stats.messages_inserted}")
        print(f"Messages skipped:  {stats.messages_skipped + result.skipped_count}")
        print(f"Handles created:   {stats.handles_inserted} (reused: {stats.handles_existing})")
        print(f"Chats created:     {stats.chats_inserted} (reused: {stats.chats_existing})")
        print(f"Attachments:       {stats.attachments_inserted}")

    if result.safety_copy_path:
        print(f"\nSafety copy:       {result.safety_copy_path}")

    if result.verification:
        v = result.verification
        status = "PASSED" if v.passed else "FAILED"
        print(f"\nVerification:      {status} ({v.checks_passed}/{v.checks_run} checks)")
        for err in v.errors:
            print(f"  ERROR: {err}")
        for warn in v.warnings:
            print(f"  WARNING: {warn}")

    if result.conversion_warnings:
        print(f"\nWarnings ({len(result.conversion_warnings)}):")
        for w in result.conversion_warnings[:10]:
            print(f"  - {w}")
        if len(result.conversion_warnings) > 10:
            print(f"  ... and {len(result.conversion_warnings) - 10} more")

    if args.dry_run:
        print("\n(Dry run — no changes were made to the backup)")

    return 0


def _confirm_backup(
    backup_info: BackupInfo,
    backup_root: Path | None = None,
) -> Path | None:
    """Show selected backup and prompt for confirmation.

    Returns:
        The confirmed backup path, or None if the user aborted.
    """
    from green2blue.ios.backup import has_restore_checkpoint

    encrypted = ", encrypted" if backup_info.is_encrypted else ""
    injected = " [already injected]" if has_restore_checkpoint(backup_info.path) else ""
    print(f"\nSelected backup: {backup_info.device_name} "
          f"(iOS {backup_info.product_version}{encrypted}){injected}")
    print(f"  UDID: {backup_info.udid}")
    if backup_info.date:
        print(f"  Date: {backup_info.date}")
    print()

    while True:
        try:
            response = input("Proceed? [Y/n/list] ").strip().lower()
        except EOFError:
            return None

        if response in ("", "y", "yes"):
            return backup_info.path
        elif response in ("n", "no"):
            return None
        elif response == "list":
            result = _show_backup_list(backup_root)
            if result is None:
                return None
            return result
        else:
            print("Please enter Y, n, or list.")


def _show_backup_list(backup_root: Path | None = None) -> Path | None:
    """Show a numbered list of all backups and let the user pick one.

    Returns:
        The chosen backup path, or None if the user quit.
    """
    from green2blue.ios.backup import has_restore_checkpoint, list_backups

    backups = list_backups(backup_root)
    if not backups:
        print("No backups found.")
        return None

    print()
    for i, b in enumerate(backups, 1):
        injected = " [already injected]" if has_restore_checkpoint(b.path) else ""
        encrypted = ", encrypted" if b.is_encrypted else ""
        print(f"  {i}. {b.device_name} (iOS {b.product_version}{encrypted}){injected}")
        print(f"     UDID: {b.udid}")
        if b.date:
            print(f"     Date: {b.date}")
    print()

    while True:
        try:
            choice = input(f"Pick a backup [1-{len(backups)}] or q to quit: ").strip().lower()
        except EOFError:
            return None

        if choice == "q":
            return None
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(backups):
                return backups[idx].path
        except ValueError:
            pass
        print(f"Please enter a number 1-{len(backups)} or q.")


def _cmd_list_backups(args: argparse.Namespace) -> int:
    """Execute the list-backups command."""
    from green2blue.ios.backup import list_backups

    backups = list_backups(args.backup_root)

    if not backups:
        print("No iPhone backups found.")
        return 0

    print(f"Found {len(backups)} backup(s):\n")
    for b in backups:
        encrypted = " [ENCRYPTED]" if b.is_encrypted else ""
        print(f"  {b.device_name} (iOS {b.product_version}){encrypted}")
        print(f"    UDID: {b.udid}")
        print(f"    Path: {b.path}")
        if b.date:
            print(f"    Date: {b.date}")
        print()

    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    """Execute the inspect command."""
    from green2blue.parser.ndjson_parser import count_messages
    from green2blue.parser.zip_reader import open_export_zip

    with open_export_zip(args.export_zip) as export:
        counts = count_messages(export.ndjson_path)
        has_attachments = export.has_attachments()

    print(f"Export: {args.export_zip}")
    print(f"  Total records:  {counts['total']}")
    print(f"  SMS messages:   {counts['sms']}")
    print(f"  MMS messages:   {counts['mms']}")
    print(f"  Unknown:        {counts['unknown']}")
    print(f"  Parse errors:   {counts['errors']}")
    print(f"  Attachments:    {'yes' if has_attachments else 'no'}")

    return 0


def _cmd_diagnose(args: argparse.Namespace) -> int:
    """Execute the diagnose command."""
    import os
    import sqlite3
    import tempfile

    from green2blue.ios.backup import find_backup, get_sms_db_path

    backup_info = find_backup(args.backup, args.backup_root)
    sms_db_path = get_sms_db_path(backup_info.path)
    temp_path = None

    try:
        # Decrypt if needed
        if backup_info.is_encrypted:
            if not args.password:
                print("Error: Encrypted backup requires --password", file=sys.stderr)
                return 1
            from green2blue.ios.crypto import EncryptedBackup
            from green2blue.ios.manifest import ManifestDB, compute_file_id

            eb = EncryptedBackup(backup_info.path, args.password)
            eb.unlock()
            temp_manifest = eb.decrypt_manifest_db()
            sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
            with ManifestDB(temp_manifest) as manifest:
                sms_enc_key, sms_prot_class = manifest.get_file_encryption_info(sms_file_id)
            encrypted_data = sms_db_path.read_bytes()
            decrypted_data = eb.decrypt_db_file(encrypted_data, sms_enc_key, sms_prot_class)
            fd, tmp = tempfile.mkstemp(suffix=".db")
            os.close(fd)
            temp_path = Path(tmp)
            temp_path.write_bytes(decrypted_data)
            temp_manifest.unlink(missing_ok=True)
            sms_db_path = temp_path

        conn = sqlite3.connect(sms_db_path)
        conn.row_factory = sqlite3.Row

        # Summary stats
        total = conn.execute("SELECT COUNT(*) as cnt FROM message").fetchone()["cnt"]
        print(f"\nBackup: {backup_info.device_name} (iOS {backup_info.product_version})")
        print(f"Total messages: {total}")

        # CK sync state distribution
        rows = conn.execute(
            "SELECT ck_sync_state, COUNT(*) as cnt FROM message "
            "GROUP BY ck_sync_state ORDER BY ck_sync_state"
        ).fetchall()
        print("\nMessage CloudKit sync state distribution:")
        for row in rows:
            state = row["ck_sync_state"]
            label = {0: "unsynced", 1: "synced", 2: "pending"}.get(state, f"unknown({state})")
            at_risk = " [AT RISK with iCloud Messages]" if state == 0 else ""
            print(f"  ck_sync_state={state} ({label}): {row['cnt']} messages{at_risk}")

        # Chat CK state distribution
        chat_rows = conn.execute(
            "SELECT ck_sync_state, COUNT(*) as cnt FROM chat "
            "GROUP BY ck_sync_state ORDER BY ck_sync_state"
        ).fetchall()
        print("\nChat CloudKit sync state distribution:")
        for row in chat_rows:
            print(f"  ck_sync_state={row['ck_sync_state']}: {row['cnt']} chats")

        # Show injected messages
        if args.injected_only:
            print("\ngreen2blue-injected messages:")
            injected = conn.execute(
                "SELECT m.guid, m.text, m.ck_sync_state, m.ck_record_id, "
                "m.ck_record_change_tag, h.id as handle "
                "FROM message m LEFT JOIN handle h ON m.handle_id = h.ROWID "
                "WHERE m.guid LIKE 'green2blue:%' ORDER BY m.date"
            ).fetchall()
            if not injected:
                print("  (none found)")
            for row in injected:
                text = row["text"]
                if text and len(text) > 40:
                    text_preview = text[:40] + "..."
                else:
                    text_preview = text or "[attachment]"
                print(f"  {row['handle']}: {text_preview}")
                print(f"    ck_sync_state={row['ck_sync_state']}, "
                      f"ck_record_id={row['ck_record_id'] or '(none)'}, "
                      f"tag={row['ck_record_change_tag'] or '(none)'}")

        # Highlight at-risk messages
        at_risk = conn.execute(
            "SELECT COUNT(*) as cnt FROM message WHERE ck_sync_state = 0"
        ).fetchone()["cnt"]
        if at_risk > 0 and at_risk < total:
            print(f"\nWARNING: {at_risk}/{total} messages have ck_sync_state=0")
            print("These may be deleted when iCloud Messages syncs.")

        conn.close()

    finally:
        if temp_path:
            temp_path.unlink(missing_ok=True)

    return 0


def _cmd_prepare_sync(args: argparse.Namespace) -> int:
    """Execute the prepare-sync command."""
    import hashlib
    import os
    import tempfile

    from green2blue.ios.backup import find_backup, get_sms_db_path
    from green2blue.ios.prepare_sync import prepare_sync

    backup_info = find_backup(args.backup, args.backup_root)
    sms_db_path = get_sms_db_path(backup_info.path)
    manifest_path = backup_info.path / "Manifest.db"
    temp_path = None

    try:
        if backup_info.is_encrypted:
            if not args.password:
                print("Error: Encrypted backup requires --password", file=sys.stderr)
                return 1
            from green2blue.ios.crypto import EncryptedBackup
            from green2blue.ios.manifest import ManifestDB, compute_file_id

            eb = EncryptedBackup(backup_info.path, args.password)
            eb.unlock()
            temp_manifest = eb.decrypt_manifest_db()
            sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
            with ManifestDB(temp_manifest) as manifest:
                sms_enc_key, sms_prot_class = manifest.get_file_encryption_info(sms_file_id)
            encrypted_data = sms_db_path.read_bytes()
            decrypted_data = eb.decrypt_db_file(encrypted_data, sms_enc_key, sms_prot_class)
            fd, tmp = tempfile.mkstemp(suffix=".db")
            os.close(fd)
            temp_path = Path(tmp)
            temp_path.write_bytes(decrypted_data)

            result = prepare_sync(temp_path)

            sms_db_size = temp_path.stat().st_size
            re_encrypted = eb.encrypt_db_file(
                temp_path.read_bytes(), sms_enc_key, sms_prot_class,
            )
            sms_db_digest = hashlib.sha1(re_encrypted).digest()
            with ManifestDB(temp_manifest) as manifest:
                manifest.update_sms_db_entry(sms_db_size, new_digest=sms_db_digest)

            # Re-encrypt sms.db and write back
            sms_db_path.write_bytes(re_encrypted)

            # Re-encrypt Manifest.db and write back
            eb.re_encrypt_manifest_db(temp_manifest)
            temp_manifest.unlink(missing_ok=True)
        else:
            result = prepare_sync(sms_db_path)

            # Update Manifest.db with new size and digest
            sms_db_digest = hashlib.sha1(sms_db_path.read_bytes()).digest()
            sms_db_size = sms_db_path.stat().st_size
            from green2blue.ios.manifest import ManifestDB

            with ManifestDB(manifest_path) as manifest:
                manifest.update_sms_db_entry(sms_db_size, new_digest=sms_db_digest)

        # Print summary
        print(f"\nBackup: {backup_info.device_name} (iOS {backup_info.product_version})")
        print("\n--- Prepare-Sync Summary ---")
        print(f"Messages reset:         {result.messages_updated}")
        print(f"Messages already clean:  {result.messages_already_clean}")
        print(f"Attachments reset:       {result.attachments_updated}")
        print(f"Attachments already clean: {result.attachments_already_clean}")
        print(f"Chat tokens cleared:     {result.chats_token_cleared}")
        print(f"Chats CK reset:          {result.chats_ck_reset}")
        print(f"Mixed chats preserved:   {result.chats_preserved}")

        total_injected = result.messages_updated + result.messages_already_clean
        if total_injected > 0:
            print(f"\nWorkflow for {total_injected} injected messages:")
            print("  1. Disable iCloud Messages on your iPhone")
            print("     Settings > [your name] > iCloud > Messages > toggle OFF")
            print("  2. Restore this backup via Finder/iTunes")
            print("  3. Re-enable iCloud Messages")
            print("     iOS will do a bidirectional merge — uploading local")
            print("     messages to iCloud instead of deleting them.")
        else:
            print("\nNo injected messages found in this backup.")

    finally:
        if temp_path:
            temp_path.unlink(missing_ok=True)

    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    """Execute the verify command."""
    from green2blue.ios.backup import get_sms_db_path, validate_backup
    from green2blue.verify import verify_backup

    backup_path = args.backup_path
    validate_backup(backup_path)

    sms_db = get_sms_db_path(backup_path)
    manifest_db = backup_path / "Manifest.db"

    result = verify_backup(
        backup_path,
        sms_db,
        manifest_db if manifest_db.exists() else None,
    )

    status = "PASSED" if result.passed else "FAILED"
    print(f"Verification: {status} ({result.checks_passed}/{result.checks_run} checks)")

    for err in result.errors:
        print(f"  ERROR: {err}")
    for warn in result.warnings:
        print(f"  WARNING: {warn}")

    return 0 if result.passed else 1


# --- Device subcommands ---


def _print_post_restore_instructions() -> None:
    """Print instructions the user should follow after a device restore."""
    print("\n\nRestore complete. Your device will reboot.")
    print("After reboot:")
    print("  1. If you see 'iPhone Partially Set Up', tap 'Continue with Partial Setup'")
    print("  2. Wait for the 'press home to upgrade' progress bar to complete")
    print("  3. Open Messages — your injected conversations should appear")


def _resolve_restore_target(backup_path: Path, target_udid: str) -> tuple[Path, Path | None, str]:
    """Resolve the restore root, selected backup dir, and restore mode."""
    if (backup_path / "Manifest.db").exists() or (backup_path / "Manifest.mbdb").exists():
        backup_dir = backup_path
        backup_root = backup_path.parent
    else:
        backup_root = backup_path
        candidate = backup_root / target_udid
        backup_dir = candidate if candidate.is_dir() else None
        if backup_dir is None:
            candidates = [
                entry for entry in backup_root.iterdir()
                if entry.is_dir()
                and ((entry / "Manifest.db").exists() or (entry / "Manifest.mbdb").exists())
            ]
            if len(candidates) == 1:
                backup_dir = candidates[0]

    restore_mode = "synthetic" if backup_dir and (backup_dir / "Manifest.mbdb").exists() else "full"
    return backup_root, backup_dir, restore_mode


def _cmd_device_list(args: argparse.Namespace) -> int:
    """List connected iOS devices."""
    from green2blue.ios.device import list_devices

    devices = list_devices()

    if not devices:
        print("No iOS devices connected.")
        return 0

    print(f"Found {len(devices)} device(s):\n")
    for d in devices:
        paired = "" if d.is_paired else " [NOT PAIRED]"
        print(f"  {d.name} (iOS {d.ios_version}){paired}")
        print(f"    UDID: {d.udid}")
    print()

    return 0


def _cmd_device_doctor(args: argparse.Namespace) -> int:
    """Probe whether a device is ready for backup/restore."""
    from green2blue.ios.device import doctor_device

    report = doctor_device(args.udid)
    _print_device_health_report(report)
    return 0 if report.ready_for_backup_restore else 1


def _cmd_device_backup(args: argparse.Namespace) -> int:
    """Create a backup from a connected device."""
    import tempfile

    from green2blue.ios.device import create_backup, doctor_device

    output_dir = args.output or Path(tempfile.mkdtemp(prefix="g2b_backup_"))
    print(f"Creating backup in: {output_dir}")

    print("\nRunning device doctor...")
    report = doctor_device(args.udid)
    if not report.ready_for_backup_restore:
        _print_device_health_report(report)
        return 1
    print(f"  Device doctor: OK ({report.name}, iOS {report.ios_version}, state={report.state})")

    with _device_run_session("device_backup", {
        "requested_udid": args.udid or "auto",
        "device_udid": report.udid,
        "device_name": report.name,
        "output_dir": str(output_dir),
    }):
        progress = _ProgressReporter("Backup")
        progress.start()
        try:
            backup_path = create_backup(
                backup_dir=output_dir,
                udid=report.udid,
                password=args.password,
                progress_cb=progress.callback,
            )
        finally:
            progress.finish()

    print(f"\n\nBackup created: {backup_path}")
    return 0


def _cmd_device_inject(args: argparse.Namespace) -> int:
    """Full automated pipeline: backup -> inject -> restore."""
    import tempfile

    from green2blue.ios.device import (
        create_backup,
        doctor_device,
        list_devices,
        restore_backup,
    )
    from green2blue.models import CKStrategy
    from green2blue.pipeline import run_pipeline

    # Step 1: Find device
    devices = list_devices()
    if not devices:
        print("No iOS devices connected.", file=sys.stderr)
        return 1

    target = None
    if args.udid:
        for d in devices:
            if d.udid == args.udid:
                target = d
                break
        if not target:
            print(f"Device {args.udid} not found.", file=sys.stderr)
            return 1
    elif len(devices) == 1:
        target = devices[0]
    else:
        print("Multiple devices connected. Use --udid to select one:", file=sys.stderr)
        for d in devices:
            print(f"  {d.udid}  {d.name} (iOS {d.ios_version})", file=sys.stderr)
        return 1

    if not target.is_paired:
        print(f"Device {target.name} is not paired. Unlock and trust first.", file=sys.stderr)
        return 1

    print("\nRunning device doctor...")
    report = doctor_device(target.udid)
    if not report.ready_for_backup_restore:
        _print_device_health_report(report)
        return 1
    print(f"  Device doctor: OK ({report.name}, iOS {report.ios_version}, state={report.state})")

    # Confirm
    if not args.yes and sys.stdin.isatty():
        print(f"\nTarget: {target.name} (iOS {target.ios_version})")
        print(f"  UDID: {target.udid}")
        print(f"  Export: {args.export_zip}")
        print("\nThis will:")
        print("  1. Create a full backup of the device")
        print(f"  2. Inject messages from {args.export_zip.name}")
        print("  3. Restore the modified backup")
        print("  4. Reboot the device")
        try:
            response = input("\nProceed? [y/N] ").strip().lower()
        except EOFError:
            return 1
        if response not in ("y", "yes"):
            print("Aborted.")
            return 1

    run_metadata = {
        "device_udid": target.udid,
        "device_name": target.name,
        "export_zip": str(args.export_zip),
        "service": args.service,
        "ck_strategy": args.ck_strategy,
    }

    with _device_run_session("device_inject", run_metadata):
        # Step 2: Create backup
        backup_root = Path(tempfile.mkdtemp(prefix="g2b_device_"))
        print("\nCreating backup...")

        backup_progress = _ProgressReporter("Backup")
        backup_progress.start()
        try:
            backup_path = create_backup(
                backup_dir=backup_root,
                udid=target.udid,
                password=args.password,
                progress_cb=backup_progress.callback,
            )
        finally:
            backup_progress.finish()
        print(f"\n  Backup saved to: {backup_path}")

        run_metadata["backup_root"] = str(backup_root)

        # Step 3: Run injection pipeline
        ck_strategy = CKStrategy(args.ck_strategy)
        print("\nInjecting messages...")
        result = run_pipeline(
            export_path=args.export_zip,
            backup_path_or_udid=str(backup_path),
            country=args.country,
            skip_duplicates=True,
            include_attachments=True,
            dry_run=False,
            password=args.password,
            ck_strategy=ck_strategy,
            service=args.service,
        )

        # Print injection summary
        stats = result.injection_stats
        if stats:
            print("\n--- Injection Summary ---")
            print(f"Messages injected: {stats.messages_inserted}")
            print(f"Handles created:   {stats.handles_inserted}")
            print(f"Chats created:     {stats.chats_inserted}")
            print(f"Attachments:       {stats.attachments_inserted}")

        if result.verification:
            v = result.verification
            status = "PASSED" if v.passed else "FAILED"
            print(f"Verification:      {status} ({v.checks_passed}/{v.checks_run})")

        # Step 4: Restore to device
        print("\nRestoring modified backup to device...")

        restore_progress = _ProgressReporter("Restore")
        restore_progress.start()
        try:
            restore_backup(
                backup_dir=backup_root,
                udid=target.udid,
                password=args.password,
                progress_cb=restore_progress.callback,
            )
        finally:
            restore_progress.finish()

    _print_post_restore_instructions()

    # Cleanup
    if not args.keep_backup:
        import shutil

        shutil.rmtree(backup_root, ignore_errors=True)
        print("\nTemporary backup cleaned up.")
    else:
        print(f"\nBackup kept at: {backup_root}")

    return 0


def _cmd_device_restore(args: argparse.Namespace) -> int:
    """Restore an already-modified backup to a device."""
    from green2blue.ios.device import (
        doctor_device,
        list_devices,
        push_synthetic_backup,
        restore_backup,
    )

    # Validate backup path
    backup_path = args.backup_path
    if not backup_path.exists():
        print(f"Backup not found: {backup_path}", file=sys.stderr)
        return 1

    # Resolve target device early to avoid confusing errors after confirmation
    devices = list_devices()
    if not devices:
        print("No iOS devices connected.", file=sys.stderr)
        return 1

    if args.udid:
        target = None
        for d in devices:
            if d.udid == args.udid:
                target = d
        if not target:
            print(f"Device {args.udid} not found.", file=sys.stderr)
            return 1
    elif len(devices) == 1:
        target = devices[0]
    else:
        print("Multiple devices connected. Use --udid to select one:", file=sys.stderr)
        for d in devices:
            print(f"  {d.udid}  {d.name} (iOS {d.ios_version})", file=sys.stderr)
        return 1

    backup_root, _, restore_mode = _resolve_restore_target(backup_path, target.udid)

    print("\nRunning device doctor...")
    report = doctor_device(target.udid)
    if not report.ready_for_backup_restore:
        _print_device_health_report(report)
        return 1
    print(f"  Device doctor: OK ({report.name}, iOS {report.ios_version}, state={report.state})")

    # Confirm
    if not args.yes and sys.stdin.isatty():
        print(f"\nTarget: {target.name} (iOS {target.ios_version})")
        print(f"  UDID: {target.udid}")
        print(f"  Backup: {backup_path}")
        print("\nThis will restore the backup and reboot the device.")
        try:
            response = input("Proceed? [y/N] ").strip().lower()
        except EOFError:
            return 1
        if response not in ("y", "yes"):
            print("Aborted.")
            return 1

    with _device_run_session("device_restore", {
        "device_udid": target.udid,
        "device_name": target.name,
        "backup_path": str(backup_path),
        "backup_root": str(backup_root),
        "restore_mode": restore_mode,
    }):
        progress = _ProgressReporter("Restore")
        progress.start()
        try:
            if restore_mode == "synthetic":
                push_synthetic_backup(
                    backup_dir=backup_root,
                    udid=target.udid,
                    progress_cb=progress.callback,
                )
            else:
                restore_backup(
                    backup_dir=backup_root,
                    udid=target.udid,
                    password=args.password,
                    progress_cb=progress.callback,
                )
        finally:
            progress.finish()

    _print_post_restore_instructions()

    return 0
