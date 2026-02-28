"""Command-line interface for green2blue."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from green2blue import __version__
from green2blue.exceptions import Green2BlueError

if TYPE_CHECKING:
    from green2blue.ios.backup import BackupInfo


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)

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
    inject_parser.add_argument(
        "--backup",
        type=str,
        default=None,
        help="iPhone backup path or UDID (auto-detect if omitted)",
    )
    inject_parser.add_argument(
        "--backup-root",
        type=Path,
        default=None,
        help="Override the default backup directory",
    )
    inject_parser.add_argument(
        "--country",
        type=str,
        default="US",
        help="Default country code for phone normalization (default: US)",
    )
    inject_parser.add_argument(
        "--skip-duplicates",
        action="store_true",
        default=True,
        help="Skip duplicate messages (default: on)",
    )
    inject_parser.add_argument(
        "--no-skip-duplicates",
        action="store_false",
        dest="skip_duplicates",
        help="Do not skip duplicate messages",
    )
    inject_parser.add_argument(
        "--no-attachments",
        action="store_true",
        default=False,
        help="Skip copying attachment files",
    )
    inject_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Parse and convert without modifying the backup",
    )
    inject_parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Backup encryption password",
    )
    inject_parser.add_argument(
        "--ck-strategy",
        type=str,
        choices=["none", "fake-synced", "pending-upload"],
        default="none",
        help="CloudKit metadata strategy for iCloud Messages sync survival (default: none)",
    )
    inject_parser.add_argument(
        "-y", "--yes",
        action="store_true",
        default=False,
        help="Skip confirmation prompt",
    )
    inject_parser.add_argument("-v", "--verbose", action="store_true")
    inject_parser.add_argument("-q", "--quiet", action="store_true")
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

    return parser


def _cmd_inject(args: argparse.Namespace) -> int:
    """Execute the inject command."""
    from green2blue.ios.backup import find_backup
    from green2blue.models import CKStrategy
    from green2blue.pipeline import run_pipeline

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
    )

    # Print summary
    stats = result.injection_stats
    if stats:
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
