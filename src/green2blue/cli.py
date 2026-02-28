"""Command-line interface for green2blue."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from green2blue import __version__
from green2blue.exceptions import Green2BlueError


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

    return parser


def _cmd_inject(args: argparse.Namespace) -> int:
    """Execute the inject command."""
    from green2blue.pipeline import run_pipeline

    result = run_pipeline(
        export_path=args.export_zip,
        backup_path_or_udid=args.backup,
        backup_root=args.backup_root,
        country=args.country,
        skip_duplicates=args.skip_duplicates,
        include_attachments=not args.no_attachments,
        dry_run=args.dry_run,
        password=args.password,
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
