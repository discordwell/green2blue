"""Interactive wizard for non-technical users.

Launched when green2blue is run with no arguments (and stdin is a TTY),
or explicitly via ``green2blue wizard``.
"""

from __future__ import annotations

from datetime import datetime
import getpass
import platform
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from green2blue import __version__
from green2blue.exceptions import Green2BlueError

if TYPE_CHECKING:
    from green2blue.ios.backup import BackupInfo
    from green2blue.pipeline import PipelineResult


def run_wizard() -> int:
    """Run the interactive wizard flow. Returns an exit code."""
    try:
        _step_welcome()
        workflow, initial_export_raw = _step_workflow_choice()
        if workflow == "merge":
            _run_merge_wizard()
        else:
            _run_classic_wizard(initial_export_raw)
        return 0
    except KeyboardInterrupt:
        print("\n\nAborted.")
        return 130
    except EOFError:
        print("\n\nAborted.")
        return 130


# ---------------------------------------------------------------------------
# Step 1: Welcome
# ---------------------------------------------------------------------------

def _step_welcome() -> None:
    print()
    print(f"  green2blue v{__version__}")
    print("  Transfer your Android messages to iPhone")
    print()
    print("  Type Ctrl+C at any time to quit.")
    print()


# ---------------------------------------------------------------------------
# Step 2: Workflow choice
# ---------------------------------------------------------------------------

def _step_workflow_choice() -> tuple[str, str | None]:
    """Choose between the direct and merged workflows.

    Returns (workflow, initial_export_raw). ``initial_export_raw`` is used to
    preserve the old "paste the ZIP immediately" behavior for the classic flow.
    """
    print("  Workflows:")
    print("    1. Android export -> iPhone backup")
    print("    2. Merge Android export + iPhone backup, then inject merged result")
    print()

    while True:
        raw = input(
            "  Choose workflow [1/2] "
            "(or drag a ZIP now for quick import): "
        ).strip()
        if raw in ("", "1"):
            print()
            return "classic", None
        if raw == "2":
            print()
            return "merge", None
        if _looks_like_path(raw):
            print()
            return "classic", raw
        print("  Please enter 1 or 2, or drag a ZIP file here.\n")


def _looks_like_path(raw: str) -> bool:
    cleaned = _clean_path(raw)
    if not cleaned:
        return False
    path = Path(cleaned)
    return path.suffix.lower() == ".zip" or "/" in cleaned or "\\" in cleaned


# ---------------------------------------------------------------------------
# Step 3: Export ZIP
# ---------------------------------------------------------------------------

def _step_export_zip(initial_raw: str | None = None) -> Path:
    while True:
        if initial_raw is not None:
            raw = initial_raw
            initial_raw = None
        else:
            try:
                raw = input("Drag your Android export ZIP here (or type the path): ")
            except EOFError:
                raise

        path = _clean_path(raw)
        if not path:
            print("  Please enter a file path.\n")
            continue

        p = Path(path)
        if not p.exists():
            print(f"  File not found: {p}\n")
            continue

        if p.suffix.lower() != ".zip":
            print("  That doesn't look like a ZIP file.\n")
            continue

        # Validate it's a real export ZIP
        try:
            from green2blue.parser.zip_reader import open_export_zip

            with open_export_zip(p):
                pass
        except (OSError, ValueError, KeyError) as e:
            print(f"  Invalid export ZIP: {e}")
            print("  Make sure this is an export from SMS Import/Export (NDJSON format).\n")
            continue
        except Green2BlueError as e:
            print(f"  Invalid export ZIP: {e}")
            print("  Make sure this is an export from SMS Import/Export (NDJSON format).\n")
            continue

        return p


def _clean_path(raw: str) -> str:
    """Clean a drag-and-dropped path (strip quotes, whitespace, escapes)."""
    s = raw.strip()
    # macOS Terminal wraps drag-and-drop paths in single quotes
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1]
    # macOS Terminal escapes spaces with backslashes
    s = s.replace("\\ ", " ")
    return s.strip()


# ---------------------------------------------------------------------------
# Step 4: Inspect
# ---------------------------------------------------------------------------

def _step_inspect(export_path: Path) -> tuple[int, int, bool]:
    """Inspect the export ZIP and print a summary.

    Returns (sms_count, mms_count, has_attachments).
    """
    from green2blue.parser.ndjson_parser import count_messages
    from green2blue.parser.zip_reader import open_export_zip

    with open_export_zip(export_path) as export:
        counts = count_messages(export.ndjson_path)
        has_attachments = export.has_attachments()

    sms = counts["sms"]
    mms = counts["mms"]
    rcs = counts["rcs"]

    parts = []
    if sms:
        parts.append(f"{sms:,} SMS")
    if mms:
        parts.append(f"{mms:,} MMS")
    if rcs:
        parts.append(f"{rcs:,} RCS")

    msg_desc = ", ".join(parts) + " messages" if parts else "0 messages"

    print(f"\n  Found {msg_desc}", end="")
    if has_attachments:
        print(" with attachments")
    else:
        print()
    print()

    return sms, mms, has_attachments


# ---------------------------------------------------------------------------
# Step 5: Country detection
# ---------------------------------------------------------------------------

def _step_country_detection(export_path: Path) -> str:
    """Detect country from phone numbers in the export.

    Returns the country code to use.
    """
    country = _detect_country(export_path)
    if country != "US":
        return country

    # US was detected (or defaulted) — check if numbers actually look like US
    if _us_numbers_pass(export_path):
        return "US"

    # Numbers don't look like US — ask
    print("  Your messages don't appear to be US numbers.")
    while True:
        try:
            raw = input("  What country are they from? (e.g. GB, AU, DE): ").strip().upper()
        except EOFError:
            raise
        if len(raw) == 2 and raw.isalpha():
            return raw
        print("  Please enter a 2-letter country code (e.g. US, GB, AU).\n")


def _detect_country(export_path: Path) -> str:
    """Pre-scan phone numbers and try to detect the country."""
    from green2blue.parser.ndjson_parser import parse_ndjson
    from green2blue.parser.zip_reader import open_export_zip

    numbers: list[str] = []
    with open_export_zip(export_path) as export:
        for msg in parse_ndjson(export.ndjson_path):
            if hasattr(msg, "address") and msg.address:
                numbers.append(msg.address)
            if len(numbers) >= 20:
                break

    if not numbers:
        return "US"

    # If most numbers already start with +, try to detect the country
    plus_numbers = [n for n in numbers if n.strip().startswith("+")]
    if len(plus_numbers) > len(numbers) * 0.5:
        # Numbers already have country codes — US detection not needed
        return "US"

    return "US"


def _us_numbers_pass(export_path: Path) -> bool:
    """Check if numbers from the export normalize as US numbers."""
    from green2blue.converter.phone import normalize_phone
    from green2blue.exceptions import PhoneNormalizationError
    from green2blue.parser.ndjson_parser import parse_ndjson
    from green2blue.parser.zip_reader import open_export_zip

    numbers: list[str] = []
    with open_export_zip(export_path) as export:
        for msg in parse_ndjson(export.ndjson_path):
            if hasattr(msg, "address") and msg.address:
                numbers.append(msg.address)
            if len(numbers) >= 20:
                break

    if not numbers:
        return True

    pass_count = 0
    for num in numbers:
        try:
            normalize_phone(num, "US")
            pass_count += 1
        except PhoneNormalizationError:
            pass

    return pass_count > len(numbers) * 0.5


# ---------------------------------------------------------------------------
# Step 6: Backup selection
# ---------------------------------------------------------------------------

def _step_backup_selection() -> BackupInfo:
    """Find and select an iPhone backup. Returns BackupInfo."""
    from green2blue.ios.backup import list_backups

    backups = list_backups()
    if not backups:
        _print_no_backups_help()
        sys.exit(1)

    if len(backups) == 1:
        b = backups[0]
        encrypted = " (encrypted)" if b.is_encrypted else ""
        print(f'  Found backup: "{b.device_name}" — iOS {b.product_version}{encrypted}')
        while True:
            try:
                response = input("  Use this backup? [Y/n]: ").strip().lower()
            except EOFError:
                raise
            if response in ("", "y", "yes"):
                print()
                return b
            elif response in ("n", "no"):
                print("\n  Aborted.")
                sys.exit(0)
            else:
                print("  Please enter Y or n.")
    else:
        return _pick_backup(backups)


def _pick_backup(backups: list[BackupInfo]) -> BackupInfo:
    """Show numbered backup list and let user pick."""
    from green2blue.ios.backup import has_restore_checkpoint

    print("  Available backups:\n")
    for i, b in enumerate(backups, 1):
        encrypted = ", encrypted" if b.is_encrypted else ""
        injected = " [already injected]" if has_restore_checkpoint(b.path) else ""
        print(f"    {i}. {b.device_name} (iOS {b.product_version}{encrypted}){injected}")
        if b.date:
            print(f"       Date: {b.date}")
    print()

    while True:
        try:
            choice = input(f"  Pick a backup [1-{len(backups)}]: ").strip()
        except EOFError:
            raise
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(backups):
                return backups[idx]
        except ValueError:
            pass
        print(f"  Please enter a number 1-{len(backups)}.\n")


def _print_no_backups_help() -> None:
    """Print instructions for creating an iPhone backup."""
    print("\n  No iPhone backups found.\n")
    print("  To create one:")
    if platform.system() == "Darwin":
        print("    1. Connect your iPhone to your Mac with a cable")
        print("    2. Open Finder and select your iPhone in the sidebar")
        print('    3. Click "Back Up Now"')
        print("    4. Wait for the backup to complete, then run green2blue again")
    else:
        print("    1. Install iTunes from the Microsoft Store")
        print("    2. Connect your iPhone to your PC with a cable")
        print("    3. Click the phone icon in iTunes")
        print('    4. Click "Back Up Now"')
        print("    5. Wait for the backup to complete, then run green2blue again")
    print()


# ---------------------------------------------------------------------------
# Step 7: Encryption
# ---------------------------------------------------------------------------

def _step_encryption(backup_info: BackupInfo) -> str | None:
    """Handle encrypted backups. Returns password or None."""
    if not backup_info.is_encrypted:
        return None

    # Check if cryptography is available
    try:
        import cryptography  # noqa: F401
    except ImportError:
        print("\n  This backup is encrypted, but the 'cryptography' package is not installed.")
        print("  Install it with: pip install green2blue[encrypted]")
        print()
        sys.exit(1)

    print("\n  This backup is encrypted. Enter your backup password.")
    print("  (This is the password you set in Finder/iTunes, NOT your Apple ID.)\n")

    for attempt in range(3):
        password = getpass.getpass("  Backup password: ")
        if not password:
            print("  Password cannot be empty.\n")
            continue

        # Validate by attempting keybag unlock
        if _validate_password(backup_info, password):
            print("  Password accepted.\n")
            return password

        remaining = 2 - attempt
        if remaining > 0:
            print(f"  Wrong password. {remaining} attempt(s) remaining.\n")
        else:
            print("  Wrong password. Too many attempts.")
            sys.exit(1)

    sys.exit(1)


def _validate_password(backup_info: BackupInfo, password: str) -> bool:
    """Try to unlock the backup keybag with the given password."""
    try:
        from green2blue.ios.crypto import EncryptedBackup

        eb = EncryptedBackup(backup_info.path, password)
        eb.unlock()
        return True
    except (Green2BlueError, ValueError, KeyError):
        return False


# ---------------------------------------------------------------------------
# Step 8: Classic/merge flow runners
# ---------------------------------------------------------------------------

def _run_classic_wizard(initial_export_raw: str | None = None) -> None:
    export_path = _step_export_zip(initial_export_raw)
    sms_count, mms_count, has_attachments = _step_inspect(export_path)
    country = _step_country_detection(export_path)
    backup_info = _step_backup_selection()
    password = _step_encryption(backup_info)
    _step_confirm_and_inject(
        export_path, backup_info, password, country,
        sms_count, mms_count, has_attachments,
    )


def _run_merge_wizard() -> None:
    export_path = _step_export_zip()
    sms_count, mms_count, has_attachments = _step_inspect(export_path)
    country = _step_country_detection(export_path)
    backup_info = _step_backup_selection()
    password = _step_encryption(backup_info)
    _step_confirm_and_merge(
        export_path, backup_info, password, country,
        sms_count, mms_count, has_attachments,
    )


# ---------------------------------------------------------------------------
# Step 9: Confirm and inject
# ---------------------------------------------------------------------------

def _step_confirm_and_inject(
    export_path: Path,
    backup_info: BackupInfo,
    password: str | None,
    country: str,
    sms_count: int,
    mms_count: int,
    has_attachments: bool,
) -> None:
    """Show summary, confirm, and run the pipeline."""
    from green2blue.models import CKStrategy, InjectionMode
    from green2blue.pipeline import run_pipeline

    total = sms_count + mms_count
    encrypted = " (encrypted)" if backup_info.is_encrypted else ""

    print("  Ready to inject:")
    print(f"    {total:,} messages -> \"{backup_info.device_name}\" "
          f"(iOS {backup_info.product_version}{encrypted})")
    print("    A safety copy will be created first.")
    print()

    while True:
        try:
            response = input("  Proceed? [Y/n]: ").strip().lower()
        except EOFError:
            raise
        if response in ("", "y", "yes"):
            break
        elif response in ("n", "no"):
            print("\n  Aborted.")
            sys.exit(0)
        else:
            print("  Please enter Y or n.")

    print()
    print("  Injecting messages...")
    print()

    result = run_pipeline(
        export_path=export_path,
        backup_path_or_udid=str(backup_info.path),
        country=country,
        skip_duplicates=True,
        include_attachments=True,
        dry_run=False,
        password=password,
        ck_strategy=CKStrategy.NONE,
        service="SMS",
        injection_mode=InjectionMode.INSERT,
    )

    _step_results(result, has_attachments, backup_info, password)


def _step_confirm_and_merge(
    export_path: Path,
    backup_info: BackupInfo,
    password: str | None,
    country: str,
    sms_count: int,
    mms_count: int,
    has_attachments: bool,
) -> None:
    """Build a merged archive, show a report, then inject the merged result."""
    from green2blue.archive import (
        build_archive_report,
        import_android_export,
        import_ios_backup,
        merge_archive,
        stage_ios_export,
        verify_archive,
    )
    from green2blue.models import CKStrategy, InjectionMode
    from green2blue.pipeline import run_pipeline

    total = sms_count + mms_count
    encrypted = " (encrypted)" if backup_info.is_encrypted else ""

    print("  Ready to build a merged archive:")
    print(f"    Android export:      {total:,} messages")
    print(f"    iPhone backup:       \"{backup_info.device_name}\" "
          f"(iOS {backup_info.product_version}{encrypted})")
    print("    The merged archive will be imported, merged, reported,")
    print("    and then injected back into this iPhone backup.")
    print()

    _confirm_yes_no("  Build merged archive? [Y/n]: ")

    archive_path = _default_archive_path(backup_info)
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print("  Building merged archive...")
    print(f"    Archive path: {archive_path}")
    print()

    android_result = import_android_export(export_path, archive_path)
    ios_result = import_ios_backup(
        backup_info.path,
        archive_path,
        password=password,
    )
    merge_result = merge_archive(archive_path, country=country)
    report = build_archive_report(archive_path)

    print("  Merge report:\n")
    print(f"    Android messages imported: {android_result.messages_imported}")
    print(f"    iPhone messages imported:  {ios_result.messages_imported}")
    print(f"    Merged conversations:      {merge_result.merged_conversations}")
    print(f"    Merged messages:           {merge_result.merged_messages}")
    print(f"    Duplicate messages:        {merge_result.duplicate_messages}")
    if report.warnings:
        print("    Warnings:")
        for warning in report.warnings:
            print(f"      - {warning}")
    print()

    verify_result = verify_archive(archive_path)
    verify_status = "PASSED" if verify_result.passed else "FAILED"
    print(f"  Archive verification: {verify_status} "
          f"({verify_result.checks_passed}/{verify_result.checks_run})")
    for warning in verify_result.warnings:
        print(f"    WARNING: {warning}")
    for error in verify_result.errors:
        print(f"    ERROR: {error}")
    print()

    if not verify_result.passed:
        print("  The merged archive was created, but injection is blocked until")
        print("  archive verification passes.")
        print()
        return

    _confirm_yes_no("  Proceed with merged injection? [Y/n]: ")

    stage_dir = _default_stage_dir(backup_info)
    stage_dir.parent.mkdir(parents=True, exist_ok=True)
    stage_result = stage_ios_export(
        archive_path,
        stage_dir,
        merge_run_id=merge_result.merge_run_id,
        country=country,
        resume=True,
    )
    if stage_result.records_written == 0:
        print("  The merged archive contains no new non-iPhone messages to inject.")
        print("  The archive and stage bundle were still created successfully.")
        print()
        return

    print("  Prepared merged stage:")
    print(f"    Stage dir:   {stage_result.stage_dir}")
    print(f"    Output ZIP:  {stage_result.output_zip}")
    print(f"    Reused:      {'yes' if stage_result.reused_existing else 'no'}")
    print(f"    Verify:      {'PASSED' if stage_result.verification_passed else 'FAILED'}")
    for error in stage_result.verification_errors:
        print(f"      ERROR: {error}")
    print()
    if not stage_result.verification_passed:
        print("  The stage bundle was created, but injection is blocked until")
        print("  stage verification passes.")
        print()
        return
    print("  Injecting merged messages...\n")
    result = run_pipeline(
        export_path=stage_result.output_zip,
        backup_path_or_udid=str(backup_info.path),
        country=country,
        skip_duplicates=True,
        include_attachments=True,
        dry_run=False,
        password=password,
        ck_strategy=CKStrategy.NONE,
        service="SMS",
        injection_mode=InjectionMode.INSERT,
    )

    _step_results(result, has_attachments, backup_info, password)


def _confirm_yes_no(prompt: str) -> None:
    while True:
        try:
            response = input(prompt).strip().lower()
        except EOFError:
            raise
        if response in ("", "y", "yes"):
            return
        if response in ("n", "no"):
            print("\n  Aborted.")
            sys.exit(0)
        print("  Please enter Y or n.")


def _ask_yes_no(prompt: str, *, default: bool) -> bool:
    """Ask a yes/no question and return the answer."""
    while True:
        try:
            response = input(prompt).strip().lower()
        except EOFError:
            raise
        if not response:
            return default
        if response in ("y", "yes"):
            return True
        if response in ("n", "no"):
            return False
        print("  Please enter y or n.")


def _default_archive_path(backup_info: BackupInfo) -> Path:
    return Path.cwd() / ".g2b_archives" / f"{backup_info.udid}.g2b.sqlite"


def _default_stage_dir(backup_info: BackupInfo) -> Path:
    return Path.cwd() / ".g2b_stages" / backup_info.udid


# ---------------------------------------------------------------------------
# Step 10: Results + next steps
# ---------------------------------------------------------------------------

def _step_results(
    result: PipelineResult,
    has_attachments: bool,
    backup_info: BackupInfo,
    password: str | None,
) -> None:
    """Print injection results and platform-aware next steps."""
    stats = result.injection_stats
    clone_stats = result.clone_stats
    overwrite_stats = result.overwrite_stats

    injected = 0
    skipped = 0
    if stats:
        injected = stats.messages_inserted
        skipped = stats.messages_skipped
    elif clone_stats:
        injected = clone_stats.messages_cloned
    elif overwrite_stats:
        injected = overwrite_stats.messages_overwritten
        skipped = overwrite_stats.messages_skipped

    print("  Done!\n")
    print(f"    Messages injected:  {injected:,}")
    if skipped:
        print(f"    Duplicates skipped: {skipped:,}")
    if result.total_attachments_copied:
        print(f"    Attachments copied: {result.total_attachments_copied:,}")

    if result.verification and result.verification.passed:
        print("    Verification:       PASSED")
    elif result.verification:
        print("    Verification:       FAILED")
        for err in result.verification.errors:
            print(f"      - {err}")

    if result.safety_copy_path:
        print(f"\n    Safety copy at: {result.safety_copy_path}")

    if result.verification and not result.verification.passed:
        print("\n  Automatic device restore is disabled because verification failed.")
        _print_manual_restore_instructions()
        return

    _step_offer_device_restore(backup_info, password)


def _step_offer_device_restore(
    backup_info: BackupInfo,
    password: str | None,
) -> None:
    """Offer to preflight and restore the modified backup to a live device."""
    if not _ask_yes_no(
        "\n  Would you like green2blue to check a connected iPhone and restore this "
        "backup now? [y/N]: ",
        default=False,
    ):
        _print_manual_restore_instructions()
        return

    from green2blue.cli import (
        _ProgressReporter,
        _device_run_session,
        _print_device_health_report,
        _print_post_restore_instructions,
    )
    from green2blue.ios.device import (
        create_backup,
        doctor_device,
        restore_backup,
    )

    print("\n  Running device doctor...\n")

    try:
        report = doctor_device()
    except Green2BlueError as exc:
        print(f"  Could not check the connected iPhone: {exc}")
        if exc.hint:
            print(f"  Hint: {exc.hint}")
        _print_manual_restore_instructions()
        return

    _print_device_health_report(report)
    print()

    if not report.ready_for_backup_restore:
        _print_manual_restore_instructions()
        return

    if report.udid != backup_info.udid:
        print("  The connected iPhone does not match the backup you modified.")
        print(f"    Backup UDID:   {backup_info.udid}")
        print(f"    Device UDID:   {report.udid}")
        print()
        print("  Wizard live restore currently supports restoring back to the")
        print("  same device that produced the selected backup.")
        _print_manual_restore_instructions()
        return

    create_rollback = _ask_yes_no(
        "  Create a fresh rollback backup from the connected iPhone first? [Y/n]: ",
        default=True,
    )
    if not _ask_yes_no(
        "  Restore the modified backup to the connected iPhone now? [Y/n]: ",
        default=True,
    ):
        _print_manual_restore_instructions()
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rollback_root = Path.cwd() / ".g2b_device_backups" / stamp
    run_artifacts = None
    run_metadata = {
        "backup_udid": backup_info.udid,
        "backup_path": str(backup_info.path),
        "device_udid": report.udid,
        "device_name": report.name,
        "wizard_flow": "interactive",
    }

    try:
        with _device_run_session("wizard_restore", run_metadata) as artifacts:
            run_artifacts = artifacts

            if create_rollback:
                print("\n  Creating rollback backup...\n")
                backup_progress = _ProgressReporter("Backup")
                backup_progress.start()
                try:
                    rollback_path = create_backup(
                        backup_dir=rollback_root,
                        udid=report.udid,
                        password=password,
                        progress_cb=backup_progress.callback,
                    )
                finally:
                    backup_progress.finish()

                print(f"\n  Rollback backup saved to: {rollback_path}")

            print("\n  Restoring modified backup to the connected iPhone...\n")
            restore_progress = _ProgressReporter("Restore")
            restore_progress.start()
            try:
                restore_backup(
                    backup_dir=backup_info.path.parent,
                    udid=report.udid,
                    password=password,
                    progress_cb=restore_progress.callback,
                )
            finally:
                restore_progress.finish()
    except Green2BlueError as exc:
        print(f"\n  Device restore failed: {exc}")
        if exc.hint:
            print(f"  Hint: {exc.hint}")
        if run_artifacts is not None:
            print(f"  Live device logs: {run_artifacts.run_dir}")
        _print_manual_restore_instructions()
        return

    if run_artifacts is not None:
        print(f"\n  Live device logs: {run_artifacts.run_dir}")
    _print_post_restore_instructions()


def _print_manual_restore_instructions() -> None:
    """Print platform-aware manual restore instructions."""
    print("\n  Next steps:\n")
    if platform.system() == "Darwin":
        print("    1. Connect your iPhone to your Mac")
        print("    2. Open Finder and select your iPhone")
        print('    3. Click "Restore Backup" and select this backup')
        print("    4. Wait for the restore to complete")
        print("    5. Your Android messages will appear in Messages!")
    else:
        print("    1. Connect your iPhone to your PC")
        print("    2. Open iTunes and click the phone icon")
        print('    3. Click "Restore Backup" and select this backup')
        print("    4. Wait for the restore to complete")
        print("    5. Your Android messages will appear in Messages!")
    print()
