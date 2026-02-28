"""Orchestrate the full green2blue injection pipeline.

Flow:
1. Find and validate iPhone backup
2. Parse Android export ZIP
3. Convert messages to iOS format
4. Create safety copy
5. Inject messages into sms.db
6. Copy attachments into backup
7. Update Manifest.db
8. Verify integrity
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from green2blue.converter.message_converter import convert_messages
from green2blue.exceptions import (
    EncryptedBackupError,
)
from green2blue.ios.attachment import copy_attachment_to_backup
from green2blue.ios.backup import (
    create_safety_copy,
    find_backup,
    get_sms_db_path,
    validate_backup,
)
from green2blue.ios.manifest import ManifestDB
from green2blue.ios.sms_db import InjectionStats, SMSDatabase
from green2blue.models import iOSAttachment, iOSMessage
from green2blue.parser.ndjson_parser import parse_ndjson
from green2blue.parser.zip_reader import ExtractedExport, open_export_zip
from green2blue.verify import VerificationResult, verify_backup

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Full result of the injection pipeline."""

    injection_stats: InjectionStats | None = None
    verification: VerificationResult | None = None
    safety_copy_path: Path | None = None
    backup_path: Path | None = None
    total_messages_parsed: int = 0
    total_attachments_copied: int = 0
    conversion_warnings: list[str] = field(default_factory=list)
    skipped_count: int = 0


def run_pipeline(
    export_path: Path | str,
    backup_path_or_udid: str | None = None,
    backup_root: Path | None = None,
    country: str = "US",
    skip_duplicates: bool = True,
    include_attachments: bool = True,
    dry_run: bool = False,
    password: str | None = None,
) -> PipelineResult:
    """Run the full injection pipeline.

    Args:
        export_path: Path to the Android export ZIP file.
        backup_path_or_udid: Explicit backup path or UDID (auto-detect if None).
        backup_root: Override default backup directory.
        country: Default country for phone normalization.
        skip_duplicates: Skip duplicate messages.
        include_attachments: Copy MMS attachment files.
        dry_run: Parse and convert but don't modify the backup.
        password: Backup encryption password (if encrypted).

    Returns:
        PipelineResult with statistics and verification results.
    """
    export_path = Path(export_path)
    result = PipelineResult()

    # Step 1: Find and validate backup
    logger.info("Finding iPhone backup...")
    backup_info = find_backup(backup_path_or_udid, backup_root)
    result.backup_path = backup_info.path
    logger.info(
        "Found backup: %s (%s, iOS %s, %s)",
        backup_info.device_name,
        backup_info.udid,
        backup_info.product_version,
        "encrypted" if backup_info.is_encrypted else "unencrypted",
    )

    validate_backup(backup_info.path)

    # Step 2: Handle encryption
    if backup_info.is_encrypted:
        raise EncryptedBackupError(
            "Encrypted backup support is not yet fully implemented. "
            "Please use an unencrypted backup (disable 'Encrypt local backup' "
            "in Finder/iTunes and create a new backup)."
        )

    # Step 3: Parse export ZIP
    logger.info("Parsing Android export: %s", export_path)
    with open_export_zip(export_path) as export:
        android_messages = list(parse_ndjson(export.ndjson_path))
        result.total_messages_parsed = len(android_messages)
        logger.info("Parsed %d messages from export", len(android_messages))

        # Step 4: Convert to iOS format
        logger.info("Converting messages...")
        conversion = convert_messages(android_messages, country, skip_duplicates)
        result.conversion_warnings = conversion.warnings
        result.skipped_count = conversion.skipped_count
        logger.info(
            "Converted: %d messages, %d handles, %d chats (%d skipped)",
            len(conversion.messages),
            len(conversion.handles),
            len(conversion.chats),
            conversion.skipped_count,
        )

        if dry_run:
            logger.info("Dry run — skipping backup modifications")
            result.injection_stats = InjectionStats()
            result.injection_stats.messages_inserted = len(conversion.messages)
            return result

        # Step 5: Create safety copy
        logger.info("Creating safety copy...")
        result.safety_copy_path = create_safety_copy(backup_info.path)
        logger.info("Safety copy at: %s", result.safety_copy_path)

        # Step 6: Inject into sms.db
        sms_db_file = get_sms_db_path(backup_info.path)

        logger.info("Injecting messages into sms.db...")
        with SMSDatabase(sms_db_file) as db:
            result.injection_stats = db.inject(conversion, skip_duplicates)

        logger.info("Injection complete: %s", result.injection_stats)

        # Step 7: Copy attachments + Step 8: Update Manifest.db
        logger.info("Updating Manifest.db and copying attachments...")
        manifest_path = backup_info.path / "Manifest.db"
        attachment_sizes: dict[str, int] = {}  # guid → file_size

        with ManifestDB(manifest_path) as manifest:
            if include_attachments and export.has_attachments():
                domain = manifest.detect_attachment_domain()
                for msg in conversion.messages:
                    for att in msg.attachments:
                        file_size = _copy_message_attachment(
                            att, msg, export, backup_info.path, manifest, domain
                        )
                        if file_size > 0:
                            attachment_sizes[att.guid] = file_size
                            result.total_attachments_copied += 1

            sms_db_size = sms_db_file.stat().st_size
            manifest.update_sms_db_entry(sms_db_size)

        # Update attachment sizes in sms.db
        if attachment_sizes:
            with SMSDatabase(sms_db_file) as db:
                cursor = db.conn.cursor()
                for guid, size in attachment_sizes.items():
                    cursor.execute(
                        "UPDATE attachment SET total_bytes = ? WHERE guid = ?",
                        (size, guid),
                    )
                db.conn.commit()

        # Step 9: Verify
        logger.info("Verifying backup integrity...")
        result.verification = verify_backup(
            backup_info.path,
            sms_db_file,
            manifest_path,
        )

        if result.verification.passed:
            logger.info("Verification PASSED (%d/%d checks)",
                        result.verification.checks_passed,
                        result.verification.checks_run)
        else:
            logger.warning("Verification FAILED: %s", result.verification.errors)

    return result


def _copy_message_attachment(
    att: iOSAttachment,
    msg: iOSMessage,
    export: ExtractedExport,
    backup_path: Path,
    manifest: ManifestDB,
    domain: str,
) -> int:
    """Copy a single attachment from the export to the backup.

    Returns:
        File size in bytes, or 0 if not found.
    """
    if not export.data_dir:
        return 0

    # Try to find the source file in the export
    source = None
    if att.source_data_path and export.data_dir:
        # Real SMS IE stores full Android paths like
        # /data/user/0/com.android.providers.telephony/app_parts/PART_123.jpg
        # The ZIP stores these as data/PART_123.jpg (basename under data/)
        basename = Path(att.source_data_path).name
        candidate = export.data_dir / basename
        if candidate.exists():
            source = candidate

        # Also try as a relative path from temp_dir (legacy test format)
        if source is None:
            candidate = export.temp_dir / att.source_data_path
            if candidate.exists():
                source = candidate

    # Fallback: search by transfer_name (display filename from cl/fn/name)
    if source is None:
        source = _find_attachment_source(att.transfer_name, export.data_dir)

    if source is None:
        logger.debug("Attachment source not found for: %s", att.transfer_name)
        return 0

    ios_relative = att.filename.removeprefix("~/")
    file_size = copy_attachment_to_backup(
        source, ios_relative, backup_path, manifest, domain,
    )
    return file_size


def _find_attachment_source(filename: str, data_dir: Path) -> Path | None:
    """Find an attachment file in the export data directory."""
    for path in data_dir.rglob("*"):
        if path.is_file() and path.name == filename:
            return path
    return None
