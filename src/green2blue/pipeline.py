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

import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from green2blue.converter.message_converter import convert_messages
from green2blue.exceptions import (
    EncryptedBackupError,
)
from green2blue.ios.attachment import copy_attachment_to_backup
from green2blue.ios.backup import (
    BackupInfo,
    create_safety_copy,
    find_backup,
    get_sms_db_path,
    validate_backup,
)
from green2blue.ios.manifest import ManifestDB, compute_file_id
from green2blue.ios.sms_db import InjectionStats, SMSDatabase
from green2blue.models import CKStrategy, iOSAttachment, iOSMessage
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
    ck_strategy: CKStrategy = CKStrategy.NONE,
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
        ck_strategy: CloudKit metadata strategy for iCloud Messages survival.

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
        if not password:
            raise EncryptedBackupError(
                "Encrypted backup requires a password.",
                hint="Provide --password <your-backup-password>.",
            )
        return _run_encrypted_pipeline(
            export_path, backup_info, password,
            skip_duplicates=skip_duplicates,
            include_attachments=include_attachments,
            dry_run=dry_run,
            country=country,
            ck_strategy=ck_strategy,
        )

    # Step 3: Parse export ZIP
    logger.info("Parsing Android export: %s", export_path)
    with open_export_zip(export_path) as export:
        android_messages = list(parse_ndjson(export.ndjson_path))
        result.total_messages_parsed = len(android_messages)
        logger.info("Parsed %d messages from export", len(android_messages))

        # Step 4: Convert to iOS format
        logger.info("Converting messages...")
        conversion = convert_messages(
            android_messages, country, skip_duplicates, ck_strategy=ck_strategy,
        )
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

        # Run prepare-sync if using icloud-reset strategy
        if ck_strategy == CKStrategy.ICLOUD_RESET:
            from green2blue.ios.prepare_sync import prepare_sync

            logger.info("Running prepare-sync for icloud-reset strategy...")
            prepare_sync(sms_db_file)

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

        # Update attachment sizes in sms.db (must happen before digest)
        if attachment_sizes:
            with SMSDatabase(sms_db_file) as db:
                db.update_attachment_sizes(attachment_sizes)

        # Update Manifest.db with final sms.db size and digest
        # (after all sms.db modifications are complete)
        with ManifestDB(manifest_path) as manifest:
            sms_db_size = sms_db_file.stat().st_size
            sms_db_digest = hashlib.sha1(sms_db_file.read_bytes()).digest()
            manifest.update_sms_db_entry(sms_db_size, new_digest=sms_db_digest)

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
    encrypted_backup: object | None = None,
    protection_class: int = 3,
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
        encrypted_backup=encrypted_backup,
        protection_class=protection_class,
    )
    return file_size


def _find_attachment_source(filename: str, data_dir: Path) -> Path | None:
    """Find an attachment file in the export data directory."""
    for path in data_dir.rglob("*"):
        if path.is_file() and path.name == filename:
            return path
    return None


def _run_encrypted_pipeline(
    export_path: Path,
    backup_info: BackupInfo,
    password: str,
    *,
    skip_duplicates: bool = True,
    include_attachments: bool = True,
    dry_run: bool = False,
    country: str = "US",
    ck_strategy: CKStrategy = CKStrategy.NONE,
) -> PipelineResult:
    """Run the injection pipeline for an encrypted backup.

    Decrypts Manifest.db and sms.db to temp files, runs the same inject/
    attachment/manifest logic against those temps, then re-encrypts and
    writes back.
    """
    from green2blue.ios.crypto import EncryptedBackup

    result = PipelineResult()
    result.backup_path = backup_info.path

    # Step 1: Unlock encrypted backup
    logger.info("Unlocking encrypted backup...")
    encrypted_backup = EncryptedBackup(backup_info.path, password)
    encrypted_backup.unlock()

    # Step 2: Decrypt Manifest.db to temp file
    logger.info("Decrypting Manifest.db...")
    temp_manifest_path = encrypted_backup.decrypt_manifest_db()

    # Step 3: Read sms.db encryption info from Manifest.db
    sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
    with ManifestDB(temp_manifest_path) as manifest:
        sms_enc_key, sms_prot_class = manifest.get_file_encryption_info(sms_file_id)

    # Step 4: Decrypt sms.db to temp file
    logger.info("Decrypting sms.db...")
    sms_db_on_disk = get_sms_db_path(backup_info.path)
    encrypted_sms_data = sms_db_on_disk.read_bytes()
    decrypted_sms_data = encrypted_backup.decrypt_db_file(
        encrypted_sms_data, sms_enc_key, sms_prot_class,
    )

    temp_sms_fd, temp_sms_str = tempfile.mkstemp(suffix=".db")
    os.close(temp_sms_fd)
    temp_sms_path = Path(temp_sms_str)
    temp_sms_path.write_bytes(decrypted_sms_data)

    try:
        # Step 5: Parse export ZIP
        logger.info("Parsing Android export: %s", export_path)
        with open_export_zip(export_path) as export:
            android_messages = list(parse_ndjson(export.ndjson_path))
            result.total_messages_parsed = len(android_messages)
            logger.info("Parsed %d messages from export", len(android_messages))

            # Step 6: Convert to iOS format
            logger.info("Converting messages...")
            conversion = convert_messages(
                android_messages, country, skip_duplicates, ck_strategy=ck_strategy,
            )
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

            # Step 7: Create safety copy (of the full backup)
            logger.info("Creating safety copy...")
            result.safety_copy_path = create_safety_copy(backup_info.path)
            logger.info("Safety copy at: %s", result.safety_copy_path)

            # Step 8: Inject into the temp decrypted sms.db
            logger.info("Injecting messages into decrypted sms.db...")
            with SMSDatabase(temp_sms_path) as db:
                result.injection_stats = db.inject(conversion, skip_duplicates)
            logger.info("Injection complete: %s", result.injection_stats)

            # Run prepare-sync if using icloud-reset strategy
            if ck_strategy == CKStrategy.ICLOUD_RESET:
                from green2blue.ios.prepare_sync import prepare_sync

                logger.info("Running prepare-sync for icloud-reset strategy...")
                prepare_sync(temp_sms_path)

            # Step 9: Copy + encrypt attachments, update temp Manifest.db
            logger.info("Updating Manifest.db and copying attachments...")
            attachment_sizes: dict[str, int] = {}

            with ManifestDB(temp_manifest_path) as manifest:
                if include_attachments and export.has_attachments():
                    domain = manifest.detect_attachment_domain()
                    for msg in conversion.messages:
                        for att in msg.attachments:
                            file_size = _copy_message_attachment(
                                att, msg, export, backup_info.path, manifest, domain,
                                encrypted_backup=encrypted_backup,
                                protection_class=sms_prot_class,
                            )
                            if file_size > 0:
                                attachment_sizes[att.guid] = file_size
                                result.total_attachments_copied += 1

            # Step 10: Update attachment sizes in temp sms.db
            # (must happen before digest computation)
            if attachment_sizes:
                with SMSDatabase(temp_sms_path) as db:
                    db.update_attachment_sizes(attachment_sizes)

            # Update Manifest.db with final sms.db size and digest
            # (after all sms.db modifications are complete)
            with ManifestDB(temp_manifest_path) as manifest:
                sms_db_size = temp_sms_path.stat().st_size
                sms_db_digest = hashlib.sha1(temp_sms_path.read_bytes()).digest()
                manifest.update_sms_db_entry(sms_db_size, new_digest=sms_db_digest)

            # Step 11: Verify on decrypted data (meaningful integrity checks)
            logger.info("Verifying backup integrity (on decrypted data)...")
            result.verification = verify_backup(
                backup_info.path,
                temp_sms_path,
                temp_manifest_path,
            )

            if result.verification.passed:
                logger.info(
                    "Verification PASSED (%d/%d checks)",
                    result.verification.checks_passed,
                    result.verification.checks_run,
                )
            else:
                logger.warning("Verification FAILED: %s", result.verification.errors)

            # Step 12: Re-encrypt sms.db and write to backup
            logger.info("Re-encrypting sms.db...")
            temp_sms_bytes = temp_sms_path.read_bytes()
            re_encrypted_sms = encrypted_backup.encrypt_db_file(
                temp_sms_bytes, sms_enc_key, sms_prot_class,
            )
            sms_db_on_disk.write_bytes(re_encrypted_sms)

            # Step 13: Re-encrypt Manifest.db and write to backup
            logger.info("Re-encrypting Manifest.db...")
            encrypted_backup.re_encrypt_manifest_db(temp_manifest_path)

    finally:
        # Clean up temp files
        temp_sms_path.unlink(missing_ok=True)
        temp_manifest_path.unlink(missing_ok=True)

    return result
