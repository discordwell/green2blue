"""Copy MMS attachment files into the iPhone backup directory structure.

Attachment files from the Android export ZIP are copied into the backup
with iOS-style paths and the correct hashed directory structure.

Backup file layout:
    {backup_dir}/{hash[:2]}/{hash}

Where hash = SHA1('{domain}-{relative_path}')

iOS SMS attachment path format:
    Library/SMS/Attachments/{aa}/{bb}/{UUID}/{filename}
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from green2blue.ios.manifest import ManifestDB, compute_file_id

logger = logging.getLogger(__name__)

_COPY_CHUNK_SIZE = 1024 * 1024


def copy_attachment_to_backup(
    source_path: Path,
    ios_relative_path: str,
    backup_dir: Path,
    manifest: ManifestDB,
    domain: str = "HomeDomain",
    encrypted_backup: object | None = None,
    protection_class: int = 3,
) -> int:
    """Copy an attachment file into the backup structure and register in Manifest.db.

    Args:
        source_path: Path to the source file (extracted from ZIP).
        ios_relative_path: iOS-style relative path, e.g.,
                          'Library/SMS/Attachments/ab/UUID/photo.jpg'.
        backup_dir: Root of the iPhone backup directory.
        manifest: Open ManifestDB instance.
        domain: Backup domain (default: HomeDomain).
        encrypted_backup: EncryptedBackup instance (if encrypted backup).
        protection_class: iOS protection class for encryption (default: 3).

    Returns:
        File size in bytes (plaintext size).
    """
    if not source_path.exists():
        logger.warning("Attachment source not found: %s", source_path)
        return 0

    file_size = source_path.stat().st_size
    if file_size == 0:
        logger.warning("Attachment is empty: %s", source_path)
        return 0

    # Compute the backup file path
    file_id = compute_file_id(domain, ios_relative_path)
    dest_dir = backup_dir / file_id[:2]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / file_id

    if encrypted_backup is not None:
        # Encrypted path: stream the plaintext from disk into the encrypted
        # backup file to avoid buffering large attachment payloads in memory.
        file_size, digest, enc_key_blob = encrypted_backup.encrypt_new_file_to_path(
            source_path,
            dest_path,
            protection_class,
        )
        logger.debug("Encrypted attachment: %s -> %s", source_path.name, dest_path)

        # iOS keeps the plaintext size but stores the ciphertext digest in Manifest.db.
        manifest.add_attachment_entry(
            ios_relative_path, file_size, domain,
            encryption_key=enc_key_blob, protection_class=protection_class,
            digest=digest,
        )
    else:
        # Unencrypted path: stream the file so large histories do not load every
        # attachment fully into memory before writing into the backup.
        digest_hasher = hashlib.sha1()
        with source_path.open("rb") as src, dest_path.open("wb") as dst:
            while True:
                chunk = src.read(_COPY_CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
                digest_hasher.update(chunk)
        digest = digest_hasher.digest()
        logger.debug("Copied attachment: %s -> %s", source_path.name, dest_path)

        # Register in Manifest.db
        manifest.add_attachment_entry(
            ios_relative_path, file_size, domain, digest=digest,
        )

    return file_size


def resolve_attachment_paths(
    attachments_data: list[tuple[str, str]],
    export_data_dir: Path | None,
) -> list[tuple[Path | None, str]]:
    """Resolve Android export paths to actual file paths.

    Args:
        attachments_data: List of (android_data_path, ios_relative_path) tuples.
        export_data_dir: Path to the extracted 'data/' directory from the ZIP.

    Returns:
        List of (resolved_source_path_or_None, ios_relative_path) tuples.
    """
    results = []
    for android_path, ios_path in attachments_data:
        if export_data_dir is None:
            results.append((None, ios_path))
            continue

        # android_path is relative to the ZIP root, e.g., "data/parts/image.jpg"
        source = export_data_dir.parent / android_path
        if source.exists():
            results.append((source, ios_path))
        else:
            logger.warning("Attachment not found in export: %s", android_path)
            results.append((None, ios_path))

    return results
