"""Manifest.db management for iPhone backups.

Manifest.db tracks every file in an iPhone backup. Each entry has:
- fileID: SHA1 hash of '{domain}-{relativePath}'
- domain: e.g., 'HomeDomain', 'MediaDomain'
- relativePath: path within the domain
- flags: 1=file, 2=directory, 4=symlink
- file: NSKeyedArchiver binary plist with MBFile metadata (size, mode, etc.)
"""

from __future__ import annotations

import hashlib
import logging
import plistlib
import sqlite3
from pathlib import Path

from green2blue.exceptions import ManifestError
from green2blue.ios.plist_utils import build_mbfile_blob, patch_mbfile_blob

logger = logging.getLogger(__name__)


class ManifestDB:
    """Interface to Manifest.db in an iPhone backup."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection | None = None

    def open(self) -> None:
        if not self.db_path.exists():
            raise ManifestError(f"Manifest.db not found: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def update_sms_db_entry(
        self, new_size: int, new_digest: bytes | None = None
    ) -> str:
        """Update the sms.db entry in Manifest.db with new size and digest.

        Args:
            new_size: New file size of sms.db in bytes.
            new_digest: New SHA1 digest of sms.db (20 bytes). If provided,
                updates the Digest field in the MBFile blob.

        Returns:
            The fileID (hash) of the sms.db entry.
        """
        file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        cursor = self.conn.cursor()

        # Read existing blob to clone-and-patch
        cursor.execute("SELECT file FROM Files WHERE fileID = ?", (file_id,))
        row = cursor.fetchone()

        if row and row["file"]:
            new_blob = patch_mbfile_blob(row["file"], new_size, new_digest=new_digest)
        else:
            new_blob = build_mbfile_blob(new_size, digest=new_digest)

        cursor.execute(
            "UPDATE Files SET file = ? WHERE fileID = ?",
            (new_blob, file_id),
        )

        if cursor.rowcount == 0:
            # Entry doesn't exist, insert it
            cursor.execute(
                """INSERT INTO Files (fileID, domain, relativePath, flags, file)
                   VALUES (?, ?, ?, ?, ?)""",
                (file_id, "HomeDomain", "Library/SMS/sms.db", 1, new_blob),
            )

        self.conn.commit()
        logger.debug("Updated sms.db entry in Manifest.db (size=%d)", new_size)
        return file_id

    def add_attachment_entry(
        self,
        relative_path: str,
        file_size: int,
        domain: str = "HomeDomain",
        encryption_key: bytes | None = None,
        protection_class: int = 3,
    ) -> str:
        """Add an attachment file entry to Manifest.db.

        Also creates flags=2 directory entries for all parent directories
        that don't already exist (required by iOS restore).

        Args:
            relative_path: Path within the domain, e.g.,
                          'Library/SMS/Attachments/ab/cd/UUID/photo.jpg'.
            file_size: Size of the attachment file in bytes.
            domain: The backup domain (default: HomeDomain).
            encryption_key: Per-file wrapped encryption key blob (encrypted backups).
            protection_class: iOS protection class (default: 3).

        Returns:
            The fileID (hash) for the new entry.
        """
        # Ensure parent directory entries exist first
        self._ensure_directory_entries(relative_path, domain)

        file_id = compute_file_id(domain, relative_path)
        blob = build_mbfile_blob(
            file_size,
            encryption_key=encryption_key,
            protection_class=protection_class,
        )

        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT OR REPLACE INTO Files (fileID, domain, relativePath, flags, file)
               VALUES (?, ?, ?, ?, ?)""",
            (file_id, domain, relative_path, 1, blob),
        )
        self.conn.commit()
        logger.debug("Added attachment entry: %s (%d bytes)", relative_path, file_size)
        return file_id

    def _ensure_directory_entries(self, relative_path: str, domain: str) -> None:
        """Create flags=2 directory entries for all parent directories.

        iOS restore expects directory entries in Manifest.db for each parent
        path component. This creates any missing entries.

        Args:
            relative_path: File path within the domain.
            domain: The backup domain.
        """
        from pathlib import PurePosixPath

        cursor = self.conn.cursor()
        path = PurePosixPath(relative_path)

        # Walk up the parent chain, collecting directories to create
        parents = list(path.parents)
        # PurePosixPath('.').parents includes '.', skip it
        parents = [p for p in parents if str(p) != "."]

        for parent in parents:
            parent_str = str(parent)
            dir_file_id = compute_file_id(domain, parent_str)

            # Check if entry already exists
            cursor.execute(
                "SELECT 1 FROM Files WHERE fileID = ?", (dir_file_id,),
            )
            if cursor.fetchone() is not None:
                continue

            # Build directory MBFile blob (mode=0o40755, size=0)
            dir_blob = build_mbfile_blob(0, mode=0o40755)
            cursor.execute(
                """INSERT INTO Files (fileID, domain, relativePath, flags, file)
                   VALUES (?, ?, ?, ?, ?)""",
                (dir_file_id, domain, parent_str, 2, dir_blob),
            )
            logger.debug("Created directory entry: %s/%s", domain, parent_str)

    def get_file_encryption_info(self, file_id: str) -> tuple[bytes, int]:
        """Extract the EncryptionKey and ProtectionClass for a file entry.

        Reads the MBFile blob for the given fileID, parses the NSKeyedArchiver
        plist, and extracts the encryption metadata.

        Args:
            file_id: The fileID (SHA1 hash) of the entry.

        Returns:
            Tuple of (encryption_key_bytes, protection_class_int).

        Raises:
            ManifestError: If the entry or encryption data is not found.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT file FROM Files WHERE fileID = ?", (file_id,))
        row = cursor.fetchone()

        if not row or not row["file"]:
            raise ManifestError(f"No file blob found for fileID: {file_id}")

        try:
            plist = plistlib.loads(row["file"])
        except Exception as e:
            raise ManifestError(f"Failed to parse MBFile blob: {e}") from e

        objects = plist.get("$objects")
        if not objects:
            raise ManifestError("MBFile blob has no $objects")

        encryption_key = None
        protection_class = None

        for obj in objects:
            if isinstance(obj, dict):
                if "EncryptionKey" in obj:
                    enc_value = obj["EncryptionKey"]
                    # Real iOS blobs use plistlib.UID references for NSData
                    if isinstance(enc_value, plistlib.UID):
                        resolved = objects[enc_value]
                        # NSKeyedArchiver wraps NSData as {'NS.data': bytes, '$class': UID}
                        if isinstance(resolved, dict) and "NS.data" in resolved:
                            encryption_key = resolved["NS.data"]
                        else:
                            encryption_key = resolved
                    else:
                        encryption_key = enc_value
                if "ProtectionClass" in obj:
                    protection_class = obj["ProtectionClass"]

        if encryption_key is None:
            raise ManifestError(f"No EncryptionKey in MBFile blob for {file_id}")
        if protection_class is None:
            raise ManifestError(f"No ProtectionClass in MBFile blob for {file_id}")

        return encryption_key, protection_class

    def get_entry(self, file_id: str) -> dict | None:
        """Get a Manifest.db entry by fileID."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT fileID, domain, relativePath, flags, file FROM Files WHERE fileID = ?",
            (file_id,),
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def detect_attachment_domain(self) -> str:
        """Detect whether attachments use HomeDomain or MediaDomain.

        Queries existing attachment entries to determine convention.
        Defaults to MediaDomain (standard on iOS 26.2+).
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT domain FROM Files
               WHERE relativePath LIKE 'Library/SMS/Attachments/%'
               LIMIT 1""",
        )
        row = cursor.fetchone()
        if row:
            return row["domain"]
        return "MediaDomain"


def compute_file_id(domain: str, relative_path: str) -> str:
    """Compute the fileID hash for a Manifest.db entry.

    The hash is SHA1('{domain}-{relativePath}').

    Args:
        domain: e.g., 'HomeDomain'
        relative_path: e.g., 'Library/SMS/sms.db'

    Returns:
        Lowercase hex SHA1 hash.
    """
    key = f"{domain}-{relative_path}"
    return hashlib.sha1(key.encode()).hexdigest()
