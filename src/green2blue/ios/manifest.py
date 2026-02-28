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

    def update_sms_db_entry(self, new_size: int) -> str:
        """Update the sms.db entry in Manifest.db with new size.

        Args:
            new_size: New file size of sms.db in bytes.

        Returns:
            The fileID (hash) of the sms.db entry.
        """
        file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
        cursor = self.conn.cursor()

        # Read existing blob to clone-and-patch
        cursor.execute("SELECT file FROM Files WHERE fileID = ?", (file_id,))
        row = cursor.fetchone()

        if row and row["file"]:
            new_blob = patch_mbfile_blob(row["file"], new_size)
        else:
            new_blob = build_mbfile_blob(new_size)

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
    ) -> str:
        """Add an attachment file entry to Manifest.db.

        Args:
            relative_path: Path within the domain, e.g.,
                          'Library/SMS/Attachments/ab/cd/UUID/photo.jpg'.
            file_size: Size of the attachment file in bytes.
            domain: The backup domain (default: HomeDomain).

        Returns:
            The fileID (hash) for the new entry.
        """
        file_id = compute_file_id(domain, relative_path)
        blob = build_mbfile_blob(file_size)

        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT OR REPLACE INTO Files (fileID, domain, relativePath, flags, file)
               VALUES (?, ?, ?, ?, ?)""",
            (file_id, domain, relative_path, 1, blob),
        )
        self.conn.commit()
        logger.debug("Added attachment entry: %s (%d bytes)", relative_path, file_size)
        return file_id

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
        Defaults to HomeDomain if none exist.
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
        return "HomeDomain"


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
