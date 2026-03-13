"""SQLite-backed canonical archive storage."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO


@dataclass(frozen=True)
class ArchiveSummary:
    archive_path: Path
    import_runs: int
    conversations: int
    participants: int
    messages: int
    attachment_parts: int
    blobs: int
    blob_bytes: int


_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS archive_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_path TEXT,
    source_fingerprint TEXT,
    imported_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    message_count INTEGER NOT NULL DEFAULT 0,
    attachment_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_key TEXT NOT NULL UNIQUE,
    source_thread_id TEXT,
    kind TEXT NOT NULL,
    title TEXT
);

CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conversation_participants (
    conversation_id INTEGER NOT NULL,
    participant_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (conversation_id, participant_id),
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES participants(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_uid TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL,
    import_run_id INTEGER NOT NULL,
    conversation_id INTEGER NOT NULL,
    direction TEXT NOT NULL,
    sent_at_ms INTEGER NOT NULL,
    read_state TEXT NOT NULL,
    service_hint TEXT,
    subject TEXT,
    body_text TEXT,
    has_attachments INTEGER NOT NULL DEFAULT 0,
    has_url INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL,
    FOREIGN KEY (import_run_id) REFERENCES import_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256 TEXT NOT NULL UNIQUE,
    byte_size INTEGER NOT NULL,
    data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS message_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    part_index INTEGER NOT NULL,
    mime_type TEXT,
    filename TEXT,
    text_content TEXT,
    blob_id INTEGER,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    FOREIGN KEY (blob_id) REFERENCES blobs(id) ON DELETE CASCADE,
    UNIQUE (message_id, part_index)
);

CREATE TABLE IF NOT EXISTS merge_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy TEXT NOT NULL,
    country TEXT NOT NULL,
    created_at TEXT NOT NULL,
    merged_conversation_count INTEGER NOT NULL DEFAULT 0,
    merged_message_count INTEGER NOT NULL DEFAULT 0,
    duplicate_message_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS merged_conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    merge_run_id INTEGER NOT NULL,
    merge_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    title TEXT,
    FOREIGN KEY (merge_run_id) REFERENCES merge_runs(id) ON DELETE CASCADE,
    UNIQUE (merge_run_id, merge_key)
);

CREATE TABLE IF NOT EXISTS merged_conversation_participants (
    merged_conversation_id INTEGER NOT NULL,
    participant_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (merged_conversation_id, participant_id),
    FOREIGN KEY (merged_conversation_id) REFERENCES merged_conversations(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES participants(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS merged_messages (
    merge_run_id INTEGER NOT NULL,
    merged_conversation_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    sort_order INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    is_duplicate INTEGER NOT NULL DEFAULT 0,
    duplicate_of_message_id INTEGER,
    PRIMARY KEY (merge_run_id, message_id),
    FOREIGN KEY (merge_run_id) REFERENCES merge_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (merged_conversation_id) REFERENCES merged_conversations(id) ON DELETE CASCADE,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    FOREIGN KEY (duplicate_of_message_id) REFERENCES messages(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_import_runs_lookup
ON import_runs(source_type, source_path, source_fingerprint, status);
"""


class CanonicalArchive:
    """SQLite-backed canonical archive."""

    def __init__(self, archive_path: Path | str):
        self.archive_path = Path(archive_path)
        self.conn: sqlite3.Connection | None = None

    def open(self) -> None:
        is_new = not self.archive_path.exists()
        self.archive_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.archive_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)
        _ensure_schema_compat(self.conn)
        self.blob_store_dir.mkdir(parents=True, exist_ok=True)
        self._migrate_inline_blobs()
        if is_new:
            self.set_meta("archive_format", "green2blue-canonical")
            self.set_meta("archive_version", "1")
            self.set_meta("created_at", datetime.now(timezone.utc).isoformat())

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> CanonicalArchive:
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.conn is not None:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        self.close()

    def set_meta(self, key: str, value: str) -> None:
        assert self.conn is not None
        self.conn.execute(
            "INSERT INTO archive_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def start_import(
        self,
        source_type: str,
        source_path: str | None,
        *,
        source_fingerprint: str | None = None,
    ) -> int:
        assert self.conn is not None
        cur = self.conn.execute(
            """
            INSERT INTO import_runs (
                source_type, source_path, source_fingerprint, imported_at, status
            ) VALUES (?, ?, ?, ?, 'running')
            """,
            (
                source_type,
                source_path,
                source_fingerprint,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.set_meta("updated_at", datetime.now(timezone.utc).isoformat())
        return int(cur.lastrowid)

    def finish_import(self, import_run_id: int, message_count: int, attachment_count: int) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            UPDATE import_runs
            SET message_count = ?,
                attachment_count = ?,
                status = 'completed'
            WHERE id = ?
            """,
            (message_count, attachment_count, import_run_id),
        )

    def find_completed_import_run(
        self,
        *,
        source_type: str,
        source_path: str | None,
        source_fingerprint: str | None,
    ) -> sqlite3.Row | None:
        assert self.conn is not None
        return self.conn.execute(
            """
            SELECT id, message_count, attachment_count
            FROM import_runs
            WHERE source_type = ?
              AND source_path IS ?
              AND source_fingerprint IS ?
              AND status = 'completed'
            ORDER BY id DESC
            LIMIT 1
            """,
            (source_type, source_path, source_fingerprint),
        ).fetchone()

    def summarize_import_run(self, import_run_id: int) -> dict[str, int]:
        assert self.conn is not None
        return {
            "messages": int(
                self.conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE import_run_id = ?",
                    (import_run_id,),
                ).fetchone()[0]
            ),
            "conversations": int(
                self.conn.execute(
                    """
                    SELECT COUNT(DISTINCT conversation_id)
                    FROM messages
                    WHERE import_run_id = ?
                    """,
                    (import_run_id,),
                ).fetchone()[0]
            ),
            "participants": int(
                self.conn.execute(
                    """
                    SELECT COUNT(DISTINCT cp.participant_id)
                    FROM messages m
                    JOIN conversation_participants cp
                      ON cp.conversation_id = m.conversation_id
                    WHERE m.import_run_id = ?
                    """,
                    (import_run_id,),
                ).fetchone()[0]
            ),
            "attachments": int(
                self.conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM message_attachments ma
                    JOIN messages m ON m.id = ma.message_id
                    WHERE m.import_run_id = ?
                    """,
                    (import_run_id,),
                ).fetchone()[0]
            ),
            "blobs": int(
                self.conn.execute(
                    """
                    SELECT COUNT(DISTINCT ma.blob_id)
                    FROM message_attachments ma
                    JOIN messages m ON m.id = ma.message_id
                    WHERE m.import_run_id = ?
                      AND ma.blob_id IS NOT NULL
                    """,
                    (import_run_id,),
                ).fetchone()[0]
            ),
        }

    def get_or_create_participant(self, address: str, kind: str) -> int:
        assert self.conn is not None
        row = self.conn.execute(
            "SELECT id FROM participants WHERE address = ?",
            (address,),
        ).fetchone()
        if row is not None:
            return int(row["id"])
        cur = self.conn.execute(
            "INSERT INTO participants (address, kind) VALUES (?, ?)",
            (address, kind),
        )
        return int(cur.lastrowid)

    def get_or_create_conversation(
        self,
        conversation_key: str,
        *,
        kind: str,
        source_thread_id: str | None,
        title: str | None,
    ) -> int:
        assert self.conn is not None
        row = self.conn.execute(
            "SELECT id FROM conversations WHERE conversation_key = ?",
            (conversation_key,),
        ).fetchone()
        if row is not None:
            return int(row["id"])
        cur = self.conn.execute(
            "INSERT INTO conversations (conversation_key, source_thread_id, kind, title) "
            "VALUES (?, ?, ?, ?)",
            (conversation_key, source_thread_id, kind, title),
        )
        return int(cur.lastrowid)

    def link_conversation_participant(
        self,
        conversation_id: int,
        participant_id: int,
        *,
        role: str,
        sort_order: int,
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            "INSERT INTO conversation_participants "
            "(conversation_id, participant_id, role, sort_order) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(conversation_id, participant_id) DO UPDATE SET "
            "role = excluded.role, sort_order = excluded.sort_order",
            (conversation_id, participant_id, role, sort_order),
        )

    def insert_message(
        self,
        *,
        source_uid: str,
        source_type: str,
        import_run_id: int,
        conversation_id: int,
        direction: str,
        sent_at_ms: int,
        read_state: str,
        service_hint: str | None,
        subject: str | None,
        body_text: str | None,
        has_attachments: bool,
        has_url: bool,
        raw_json: str,
    ) -> tuple[int, bool]:
        assert self.conn is not None
        row = self.conn.execute(
            "SELECT id FROM messages WHERE source_uid = ?",
            (source_uid,),
        ).fetchone()
        if row is not None:
            return int(row["id"]), False
        cur = self.conn.execute(
            """
            INSERT INTO messages (
                source_uid, source_type, import_run_id, conversation_id,
                direction, sent_at_ms, read_state, service_hint, subject,
                body_text, has_attachments, has_url, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_uid,
                source_type,
                import_run_id,
                conversation_id,
                direction,
                sent_at_ms,
                read_state,
                service_hint,
                subject,
                body_text,
                int(has_attachments),
                int(has_url),
                raw_json,
            ),
        )
        return int(cur.lastrowid), True

    def upsert_blob(self, data: bytes) -> tuple[int, str]:
        assert self.conn is not None
        sha256 = hashlib.sha256(data).hexdigest()
        byte_size = len(data)
        relpath = self._blob_relpath(sha256)
        self._write_blob_bytes(self._blob_absolute_path(relpath), data)
        row = self.conn.execute(
            "SELECT id FROM blobs WHERE sha256 = ?",
            (sha256,),
        ).fetchone()
        if row is not None:
            self.conn.execute(
                """
                UPDATE blobs
                SET byte_size = ?,
                    storage_kind = 'external',
                    external_relpath = ?,
                    data = ?
                WHERE id = ?
                """,
                (byte_size, relpath, sqlite3.Binary(b""), int(row["id"])),
            )
            return int(row["id"]), sha256
        cur = self.conn.execute(
            """
            INSERT INTO blobs (sha256, byte_size, data, storage_kind, external_relpath)
            VALUES (?, ?, ?, 'external', ?)
            """,
            (sha256, byte_size, sqlite3.Binary(b""), relpath),
        )
        return int(cur.lastrowid), sha256

    def upsert_blob_path(self, source_path: Path | str) -> tuple[int, str]:
        assert self.conn is not None
        source = Path(source_path)
        sha256, byte_size = _hash_path(source)
        relpath = self._blob_relpath(sha256)
        self._copy_blob_file(source, self._blob_absolute_path(relpath))
        row = self.conn.execute(
            "SELECT id FROM blobs WHERE sha256 = ?",
            (sha256,),
        ).fetchone()
        if row is not None:
            self.conn.execute(
                """
                UPDATE blobs
                SET byte_size = ?,
                    storage_kind = 'external',
                    external_relpath = ?,
                    data = ?
                WHERE id = ?
                """,
                (byte_size, relpath, sqlite3.Binary(b""), int(row["id"])),
            )
            return int(row["id"]), sha256
        cur = self.conn.execute(
            """
            INSERT INTO blobs (sha256, byte_size, data, storage_kind, external_relpath)
            VALUES (?, ?, ?, 'external', ?)
            """,
            (sha256, byte_size, sqlite3.Binary(b""), relpath),
        )
        return int(cur.lastrowid), sha256

    def get_blob_path(self, blob_id: int) -> Path:
        assert self.conn is not None
        row = self.conn.execute(
            """
            SELECT id, sha256, byte_size, data, storage_kind, external_relpath
            FROM blobs
            WHERE id = ?
            """,
            (blob_id,),
        ).fetchone()
        if row is None:
            raise FileNotFoundError(f"Blob {blob_id} does not exist in archive.")
        return self._ensure_blob_row_externalized(row)

    def count_missing_blob_files(self) -> int:
        assert self.conn is not None
        missing = 0
        for row in self.conn.execute(
            "SELECT sha256, external_relpath FROM blobs WHERE COALESCE(storage_kind, 'inline') = 'external'"
        ).fetchall():
            relpath = row["external_relpath"] or self._blob_relpath(str(row["sha256"]))
            if not self._blob_absolute_path(str(relpath)).exists():
                missing += 1
        return missing

    def start_merge_run(self, strategy: str, country: str) -> int:
        assert self.conn is not None
        cur = self.conn.execute(
            "INSERT INTO merge_runs (strategy, country, created_at) VALUES (?, ?, ?)",
            (strategy, country, datetime.now(timezone.utc).isoformat()),
        )
        self.set_meta("updated_at", datetime.now(timezone.utc).isoformat())
        return int(cur.lastrowid)

    def finish_merge_run(
        self,
        merge_run_id: int,
        *,
        merged_conversation_count: int,
        merged_message_count: int,
        duplicate_message_count: int,
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            UPDATE merge_runs
            SET merged_conversation_count = ?,
                merged_message_count = ?,
                duplicate_message_count = ?
            WHERE id = ?
            """,
            (
                merged_conversation_count,
                merged_message_count,
                duplicate_message_count,
                merge_run_id,
            ),
        )

    def get_or_create_merged_conversation(
        self,
        merge_run_id: int,
        *,
        merge_key: str,
        kind: str,
        title: str | None,
    ) -> int:
        assert self.conn is not None
        row = self.conn.execute(
            "SELECT id FROM merged_conversations WHERE merge_run_id = ? AND merge_key = ?",
            (merge_run_id, merge_key),
        ).fetchone()
        if row is not None:
            return int(row["id"])
        cur = self.conn.execute(
            """
            INSERT INTO merged_conversations (merge_run_id, merge_key, kind, title)
            VALUES (?, ?, ?, ?)
            """,
            (merge_run_id, merge_key, kind, title),
        )
        return int(cur.lastrowid)

    def link_merged_conversation_participant(
        self,
        merged_conversation_id: int,
        participant_id: int,
        *,
        role: str,
        sort_order: int,
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT INTO merged_conversation_participants (
                merged_conversation_id, participant_id, role, sort_order
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(merged_conversation_id, participant_id) DO UPDATE SET
                role = excluded.role,
                sort_order = excluded.sort_order
            """,
            (merged_conversation_id, participant_id, role, sort_order),
        )

    def insert_merged_message(
        self,
        *,
        merge_run_id: int,
        merged_conversation_id: int,
        message_id: int,
        sort_order: int,
        fingerprint: str,
        is_duplicate: bool,
        duplicate_of_message_id: int | None,
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT OR REPLACE INTO merged_messages (
                merge_run_id,
                merged_conversation_id,
                message_id,
                sort_order,
                fingerprint,
                is_duplicate,
                duplicate_of_message_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                merge_run_id,
                merged_conversation_id,
                message_id,
                sort_order,
                fingerprint,
                int(is_duplicate),
                duplicate_of_message_id,
            ),
        )

    def insert_attachment(
        self,
        *,
        message_id: int,
        part_index: int,
        mime_type: str | None,
        filename: str | None,
        text_content: str | None,
        blob_id: int | None,
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT OR REPLACE INTO message_attachments (
                message_id, part_index, mime_type, filename, text_content, blob_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (message_id, part_index, mime_type, filename, text_content, blob_id),
        )

    def summary(self) -> ArchiveSummary:
        assert self.conn is not None
        counts = {
            "import_runs": self.conn.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0],
            "conversations": self.conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0],
            "participants": self.conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0],
            "messages": self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
            "attachment_parts": self.conn.execute(
                "SELECT COUNT(*) FROM message_attachments"
            ).fetchone()[0],
            "blobs": self.conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0],
            "blob_bytes": self.conn.execute(
                "SELECT COALESCE(SUM(byte_size), 0) FROM blobs"
            ).fetchone()[0],
        }
        return ArchiveSummary(
            archive_path=self.archive_path,
            import_runs=int(counts["import_runs"]),
            conversations=int(counts["conversations"]),
            participants=int(counts["participants"]),
            messages=int(counts["messages"]),
            attachment_parts=int(counts["attachment_parts"]),
            blobs=int(counts["blobs"]),
            blob_bytes=int(counts["blob_bytes"]),
        )

    @property
    def blob_store_dir(self) -> Path:
        return Path(f"{self.archive_path}.blobs")

    def _migrate_inline_blobs(self) -> None:
        assert self.conn is not None
        rows = self.conn.execute(
            """
            SELECT id, sha256, byte_size, data, storage_kind, external_relpath
            FROM blobs
            WHERE COALESCE(storage_kind, 'inline') != 'external'
               OR external_relpath IS NULL
               OR length(data) > 0
            """
        ).fetchall()
        for row in rows:
            self._ensure_blob_row_externalized(row)

    def _ensure_blob_row_externalized(self, row: sqlite3.Row) -> Path:
        assert self.conn is not None
        sha256 = str(row["sha256"])
        relpath = str(row["external_relpath"] or self._blob_relpath(sha256))
        dest_path = self._blob_absolute_path(relpath)
        if dest_path.exists():
            self.conn.execute(
                """
                UPDATE blobs
                SET storage_kind = 'external',
                    external_relpath = ?,
                    data = ?
                WHERE id = ?
                """,
                (relpath, sqlite3.Binary(b""), int(row["id"])),
            )
            return dest_path

        payload = bytes(row["data"] or b"")
        if not payload:
            raise FileNotFoundError(
                f"Blob {row['id']} ({sha256}) has no external file and no inline payload."
            )
        self._write_blob_bytes(dest_path, payload)
        self.conn.execute(
            """
            UPDATE blobs
            SET storage_kind = 'external',
                external_relpath = ?,
                data = ?
            WHERE id = ?
            """,
            (relpath, sqlite3.Binary(b""), int(row["id"])),
        )
        return dest_path

    def _blob_relpath(self, sha256: str) -> str:
        return f"{sha256[:2]}/{sha256}"

    def _blob_absolute_path(self, relpath: str) -> Path:
        return self.blob_store_dir / relpath

    def _write_blob_bytes(self, dest_path: Path, data: bytes) -> None:
        if dest_path.exists():
            return
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest_path.with_name(f".{dest_path.name}.tmp")
        tmp_path.write_bytes(data)
        tmp_path.replace(dest_path)

    def _copy_blob_file(self, source_path: Path, dest_path: Path) -> None:
        if dest_path.exists():
            return
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest_path.with_name(f".{dest_path.name}.tmp")
        with source_path.open("rb") as src, tmp_path.open("wb") as dst:
            _copy_stream(src, dst)
        tmp_path.replace(dest_path)


def detect_address_kind(address: str) -> str:
    if "@" in address:
        return "email"
    if any(ch.isdigit() for ch in address):
        return "phone"
    return "opaque"


def json_dumps_stable(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _ensure_schema_compat(conn: sqlite3.Connection) -> None:
    _ensure_column(conn, "import_runs", "source_fingerprint", "TEXT")
    _ensure_column(conn, "import_runs", "status", "TEXT NOT NULL DEFAULT 'completed'")
    _ensure_column(conn, "blobs", "storage_kind", "TEXT NOT NULL DEFAULT 'inline'")
    _ensure_column(conn, "blobs", "external_relpath", "TEXT")
    conn.execute("UPDATE import_runs SET status = 'completed' WHERE status IS NULL")
    conn.execute("UPDATE blobs SET storage_kind = 'inline' WHERE storage_kind IS NULL")


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _hash_path(path: Path, *, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
            byte_size += len(chunk)
    return digest.hexdigest(), byte_size


def _copy_stream(src: BinaryIO, dst: BinaryIO, *, chunk_size: int = 1024 * 1024) -> None:
    while chunk := src.read(chunk_size):
        dst.write(chunk)
