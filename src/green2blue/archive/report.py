"""Archive-level reporting for canonical green2blue archives."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from green2blue.archive.db import ArchiveSummary, CanonicalArchive


@dataclass(frozen=True)
class ArchiveReport:
    summary: ArchiveSummary
    source_type_counts: dict[str, int]
    conversation_kind_counts: dict[str, int]
    direction_counts: dict[str, int]
    service_hint_counts: dict[str, int]
    messages_with_attachments: int
    messages_with_url: int
    top_attachment_mime_types: tuple[tuple[str, int], ...]
    warnings: tuple[str, ...]


def build_archive_report(archive_path: Path | str) -> ArchiveReport:
    with CanonicalArchive(archive_path) as archive:
        summary = archive.summary()
        conn = archive.conn
        assert conn is not None
        report = ArchiveReport(
            summary=summary,
            source_type_counts=_count_map(conn, "SELECT source_type, COUNT(*) FROM messages GROUP BY source_type"),
            conversation_kind_counts=_count_map(conn, "SELECT kind, COUNT(*) FROM conversations GROUP BY kind"),
            direction_counts=_count_map(conn, "SELECT direction, COUNT(*) FROM messages GROUP BY direction"),
            service_hint_counts=_count_map(
                conn,
                "SELECT COALESCE(service_hint, '(unknown)'), COUNT(*) FROM messages GROUP BY COALESCE(service_hint, '(unknown)')",
            ),
            messages_with_attachments=_scalar(
                conn,
                "SELECT COUNT(*) FROM messages WHERE has_attachments = 1",
            ),
            messages_with_url=_scalar(
                conn,
                "SELECT COUNT(*) FROM messages WHERE has_url = 1",
            ),
            top_attachment_mime_types=_top_attachment_mime_types(conn),
            warnings=_build_warnings(conn),
        )
    return report


def _count_map(conn: sqlite3.Connection, query: str) -> dict[str, int]:
    return {
        str(key): int(count)
        for key, count in conn.execute(query).fetchall()
    }


def _scalar(conn: sqlite3.Connection, query: str) -> int:
    return int(conn.execute(query).fetchone()[0])


def _top_attachment_mime_types(conn: sqlite3.Connection) -> tuple[tuple[str, int], ...]:
    rows = conn.execute(
        """
        SELECT COALESCE(mime_type, '(unknown)') AS mime, COUNT(*) AS cnt
        FROM message_attachments
        WHERE blob_id IS NOT NULL
        GROUP BY COALESCE(mime_type, '(unknown)')
        ORDER BY cnt DESC, mime ASC
        LIMIT 10
        """
    ).fetchall()
    return tuple((str(row["mime"]), int(row["cnt"])) for row in rows)


def _build_warnings(conn: sqlite3.Connection) -> tuple[str, ...]:
    warnings: list[str] = []

    import_source_types = {
        row[0]
        for row in conn.execute("SELECT DISTINCT source_type FROM import_runs").fetchall()
    }
    if len(import_source_types) > 1:
        warnings.append(
            "Archive contains multiple source imports, but cross-source merge and dedupe are not implemented yet.",
        )

    rcs_like_count = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM messages
        WHERE source_type LIKE 'android.%'
          AND raw_json LIKE '%"rcs_message_type"%'
        """,
    )
    if rcs_like_count:
        warnings.append(
            f"{rcs_like_count} RCS-like Android records are currently preserved through the SMS/MMS compatibility path.",
        )

    return tuple(warnings)
