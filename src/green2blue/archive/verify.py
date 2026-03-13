"""Consistency verification for canonical green2blue archives."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from green2blue.archive.db import CanonicalArchive


@dataclass(frozen=True)
class ArchiveVerificationResult:
    archive_path: Path
    passed: bool
    checks_run: int
    checks_passed: int
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    latest_merge_id: int | None
    ios_inject_candidate_messages: int


def verify_archive(archive_path: Path | str) -> ArchiveVerificationResult:
    errors: list[str] = []
    warnings: list[str] = []
    checks_run = 0
    checks_passed = 0

    with CanonicalArchive(archive_path) as archive:
        conn = archive.conn
        assert conn is not None

        latest_merge_id = _latest_merge_id(conn)
        ios_inject_candidate_messages = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM merged_messages mm
            JOIN messages m ON m.id = mm.message_id
            WHERE mm.merge_run_id = COALESCE(
                (SELECT id FROM merge_runs ORDER BY id DESC LIMIT 1),
                -1
            )
              AND mm.is_duplicate = 0
              AND m.source_type != 'ios.message'
            """,
        )

        checks_run += 1
        mismatches = _import_run_mismatches(conn)
        if mismatches:
            errors.extend(mismatches)
        else:
            checks_passed += 1

        checks_run += 1
        running_count = _scalar(
            conn,
            "SELECT COUNT(*) FROM import_runs WHERE COALESCE(status, 'completed') != 'completed'",
        )
        if running_count:
            errors.append(f"{running_count} import runs are not marked completed.")
        else:
            checks_passed += 1

        checks_run += 1
        bad_attachment_flag_count = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM messages m
            WHERE m.has_attachments = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM message_attachments ma
                  WHERE ma.message_id = m.id
              )
            """,
        )
        if bad_attachment_flag_count:
            errors.append(
                f"{bad_attachment_flag_count} messages are flagged with attachments but have no attachment parts.",
            )
        else:
            checks_passed += 1

        checks_run += 1
        if latest_merge_id is None:
            checks_passed += 1
        else:
            merge_errors = _merge_mismatches(conn, latest_merge_id)
            if merge_errors:
                errors.extend(merge_errors)
            else:
                checks_passed += 1

        checks_run += 1
        missing_blob_count = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM message_attachments
            WHERE blob_id IS NULL
              AND text_content IS NULL
            """,
        )
        if missing_blob_count:
            warnings.append(
                f"{missing_blob_count} attachment parts are metadata-only and lack blob payloads.",
            )
        checks_passed += 1

        checks_run += 1
        missing_blob_files = archive.count_missing_blob_files()
        if missing_blob_files:
            errors.append(
                f"{missing_blob_files} external blob files referenced by the archive are missing from disk.",
            )
        else:
            checks_passed += 1

        checks_run += 1
        source_type_count = _scalar(conn, "SELECT COUNT(DISTINCT source_type) FROM import_runs")
        merge_run_count = _scalar(conn, "SELECT COUNT(*) FROM merge_runs")
        if source_type_count > 1 and merge_run_count == 0:
            warnings.append(
                "Archive contains multiple source types but no merged view has been materialized yet.",
            )
        checks_passed += 1

    return ArchiveVerificationResult(
        archive_path=Path(archive_path),
        passed=not errors,
        checks_run=checks_run,
        checks_passed=checks_passed if not errors else checks_passed,
        errors=tuple(errors),
        warnings=tuple(warnings),
        latest_merge_id=latest_merge_id,
        ios_inject_candidate_messages=ios_inject_candidate_messages,
    )


def _import_run_mismatches(conn: sqlite3.Connection) -> list[str]:
    errors: list[str] = []
    rows = conn.execute(
        """
        SELECT id, source_type, message_count, attachment_count
        FROM import_runs
        ORDER BY id
        """
    ).fetchall()
    for row in rows:
        import_run_id = int(row["id"])
        actual_messages = _scalar(
            conn,
            "SELECT COUNT(*) FROM messages WHERE import_run_id = ?",
            (import_run_id,),
        )
        actual_attachments = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM message_attachments ma
            JOIN messages m ON m.id = ma.message_id
            WHERE m.import_run_id = ?
              AND ma.text_content IS NULL
            """,
            (import_run_id,),
        )
        if actual_messages != int(row["message_count"]):
            errors.append(
                f"Import run {import_run_id} ({row['source_type']}) records {row['message_count']} messages but stores {actual_messages}.",
            )
        if actual_attachments != int(row["attachment_count"]):
            errors.append(
                f"Import run {import_run_id} ({row['source_type']}) records {row['attachment_count']} attachments but stores {actual_attachments}.",
            )
    return errors


def _merge_mismatches(conn: sqlite3.Connection, merge_run_id: int) -> list[str]:
    errors: list[str] = []
    row = conn.execute(
        """
        SELECT merged_conversation_count, merged_message_count, duplicate_message_count
        FROM merge_runs
        WHERE id = ?
        """,
        (merge_run_id,),
    ).fetchone()
    if row is None:
        return [f"Latest merge run {merge_run_id} does not exist."]

    actual_conversations = _scalar(
        conn,
        "SELECT COUNT(*) FROM merged_conversations WHERE merge_run_id = ?",
        (merge_run_id,),
    )
    actual_winners = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM merged_messages
        WHERE merge_run_id = ?
          AND is_duplicate = 0
        """,
        (merge_run_id,),
    )
    actual_duplicates = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM merged_messages
        WHERE merge_run_id = ?
          AND is_duplicate = 1
        """,
        (merge_run_id,),
    )

    if actual_conversations != int(row["merged_conversation_count"]):
        errors.append(
            f"Merge run {merge_run_id} records {row['merged_conversation_count']} merged conversations but stores {actual_conversations}.",
        )
    if actual_winners != int(row["merged_message_count"]):
        errors.append(
            f"Merge run {merge_run_id} records {row['merged_message_count']} merged winner messages but stores {actual_winners}.",
        )
    if actual_duplicates != int(row["duplicate_message_count"]):
        errors.append(
            f"Merge run {merge_run_id} records {row['duplicate_message_count']} duplicates but stores {actual_duplicates}.",
        )
    return errors


def _latest_merge_id(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT id FROM merge_runs ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:
        return None
    return int(row["id"])


def _scalar(conn: sqlite3.Connection, query: str, params: tuple[object, ...] = ()) -> int:
    return int(conn.execute(query, params).fetchone()[0])
