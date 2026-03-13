"""Target-side verification for archive-rendered iPhone backup injections."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from green2blue.converter.message_converter import convert_messages
from green2blue.exceptions import EncryptedBackupError
from green2blue.ios.attributed_body import build_attributed_body_with_metadata
from green2blue.ios.backup import find_backup, get_sms_db_path
from green2blue.ios.crypto import EncryptedBackup
from green2blue.ios.manifest import ManifestDB, compute_file_id
from green2blue.ios.sms_db import _build_hackpatrol_attributed_body
from green2blue.models import (
    CKStrategy,
    InjectionMode,
    compose_message_text,
    iOSAttachment,
    iOSMessage,
)
from green2blue.parser.ndjson_parser import parse_ndjson
from green2blue.parser.zip_reader import open_export_zip
from green2blue.pipeline import PipelineResult


@dataclass(frozen=True)
class IOSRenderedTargetVerificationResult:
    backup_path: Path
    export_zip: Path
    passed: bool
    checks_run: int
    checks_passed: int
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    injection_mode: str
    expected_messages: int
    actual_messages: int
    expected_attachments: int
    actual_attachments: int
    message_rowids: tuple[int, ...]
    attachment_rowids: tuple[int, ...]


def verify_ios_render_target(
    export_zip: Path | str,
    backup: Path | str,
    pipeline_result: PipelineResult,
    *,
    country: str = "US",
    skip_duplicates: bool = True,
    password: str | None = None,
    ck_strategy: CKStrategy = CKStrategy.NONE,
    service: str = "SMS",
) -> IOSRenderedTargetVerificationResult:
    """Verify that the modified iPhone backup matches the rendered export.

    This compares the specific message/attachment rowids touched by the latest
    pipeline run against the stage export re-rendered through the normal
    conversion layer. It is intentionally narrower than backup integrity
    verification: the goal is to prove that the rows we just wrote look like
    the rows we intended to write.
    """
    export_zip = Path(export_zip)
    checks_run = 0
    checks_passed = 0
    errors: list[str] = []
    warnings: list[str] = []

    injection_mode, message_rowids, attachment_rowids = _resolve_pipeline_targets(pipeline_result)

    expected_signatures, expected_attachment_signatures = _load_expected_render_signatures(
        export_zip,
        country=country,
        skip_duplicates=skip_duplicates,
        ck_strategy=ck_strategy,
        service=service,
        injection_mode=injection_mode,
    )

    if not message_rowids and not attachment_rowids:
        warnings.append("No targeted rowids were recorded for the completed pipeline run.")
        backup_info = find_backup(str(backup))
        return IOSRenderedTargetVerificationResult(
            backup_path=backup_info.path,
            export_zip=export_zip,
            passed=True,
            checks_run=0,
            checks_passed=0,
            errors=(),
            warnings=tuple(warnings),
            injection_mode=injection_mode.value,
            expected_messages=sum(expected_signatures.values()),
            actual_messages=0,
            expected_attachments=sum(expected_attachment_signatures.values()),
            actual_attachments=0,
            message_rowids=message_rowids,
            attachment_rowids=attachment_rowids,
        )

    with _open_backup_sms_db(backup, password=password) as (backup_path, conn):
        (
            actual_message_signatures,
            actual_attachment_signatures,
            missing_message_rowids,
            missing_attachment_rowids,
            unchatted_message_rowids,
            unjoined_attachment_rowids,
        ) = _load_actual_render_signatures(
            conn,
            message_rowids=message_rowids,
            attachment_rowids=attachment_rowids,
            injection_mode=injection_mode,
        )

    checks_run += 1
    if missing_message_rowids:
        errors.append(
            "Target sms.db is missing message rowids from the completed pipeline run: "
            + ", ".join(str(rowid) for rowid in missing_message_rowids[:10]),
        )
    else:
        checks_passed += 1

    checks_run += 1
    if missing_attachment_rowids:
        errors.append(
            "Target sms.db is missing attachment rowids from the completed pipeline run: "
            + ", ".join(str(rowid) for rowid in missing_attachment_rowids[:10]),
        )
    else:
        checks_passed += 1

    checks_run += 1
    if unchatted_message_rowids:
        errors.append(
            "Injected messages are missing chat_message_join links: "
            + ", ".join(str(rowid) for rowid in unchatted_message_rowids[:10]),
        )
    else:
        checks_passed += 1

    checks_run += 1
    if unjoined_attachment_rowids:
        errors.append(
            "Injected attachments are not linked back to the targeted messages: "
            + ", ".join(str(rowid) for rowid in unjoined_attachment_rowids[:10]),
        )
    else:
        checks_passed += 1

    checks_run += 1
    unexpected_messages = actual_message_signatures - expected_signatures
    if unexpected_messages:
        errors.append("Target sms.db messages do not match the rendered stage export.")
    else:
        checks_passed += 1

    checks_run += 1
    unexpected_attachments = actual_attachment_signatures - expected_attachment_signatures
    if unexpected_attachments:
        errors.append("Target sms.db attachments do not match the rendered stage export.")
    else:
        checks_passed += 1

    expected_message_count = sum(expected_signatures.values())
    actual_message_count = sum(actual_message_signatures.values())
    if expected_message_count > actual_message_count:
        warnings.append(
            f"{expected_message_count - actual_message_count} rendered stage messages did not map "
            "to new targeted rows (likely duplicate-skipped or intentionally reused).",
        )

    expected_attachment_count = sum(expected_attachment_signatures.values())
    actual_attachment_count = sum(actual_attachment_signatures.values())
    if expected_attachment_count > actual_attachment_count:
        warnings.append(
            f"{expected_attachment_count - actual_attachment_count} rendered attachment parts did "
            "not map to targeted attachment rows.",
        )

    return IOSRenderedTargetVerificationResult(
        backup_path=backup_path,
        export_zip=export_zip,
        passed=not errors,
        checks_run=checks_run,
        checks_passed=checks_passed,
        errors=tuple(errors),
        warnings=tuple(warnings),
        injection_mode=injection_mode.value,
        expected_messages=expected_message_count,
        actual_messages=actual_message_count,
        expected_attachments=expected_attachment_count,
        actual_attachments=actual_attachment_count,
        message_rowids=message_rowids,
        attachment_rowids=attachment_rowids,
    )


def _load_expected_render_signatures(
    export_zip: Path,
    *,
    country: str,
    skip_duplicates: bool,
    ck_strategy: CKStrategy,
    service: str,
    injection_mode: InjectionMode,
) -> tuple[Counter[str], Counter[str]]:
    with open_export_zip(export_zip) as export:
        android_messages = list(parse_ndjson(export.ndjson_path))
        conversion = convert_messages(
            android_messages,
            country,
            skip_duplicates,
            ck_strategy=ck_strategy,
            service=service,
        )

    message_signatures: Counter[str] = Counter()
    attachment_signatures: Counter[str] = Counter()
    for message in conversion.messages:
        message_signatures[_expected_message_signature(message, injection_mode)] += 1
        for attachment in message.attachments:
            attachment_signatures[_attachment_signature_from_expected(attachment)] += 1

    return message_signatures, attachment_signatures


@contextmanager
def _open_backup_sms_db(
    backup: Path | str,
    *,
    password: str | None,
):
    backup_info = find_backup(str(backup))
    temp_paths: list[Path] = []
    sms_db_path = get_sms_db_path(backup_info.path)

    try:
        if backup_info.is_encrypted:
            if not password:
                raise EncryptedBackupError(
                    "Encrypted backup requires a password for rendered target verification.",
                )

            encrypted_backup = EncryptedBackup(backup_info.path, password)
            encrypted_backup.unlock()
            manifest_path = encrypted_backup.decrypt_manifest_db()
            temp_paths.append(manifest_path)

            sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
            with ManifestDB(manifest_path) as manifest:
                sms_enc_key, sms_protection_class = manifest.get_file_encryption_info(sms_file_id)

            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                sms_db_path = Path(tmp.name)
            encrypted_backup.decrypt_db_file_to_path(
                get_sms_db_path(backup_info.path),
                sms_enc_key,
                sms_protection_class,
                sms_db_path,
            )
            temp_paths.append(sms_db_path)

        conn = sqlite3.connect(sms_db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield backup_info.path, conn
        finally:
            conn.close()
    finally:
        for path in temp_paths:
            path.unlink(missing_ok=True)


def _load_actual_render_signatures(
    conn: sqlite3.Connection,
    *,
    message_rowids: tuple[int, ...],
    attachment_rowids: tuple[int, ...],
    injection_mode: InjectionMode,
) -> tuple[
    Counter[str], Counter[str], tuple[int, ...], tuple[int, ...], tuple[int, ...], tuple[int, ...]
]:
    resolved_message_rowids = _dedupe_rowids(message_rowids)
    resolved_attachment_rowids = _dedupe_rowids(attachment_rowids)

    message_rows = _fetch_message_rows(conn, resolved_message_rowids)
    chat_ids_by_message = _fetch_chat_ids(conn, resolved_message_rowids)
    chat_identifier_map = _fetch_chat_identifiers(conn, tuple(chat_ids_by_message.values()))
    participant_map = _fetch_chat_participants(conn, tuple(chat_ids_by_message.values()))
    attachment_map, joined_attachment_rowids = _fetch_message_attachments(
        conn, resolved_message_rowids
    )
    present_attachment_rowids = _fetch_attachment_rowids(conn, resolved_attachment_rowids)

    actual_message_signatures: Counter[str] = Counter()
    actual_attachment_signatures: Counter[str] = Counter()

    for rowid in resolved_message_rowids:
        row = message_rows.get(rowid)
        if row is None:
            continue

        chat_id = chat_ids_by_message.get(rowid)
        attachments = attachment_map.get(rowid, ())
        actual_message_signatures[
            _actual_message_signature(
                row,
                chat_identifier=chat_identifier_map.get(chat_id, row["handle_id_text"] or ""),
                group_members=participant_map.get(chat_id, ()),
                attachments=attachments,
                injection_mode=injection_mode,
            )
        ] += 1
        for attachment in attachments:
            actual_attachment_signatures[_attachment_signature_from_actual(attachment)] += 1

    missing_message_rowids = tuple(
        rowid for rowid in resolved_message_rowids if rowid not in message_rows
    )
    missing_attachment_rowids = tuple(
        rowid for rowid in resolved_attachment_rowids if rowid not in present_attachment_rowids
    )
    unchatted_message_rowids = tuple(
        rowid
        for rowid in resolved_message_rowids
        if rowid in message_rows and rowid not in chat_ids_by_message
    )
    unjoined_attachment_rowids = tuple(
        rowid for rowid in resolved_attachment_rowids if rowid not in joined_attachment_rowids
    )

    return (
        actual_message_signatures,
        actual_attachment_signatures,
        missing_message_rowids,
        missing_attachment_rowids,
        unchatted_message_rowids,
        unjoined_attachment_rowids,
    )


def _fetch_message_rows(
    conn: sqlite3.Connection,
    rowids: tuple[int, ...],
) -> dict[int, sqlite3.Row]:
    if not rowids:
        return {}
    placeholders = ",".join("?" for _ in rowids)
    rows = conn.execute(
        f"""
        SELECT
            m.ROWID AS rowid,
            m.text,
            m.service,
            m.date,
            m.is_from_me,
            m.handle_id,
            m.cache_has_attachments,
            m.part_count,
            m.has_dd_results,
            m.attributedBody,
            h.id AS handle_id_text
        FROM message m
        LEFT JOIN handle h ON h.ROWID = m.handle_id
        WHERE m.ROWID IN ({placeholders})
        """,
        rowids,
    ).fetchall()
    return {int(row["rowid"]): row for row in rows}


def _fetch_chat_ids(
    conn: sqlite3.Connection,
    message_rowids: tuple[int, ...],
) -> dict[int, int]:
    if not message_rowids:
        return {}
    placeholders = ",".join("?" for _ in message_rowids)
    rows = conn.execute(
        f"""
        SELECT message_id, MIN(chat_id) AS chat_id
        FROM chat_message_join
        WHERE message_id IN ({placeholders})
        GROUP BY message_id
        """,
        message_rowids,
    ).fetchall()
    return {int(row["message_id"]): int(row["chat_id"]) for row in rows}


def _fetch_chat_participants(
    conn: sqlite3.Connection,
    chat_ids: tuple[int, ...],
) -> dict[int, tuple[str, ...]]:
    if not chat_ids:
        return {}
    placeholders = ",".join("?" for _ in chat_ids)
    rows = conn.execute(
        f"""
        SELECT chj.chat_id, h.id AS address
        FROM chat_handle_join chj
        JOIN handle h ON h.ROWID = chj.handle_id
        WHERE chj.chat_id IN ({placeholders})
        ORDER BY chj.chat_id, h.id
        """,
        chat_ids,
    ).fetchall()
    grouped: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        grouped[int(row["chat_id"])].append(str(row["address"]))
    return {chat_id: tuple(sorted(addresses)) for chat_id, addresses in grouped.items()}


def _fetch_chat_identifiers(
    conn: sqlite3.Connection,
    chat_ids: tuple[int, ...],
) -> dict[int, str]:
    if not chat_ids:
        return {}
    placeholders = ",".join("?" for _ in chat_ids)
    rows = conn.execute(
        f"SELECT ROWID, chat_identifier FROM chat WHERE ROWID IN ({placeholders})",
        chat_ids,
    ).fetchall()
    return {int(row["ROWID"]): str(row["chat_identifier"] or "") for row in rows}


def _fetch_message_attachments(
    conn: sqlite3.Connection,
    message_rowids: tuple[int, ...],
) -> tuple[dict[int, tuple[sqlite3.Row, ...]], set[int]]:
    if not message_rowids:
        return {}, set()
    placeholders = ",".join("?" for _ in message_rowids)
    rows = conn.execute(
        f"""
        SELECT
            maj.message_id,
            a.ROWID AS attachment_rowid,
            a.guid,
            a.filename,
            a.mime_type,
            a.transfer_name,
            a.total_bytes
        FROM message_attachment_join maj
        JOIN attachment a ON a.ROWID = maj.attachment_id
        WHERE maj.message_id IN ({placeholders})
        ORDER BY maj.message_id, a.ROWID
        """,
        message_rowids,
    ).fetchall()
    grouped: dict[int, list[sqlite3.Row]] = defaultdict(list)
    joined_rowids: set[int] = set()
    for row in rows:
        grouped[int(row["message_id"])].append(row)
        joined_rowids.add(int(row["attachment_rowid"]))
    return {message_id: tuple(values) for message_id, values in grouped.items()}, joined_rowids


def _fetch_attachment_rowids(
    conn: sqlite3.Connection,
    attachment_rowids: tuple[int, ...],
) -> set[int]:
    if not attachment_rowids:
        return set()
    placeholders = ",".join("?" for _ in attachment_rowids)
    rows = conn.execute(
        f"SELECT ROWID FROM attachment WHERE ROWID IN ({placeholders})",
        attachment_rowids,
    ).fetchall()
    return {int(row[0]) for row in rows}


def _expected_message_signature(msg: iOSMessage, injection_mode: InjectionMode) -> str:
    display_text = compose_message_text(msg.text, len(msg.attachments)) or ""
    caption_text = (
        display_text[len(msg.attachments) :] if msg.attachments and display_text else display_text
    )
    part_count = len(msg.attachments) + (1 if caption_text else 0)
    if part_count == 0:
        part_count = 1

    attributed_sha1 = None
    has_dd_results = 0
    if injection_mode == InjectionMode.CLONE:
        attributed = _build_hackpatrol_attributed_body(msg.text)
        attributed_sha1 = _sha1_or_none(attributed)
    else:
        attributed, has_dd_results = build_attributed_body_with_metadata(
            display_text,
            attachment_guids=tuple(att.guid for att in msg.attachments),
        )
        if not msg.attachments:
            attributed_sha1 = _sha1_or_none(attributed)

    group_members = tuple(sorted(msg.group_members))
    if len(group_members) <= 1:
        group_members = ()

    if injection_mode == InjectionMode.CLONE:
        payload = {
            "attributed_body_sha1": attributed_sha1,
            "chat_identifier": msg.chat_identifier or msg.handle_id,
            "date": msg.date,
            "handle_id": msg.handle_id,
            "is_from_me": int(msg.is_from_me),
            "service": msg.service,
            "text": msg.text or "",
        }
    else:
        payload = {
            "attachments": sorted(
                _attachment_signature_payload_from_expected(att) for att in msg.attachments
            ),
            "attributed_body_sha1": attributed_sha1,
            "cache_has_attachments": 1 if msg.attachments else 0,
            "chat_identifier": msg.chat_identifier or msg.handle_id,
            "date": msg.date,
            "group_members": list(group_members),
            "handle_id": msg.handle_id,
            "has_dd_results": int(has_dd_results),
            "is_from_me": int(msg.is_from_me),
            "part_count": part_count,
            "service": msg.service,
            "text": display_text,
        }
    return _stable_json(payload)


def _actual_message_signature(
    row: sqlite3.Row,
    *,
    chat_identifier: str,
    group_members: tuple[str, ...],
    attachments: tuple[sqlite3.Row, ...],
    injection_mode: InjectionMode,
) -> str:
    has_dd_results = 0 if row["has_dd_results"] is None else int(row["has_dd_results"])
    attributed_sha1 = None
    if injection_mode == InjectionMode.CLONE or not attachments:
        attributed_sha1 = _sha1_or_none(row["attributedBody"])

    normalized_group_members = tuple(sorted(group_members))
    if len(normalized_group_members) <= 1:
        normalized_group_members = ()

    if injection_mode == InjectionMode.CLONE:
        payload = {
            "attributed_body_sha1": attributed_sha1,
            "chat_identifier": chat_identifier,
            "date": int(row["date"] or 0),
            "handle_id": row["handle_id_text"] or "",
            "is_from_me": int(row["is_from_me"] or 0),
            "service": row["service"] or "",
            "text": row["text"] or "",
        }
    else:
        payload = {
            "attachments": sorted(
                _attachment_signature_payload_from_actual(attachment) for attachment in attachments
            ),
            "attributed_body_sha1": attributed_sha1,
            "cache_has_attachments": int(row["cache_has_attachments"] or 0),
            "chat_identifier": chat_identifier,
            "date": int(row["date"] or 0),
            "group_members": list(normalized_group_members),
            "handle_id": row["handle_id_text"] or "",
            "has_dd_results": has_dd_results,
            "is_from_me": int(row["is_from_me"] or 0),
            "part_count": int(row["part_count"] or 0),
            "service": row["service"] or "",
            "text": row["text"] or "",
        }
    return _stable_json(payload)


def _attachment_signature_from_expected(att: iOSAttachment) -> str:
    return _stable_json(_attachment_signature_payload_from_expected(att))


def _attachment_signature_payload_from_expected(att: iOSAttachment) -> dict[str, object]:
    return {
        "filename_basename": _basename(att.filename),
        "mime_type": att.mime_type or "",
        "transfer_name": att.transfer_name or "",
    }


def _attachment_signature_from_actual(row: sqlite3.Row) -> str:
    return _stable_json(_attachment_signature_payload_from_actual(row))


def _attachment_signature_payload_from_actual(row: sqlite3.Row) -> dict[str, object]:
    return {
        "filename_basename": _basename(row["filename"]),
        "mime_type": row["mime_type"] or "",
        "transfer_name": row["transfer_name"] or "",
    }


def _resolve_pipeline_targets(
    pipeline_result: PipelineResult,
) -> tuple[InjectionMode, tuple[int, ...], tuple[int, ...]]:
    if pipeline_result.clone_stats is not None:
        return (
            InjectionMode.CLONE,
            tuple(pipeline_result.clone_stats.message_rowids),
            tuple(pipeline_result.clone_stats.attachment_rowids),
        )
    if pipeline_result.overwrite_stats is not None:
        return (
            InjectionMode.OVERWRITE,
            tuple(pipeline_result.overwrite_stats.message_rowids),
            tuple(pipeline_result.overwrite_stats.attachment_rowids),
        )
    if pipeline_result.injection_stats is not None:
        return (
            InjectionMode.INSERT,
            tuple(pipeline_result.injection_stats.message_rowids),
            tuple(pipeline_result.injection_stats.attachment_rowids),
        )
    return InjectionMode.INSERT, (), ()


def _dedupe_rowids(rowids: tuple[int, ...]) -> tuple[int, ...]:
    return tuple(dict.fromkeys(int(rowid) for rowid in rowids))


def _sha1_or_none(payload: bytes | None) -> str | None:
    if not payload:
        return None
    return hashlib.sha1(payload).hexdigest()


def _basename(value: str | None) -> str:
    if not value:
        return ""
    return Path(str(value).removeprefix("~/")).name


def _stable_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
