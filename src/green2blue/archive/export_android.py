"""Export merged canonical archives as Android-style ZIPs."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from green2blue.archive.db import CanonicalArchive
from green2blue.archive.merge import merge_archive

ANDROID_ATTACHMENT_ROOT = "/data/user/0/com.android.providers.telephony/app_parts"


@dataclass(frozen=True)
class AndroidArchiveExportResult:
    archive_path: Path
    output_zip: Path
    merge_run_id: int
    records_written: int
    attachment_files_written: int
    attachments_missing_data: int


def export_merged_android_zip(
    archive_path: Path | str,
    output_zip: Path | str,
    *,
    merge_run_id: int | None = None,
    country: str = "US",
    mode: str = "full",
) -> AndroidArchiveExportResult:
    archive_path = Path(archive_path)
    output_zip = Path(output_zip)
    resolved_merge_run_id = _resolve_merge_run_id(archive_path, merge_run_id, country)

    with CanonicalArchive(archive_path) as archive:
        conn = archive.conn
        assert conn is not None
        participants = _load_merged_participants(conn, resolved_merge_run_id)
        attachments = _load_message_parts(conn, resolved_merge_run_id)

        output_zip.parent.mkdir(parents=True, exist_ok=True)
        records_written = 0
        attachment_files_written = 0
        attachments_missing_data = 0

        with tempfile.NamedTemporaryFile(suffix=".ndjson", delete=False) as tmp_ndjson:
            tmp_ndjson_path = Path(tmp_ndjson.name)
            try:
                with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    thread_map: dict[int, int] = {}
                    for index, message in enumerate(
                        _iter_merged_winners(conn, resolved_merge_run_id, mode=mode),
                        start=1,
                    ):
                        merged_conversation_id = int(message["merged_conversation_id"])
                        thread_id = thread_map.setdefault(merged_conversation_id, len(thread_map) + 1)
                        participant_rows = participants.get(merged_conversation_id, ())
                        part_rows = attachments.get(int(message["id"]), ())
                        record, new_files, missing = _build_android_record(
                            message,
                            participant_rows,
                            part_rows,
                            thread_id=thread_id,
                            ordinal=index,
                        )
                        tmp_ndjson.write(
                            (json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
                        )
                        for rel_path, payload in new_files:
                            zf.writestr(rel_path, payload)
                        records_written += 1
                        attachments_missing_data += missing
                        attachment_files_written += len(new_files)
                    tmp_ndjson.flush()
                    zf.write(tmp_ndjson_path, arcname="messages.ndjson")
            finally:
                tmp_ndjson_path.unlink(missing_ok=True)

    return AndroidArchiveExportResult(
        archive_path=archive_path,
        output_zip=output_zip,
        merge_run_id=resolved_merge_run_id,
        records_written=records_written,
        attachment_files_written=attachment_files_written,
        attachments_missing_data=attachments_missing_data,
    )


def _resolve_merge_run_id(
    archive_path: Path,
    merge_run_id: int | None,
    country: str,
) -> int:
    if merge_run_id is not None:
        return merge_run_id

    with CanonicalArchive(archive_path) as archive:
        conn = archive.conn
        assert conn is not None
        row = conn.execute("SELECT id FROM merge_runs ORDER BY id DESC LIMIT 1").fetchone()
        if row is not None:
            return int(row["id"])

    result = merge_archive(archive_path, country=country)
    return result.merge_run_id


def _load_merged_winners(
    conn: sqlite3.Connection,
    merge_run_id: int,
    *,
    mode: str,
) -> list[sqlite3.Row]:
    return list(_iter_merged_winners(conn, merge_run_id, mode=mode))


def _iter_merged_winners(
    conn: sqlite3.Connection,
    merge_run_id: int,
    *,
    mode: str,
) -> sqlite3.Cursor:
    query = """
        SELECT
            mm.merged_conversation_id,
            mm.sort_order,
            m.*
        FROM merged_messages mm
        JOIN messages m ON m.id = mm.message_id
        WHERE mm.merge_run_id = ?
          AND mm.is_duplicate = 0
    """
    params: list[object] = [merge_run_id]
    if mode == "ios-inject":
        query += " AND m.source_type != 'ios.message'"
    query += " ORDER BY mm.merged_conversation_id, mm.sort_order, m.sent_at_ms, m.id"
    return conn.execute(query, params)


def _load_merged_participants(
    conn: sqlite3.Connection,
    merge_run_id: int,
) -> dict[int, tuple[sqlite3.Row, ...]]:
    rows = conn.execute(
        """
        SELECT
            mcp.merged_conversation_id,
            mcp.sort_order,
            p.address,
            p.kind
        FROM merged_conversation_participants mcp
        JOIN participants p ON p.id = mcp.participant_id
        JOIN merged_conversations mc ON mc.id = mcp.merged_conversation_id
        WHERE mc.merge_run_id = ?
        ORDER BY mcp.merged_conversation_id, mcp.sort_order, p.id
        """,
        (merge_run_id,),
    ).fetchall()
    grouped: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(int(row["merged_conversation_id"]), []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def _load_message_parts(
    conn: sqlite3.Connection,
    merge_run_id: int,
) -> dict[int, tuple[sqlite3.Row, ...]]:
    rows = conn.execute(
        """
        SELECT
            ma.message_id,
            ma.part_index,
            ma.mime_type,
            ma.filename,
            ma.text_content,
            b.data AS blob_data
        FROM merged_messages mm
        JOIN message_attachments ma ON ma.message_id = mm.message_id
        LEFT JOIN blobs b ON b.id = ma.blob_id
        WHERE mm.merge_run_id = ?
          AND mm.is_duplicate = 0
        ORDER BY ma.message_id, ma.part_index
        """,
        (merge_run_id,),
    ).fetchall()
    grouped: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(int(row["message_id"]), []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def _build_android_record(
    message: sqlite3.Row,
    participants: tuple[sqlite3.Row, ...],
    parts: tuple[sqlite3.Row, ...],
    *,
    thread_id: int,
    ordinal: int,
) -> tuple[dict[str, object], list[tuple[str, bytes]], int]:
    direction = str(message["direction"])
    body_text = message["body_text"]
    subject = message["subject"]
    sent_at_ms = int(message["sent_at_ms"])
    read_flag = 0 if str(message["read_state"]) == "unread" else 1
    participant_addresses = [str(row["address"]) for row in participants]
    primary_address = participant_addresses[0] if participant_addresses else _fallback_address(message)
    has_binary_parts = any(row["blob_data"] is not None for row in parts)
    is_group = len(participant_addresses) > 1

    if not has_binary_parts and not is_group and not subject:
        sms_record = {
            "address": primary_address,
            "body": body_text or "",
            "date": str(sent_at_ms),
            "type": "2" if direction == "outgoing" else "1",
            "read": str(read_flag),
            "date_sent": str(sent_at_ms),
            "thread_id": str(thread_id),
        }
        return sms_record, [], 0

    mms_parts: list[dict[str, object]] = []
    files_to_write: list[tuple[str, bytes]] = []
    attachments_missing_data = 0
    seq = 0

    if body_text:
        mms_parts.append({
            "seq": str(seq),
            "ct": "text/plain",
            "text": body_text,
        })
        seq += 1

    for part_index, part in enumerate(parts):
        if part["blob_data"] is None:
            if part["text_content"]:
                mms_parts.append({
                    "seq": str(seq),
                    "ct": part["mime_type"] or "text/plain",
                    "text": part["text_content"],
                })
                seq += 1
            else:
                attachments_missing_data += 1
            continue

        basename = _part_basename(
            filename=part["filename"],
            mime_type=part["mime_type"],
            ordinal=ordinal,
            part_index=part_index,
        )
        mms_parts.append({
            "seq": str(seq),
            "ct": part["mime_type"] or "application/octet-stream",
            "_data": f"{ANDROID_ATTACHMENT_ROOT}/{basename}",
            "cl": basename,
        })
        files_to_write.append((f"data/{basename}", bytes(part["blob_data"])))
        seq += 1

    sender, recipients = _sender_and_recipients(
        direction=direction,
        primary_address=primary_address,
        participant_addresses=participant_addresses,
    )
    record = {
        "date": str(max(1, sent_at_ms // 1000)),
        "date_sent": str(max(1, sent_at_ms // 1000)),
        "msg_box": "2" if direction == "outgoing" else "1",
        "read": str(read_flag),
        "sub": subject,
        "ct_t": "application/vnd.wap.multipart.related",
        "thread_id": str(thread_id),
        "__parts": mms_parts,
        "__sender_address": {
            "address": sender,
            "type": "137",
            "charset": "106",
        },
        "__recipient_addresses": [
            {
                "address": recipient,
                "type": "151",
                "charset": "106",
            }
            for recipient in recipients
        ],
    }
    return record, files_to_write, attachments_missing_data


def _sender_and_recipients(
    *,
    direction: str,
    primary_address: str,
    participant_addresses: list[str],
) -> tuple[str, list[str]]:
    if not participant_addresses:
        return primary_address, [primary_address]

    if direction == "incoming":
        sender = primary_address
        recipients = [addr for addr in participant_addresses if addr != sender]
        if not recipients:
            recipients = [sender]
        return sender, recipients

    sender = primary_address
    recipients = participant_addresses[:]
    return sender, recipients


def _part_basename(
    *,
    filename: str | None,
    mime_type: str | None,
    ordinal: int,
    part_index: int,
) -> str:
    raw_name = Path(filename).name if filename else ""
    if raw_name:
        return raw_name
    ext = _extension_for_mime(mime_type)
    return f"PART_{ordinal:06d}_{part_index:02d}{ext}"


def _extension_for_mime(mime_type: str | None) -> str:
    mapping = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/heic": ".heic",
        "video/mp4": ".mp4",
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
    }
    return mapping.get(mime_type or "", ".bin")


def _fallback_address(message: sqlite3.Row) -> str:
    return f"+1999000{int(message['id']):04d}"
