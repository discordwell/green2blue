"""Import iPhone backups into the canonical archive."""

from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from green2blue.archive.db import CanonicalArchive, detect_address_kind, json_dumps_stable
from green2blue.converter.timestamp import ios_ns_to_unix_ms
from green2blue.exceptions import EncryptedBackupError
from green2blue.ios.backup import find_backup, get_sms_db_path
from green2blue.ios.manifest import ManifestDB, compute_file_id
from green2blue.models import ATTACHMENT_PLACEHOLDER


@dataclass(frozen=True)
class IOSArchiveImportResult:
    archive_path: Path
    import_run_id: int
    reused_existing: bool
    messages_imported: int
    messages_deduped: int
    conversations_touched: int
    participants_touched: int
    attachments_imported: int
    blobs_imported: int
    backup_path: Path
    backup_udid: str


@dataclass(frozen=True)
class _AttachmentRow:
    rowid: int
    guid: str
    mime_type: str | None
    transfer_name: str | None
    filename: str | None
    total_bytes: int | None


def import_ios_backup(
    backup: str | Path | None,
    archive_path: Path | str,
    *,
    backup_root: Path | None = None,
    password: str | None = None,
    resume: bool = True,
) -> IOSArchiveImportResult:
    """Import an iPhone backup's Messages data into the canonical archive."""
    backup_info = find_backup(str(backup) if backup is not None else None, backup_root)
    archive_path = Path(archive_path)

    temp_paths: list[Path] = []
    manifest_path = backup_info.path / "Manifest.db"
    sms_db_path = get_sms_db_path(backup_info.path)
    encrypted_backup = None

    try:
        source_fingerprint = _backup_fingerprint(manifest_path, sms_db_path)

        if backup_info.is_encrypted:
            if not password:
                raise EncryptedBackupError(
                    "Encrypted backup requires a password. Re-run with --password.",
                )
            from green2blue.ios.crypto import EncryptedBackup

            encrypted_backup = EncryptedBackup(backup_info.path, password)
            encrypted_backup.unlock()

            manifest_path = encrypted_backup.decrypt_manifest_db()
            temp_paths.append(manifest_path)

            with ManifestDB(manifest_path) as manifest:
                sms_file_id = compute_file_id("HomeDomain", "Library/SMS/sms.db")
                sms_enc_key, sms_protection_class = manifest.get_file_encryption_info(
                    sms_file_id,
                )

            encrypted_sms_path = sms_db_path
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                sms_db_path = Path(tmp.name)
            encrypted_backup.decrypt_db_file_to_path(
                encrypted_sms_path,
                sms_enc_key,
                sms_protection_class,
                sms_db_path,
            )
            temp_paths.append(sms_db_path)

        with (
            CanonicalArchive(archive_path) as archive,
            ManifestDB(manifest_path) as manifest,
            sqlite3.connect(sms_db_path) as conn,
        ):
            conn.row_factory = sqlite3.Row
            source_path_text = str(backup_info.path)
            if resume:
                existing = archive.find_completed_import_run(
                    source_type="ios-backup",
                    source_path=source_path_text,
                    source_fingerprint=source_fingerprint,
                )
                if existing is not None:
                    summary = archive.summarize_import_run(int(existing["id"]))
                    return IOSArchiveImportResult(
                        archive_path=archive_path,
                        import_run_id=int(existing["id"]),
                        reused_existing=True,
                        messages_imported=int(existing["message_count"]),
                        messages_deduped=0,
                        conversations_touched=summary["conversations"],
                        participants_touched=summary["participants"],
                        attachments_imported=int(existing["attachment_count"]),
                        blobs_imported=summary["blobs"],
                        backup_path=backup_info.path,
                        backup_udid=backup_info.udid,
                    )

            import_run_id = archive.start_import(
                "ios-backup",
                source_path_text,
                source_fingerprint=source_fingerprint,
            )
            result = _import_messages(
                archive,
                conn,
                manifest,
                backup_info.path,
                import_run_id,
                encrypted_backup=encrypted_backup,
            )
            archive.finish_import(
                import_run_id,
                result["messages_imported"],
                result["attachments_imported"],
            )
            archive.conn.commit()

        return IOSArchiveImportResult(
            archive_path=archive_path,
            import_run_id=import_run_id,
            reused_existing=False,
            messages_imported=result["messages_imported"],
            messages_deduped=result["messages_deduped"],
            conversations_touched=len(result["conversation_ids"]),
            participants_touched=len(result["participant_ids"]),
            attachments_imported=result["attachments_imported"],
            blobs_imported=len(result["blob_ids"]),
            backup_path=backup_info.path,
            backup_udid=backup_info.udid,
        )
    finally:
        for path in temp_paths:
            path.unlink(missing_ok=True)


def _import_messages(
    archive: CanonicalArchive,
    conn: sqlite3.Connection,
    manifest: ManifestDB,
    backup_path: Path,
    import_run_id: int,
    *,
    encrypted_backup,
) -> dict[str, object]:
    chat_rows = _load_chat_rows(conn)
    chat_participants = _load_chat_participants(conn)
    message_attachments = _load_message_attachments(conn)

    conversation_ids: set[int] = set()
    participant_ids: set[int] = set()
    blob_ids: set[int] = set()
    messages_imported = 0
    messages_deduped = 0
    attachments_imported = 0

    cursor = conn.execute(
        """
        SELECT
            m.ROWID AS rowid,
            m.guid,
            m.text,
            m.subject,
            m.service,
            m.date,
            m.date_read,
            m.is_from_me,
            m.is_read,
            m.cache_has_attachments,
            m.associated_message_guid,
            m.reply_to_guid,
            m.date_edited,
            m.balloon_bundle_id,
            m.expressive_send_style_id,
            h.id AS handle_address,
            cm.chat_id
        FROM message m
        LEFT JOIN (
            SELECT message_id, MIN(chat_id) AS chat_id
            FROM chat_message_join
            GROUP BY message_id
        ) cm ON cm.message_id = m.ROWID
        LEFT JOIN handle h ON h.ROWID = m.handle_id
        ORDER BY m.date, m.ROWID
        """
    )

    for row in cursor.fetchall():
        attachments = message_attachments.get(int(row["rowid"]), ())
        body_text = _normalize_message_text(row["text"], len(attachments))
        has_url = bool(body_text and ("http://" in body_text or "https://" in body_text))

        conversation_id, linked_participants = _ensure_conversation(
            archive,
            row,
            chat_rows=chat_rows,
            chat_participants=chat_participants,
        )
        conversation_ids.add(conversation_id)
        participant_ids.update(linked_participants)

        raw_payload = {
            "rowid": int(row["rowid"]),
            "guid": row["guid"],
            "text": row["text"],
            "subject": row["subject"],
            "service": row["service"],
            "date_ns": int(row["date"] or 0),
            "date_read_ns": int(row["date_read"] or 0),
            "is_from_me": int(row["is_from_me"] or 0),
            "is_read": int(row["is_read"] or 0),
            "associated_message_guid": row["associated_message_guid"],
            "reply_to_guid": row["reply_to_guid"],
            "date_edited_ns": int(row["date_edited"] or 0),
            "balloon_bundle_id": row["balloon_bundle_id"],
            "expressive_send_style_id": row["expressive_send_style_id"],
            "chat_id": int(row["chat_id"]) if row["chat_id"] is not None else None,
            "handle_address": row["handle_address"],
            "attachments": [
                {
                    "rowid": att.rowid,
                    "guid": att.guid,
                    "mime_type": att.mime_type,
                    "transfer_name": att.transfer_name,
                    "filename": att.filename,
                    "total_bytes": att.total_bytes,
                }
                for att in attachments
            ],
        }

        _, inserted = archive.insert_message(
            source_uid=f"ios:{row['guid']}",
            source_type="ios.message",
            import_run_id=import_run_id,
            conversation_id=conversation_id,
            direction="outgoing" if row["is_from_me"] else "incoming",
            sent_at_ms=ios_ns_to_unix_ms(int(row["date"] or 0)),
            read_state=_ios_read_state(row),
            service_hint=row["service"],
            subject=row["subject"],
            body_text=body_text,
            has_attachments=bool(row["cache_has_attachments"]),
            has_url=has_url,
            raw_json=json_dumps_stable(raw_payload),
        )
        if not inserted:
            messages_deduped += 1
            continue

        messages_imported += 1
        message_id = archive.conn.execute(
            "SELECT id FROM messages WHERE source_uid = ?",
            (f"ios:{row['guid']}",),
        ).fetchone()[0]

        for part_index, attachment in enumerate(attachments):
            blob_id = None
            blob_id = _import_attachment_blob(
                archive,
                backup_path,
                manifest,
                attachment.filename,
                encrypted_backup=encrypted_backup,
            )
            if blob_id is not None:
                blob_ids.add(blob_id)
            archive.insert_attachment(
                message_id=int(message_id),
                part_index=part_index,
                mime_type=attachment.mime_type,
                filename=attachment.transfer_name or _basename_or_none(attachment.filename),
                text_content=None,
                blob_id=blob_id,
            )
            attachments_imported += 1

    return {
        "messages_imported": messages_imported,
        "messages_deduped": messages_deduped,
        "attachments_imported": attachments_imported,
        "conversation_ids": conversation_ids,
        "participant_ids": participant_ids,
        "blob_ids": blob_ids,
    }


def _load_chat_rows(conn: sqlite3.Connection) -> dict[int, sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT
            ROWID AS rowid,
            guid,
            chat_identifier,
            service_name,
            display_name,
            room_name,
            style
        FROM chat
        """
    ).fetchall()
    return {int(row["rowid"]): row for row in rows}


def _load_chat_participants(
    conn: sqlite3.Connection,
) -> dict[int, list[tuple[str, str]]]:
    participants: dict[int, list[tuple[str, str]]] = defaultdict(list)
    rows = conn.execute(
        """
        SELECT chj.chat_id, h.id, h.service
        FROM chat_handle_join chj
        JOIN handle h ON h.ROWID = chj.handle_id
        ORDER BY chj.chat_id, h.ROWID
        """
    ).fetchall()
    for row in rows:
        participants[int(row["chat_id"])].append((row["id"], row["service"]))
    return participants


def _load_message_attachments(
    conn: sqlite3.Connection,
) -> dict[int, tuple[_AttachmentRow, ...]]:
    attachments: dict[int, list[_AttachmentRow]] = defaultdict(list)
    rows = conn.execute(
        """
        SELECT
            maj.message_id,
            att.ROWID AS attachment_rowid,
            att.guid,
            COALESCE(att.mime_type, att.uti) AS mime_type,
            att.transfer_name,
            att.filename,
            att.total_bytes
        FROM message_attachment_join maj
        JOIN attachment att ON att.ROWID = maj.attachment_id
        ORDER BY maj.message_id, maj.attachment_id
        """
    ).fetchall()
    for row in rows:
        attachments[int(row["message_id"])].append(
            _AttachmentRow(
                rowid=int(row["attachment_rowid"]),
                guid=row["guid"],
                mime_type=row["mime_type"],
                transfer_name=row["transfer_name"],
                filename=row["filename"],
                total_bytes=int(row["total_bytes"] or 0),
            ),
        )
    return {key: tuple(value) for key, value in attachments.items()}


def _ensure_conversation(
    archive: CanonicalArchive,
    message_row: sqlite3.Row,
    *,
    chat_rows: dict[int, sqlite3.Row],
    chat_participants: dict[int, list[tuple[str, str]]],
) -> tuple[int, set[int]]:
    chat_id = int(message_row["chat_id"]) if message_row["chat_id"] is not None else None
    participants: list[tuple[str, str]]

    if chat_id is not None and chat_id in chat_rows:
        chat_row = chat_rows[chat_id]
        participants = chat_participants.get(chat_id, [])
        kind = "group" if len(participants) > 1 else "direct"
        conversation_key = f"ios:chat:{chat_row['guid'] or chat_id}"
        title = (
            chat_row["display_name"]
            or chat_row["room_name"]
            or chat_row["chat_identifier"]
            or message_row["handle_address"]
        )
    else:
        chat_row = None
        participants = []
        if message_row["handle_address"]:
            participants = [(message_row["handle_address"], message_row["service"] or "SMS")]
        kind = "direct"
        conversation_key = (
            f"ios:peer:{message_row['service'] or 'SMS'}:{message_row['handle_address']}"
            if message_row["handle_address"]
            else f"ios:message:{message_row['guid']}"
        )
        title = message_row["handle_address"]

    conversation_id = archive.get_or_create_conversation(
        conversation_key,
        kind=kind,
        source_thread_id=str(chat_id) if chat_id is not None else None,
        title=title,
    )

    participant_ids: set[int] = set()
    for sort_order, (address, _service) in enumerate(participants):
        participant_id = archive.get_or_create_participant(
            address,
            detect_address_kind(address),
        )
        archive.link_conversation_participant(
            conversation_id,
            participant_id,
            role="peer" if kind == "direct" else "member",
            sort_order=sort_order,
        )
        participant_ids.add(participant_id)

    if not participant_ids and message_row["handle_address"]:
        participant_id = archive.get_or_create_participant(
            message_row["handle_address"],
            detect_address_kind(message_row["handle_address"]),
        )
        archive.link_conversation_participant(
            conversation_id,
            participant_id,
            role="peer",
            sort_order=0,
        )
        participant_ids.add(participant_id)

    return conversation_id, participant_ids


def _normalize_message_text(text: str | None, attachment_count: int) -> str | None:
    if text is None:
        return None
    if attachment_count <= 0:
        return text
    prefix = ATTACHMENT_PLACEHOLDER * attachment_count
    if text.startswith(prefix):
        stripped = text[len(prefix) :]
        return stripped or None
    return text


def _ios_read_state(row: sqlite3.Row) -> str:
    if int(row["is_from_me"] or 0):
        return "sent"
    if int(row["is_read"] or 0) or int(row["date_read"] or 0) > 0:
        return "read"
    return "unread"


def _import_attachment_blob(
    archive: CanonicalArchive,
    backup_path: Path,
    manifest: ManifestDB,
    filename: str | None,
    *,
    encrypted_backup,
) -> int | None:
    if not filename:
        return None

    relative_path = filename.removeprefix("~/")
    for domain in ("HomeDomain", "MediaDomain"):
        file_id = compute_file_id(domain, relative_path)
        backup_file = backup_path / file_id[:2] / file_id
        if not backup_file.exists():
            continue
        if encrypted_backup is None:
            blob_id, _ = archive.upsert_blob_path(backup_file)
            return blob_id
        encryption_key, protection_class = manifest.get_file_encryption_info(file_id)
        with tempfile.NamedTemporaryFile(suffix=".blob", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            encrypted_backup.decrypt_db_file_to_path(
                backup_file,
                encryption_key,
                protection_class,
                tmp_path,
            )
            blob_id, _ = archive.upsert_blob_path(tmp_path)
            return blob_id
        finally:
            tmp_path.unlink(missing_ok=True)
    return None


def _basename_or_none(path_str: str | None) -> str | None:
    if not path_str:
        return None
    return Path(path_str).name


def _backup_fingerprint(manifest_path: Path, sms_db_path: Path) -> str:
    digest = hashlib.sha256()
    for path in (manifest_path, sms_db_path):
        digest.update(path.name.encode("utf-8"))
        with path.open("rb") as fh:
            while chunk := fh.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()
