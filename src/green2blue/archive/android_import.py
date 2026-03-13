"""Import Android SMS Import/Export ZIPs into the canonical archive."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path

from green2blue.archive.db import CanonicalArchive, detect_address_kind, json_dumps_stable
from green2blue.models import AndroidMMS, AndroidSMS, MMSAddress, MMSPart
from green2blue.parser.ndjson_parser import parse_ndjson
from green2blue.parser.zip_reader import ExtractedExport, open_export_zip


@dataclass(frozen=True)
class AndroidArchiveImportResult:
    archive_path: Path
    import_run_id: int
    reused_existing: bool
    messages_imported: int
    messages_deduped: int
    conversations_touched: int
    participants_touched: int
    attachments_imported: int
    blobs_imported: int


def import_android_export(
    export_zip: Path | str,
    archive_path: Path | str,
    *,
    resume: bool = True,
) -> AndroidArchiveImportResult:
    archive_path = Path(archive_path)
    export_path = Path(export_zip)
    source_fingerprint = _fingerprint_path(export_path)

    with CanonicalArchive(archive_path) as archive:
        source_path_text = str(export_path)
        if resume:
            existing = archive.find_completed_import_run(
                source_type="android-export",
                source_path=source_path_text,
                source_fingerprint=source_fingerprint,
            )
            if existing is not None:
                summary = archive.summarize_import_run(int(existing["id"]))
                return AndroidArchiveImportResult(
                    archive_path=archive_path,
                    import_run_id=int(existing["id"]),
                    reused_existing=True,
                    messages_imported=int(existing["message_count"]),
                    messages_deduped=0,
                    conversations_touched=summary["conversations"],
                    participants_touched=summary["participants"],
                    attachments_imported=int(existing["attachment_count"]),
                    blobs_imported=summary["blobs"],
                )

        import_run_id = archive.start_import(
            "android-export",
            source_path_text,
            source_fingerprint=source_fingerprint,
        )
        seen_conversations: set[int] = set()
        seen_participants: set[int] = set()
        messages_imported = 0
        messages_deduped = 0
        attachments_imported = 0
        blob_ids_seen: set[int] = set()

        with open_export_zip(export_zip) as export:
            for msg in parse_ndjson(export.ndjson_path):
                imported, attachment_count, conversation_id, participant_ids, blob_ids = (
                    _import_message(
                        archive,
                        export,
                        import_run_id,
                        msg,
                    )
                )
                if imported:
                    messages_imported += 1
                else:
                    messages_deduped += 1
                attachments_imported += attachment_count
                seen_conversations.add(conversation_id)
                seen_participants.update(participant_ids)
                blob_ids_seen.update(blob_ids)

        archive.finish_import(import_run_id, messages_imported, attachments_imported)
        archive.conn.commit()

        return AndroidArchiveImportResult(
            archive_path=archive_path,
            import_run_id=import_run_id,
            reused_existing=False,
            messages_imported=messages_imported,
            messages_deduped=messages_deduped,
            conversations_touched=len(seen_conversations),
            participants_touched=len(seen_participants),
            attachments_imported=attachments_imported,
            blobs_imported=len(blob_ids_seen),
        )


def _import_message(
    archive: CanonicalArchive,
    export: ExtractedExport,
    import_run_id: int,
    msg: AndroidSMS | AndroidMMS,
) -> tuple[bool, int, int, set[int], set[int]]:
    if isinstance(msg, AndroidSMS):
        return _import_sms(archive, import_run_id, msg)
    return _import_mms(archive, export, import_run_id, msg)


def _import_sms(
    archive: CanonicalArchive,
    import_run_id: int,
    msg: AndroidSMS,
) -> tuple[bool, int, int, set[int], set[int]]:
    participant_id = archive.get_or_create_participant(
        msg.address,
        detect_address_kind(msg.address),
    )
    conversation_key = (
        f"android:sms:thread:{msg.thread_id}"
        if msg.thread_id is not None
        else f"android:sms:peer:{msg.address}"
    )
    conversation_id = archive.get_or_create_conversation(
        conversation_key,
        kind="direct",
        source_thread_id=str(msg.thread_id) if msg.thread_id is not None else None,
        title=msg.address,
    )
    archive.link_conversation_participant(
        conversation_id,
        participant_id,
        role="peer",
        sort_order=0,
    )
    raw_json = json_dumps_stable(asdict(msg))
    source_uid = hashlib.sha256(f"sms:{raw_json}".encode()).hexdigest()
    message_id, inserted = archive.insert_message(
        source_uid=source_uid,
        source_type="android.sms",
        import_run_id=import_run_id,
        conversation_id=conversation_id,
        direction=_sms_direction(msg.type),
        sent_at_ms=msg.date,
        read_state="read" if msg.read else "unread",
        service_hint="SMS",
        subject=None,
        body_text=msg.body,
        has_attachments=False,
        has_url="http://" in msg.body or "https://" in msg.body,
        raw_json=raw_json,
    )
    return inserted, 0, conversation_id, {participant_id}, set()


def _import_mms(
    archive: CanonicalArchive,
    export: ExtractedExport,
    import_run_id: int,
    msg: AndroidMMS,
) -> tuple[bool, int, int, set[int], set[int]]:
    participants = _mms_unique_addresses(msg.addresses)
    conversation_key = (
        f"android:mms:thread:{msg.thread_id}"
        if msg.thread_id is not None
        else _fallback_mms_conversation_key(participants)
    )
    conversation_kind = "group" if len(participants) > 2 else "direct"
    conversation_id = archive.get_or_create_conversation(
        conversation_key,
        kind=conversation_kind,
        source_thread_id=str(msg.thread_id) if msg.thread_id is not None else None,
        title=msg.sub,
    )

    participant_ids: set[int] = set()
    for sort_order, address in enumerate(participants):
        participant_id = archive.get_or_create_participant(
            address,
            detect_address_kind(address),
        )
        role = _role_for_mms_address(address, msg.addresses)
        archive.link_conversation_participant(
            conversation_id,
            participant_id,
            role=role,
            sort_order=sort_order,
        )
        participant_ids.add(participant_id)

    body_text = "\n".join(part.text for part in msg.parts if part.text) or None
    raw_json = json_dumps_stable(asdict(msg))
    source_uid = hashlib.sha256(f"mms:{raw_json}".encode()).hexdigest()
    has_url = bool(body_text and ("http://" in body_text or "https://" in body_text))
    message_id, inserted = archive.insert_message(
        source_uid=source_uid,
        source_type="android.mms",
        import_run_id=import_run_id,
        conversation_id=conversation_id,
        direction="incoming" if msg.msg_box == 1 else "outgoing" if msg.msg_box == 2 else "unknown",
        sent_at_ms=_mms_timestamp_ms(msg),
        read_state="read" if msg.read else "unread",
        service_hint="MMS",
        subject=msg.sub,
        body_text=body_text,
        has_attachments=any(part.data_path for part in msg.parts),
        has_url=has_url,
        raw_json=raw_json,
    )
    if not inserted:
        return False, 0, conversation_id, participant_ids, set()

    attachment_count = 0
    blob_ids: set[int] = set()
    for part_index, part in enumerate(msg.parts):
        if part.data_path:
            attachment_path = _resolve_attachment_path(export, part)
            blob_id = None
            if attachment_path is not None:
                blob_id, _ = archive.upsert_blob_path(attachment_path)
                blob_ids.add(blob_id)
            archive.insert_attachment(
                message_id=message_id,
                part_index=part_index,
                mime_type=part.content_type,
                filename=part.filename,
                text_content=None,
                blob_id=blob_id,
            )
            attachment_count += 1
        elif part.text:
            archive.insert_attachment(
                message_id=message_id,
                part_index=part_index,
                mime_type=part.content_type,
                filename=None,
                text_content=part.text,
                blob_id=None,
            )

    return True, attachment_count, conversation_id, participant_ids, blob_ids


def _sms_direction(msg_type: int) -> str:
    if msg_type == 1:
        return "incoming"
    if msg_type == 2:
        return "outgoing"
    return "unknown"


def _mms_timestamp_ms(msg: AndroidMMS) -> int:
    return msg.date * 1000 if msg.date < 10_000_000_000 else msg.date


def _mms_unique_addresses(addresses: tuple[MMSAddress, ...]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for address in addresses:
        if address.address not in seen:
            seen.add(address.address)
            ordered.append(address.address)
    return ordered


def _fallback_mms_conversation_key(addresses: list[str]) -> str:
    digest = hashlib.sha256(",".join(sorted(addresses)).encode("utf-8")).hexdigest()[:16]
    return f"android:mms:participants:{digest}"


def _role_for_mms_address(address: str, addresses: tuple[MMSAddress, ...]) -> str:
    for entry in addresses:
        if entry.address != address:
            continue
        if entry.type == 137:
            return "sender"
        if entry.type == 151:
            return "recipient"
    return "member"


def _resolve_attachment_path(export: ExtractedExport, part: MMSPart) -> Path | None:
    if export.data_dir is None or not part.data_path:
        return None

    raw = Path(part.data_path)
    candidates: list[Path] = []
    if raw.is_absolute():
        candidates.append(export.data_dir / raw.name)
    else:
        candidates.append(export.temp_dir / raw)
        if raw.parts and raw.parts[0] == "data":
            candidates.append(export.temp_dir / raw)
            candidates.append(export.data_dir / Path(*raw.parts[1:]))
        candidates.append(export.data_dir / raw.name)

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            return candidate
    return None


def _fingerprint_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
