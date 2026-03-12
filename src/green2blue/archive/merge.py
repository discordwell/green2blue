"""Cross-source merge planning for canonical archives."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from green2blue.archive.db import CanonicalArchive
from green2blue.converter.phone import normalize_phone
from green2blue.exceptions import PhoneNormalizationError

_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class ArchiveMergeResult:
    archive_path: Path
    merge_run_id: int
    merged_conversations: int
    merged_messages: int
    duplicate_messages: int


def merge_archive(
    archive_path: Path | str,
    *,
    country: str = "US",
    strategy: str = "participant-set-v1",
) -> ArchiveMergeResult:
    """Materialize a merged cross-source view inside a canonical archive."""
    with CanonicalArchive(archive_path) as archive:
        conn = archive.conn
        assert conn is not None

        participants = _load_participants(conn, country)
        conversations = _load_conversations(conn, participants)
        attachments = _load_attachment_signatures(conn)
        messages = _load_messages(conn)

        merge_run_id = archive.start_merge_run(strategy, country.upper())

        merged_conversation_ids: dict[str, int] = {}
        merged_conversation_messages: dict[str, list[dict[str, object]]] = defaultdict(list)

        for conversation in conversations.values():
            merged_key = _merged_conversation_key(conversation)
            merged_id = archive.get_or_create_merged_conversation(
                merge_run_id,
                merge_key=merged_key,
                kind=conversation["kind"],
                title=conversation["title"],
            )
            merged_conversation_ids[merged_key] = merged_id
            for sort_order, participant_id in enumerate(conversation["participant_ids"]):
                archive.link_merged_conversation_participant(
                    merged_id,
                    participant_id,
                    role="peer" if conversation["kind"] == "direct" else "member",
                    sort_order=sort_order,
                )

        for message in messages:
            conversation = conversations[message["conversation_id"]]
            merged_key = _merged_conversation_key(conversation)
            merged_conversation_messages[merged_key].append({
                **message,
                "attachment_signature": attachments.get(message["id"], ()),
            })

        merged_message_count = 0
        duplicate_message_count = 0

        for merged_key, merged_messages in merged_conversation_messages.items():
            merged_conversation_id = merged_conversation_ids[merged_key]
            ranked_messages = sorted(
                merged_messages,
                key=lambda row: (
                    int(row["sent_at_ms"]),
                    _source_priority(str(row["source_type"])),
                    int(row["id"]),
                ),
            )
            fingerprint_groups: dict[str, list[dict[str, object]]] = defaultdict(list)
            winners: dict[str, dict[str, object]] = {}

            for message in ranked_messages:
                fingerprint = _message_fingerprint(message)
                fingerprint_groups[fingerprint].append(message)
                winner = winners.get(fingerprint)
                if winner is None or _message_rank(message) < _message_rank(winner):
                    winners[fingerprint] = message

            merged_message_count += len(winners)
            duplicate_message_count += sum(len(group) - 1 for group in fingerprint_groups.values())

            for sort_order, message in enumerate(ranked_messages):
                fingerprint = _message_fingerprint(message)
                winner = winners[fingerprint]
                archive.insert_merged_message(
                    merge_run_id=merge_run_id,
                    merged_conversation_id=merged_conversation_id,
                    message_id=int(message["id"]),
                    sort_order=sort_order,
                    fingerprint=fingerprint,
                    is_duplicate=int(message["id"]) != int(winner["id"]),
                    duplicate_of_message_id=(
                        None if int(message["id"]) == int(winner["id"]) else int(winner["id"])
                    ),
                )

        archive.finish_merge_run(
            merge_run_id,
            merged_conversation_count=len(merged_conversation_ids),
            merged_message_count=merged_message_count,
            duplicate_message_count=duplicate_message_count,
        )
        archive.conn.commit()

        return ArchiveMergeResult(
            archive_path=Path(archive_path),
            merge_run_id=merge_run_id,
            merged_conversations=len(merged_conversation_ids),
            merged_messages=merged_message_count,
            duplicate_messages=duplicate_message_count,
        )


def _load_participants(conn, country: str) -> dict[int, dict[str, str]]:
    rows = conn.execute("SELECT id, address, kind FROM participants").fetchall()
    participants: dict[int, dict[str, str]] = {}
    for row in rows:
        participants[int(row["id"])] = {
            "address": row["address"],
            "kind": row["kind"],
            "identity": _normalize_identity(row["address"], row["kind"], country),
        }
    return participants


def _load_conversations(conn, participants) -> dict[int, dict[str, object]]:
    conversation_rows = conn.execute(
        "SELECT id, conversation_key, kind, title FROM conversations",
    ).fetchall()
    participant_rows = conn.execute(
        """
        SELECT conversation_id, participant_id, sort_order
        FROM conversation_participants
        ORDER BY conversation_id, sort_order, participant_id
        """
    ).fetchall()
    members: dict[int, list[int]] = defaultdict(list)
    for row in participant_rows:
        members[int(row["conversation_id"])].append(int(row["participant_id"]))

    conversations: dict[int, dict[str, object]] = {}
    for row in conversation_rows:
        participant_ids = members.get(int(row["id"]), [])
        identity_keys = sorted({
            participants[participant_id]["identity"]
            for participant_id in participant_ids
            if participant_id in participants
        })
        conversations[int(row["id"])] = {
            "id": int(row["id"]),
            "conversation_key": row["conversation_key"],
            "kind": row["kind"],
            "title": row["title"],
            "participant_ids": participant_ids,
            "identity_keys": tuple(identity_keys),
        }
    return conversations


def _load_messages(conn) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            id,
            conversation_id,
            source_type,
            direction,
            sent_at_ms,
            read_state,
            service_hint,
            subject,
            body_text,
            has_attachments
        FROM messages
        ORDER BY sent_at_ms, id
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _load_attachment_signatures(conn) -> dict[int, tuple[tuple[str, str, str, str], ...]]:
    rows = conn.execute(
        """
        SELECT
            ma.message_id,
            ma.part_index,
            COALESCE(ma.mime_type, '(unknown)') AS mime_type,
            COALESCE(ma.filename, '') AS filename,
            COALESCE(ma.text_content, '') AS text_content,
            COALESCE(b.sha256, '') AS sha256
        FROM message_attachments ma
        LEFT JOIN blobs b ON b.id = ma.blob_id
        ORDER BY ma.message_id, ma.part_index
        """
    ).fetchall()
    signatures: dict[int, list[tuple[str, str, str, str]]] = defaultdict(list)
    for row in rows:
        signatures[int(row["message_id"])].append(
            (
                row["mime_type"],
                row["filename"],
                row["text_content"],
                row["sha256"],
            ),
        )
    return {key: tuple(value) for key, value in signatures.items()}


def _merged_conversation_key(conversation: dict[str, object]) -> str:
    identity_keys = conversation["identity_keys"]
    if identity_keys:
        return f"{conversation['kind']}:{'|'.join(identity_keys)}"
    return f"{conversation['kind']}:{conversation['conversation_key']}"


def _normalize_identity(address: str, kind: str, country: str) -> str:
    if kind == "email":
        return f"email:{address.strip().lower()}"
    if kind == "phone":
        try:
            normalized = normalize_phone(address, country)
        except PhoneNormalizationError:
            normalized = address
        return f"phone:{normalized}"
    return f"opaque:{address.strip().lower()}"


def _message_fingerprint(message: dict[str, object]) -> str:
    payload = {
        "direction": message["direction"],
        "timestamp_bucket_s": int(message["sent_at_ms"]) // 1000,
        "subject": (message["subject"] or "").strip(),
        "body": _normalize_body(message["body_text"]),
        "attachments": list(message["attachment_signature"]),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    ).hexdigest()


def _normalize_body(body_text: object) -> str:
    if body_text is None:
        return ""
    return _WS_RE.sub(" ", str(body_text).strip())


def _message_rank(message: dict[str, object]) -> tuple[int, int, int]:
    return (
        _source_priority(str(message["source_type"])),
        0 if message["has_attachments"] else 1,
        int(message["id"]),
    )


def _source_priority(source_type: str) -> int:
    if source_type == "ios.message":
        return 0
    if source_type.startswith("android."):
        return 1
    return 2
