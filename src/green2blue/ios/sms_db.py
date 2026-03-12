"""Core sms.db injection logic.

Handles reading/writing the iOS Messages SQLite database:
- Handle (contact) creation and deduplication
- Chat (conversation) creation and deduplication
- Message insertion with all ~35+ columns
- Attachment insertion
- Join table management (chat_handle_join, chat_message_join, message_attachment_join)
- Trigger management (drop before inject, restore after)
- Duplicate detection
- Single-transaction safety
"""

from __future__ import annotations

import hashlib
import logging
import plistlib
import sqlite3
import uuid
from collections import deque
from dataclasses import replace as dc_replace
from pathlib import Path

from green2blue.exceptions import CloneSourceError, DatabaseError, InsufficientSacrificeError
from green2blue.ios.attributed_body import (
    build_attributed_body,
    build_attributed_body_with_metadata,
)
from green2blue.ios.message_summary import build_message_summary_info
from green2blue.ios.trigger_utils import (
    drop_triggers,
    restore_triggers,
)
from green2blue.models import (
    ConversionResult,
    compose_message_text,
    compute_chat_guid,
    compute_ck_chat_id,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
    message_content_hash,
)

# Real file-backed SMS/MMS attachments most often use preview_generation_state=0.
# Treat it as a processing-state field, not a media-type enum, and preserve
# cloned values from real attachment templates whenever possible.
_DEFAULT_PREVIEW_GENERATION_STATE = 0
_DEFAULT_ATTRIBUTION_INFO = plistlib.dumps(
    {"pgenp": True},
    fmt=plistlib.FMT_BINARY,
    sort_keys=False,
)

logger = logging.getLogger(__name__)


def _default_preview_generation_state(mime_type: str | None) -> int:
    """Return a sane fallback preview state for imported local attachments."""
    if mime_type and (
        mime_type.startswith("image/") or mime_type.startswith("video/")
    ):
        return _DEFAULT_PREVIEW_GENERATION_STATE
    return 0


class SMSDatabase:
    """Interface to an iOS sms.db file."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection | None = None
        self._saved_triggers: list[str] = []

    def open(self) -> None:
        """Open the database connection."""
        if not self.db_path.exists():
            raise DatabaseError(f"sms.db not found: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self._inspect_schema()

    def _inspect_schema(self) -> None:
        """Detect optional columns that vary by iOS version."""
        cursor = self.conn.cursor()
        self._tables: set[str] = {
            r[0]
            for r in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        self._msg_schema: set[str] = {
            r[1] for r in cursor.execute("PRAGMA table_info(message)").fetchall()
        }
        self._att_schema: set[str] = {
            r[1] for r in cursor.execute("PRAGMA table_info(attachment)").fetchall()
        }
        self._chat_schema: set[str] = {
            r[1] for r in cursor.execute("PRAGMA table_info(chat)").fetchall()
        }

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def update_attachment_sizes(self, sizes: dict[str, int]) -> None:
        """Update total_bytes for attachments by GUID.

        Args:
            sizes: Dict of {attachment_guid: file_size_bytes}.
        """
        cursor = self.conn.cursor()
        for guid, size in sizes.items():
            cursor.execute(
                "UPDATE attachment SET total_bytes = ? WHERE guid = ?",
                (size, guid),
            )
        self.conn.commit()

    def inject(self, result: ConversionResult, skip_duplicates: bool = True) -> InjectionStats:
        """Inject converted messages into sms.db.

        All operations run in a single transaction. On any failure,
        the entire transaction is rolled back.

        Args:
            result: The conversion result with messages, handles, and chats.
            skip_duplicates: Skip messages that already exist in the database.

        Returns:
            InjectionStats with counts of inserted records.
        """
        if not self.conn:
            raise DatabaseError("Database not open. Call open() first.")

        stats = InjectionStats()

        try:
            # Drop triggers to avoid iOS internal function calls
            self._drop_triggers()

            cursor = self.conn.cursor()

            # Load existing message hashes for dedup
            existing_hashes = set()
            if skip_duplicates:
                existing_hashes = self._load_existing_message_hashes()

            # Create/dedup handles, chats, and join links
            handle_rowids, chat_rowids = self._resolve_handles_and_chats(
                cursor, result, stats,
            )

            # Detect inject-specific metadata from existing data
            injected_service = result.handles[0].service if result.handles else "SMS"
            detected_caller_id = self._detect_destination_caller_id(injected_service)
            detected_account = self._detect_account(injected_service)
            detected_account_guid = self._detect_account_guid(injected_service)

            # Insert messages
            for msg in result.messages:
                # Duplicate check
                if skip_duplicates:
                    msg_hash = message_content_hash(msg)
                    if msg_hash in existing_hashes:
                        stats.messages_skipped += 1
                        continue

                handle_rowid = handle_rowids.get((msg.handle_id, msg.service))
                if handle_rowid is None:
                    logger.warning("No handle for %r — skipping message", msg.handle_id)
                    stats.messages_skipped += 1
                    continue

                # Determine which chat this message belongs to
                chat_key = msg.chat_identifier or msg.handle_id
                chat_guid = compute_chat_guid(chat_key, msg.group_members)

                ck_chat_id = compute_ck_chat_id(
                    msg.service, chat_key, msg.group_members,
                )

                msg_rowid = self._insert_message(
                    cursor, msg, handle_rowid,
                    destination_caller_id=detected_caller_id,
                    ck_chat_id=ck_chat_id,
                    account_override=detected_account,
                    account_guid_override=detected_account_guid,
                )
                stats.messages_inserted += 1
                stats.message_rowids.append(msg_rowid)

                chat_rowid = chat_rowids.get(chat_guid)
                if chat_rowid:
                    self._insert_chat_message_join(cursor, chat_rowid, msg_rowid, msg.date)

                # Insert attachments
                for att in msg.attachments:
                    att_rowid = self._insert_attachment(
                        cursor, att, msg.is_from_me, msg.service,
                    )
                    self._insert_message_attachment_join(cursor, msg_rowid, att_rowid)
                    stats.attachments_inserted += 1
                    stats.attachment_rowids.append(att_rowid)

            self.conn.commit()
            return stats

        except Exception:
            self.conn.rollback()
            raise

        finally:
            # Always restore triggers, whether injection succeeded or failed
            self._restore_triggers()

    def _resolve_handles_and_chats(
        self,
        cursor: sqlite3.Cursor,
        result: ConversionResult,
        stats: _BaseStats,
    ) -> tuple[dict[tuple[str, str], int], dict[str, int]]:
        """Create/dedup handles and chats, link via join table.

        Shared between inject() and overwrite().

        Returns:
            (handle_rowids, chat_rowids) mappings.
        """
        existing_handles = self._load_existing_handles()
        existing_chats = self._load_existing_chats()

        injected_service = result.handles[0].service if result.handles else "SMS"
        detected_account_id = self._detect_account_id(injected_service)
        detected_account_login = self._detect_account_login(injected_service)

        handle_rowids: dict[tuple[str, str], int] = {}
        for handle in result.handles:
            key = (handle.id, handle.service)
            if key in existing_handles:
                handle_rowids[key] = existing_handles[key]
                stats.handles_existing += 1
            else:
                rowid = self._insert_handle(cursor, handle)
                handle_rowids[key] = rowid
                stats.handles_inserted += 1

        chat_rowids: dict[str, int] = {}
        for chat in result.chats:
            if chat.guid in existing_chats:
                rowid = existing_chats[chat.guid]
                chat_rowids[chat.guid] = rowid
                self._backfill_chat_visibility_fields(cursor, rowid)
                stats.chats_existing += 1
            else:
                updates = {}
                if detected_account_id and not chat.account_id:
                    updates["account_id"] = detected_account_id
                if detected_account_login:
                    updates["account_login"] = detected_account_login
                if updates:
                    chat = dc_replace(chat, **updates)
                rowid = self._insert_chat(cursor, chat)
                chat_rowids[chat.guid] = rowid
                stats.chats_inserted += 1

            self._ensure_chat_auxiliary_rows(cursor, rowid, chat)

        # Link handles to chats
        for chat in result.chats:
            chat_rowid = chat_rowids.get(chat.guid)
            if chat_rowid is None:
                continue
            if chat.style == 45:
                handle_rowid = handle_rowids.get(
                    (chat.chat_identifier, chat.service_name)
                )
                if handle_rowid:
                    self._insert_chat_handle_join(cursor, chat_rowid, handle_rowid)
            else:
                for phone in chat.participants:
                    handle_rowid = handle_rowids.get(
                        (phone, chat.service_name)
                    )
                    if handle_rowid:
                        self._insert_chat_handle_join(cursor, chat_rowid, handle_rowid)

        return handle_rowids, chat_rowids

    def _drop_triggers(self) -> None:
        """Drop all triggers from sms.db, saving their CREATE SQL.

        DDL statements (DROP TRIGGER) auto-commit in Python's sqlite3,
        so this must run outside the data transaction. Trigger restoration
        is guaranteed by the try/finally in inject().
        """
        self._saved_triggers = drop_triggers(self.conn)

    def _restore_triggers(self) -> None:
        """Restore previously dropped triggers."""
        restore_triggers(self.conn, self._saved_triggers)
        self._saved_triggers = []

    def _detect_account_id(self, service: str = "SMS") -> str:
        """Detect the account_id from existing chats for the given service.

        Real iOS assigns a UUID to each service account. We read it from
        existing chats so injected chats match.

        Returns:
            Account UUID string, or empty string if no existing chats.
        """
        cursor = self.conn.cursor()
        row = cursor.execute(
            "SELECT account_id, COUNT(*) as cnt FROM chat "
            "WHERE service_name = ? AND account_id != '' "
            "AND account_id IS NOT NULL "
            "GROUP BY account_id "
            "ORDER BY cnt DESC LIMIT 1",
            (service,),
        ).fetchone()
        return row["account_id"] if row else ""

    def _detect_account_login(self, service: str = "SMS") -> str:
        """Detect the account_login from existing chats for the given service."""
        cursor = self.conn.cursor()
        row = cursor.execute(
            "SELECT account_login, COUNT(*) as cnt FROM chat "
            "WHERE service_name = ? AND account_login != '' "
            "AND account_login IS NOT NULL "
            "GROUP BY account_login "
            "ORDER BY cnt DESC LIMIT 1",
            (service,),
        ).fetchone()
        return row["account_login"] if row else ""

    def _detect_account(self, service: str = "SMS") -> str:
        """Detect the account string from existing messages for the given service.

        For SMS, real iOS sets account to 'P:+{owner_phone}' on ~81%
        and 'E:' on ~19%. For iMessage, it's typically an Apple ID email.
        We detect the most common value.

        Returns:
            Account string (e.g., 'P:+15052289549'), or empty string.
        """
        cursor = self.conn.cursor()
        row = cursor.execute(
            "SELECT account, COUNT(*) as cnt "
            "FROM message "
            "WHERE account IS NOT NULL AND account != '' "
            "AND service = ? "
            "GROUP BY account "
            "ORDER BY cnt DESC LIMIT 1",
            (service,),
        ).fetchone()
        return row["account"] if row else ""

    def _detect_account_guid(self, service: str = "SMS") -> str:
        """Detect the account_guid from existing messages for the given service.

        Real iOS uses a single device UUID for account_guid on
        virtually all messages.

        Returns:
            Account GUID string, or empty string.
        """
        cursor = self.conn.cursor()
        row = cursor.execute(
            "SELECT account_guid, COUNT(*) as cnt "
            "FROM message "
            "WHERE account_guid IS NOT NULL AND account_guid != '' "
            "AND service = ? "
            "GROUP BY account_guid "
            "ORDER BY cnt DESC LIMIT 1",
            (service,),
        ).fetchone()
        return row["account_guid"] if row else ""

    def _detect_destination_caller_id(self, service: str = "SMS") -> str:
        """Detect the device owner's phone from existing messages for the given service.

        Real iOS sets destination_caller_id to the device owner's
        E.164 phone on virtually every message. We detect it
        from the most frequent value in existing messages.

        Returns:
            Owner's phone string, or empty string if no existing messages.
        """
        if "destination_caller_id" not in self._msg_schema:
            return ""
        cursor = self.conn.cursor()
        row = cursor.execute(
            "SELECT destination_caller_id, COUNT(*) as cnt "
            "FROM message "
            "WHERE destination_caller_id IS NOT NULL "
            "AND destination_caller_id != '' "
            "AND service = ? "
            "GROUP BY destination_caller_id "
            "ORDER BY cnt DESC LIMIT 1",
            (service,),
        ).fetchone()
        return row["destination_caller_id"] if row else ""

    def _load_existing_handles(self) -> dict[tuple[str, str], int]:
        """Load existing handles as {(id, service): ROWID}."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT ROWID, id, service FROM handle")
        return {(row["id"], row["service"]): row["ROWID"] for row in cursor.fetchall()}

    def _load_existing_chats(self) -> dict[str, int]:
        """Load existing chats as {guid: ROWID}."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT ROWID, guid FROM chat")
        return {row["guid"]: row["ROWID"] for row in cursor.fetchall()}

    def _load_existing_message_hashes(self) -> set[str]:
        """Load content hashes of existing messages for dedup."""
        cursor = self.conn.cursor()
        hashes = set()
        cursor.execute("""
            SELECT DISTINCT
                m.service,
                m.text,
                m.date,
                h.id as handle_id,
                COALESCE(c.chat_identifier, '') as chat_identifier,
                CASE
                    WHEN c.style = 43 THEN COALESCE((
                        SELECT GROUP_CONCAT(sorted_handles.id, ',')
                        FROM (
                            SELECT h2.id
                            FROM chat_handle_join chj2
                            JOIN handle h2 ON h2.ROWID = chj2.handle_id
                            WHERE chj2.chat_id = c.ROWID
                            ORDER BY h2.id
                        ) AS sorted_handles
                    ), '')
                    ELSE ''
                END AS group_members
            FROM message m
            LEFT JOIN handle h ON m.handle_id = h.ROWID
            LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
            LEFT JOIN chat c ON c.ROWID = cmj.chat_id
        """)
        for row in cursor.fetchall():
            content = (
                f"{row['service']}|{row['handle_id'] or ''}|"
                f"{row['chat_identifier']}|{row['group_members']}|"
                f"{row['date']}|{row['text'] or ''}"
            )
            hashes.add(hashlib.sha256(content.encode()).hexdigest())
        return hashes

    def _insert_handle(self, cursor: sqlite3.Cursor, handle: iOSHandle) -> int:
        """Insert a handle and return its ROWID."""
        cursor.execute(
            """INSERT INTO handle (id, country, service, uncanonicalized_id)
               VALUES (?, ?, ?, ?)""",
            (handle.id, handle.country, handle.service, handle.uncanonicalized_id),
        )
        return cursor.lastrowid

    def _insert_chat(self, cursor: sqlite3.Cursor, chat: iOSChat) -> int:
        """Insert a chat and return its ROWID."""
        group_id = str(uuid.uuid4()).upper()
        values: dict[str, object] = {
            "guid": chat.guid,
            "style": chat.style,
            "state": 3,
            "account_id": chat.account_id,
            "chat_identifier": chat.chat_identifier,
            "service_name": chat.service_name,
            "display_name": chat.display_name,
            "account_login": chat.account_login,
            "group_id": group_id,
            "original_group_id": group_id,
            "last_addressed_handle": "",
            "is_filtered": 0,
            "successful_query": 0,
            "server_change_token": "",
            "ck_sync_state": chat.ck_sync_state,
            "cloudkit_record_id": chat.cloudkit_record_id,
        }
        cols = [col for col in values if col in self._chat_schema]
        placeholders = ", ".join("?" for _ in cols)
        cursor.execute(
            f"INSERT INTO chat ({', '.join(cols)}) VALUES ({placeholders})",
            tuple(values[col] for col in cols),
        )
        return cursor.lastrowid

    def _backfill_chat_visibility_fields(
        self,
        cursor: sqlite3.Cursor,
        chat_id: int,
    ) -> None:
        """Populate nullable chat visibility fields expected by Messages UI."""
        row = cursor.execute(
            """SELECT group_id, original_group_id, last_addressed_handle,
                      is_filtered, successful_query
               FROM chat WHERE ROWID = ?""",
            (chat_id,),
        ).fetchone()
        if row is None:
            return

        updates: dict[str, object] = {}
        if "original_group_id" in self._chat_schema and not row["original_group_id"]:
            updates["original_group_id"] = row["group_id"] or str(uuid.uuid4()).upper()
        if "last_addressed_handle" in self._chat_schema and row["last_addressed_handle"] is None:
            updates["last_addressed_handle"] = ""
        if "is_filtered" in self._chat_schema and row["is_filtered"] is None:
            updates["is_filtered"] = 0
        if "successful_query" in self._chat_schema and row["successful_query"] is None:
            updates["successful_query"] = 0

        if not updates:
            return

        set_clause = ", ".join(f"{col} = ?" for col in updates)
        cursor.execute(
            f"UPDATE chat SET {set_clause} WHERE ROWID = ?",
            (*updates.values(), chat_id),
        )

    def _ensure_chat_auxiliary_rows(
        self,
        cursor: sqlite3.Cursor,
        chat_id: int,
        chat: iOSChat,
    ) -> None:
        """Backfill auxiliary chat index tables used by Messages UI."""
        if "chat_service" in self._tables:
            cursor.execute(
                "INSERT OR IGNORE INTO chat_service (service, chat) VALUES (?, ?)",
                (chat.service_name, chat_id),
            )

        if (
            chat.style == 43
            and "chat_lookup" in self._tables
        ):
            group_id_row = cursor.execute(
                "SELECT group_id FROM chat WHERE ROWID = ?",
                (chat_id,),
            ).fetchone()
            group_id = group_id_row["group_id"] if group_id_row else ""
            if group_id:
                domain = "iMessageGroupID" if chat.service_name == "iMessage" else "SMSGroupID"
                cursor.execute(
                    """INSERT OR IGNORE INTO chat_lookup
                       (identifier, domain, chat, priority)
                       VALUES (?, ?, ?, 0)""",
                    (group_id, domain, chat_id),
                )

    def _insert_message(
        self,
        cursor: sqlite3.Cursor,
        msg: iOSMessage,
        handle_rowid: int,
        *,
        destination_caller_id: str = "",
        ck_chat_id: str = "",
        account_override: str = "",
        account_guid_override: str = "",
    ) -> int:
        """Insert a message and return its ROWID."""
        display_text = compose_message_text(msg.text, len(msg.attachments))
        cache_has_attachments = 1 if msg.attachments else 0
        caption_text = (
            display_text[len(msg.attachments):]
            if msg.attachments and display_text else (display_text or "")
        )
        part_count = len(msg.attachments) + (1 if caption_text else 0)
        if part_count == 0:
            part_count = 1

        # Use detected account values if the message has none
        account = msg.account if msg.account else (account_override or None)
        account_guid = (
            msg.account_guid if msg.account_guid
            else (account_guid_override or None)
        )

        # Build column list and values dynamically for optional columns
        has_sr_ck = "sr_ck_sync_state" in self._msg_schema
        sr_ck_cols = "\n                sr_ck_sync_state," if has_sr_ck else ""
        sr_ck_vals = "\n                0," if has_sr_ck else ""

        has_msi = "message_summary_info" in self._msg_schema
        msi_cols = "\n                message_summary_info," if has_msi else ""
        msi_vals = "\n                ?," if has_msi else ""

        # Generate message_summary_info blob
        msi_blob = build_message_summary_info(
            service=msg.service,
            is_from_me=msg.is_from_me,
            has_text=bool(display_text),
        ) if has_msi else None

        # Generate attributedBody typedstream blob
        has_ab = "attributedBody" in self._msg_schema
        ab_cols = "\n                attributedBody," if has_ab else ""
        ab_vals = "\n                ?," if has_ab else ""
        ab_blob, has_dd_results = build_attributed_body_with_metadata(
            display_text,
            attachment_guids=tuple(att.guid for att in msg.attachments),
        ) if has_ab else (None, 0)

        # destination_caller_id (device owner's phone)
        has_dci = "destination_caller_id" in self._msg_schema
        dci_cols = "\n                destination_caller_id," if has_dci else ""
        dci_vals = "\n                ?," if has_dci else ""

        # ck_chat_id (service;-;chat_identifier)
        has_cci = "ck_chat_id" in self._msg_schema
        cci_cols = "\n                ck_chat_id," if has_cci else ""
        cci_vals = "\n                ?," if has_cci else ""

        # Build optional params
        msi_params = (msi_blob,) if has_msi else ()
        ab_params = (ab_blob,) if has_ab else ()
        dci_params = (destination_caller_id or None,) if has_dci else ()
        cci_params = (ck_chat_id,) if has_cci else ()

        cursor.execute(
            f"""INSERT INTO message (
                guid, text, handle_id, service, account, account_guid,
                date, date_read, date_delivered,
                is_from_me, is_sent, is_delivered, is_read, is_finished,
                is_empty, was_downgraded, cache_has_attachments,
                group_title, type, error, replace,
                version, is_emote, is_delayed, is_auto_reply, is_prepared,
                is_system_message, has_dd_results, is_service_message,
                is_forward, is_archive, was_data_detected, was_deduplicated,
                is_audio_message, is_played, date_played,
                item_type, other_handle, group_action_type,
                share_status, share_direction, is_expirable, expire_state,
                message_action_type, message_source,
                associated_message_type, associated_message_range_location,
                associated_message_range_length, time_expressive_send_played,
                ck_sync_state, ck_record_id, ck_record_change_tag,{sr_ck_cols}{msi_cols}{ab_cols}
                {dci_cols}{cci_cols}
                is_corrupt, date_recovered,
                sort_id, is_spam, has_unseen_mention,
                was_delivered_quietly, did_notify_recipient,
                date_retracted, date_edited, was_detonated,
                part_count, is_stewie, is_kt_verified, is_sos, is_critical
            ) VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, 0, 0, 0,
                10, 0, 0, 0, 0,
                0, ?, 0,
                0, 0, 1, 0,
                0, 0, 0,
                0, 0, 0,
                0, 0, 0, 0,
                0, 0,
                0, 0,
                0, 0,
                ?, ?, ?,{sr_ck_vals}{msi_vals}{ab_vals}{dci_vals}{cci_vals}
                0, 0,
                0, 0, 0,
                0, 0,
                0, 0, 0,
                ?, 0, 0, 0, 0
            )""",
            (
                msg.guid,
                display_text,
                handle_rowid,
                msg.service,
                account,
                account_guid,
                msg.date,
                msg.date_read,
                msg.date_delivered,
                int(msg.is_from_me),
                int(msg.is_sent),
                int(msg.is_delivered),
                int(msg.is_read),
                int(msg.is_finished),
                0 if display_text else 1,  # is_empty
                int(msg.was_downgraded),
                cache_has_attachments,
                msg.group_title,
                int(has_dd_results),
                msg.ck_sync_state,
                msg.ck_record_id,
                msg.ck_record_change_tag,
                *msi_params,
                *ab_params,
                *dci_params,
                *cci_params,
                part_count,
            ),
        )
        return cursor.lastrowid

    def _insert_attachment(
        self,
        cursor: sqlite3.Cursor,
        att: iOSAttachment,
        is_outgoing: bool,
        service: str,
    ) -> int:
        """Insert an attachment and return its ROWID."""
        template = self._find_attachment_template(cursor, att, is_outgoing, service)
        if template is not None:
            values = dict(template)
            values.pop("ROWID", None)
            values["guid"] = att.guid
            values["created_date"] = att.created_date
            values["start_date"] = 0
            values["filename"] = att.filename
            values["uti"] = att.uti
            values["mime_type"] = att.mime_type
            values["transfer_state"] = 5
            values["is_outgoing"] = int(is_outgoing)
            values["transfer_name"] = att.transfer_name
            values["total_bytes"] = att.total_bytes
            values["is_sticker"] = 0
            values["hide_attachment"] = 0
            values["ck_sync_state"] = 0
            values["is_commsafety_sensitive"] = 0
            if "attribution_info" in values:
                values["attribution_info"] = _DEFAULT_ATTRIBUTION_INFO
            if "user_info" in values:
                values["user_info"] = None
            if "sticker_user_info" in values:
                values["sticker_user_info"] = None
            if "sr_ck_sync_state" in values:
                values["sr_ck_sync_state"] = 0
            if "ck_record_id" in values:
                values["ck_record_id"] = None
            if "ck_server_change_token_blob" in values:
                values["ck_server_change_token_blob"] = None
            if (
                "preview_generation_state" in values
                and values["preview_generation_state"] in (None, 0)
            ):
                values["preview_generation_state"] = _default_preview_generation_state(
                    att.mime_type
                )
            if "original_guid" in values:
                values["original_guid"] = att.guid

            cols = [col for col in values if col in self._att_schema]
            placeholders = ", ".join("?" for _ in cols)
            cursor.execute(
                f"INSERT INTO attachment ({', '.join(cols)}) VALUES ({placeholders})",
                tuple(values[col] for col in cols),
            )
            return cursor.lastrowid

        preview_state = _default_preview_generation_state(att.mime_type)

        # Build optional column fragments (schema detected in _inspect_schema)
        opt_cols = ""
        opt_vals = ""
        if "sr_ck_sync_state" in self._att_schema:
            opt_cols += ", sr_ck_sync_state"
            opt_vals += ", 0"
        if "attribution_info" in self._att_schema:
            opt_cols += ", attribution_info"
            opt_vals += ", ?"
        if "preview_generation_state" in self._att_schema:
            opt_cols += ", preview_generation_state"
            opt_vals += ", ?"
        if "original_guid" in self._att_schema:
            opt_cols += ", original_guid"
            opt_vals += ", ?"

        # Build params for optional columns
        opt_params = []
        if "attribution_info" in self._att_schema:
            opt_params.append(_DEFAULT_ATTRIBUTION_INFO)
        if "preview_generation_state" in self._att_schema:
            opt_params.append(preview_state)
        if "original_guid" in self._att_schema:
            opt_params.append(att.guid)

        cursor.execute(
            f"""INSERT INTO attachment (
                guid, created_date, start_date, filename, uti, mime_type,
                transfer_state, is_outgoing, transfer_name, total_bytes,
                is_sticker, hide_attachment, ck_sync_state,
                is_commsafety_sensitive{opt_cols}
            ) VALUES (?, ?, 0, ?, ?, ?, 5, ?, ?, ?,
                0, 0, 0, 0{opt_vals})""",
            (
                att.guid,
                att.created_date,
                att.filename,
                att.uti,
                att.mime_type,
                int(is_outgoing),
                att.transfer_name,
                att.total_bytes,
                *opt_params,
            ),
        )
        return cursor.lastrowid

    def _find_attachment_template(
        self,
        cursor: sqlite3.Cursor,
        att: iOSAttachment,
        is_outgoing: bool,
        service: str,
    ) -> sqlite3.Row | None:
        """Find a real attachment row to clone for a new attachment."""
        family = None
        if att.mime_type and "/" in att.mime_type:
            family = att.mime_type.split("/", 1)[0] + "/%"

        template_base = (
            "SELECT * FROM attachment a "
            "WHERE a.filename IS NOT NULL "
            "AND NOT EXISTS ("
            "    SELECT 1 FROM message_attachment_join maj "
            "    JOIN message m ON m.ROWID = maj.message_id "
            "    WHERE maj.attachment_id = a.ROWID "
            "      AND m.guid LIKE 'green2blue:%'"
            ")"
        )
        order_terms = [
            "CASE WHEN EXISTS ("
            "    SELECT 1 FROM message_attachment_join maj "
            "    JOIN message m ON m.ROWID = maj.message_id "
            "    WHERE maj.attachment_id = a.ROWID "
            "      AND m.service = ?"
            ") THEN 0 ELSE 1 END",
        ]
        if "attribution_info" in self._att_schema:
            order_terms.append("CASE WHEN a.attribution_info IS NOT NULL THEN 0 ELSE 1 END")
        if "preview_generation_state" in self._att_schema:
            order_terms.append(
                "CASE WHEN COALESCE(a.preview_generation_state, 0) = 0 THEN 0 ELSE 1 END"
            )
        order_terms.append("a.ROWID DESC")
        order_clause = " ORDER BY " + ", ".join(order_terms) + " LIMIT 1"

        queries: list[tuple[str, tuple[object, ...]]] = [
            (
                template_base
                + " AND a.mime_type = ? AND a.is_outgoing = ?"
                + order_clause,
                (service, att.mime_type, int(is_outgoing)),
            ),
            (
                template_base
                + " AND a.uti = ? AND a.is_outgoing = ?"
                + order_clause,
                (service, att.uti, int(is_outgoing)),
            ),
        ]
        if family is not None:
            queries.append(
                (
                    template_base
                    + " AND a.mime_type LIKE ? AND a.is_outgoing = ?"
                    + order_clause,
                    (service, family, int(is_outgoing)),
                ),
            )
        queries.extend([
            (
                template_base
                + " AND a.mime_type = ?"
                + order_clause,
                (service, att.mime_type),
            ),
            (
                template_base
                + " AND a.uti = ?"
                + order_clause,
                (service, att.uti),
            ),
        ])
        if family is not None:
            queries.append(
                (
                    template_base
                    + " AND a.mime_type LIKE ?"
                    + order_clause,
                    (service, family),
                ),
            )
        queries.append(
            (
                template_base + order_clause,
                (service,),
            ),
        )

        for sql, params in queries:
            row = cursor.execute(sql, params).fetchone()
            if row is not None:
                return row
        return None

    def _insert_chat_handle_join(
        self, cursor: sqlite3.Cursor, chat_id: int, handle_id: int
    ) -> None:
        """Insert a chat-handle join, ignoring duplicates."""
        cursor.execute(
            "INSERT OR IGNORE INTO chat_handle_join (chat_id, handle_id) VALUES (?, ?)",
            (chat_id, handle_id),
        )

    def _insert_chat_message_join(
        self, cursor: sqlite3.Cursor, chat_id: int, message_id: int, message_date: int
    ) -> None:
        """Insert a chat-message join."""
        cursor.execute(
            """INSERT OR IGNORE INTO chat_message_join (chat_id, message_id, message_date)
               VALUES (?, ?, ?)""",
            (chat_id, message_id, message_date),
        )

    def _insert_message_attachment_join(
        self, cursor: sqlite3.Cursor, message_id: int, attachment_id: int
    ) -> None:
        """Insert a message-attachment join."""
        cursor.execute(
            """INSERT OR IGNORE INTO message_attachment_join (message_id, attachment_id)
               VALUES (?, ?)""",
            (message_id, attachment_id),
        )

    def get_message_count(self) -> int:
        """Return the total number of messages in the database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM message")
        return cursor.fetchone()["cnt"]

    def overwrite(
        self,
        result: ConversionResult,
        sacrifice_chat_ids: list[int],
    ) -> OverwriteStats:
        """Overwrite sacrifice messages with converted Android messages.

        UPDATE existing rows instead of INSERTing new ones, preserving
        ROWIDs, GUIDs, and CloudKit metadata so messages appear
        indistinguishable from real iOS messages.

        Args:
            result: The conversion result with messages, handles, and chats.
            sacrifice_chat_ids: ROWIDs of chats whose messages form the sacrifice pool.

        Returns:
            OverwriteStats with counts of overwritten records.
        """
        if not self.conn:
            raise DatabaseError("Database not open. Call open() first.")

        stats = OverwriteStats()

        try:
            self._drop_triggers()

            cursor = self.conn.cursor()

            # Load sacrifice messages (oldest first)
            sacrifice_pool = self._load_sacrifice_messages(cursor, sacrifice_chat_ids)
            stats.sacrifice_pool_size = len(sacrifice_pool)

            if len(sacrifice_pool) < len(result.messages):
                raise InsufficientSacrificeError(
                    f"Sacrifice pool has {len(sacrifice_pool)} messages but "
                    f"{len(result.messages)} are needed.",
                )

            # Create/dedup handles, chats, and join links
            handle_rowids, chat_rowids = self._resolve_handles_and_chats(
                cursor, result, stats,
            )

            # Detect inject-specific metadata from existing data
            injected_service = result.handles[0].service if result.handles else "SMS"
            detected_caller_id = self._detect_destination_caller_id(injected_service)
            detected_account = self._detect_account(injected_service)
            detected_account_guid = self._detect_account_guid(injected_service)

            # Overwrite messages
            pool = deque(sacrifice_pool)
            for msg in result.messages:
                handle_rowid = handle_rowids.get((msg.handle_id, msg.service))
                if handle_rowid is None:
                    logger.warning("No handle for %r — skipping message", msg.handle_id)
                    stats.messages_skipped += 1
                    continue

                sacrifice = pool.popleft()

                # Determine target chat
                chat_key = msg.chat_identifier or msg.handle_id
                chat_guid = compute_chat_guid(chat_key, msg.group_members)
                target_chat_rowid = chat_rowids.get(chat_guid)
                ck_chat_id = compute_ck_chat_id(
                    msg.service, chat_key, msg.group_members,
                )

                self._overwrite_message(
                    cursor, sacrifice["rowid"], msg, handle_rowid,
                    destination_caller_id=detected_caller_id,
                    ck_chat_id=ck_chat_id,
                    account_override=detected_account,
                    account_guid_override=detected_account_guid,
                )
                stats.messages_overwritten += 1
                stats.message_rowids.append(sacrifice["rowid"])

                # Move message from sacrifice chat to target chat
                if target_chat_rowid:
                    self._move_message_to_chat(
                        cursor, sacrifice["rowid"], target_chat_rowid, msg.date,
                    )

                # Remove old attachment joins and add new ones
                self._remove_old_attachments(cursor, sacrifice["rowid"])
                for att in msg.attachments:
                    att_rowid = self._insert_attachment(
                        cursor, att, msg.is_from_me, msg.service,
                    )
                    self._insert_message_attachment_join(cursor, sacrifice["rowid"], att_rowid)
                    stats.attachments_inserted += 1
                    stats.attachment_rowids.append(att_rowid)

            self.conn.commit()
            return stats

        except Exception:
            self.conn.rollback()
            raise

        finally:
            self._restore_triggers()

    def _load_sacrifice_messages(
        self, cursor: sqlite3.Cursor, chat_ids: list[int],
    ) -> list[dict]:
        """Load messages from sacrifice chats, oldest first.

        Returns list of dicts with rowid, guid, ck_sync_state,
        ck_record_id, ck_record_change_tag.
        """
        if not chat_ids:
            return []

        placeholders = ",".join("?" for _ in chat_ids)
        cursor.execute(
            f"""SELECT m.ROWID as rowid, m.guid, m.ck_sync_state,
                       m.ck_record_id, m.ck_record_change_tag
                FROM message m
                JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
                WHERE cmj.chat_id IN ({placeholders})
                ORDER BY m.date ASC""",
            chat_ids,
        )
        return [dict(row) for row in cursor.fetchall()]

    def _overwrite_message(
        self,
        cursor: sqlite3.Cursor,
        sacrifice_rowid: int,
        msg: iOSMessage,
        handle_rowid: int,
        *,
        destination_caller_id: str = "",
        ck_chat_id: str = "",
        account_override: str = "",
        account_guid_override: str = "",
    ) -> None:
        """UPDATE content columns of a sacrifice message, preserving CK metadata."""
        display_text = compose_message_text(msg.text, len(msg.attachments))
        cache_has_attachments = 1 if msg.attachments else 0
        caption_text = (
            display_text[len(msg.attachments):]
            if msg.attachments and display_text else (display_text or "")
        )
        part_count = len(msg.attachments) + (1 if caption_text else 0)
        if part_count == 0:
            part_count = 1
        account = msg.account if msg.account else (account_override or None)
        account_guid = (
            msg.account_guid if msg.account_guid
            else (account_guid_override or None)
        )

        # Build optional column updates
        has_msi = "message_summary_info" in self._msg_schema
        has_ab = "attributedBody" in self._msg_schema

        msi_blob = build_message_summary_info(
            service=msg.service,
            is_from_me=msg.is_from_me,
            has_text=bool(display_text),
        ) if has_msi else None

        ab_blob, has_dd_results = build_attributed_body_with_metadata(
            display_text,
            attachment_guids=tuple(att.guid for att in msg.attachments),
        ) if has_ab else (None, 0)

        opt_sets = ""
        opt_params: list = []
        if has_msi:
            opt_sets += ", message_summary_info = ?"
            opt_params.append(msi_blob)
        if has_ab:
            opt_sets += ", attributedBody = ?"
            opt_params.append(ab_blob)
        opt_sets += ", has_dd_results = ?"
        opt_params.append(int(has_dd_results))
        if "destination_caller_id" in self._msg_schema:
            opt_sets += ", destination_caller_id = ?"
            opt_params.append(destination_caller_id or None)
        if "ck_chat_id" in self._msg_schema:
            opt_sets += ", ck_chat_id = ?"
            opt_params.append(ck_chat_id)
        if "part_count" in self._msg_schema:
            opt_sets += ", part_count = ?"
            opt_params.append(part_count)

        cursor.execute(
            f"""UPDATE message SET
                text = ?,
                handle_id = ?,
                service = ?,
                account = ?,
                account_guid = ?,
                date = ?,
                date_read = ?,
                date_delivered = ?,
                is_from_me = ?,
                is_sent = ?,
                is_delivered = ?,
                is_read = ?,
                is_finished = ?,
                is_empty = ?,
                was_downgraded = ?,
                cache_has_attachments = ?,
                group_title = ?{opt_sets}
            WHERE ROWID = ?""",
            (
                display_text,
                handle_rowid,
                msg.service,
                account,
                account_guid,
                msg.date,
                msg.date_read,
                msg.date_delivered,
                int(msg.is_from_me),
                int(msg.is_sent),
                int(msg.is_delivered),
                int(msg.is_read),
                int(msg.is_finished),
                0 if display_text else 1,
                int(msg.was_downgraded),
                cache_has_attachments,
                msg.group_title,
                *opt_params,
                sacrifice_rowid,
            ),
        )

    def _move_message_to_chat(
        self,
        cursor: sqlite3.Cursor,
        message_id: int,
        new_chat_id: int,
        new_date: int,
    ) -> None:
        """Move a message from its current chat to a new chat."""
        cursor.execute(
            "DELETE FROM chat_message_join WHERE message_id = ?",
            (message_id,),
        )
        cursor.execute(
            """INSERT OR IGNORE INTO chat_message_join
               (chat_id, message_id, message_date) VALUES (?, ?, ?)""",
            (new_chat_id, message_id, new_date),
        )

    def _remove_old_attachments(
        self, cursor: sqlite3.Cursor, message_id: int,
    ) -> None:
        """Remove old attachment joins for a message."""
        cursor.execute(
            "DELETE FROM message_attachment_join WHERE message_id = ?",
            (message_id,),
        )

    # --- Clone mode (Hack Patrol approach) ---

    def _clone_last_incoming_message(self, cursor: sqlite3.Cursor) -> sqlite3.Row | None:
        """Find the last incoming SMS message to use as clone source.

        # HACK_PATROL_NOTE: Cloning from the last row inherits CK metadata,
        # but all cloned messages will share the same ck_record_id — CloudKit
        # will detect these as duplicates. green2blue's INSERT mode generates
        # unique IDs; OVERWRITE mode preserves original unique IDs.
        """
        return cursor.execute(
            "SELECT * FROM message "
            "WHERE is_from_me = 0 AND service = 'SMS' "
            "ORDER BY ROWID DESC LIMIT 1",
        ).fetchone()

    def _clone_last_sms_handle(self, cursor: sqlite3.Cursor) -> sqlite3.Row | None:
        """Find the last SMS handle to clone from."""
        return cursor.execute(
            "SELECT * FROM handle "
            "WHERE service = 'SMS' "
            "ORDER BY ROWID DESC LIMIT 1",
        ).fetchone()

    def _clone_last_sms_chat(self, cursor: sqlite3.Cursor) -> sqlite3.Row | None:
        """Find the last SMS chat to clone from."""
        return cursor.execute(
            "SELECT * FROM chat "
            "WHERE service_name = 'SMS' "
            "ORDER BY ROWID DESC LIMIT 1",
        ).fetchone()

    def _clone_insert_handle(
        self,
        cursor: sqlite3.Cursor,
        source: sqlite3.Row,
        phone: str,
    ) -> int:
        """Clone a handle from source, overriding id and uncanonicalized_id.

        # HACK_PATROL_NOTE: Sets uncanonicalized_id = id. Real iOS may differ
        # (e.g., uncanonicalized_id could be the raw dialed number).
        # green2blue's INSERT mode uses explicit iOSHandle model values.
        """
        col_names = source.keys()
        values = dict(zip(col_names, tuple(source), strict=True))
        values["id"] = phone
        values["uncanonicalized_id"] = phone  # HACK_PATROL_NOTE: same as id

        # Remove ROWID so SQLite assigns a new one
        values.pop("ROWID", None)

        cols = list(values.keys())
        placeholders = ", ".join("?" for _ in cols)
        cursor.execute(
            f"INSERT INTO handle ({', '.join(cols)}) VALUES ({placeholders})",
            tuple(values.values()),
        )
        return cursor.lastrowid

    def _clone_insert_chat(
        self,
        cursor: sqlite3.Cursor,
        source: sqlite3.Row,
        phone: str,
    ) -> int:
        """Clone a chat from source with Hack Patrol overrides.

        # HACK_PATROL_NOTE: Uses "SMS;-;" prefix instead of "any;-;".
        # Real iOS 17+ uses "any;-;" for all chats.
        # green2blue's INSERT mode uses "any;-;" (correct for modern iOS).

        # HACK_PATROL_NOTE: Sets is_filtered=1, hiding the chat from the
        # primary inbox. green2blue's INSERT mode uses is_filtered=0 (visible).
        """
        col_names = source.keys()
        values = dict(zip(col_names, tuple(source), strict=True))

        # Hack Patrol overrides
        values["guid"] = f"SMS;-;{phone}"  # HACK_PATROL_NOTE: "SMS;-;" not "any;-;"
        values["chat_identifier"] = phone
        values["is_filtered"] = 1  # HACK_PATROL_NOTE: hidden from inbox
        values["group_id"] = str(uuid.uuid4()).upper()

        # Remove ROWID so SQLite assigns a new one
        values.pop("ROWID", None)

        cols = list(values.keys())
        placeholders = ", ".join("?" for _ in cols)
        cursor.execute(
            f"INSERT INTO chat ({', '.join(cols)}) VALUES ({placeholders})",
            tuple(values.values()),
        )
        return cursor.lastrowid

    def _clone_insert_message(
        self,
        cursor: sqlite3.Cursor,
        source: sqlite3.Row,
        overrides: dict,
    ) -> int:
        """Clone ALL columns from source message, applying overrides.

        # HACK_PATROL_NOTE: Inherits message_summary_info from source instead
        # of generating per-message. Wrong blob for new content/direction.
        # green2blue's INSERT mode generates correct MSI per message.

        # HACK_PATROL_NOTE: CK metadata (ck_sync_state, ck_record_id,
        # ck_record_change_tag) is duplicated from source. All cloned messages
        # share the same ck_record_id — CloudKit detects duplicates.
        # green2blue's INSERT mode generates unique IDs; OVERWRITE preserves
        # original unique IDs from sacrifice messages.
        """
        col_names = source.keys()
        values = dict(zip(col_names, tuple(source), strict=True))

        # Apply overrides
        values.update(overrides)

        # Remove ROWID so SQLite assigns a new one
        values.pop("ROWID", None)

        cols = list(values.keys())
        placeholders = ", ".join("?" for _ in cols)
        cursor.execute(
            f"INSERT INTO message ({', '.join(cols)}) VALUES ({placeholders})",
            tuple(values.values()),
        )
        return cursor.lastrowid

    def clone(self, result: ConversionResult) -> CloneStats:
        """Clone existing messages to inject new content (Hack Patrol approach).

        Faithfully reproduces the Hack Patrol (2022) single-SMS injection
        technique: clone the last existing database row and modify specific
        fields, inheriting ALL other columns including CloudKit metadata.

        # HACK_PATROL_NOTE: Does NOT drop triggers. Real iOS triggers call
        # internal functions that fail outside the device, so green2blue's
        # INSERT/OVERWRITE modes drop triggers before injection and restore
        # them after. Hack Patrol leaves them in place.

        # HACK_PATROL_NOTE: No duplicate detection. green2blue's INSERT mode
        # uses content-hash dedup to avoid creating duplicate messages.

        Args:
            result: The conversion result with messages, handles, and chats.

        Returns:
            CloneStats with counts of cloned records.
        """
        if not self.conn:
            raise DatabaseError("Database not open. Call open() first.")

        stats = CloneStats()

        try:
            cursor = self.conn.cursor()

            # Find clone sources
            source_msg = self._clone_last_incoming_message(cursor)
            if source_msg is None:
                raise CloneSourceError(
                    "No incoming SMS message found to clone from.",
                )

            source_handle = self._clone_last_sms_handle(cursor)
            if source_handle is None:
                raise CloneSourceError(
                    "No SMS handle found to clone from.",
                )

            source_chat = self._clone_last_sms_chat(cursor)
            if source_chat is None:
                raise CloneSourceError(
                    "No SMS chat found to clone from.",
                )

            stats.clone_source_rowid = source_msg["ROWID"]
            stats.ck_metadata_duplicated = bool(source_msg["ck_record_id"])

            # Track created handles and chats for reuse
            handle_cache: dict[str, int] = {}  # phone -> rowid
            chat_cache: dict[str, int] = {}  # phone -> rowid

            for msg in result.messages:
                phone = msg.handle_id

                # Create or reuse handle
                if phone in handle_cache:
                    handle_rowid = handle_cache[phone]
                else:
                    # Check if handle already exists
                    existing = cursor.execute(
                        "SELECT ROWID FROM handle WHERE id = ? AND service = 'SMS'",
                        (phone,),
                    ).fetchone()
                    if existing:
                        handle_rowid = existing[0]
                        stats.handles_existing += 1
                    else:
                        handle_rowid = self._clone_insert_handle(
                            cursor, source_handle, phone,
                        )
                        stats.handles_inserted += 1
                    handle_cache[phone] = handle_rowid

                # Create or reuse chat
                # HACK_PATROL_NOTE: "SMS;-;" prefix, not "any;-;"
                chat_guid = f"SMS;-;{phone}"
                if phone in chat_cache:
                    chat_rowid = chat_cache[phone]
                else:
                    existing_chat = cursor.execute(
                        "SELECT ROWID FROM chat WHERE guid = ?",
                        (chat_guid,),
                    ).fetchone()
                    if existing_chat:
                        chat_rowid = existing_chat[0]
                        stats.chats_existing += 1
                    else:
                        chat_rowid = self._clone_insert_chat(
                            cursor, source_chat, phone,
                        )
                        stats.chats_inserted += 1

                        # Link handle to chat
                        cursor.execute(
                            "INSERT OR IGNORE INTO chat_handle_join "
                            "(chat_id, handle_id) VALUES (?, ?)",
                            (chat_rowid, handle_rowid),
                        )
                    clone_chat = iOSChat(
                        guid=chat_guid,
                        style=45,
                        chat_identifier=phone,
                        service_name="SMS",
                    )
                    self._ensure_chat_auxiliary_rows(cursor, chat_rowid, clone_chat)
                    chat_cache[phone] = chat_rowid

                # Clone message with overrides
                # HACK_PATROL_NOTE: Plain UUID guid, no "green2blue:" prefix.
                # green2blue's INSERT mode prefixes with "green2blue:" for
                # easy identification of injected messages.
                msg_guid = str(uuid.uuid4()).upper()

                ab_blob = _build_hackpatrol_attributed_body(msg.text)

                overrides = {
                    "guid": msg_guid,
                    "handle_id": handle_rowid,
                    "text": msg.text,
                    "date": msg.date,
                    "date_read": msg.date_read,
                    "date_delivered": msg.date_delivered,
                    "is_from_me": int(msg.is_from_me),
                    "is_sent": int(msg.is_sent),
                    "is_read": int(msg.is_read),
                }

                # Only override attributedBody if schema has it
                if "attributedBody" in self._msg_schema:
                    overrides["attributedBody"] = ab_blob

                msg_rowid = self._clone_insert_message(
                    cursor, source_msg, overrides,
                )
                stats.message_rowids.append(msg_rowid)

                # Create chat_message_join
                cursor.execute(
                    "INSERT OR IGNORE INTO chat_message_join "
                    "(chat_id, message_id, message_date) VALUES (?, ?, ?)",
                    (chat_rowid, msg_rowid, msg.date),
                )

                stats.messages_cloned += 1

            self.conn.commit()
            return stats

        except Exception:
            self.conn.rollback()
            raise

    def integrity_check(self) -> bool:
        """Run PRAGMA integrity_check on the database."""
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA integrity_check")
        result = cursor.fetchone()[0]
        return result == "ok"


class _BaseStats:
    """Shared metrics between injection modes."""

    def __init__(self):
        self.handles_inserted: int = 0
        self.handles_existing: int = 0
        self.chats_inserted: int = 0
        self.chats_existing: int = 0
        self.messages_skipped: int = 0
        self.attachments_inserted: int = 0


class InjectionStats(_BaseStats):
    """Statistics from an injection operation."""

    def __init__(self):
        super().__init__()
        self.messages_inserted: int = 0
        self.message_rowids: list[int] = []
        self.attachment_rowids: list[int] = []

    def __repr__(self) -> str:
        return (
            f"InjectionStats(messages={self.messages_inserted}, "
            f"skipped={self.messages_skipped}, "
            f"handles={self.handles_inserted} new/{self.handles_existing} existing, "
            f"chats={self.chats_inserted} new/{self.chats_existing} existing, "
            f"attachments={self.attachments_inserted})"
        )


class OverwriteStats(_BaseStats):
    """Statistics from an overwrite operation."""

    def __init__(self):
        super().__init__()
        self.sacrifice_pool_size: int = 0
        self.messages_overwritten: int = 0
        self.message_rowids: list[int] = []
        self.attachment_rowids: list[int] = []

    def __repr__(self) -> str:
        return (
            f"OverwriteStats(overwritten={self.messages_overwritten}, "
            f"skipped={self.messages_skipped}, "
            f"sacrifice_pool={self.sacrifice_pool_size}, "
            f"handles={self.handles_inserted} new/{self.handles_existing} existing, "
            f"chats={self.chats_inserted} new/{self.chats_existing} existing, "
            f"attachments={self.attachments_inserted})"
        )


class CloneStats:
    """Statistics from a clone operation (Hack Patrol approach)."""

    def __init__(self):
        self.messages_cloned: int = 0
        self.clone_source_rowid: int = 0
        self.ck_metadata_duplicated: bool = False
        self.handles_inserted: int = 0
        self.handles_existing: int = 0
        self.chats_inserted: int = 0
        self.chats_existing: int = 0
        self.message_rowids: list[int] = []
        self.attachment_rowids: list[int] = []

    def __repr__(self) -> str:
        return (
            f"CloneStats(cloned={self.messages_cloned}, "
            f"source_rowid={self.clone_source_rowid}, "
            f"ck_duplicated={self.ck_metadata_duplicated}, "
            f"handles={self.handles_inserted} new/{self.handles_existing} existing, "
            f"chats={self.chats_inserted} new/{self.chats_existing} existing)"
        )


def _build_hackpatrol_attributed_body(text: str | None) -> bytes | None:
    """Build an attributedBody blob using the Hack Patrol binary template.

    This uses a simplified binary template approach with a single-byte
    length prefix, limiting text to 255 UTF-8 bytes. For longer text,
    falls back to the proper typedstream builder.

    # HACK_PATROL_NOTE: 255 char limit is fragile. green2blue's
    # build_attributed_body() handles any length via proper typedstream encoding.
    """
    if not text:
        return None

    text_bytes = text.encode("utf-8")
    if len(text_bytes) > 255:
        # HACK_PATROL_NOTE: Fallback to proper builder for long messages.
        # Real Hack Patrol would truncate or fail here.
        return build_attributed_body(text)

    utf16_len = len(text.encode("utf-16-le")) // 2

    # Simplified template: header + single-byte length + text + attributes
    # This matches the Hack Patrol binary template approach
    header = bytes.fromhex(
        "040b73747265616d747970656481e803"
        "840140"
        "848484124e53417474726962757465645374"
        "72696e6700"
        "8484084e534f626a65637400"
        "85"
        "92"
        "848484084e53537472696e6701"
        "94"
        "84012b"
    )

    middle = bytes.fromhex("86" "84026949")

    suffix = bytes.fromhex(
        "92"
        "8484840c4e5344696374696f6e61727900"
        "94"
        "840169"
        "01"
        "92"
        "8496"
        "96"
        "1d"
        "5f5f6b494d4d6573736167655061727441"
        "74747269627574654e616d65"
        "86"
        "92"
        "848484084e534e756d62657200"
        "8484074e5356616c756500"
        "94"
        "84012a"
        "84"
        "9999"
        "00"
        "868686"
    )

    # Single-byte length for UTF-16 attribute run length
    if utf16_len < 128:
        run_len_byte = bytes([utf16_len])
    else:
        # HACK_PATROL_NOTE: Single-byte encoding can't represent >= 128.
        # Proper typedstream uses multi-byte format. Fall back.
        return build_attributed_body(text)

    # Single-byte length prefix (max 255)
    return (
        header
        + bytes([len(text_bytes)])
        + text_bytes
        + middle
        + b"\x01"
        + run_len_byte
        + suffix
    )
