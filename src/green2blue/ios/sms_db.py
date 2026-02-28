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
import sqlite3
from pathlib import Path

from green2blue.exceptions import DatabaseError
from green2blue.models import (
    ConversionResult,
    compute_chat_guid,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
    message_content_hash,
)

logger = logging.getLogger(__name__)


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

            # Load existing data for dedup
            existing_handles = self._load_existing_handles()
            existing_chats = self._load_existing_chats()
            existing_hashes = set()
            if skip_duplicates:
                existing_hashes = self._load_existing_message_hashes()

            # Insert handles
            handle_rowids: dict[str, int] = {}
            for handle in result.handles:
                if handle.id in existing_handles:
                    handle_rowids[handle.id] = existing_handles[handle.id]
                    stats.handles_existing += 1
                else:
                    rowid = self._insert_handle(cursor, handle)
                    handle_rowids[handle.id] = rowid
                    stats.handles_inserted += 1

            # Insert chats
            chat_rowids: dict[str, int] = {}
            for chat in result.chats:
                if chat.guid in existing_chats:
                    chat_rowids[chat.guid] = existing_chats[chat.guid]
                    stats.chats_existing += 1
                else:
                    rowid = self._insert_chat(cursor, chat)
                    chat_rowids[chat.guid] = rowid
                    stats.chats_inserted += 1

            # Link handles to chats
            for chat in result.chats:
                chat_rowid = chat_rowids.get(chat.guid)
                if chat_rowid is None:
                    continue
                # For 1:1 chats, link the single handle
                if chat.style == 45:
                    handle_rowid = handle_rowids.get(chat.chat_identifier)
                    if handle_rowid:
                        self._insert_chat_handle_join(cursor, chat_rowid, handle_rowid)
                else:
                    # Group chat: link all handles in the identifier
                    for phone in chat.chat_identifier.split(","):
                        phone = phone.strip()
                        handle_rowid = handle_rowids.get(phone)
                        if handle_rowid:
                            self._insert_chat_handle_join(cursor, chat_rowid, handle_rowid)

            # Insert messages
            for msg in result.messages:
                # Duplicate check
                if skip_duplicates:
                    msg_hash = message_content_hash(msg)
                    if msg_hash in existing_hashes:
                        stats.messages_skipped += 1
                        continue

                handle_rowid = handle_rowids.get(msg.handle_id, 0)

                msg_rowid = self._insert_message(cursor, msg, handle_rowid)
                stats.messages_inserted += 1

                # Determine which chat this message belongs to
                chat_key = msg.chat_identifier or msg.handle_id
                chat_guid = compute_chat_guid(chat_key, msg.group_members)

                chat_rowid = chat_rowids.get(chat_guid)
                if chat_rowid:
                    self._insert_chat_message_join(cursor, chat_rowid, msg_rowid, msg.date)

                # Insert attachments
                for att in msg.attachments:
                    att_rowid = self._insert_attachment(cursor, att, msg.is_from_me)
                    self._insert_message_attachment_join(cursor, msg_rowid, att_rowid)
                    stats.attachments_inserted += 1

            self.conn.commit()
            return stats

        except Exception:
            self.conn.rollback()
            raise

        finally:
            # Always restore triggers, whether injection succeeded or failed
            self._restore_triggers()

    def _drop_triggers(self) -> None:
        """Drop all triggers from sms.db, saving their CREATE SQL.

        DDL statements (DROP TRIGGER) auto-commit in Python's sqlite3,
        so this must run outside the data transaction. Trigger restoration
        is guaranteed by the try/finally in inject().
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")
        self._saved_triggers = []
        trigger_names = []
        for row in cursor.fetchall():
            if row["sql"]:
                self._saved_triggers.append(row["sql"])
            trigger_names.append(row["name"])

        for name in trigger_names:
            # Parameterized queries can't be used for DDL identifiers,
            # so we validate the name contains only safe characters
            if not all(c.isalnum() or c == '_' for c in name):
                logger.warning("Skipping trigger with unsafe name: %r", name)
                continue
            cursor.execute(f"DROP TRIGGER IF EXISTS [{name}]")
        self.conn.commit()
        logger.debug("Dropped %d triggers", len(trigger_names))

    def _restore_triggers(self) -> None:
        """Restore previously dropped triggers."""
        if not self._saved_triggers:
            return
        cursor = self.conn.cursor()
        restored = 0
        for sql in self._saved_triggers:
            try:
                cursor.execute(sql)
                restored += 1
            except sqlite3.Error as e:
                logger.warning("Failed to restore trigger: %s", e)
        self.conn.commit()
        logger.debug("Restored %d/%d triggers", restored, len(self._saved_triggers))
        self._saved_triggers = []

    def _load_existing_handles(self) -> dict[str, int]:
        """Load existing handles as {id: ROWID}."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT ROWID, id FROM handle")
        return {row["id"]: row["ROWID"] for row in cursor.fetchall()}

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
            SELECT m.text, m.date, h.id as handle_id
            FROM message m
            LEFT JOIN handle h ON m.handle_id = h.ROWID
        """)
        for row in cursor.fetchall():
            content = f"{row['handle_id'] or ''}|{row['date']}|{row['text'] or ''}"
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
        cursor.execute(
            """INSERT INTO chat (guid, style, state, account_id, chat_identifier,
                                 service_name, display_name, account_login,
                                 ck_sync_state, cloudkit_record_id)
               VALUES (?, ?, 3, ?, ?, ?, ?, ?, ?, ?)""",
            (
                chat.guid,
                chat.style,
                chat.account_id,
                chat.chat_identifier,
                chat.service_name,
                chat.display_name,
                chat.account_id,
                chat.ck_sync_state,
                chat.cloudkit_record_id,
            ),
        )
        return cursor.lastrowid

    def _insert_message(
        self, cursor: sqlite3.Cursor, msg: iOSMessage, handle_rowid: int
    ) -> int:
        """Insert a message and return its ROWID."""
        cache_has_attachments = 1 if msg.attachments else 0

        cursor.execute(
            """INSERT INTO message (
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
                ck_sync_state, ck_record_id, ck_record_change_tag,
                sr_ck_sync_state, is_corrupt,
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
                1, 0, 0, 0, 0,
                0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0,
                0, 0, 0,
                0, 0, 0, 0,
                0, 0,
                0, 0,
                0, 0,
                ?, ?, ?,
                0, 0,
                0, 0, 0,
                0, 0,
                0, 0, 0,
                1, 0, 0, 0, 0
            )""",
            (
                msg.guid,
                msg.text,
                handle_rowid,
                msg.service,
                msg.account_guid,
                msg.account_guid,
                msg.date,
                msg.date_read,
                msg.date_delivered,
                int(msg.is_from_me),
                int(msg.is_sent),
                int(msg.is_delivered),
                int(msg.is_read),
                int(msg.is_finished),
                0 if msg.text else 1,  # is_empty
                int(msg.was_downgraded),
                cache_has_attachments,
                msg.group_title,
                msg.ck_sync_state,
                msg.ck_record_id,
                msg.ck_record_change_tag,
            ),
        )
        return cursor.lastrowid

    def _insert_attachment(
        self, cursor: sqlite3.Cursor, att: iOSAttachment, is_outgoing: bool
    ) -> int:
        """Insert an attachment and return its ROWID."""
        cursor.execute(
            """INSERT INTO attachment (
                guid, created_date, start_date, filename, uti, mime_type,
                transfer_state, is_outgoing, transfer_name, total_bytes,
                is_sticker, hide_attachment, ck_sync_state, sr_ck_sync_state,
                is_commsafety_sensitive
            ) VALUES (?, ?, ?, ?, ?, ?, 5, ?, ?, ?, 0, 0, 0, 0, 0)""",
            (
                att.guid,
                att.created_date,
                att.created_date,
                att.filename,
                att.uti,
                att.mime_type,
                int(is_outgoing),
                att.transfer_name,
                att.total_bytes,
            ),
        )
        return cursor.lastrowid

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

    def integrity_check(self) -> bool:
        """Run PRAGMA integrity_check on the database."""
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA integrity_check")
        result = cursor.fetchone()[0]
        return result == "ok"


class InjectionStats:
    """Statistics from an injection operation."""

    def __init__(self):
        self.handles_inserted: int = 0
        self.handles_existing: int = 0
        self.chats_inserted: int = 0
        self.chats_existing: int = 0
        self.messages_inserted: int = 0
        self.messages_skipped: int = 0
        self.attachments_inserted: int = 0

    def __repr__(self) -> str:
        return (
            f"InjectionStats(messages={self.messages_inserted}, "
            f"skipped={self.messages_skipped}, "
            f"handles={self.handles_inserted} new/{self.handles_existing} existing, "
            f"chats={self.chats_inserted} new/{self.chats_existing} existing, "
            f"attachments={self.attachments_inserted})"
        )


