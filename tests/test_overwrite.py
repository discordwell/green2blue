"""Tests for overwrite mode (UPDATE existing messages instead of INSERT)."""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from green2blue.exceptions import InsufficientSacrificeError
from green2blue.ios.sms_db import OverwriteStats, SMSDatabase
from green2blue.models import (
    ConversionResult,
    compute_ck_chat_id,
    iOSAttachment,
    iOSChat,
    iOSHandle,
    iOSMessage,
)

# --- Helpers ---


def _make_handle(phone: str, service: str = "SMS") -> iOSHandle:
    return iOSHandle(id=phone, country="us", service=service)


def _make_chat(phone: str, service: str = "SMS") -> iOSChat:
    return iOSChat(
        guid=f"any;-;{phone}",
        style=45,
        chat_identifier=phone,
        service_name=service,
    )


def _make_message(
    phone: str,
    text: str,
    date: int,
    service: str = "SMS",
    is_from_me: bool = False,
    attachments: tuple = (),
) -> iOSMessage:
    return iOSMessage(
        guid=f"green2blue:{uuid.uuid4()}",
        text=text,
        handle_id=phone,
        date=date,
        date_read=date if not is_from_me else 0,
        date_delivered=date,
        is_from_me=is_from_me,
        service=service,
        chat_identifier=phone,
        attachments=attachments,
    )


def _make_result(
    messages: list[iOSMessage],
    handles: list[iOSHandle],
    chats: list[iOSChat],
) -> ConversionResult:
    return ConversionResult(
        messages=messages,
        handles=handles,
        chats=chats,
    )


def _populate_sacrifice_db(
    db_path: Path,
    chat_identifier: str,
    count: int,
    *,
    service: str = "SMS",
    ck_sync_state: int = 1,
) -> tuple[int, list[int]]:
    """Insert sacrifice messages with handles, chats, join tables, and CK metadata.

    Returns (chat_rowid, list_of_message_rowids).
    """
    conn = sqlite3.connect(db_path)

    # Create handle
    conn.execute(
        "INSERT INTO handle (id, country, service) VALUES (?, 'us', ?)",
        (chat_identifier, service),
    )
    handle_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Create chat
    chat_guid = f"any;-;{chat_identifier}"
    conn.execute(
        """INSERT INTO chat (guid, style, state, chat_identifier, service_name,
                             account_login, group_id, server_change_token,
                             ck_sync_state, cloudkit_record_id)
           VALUES (?, 45, 3, ?, ?, 'E:', ?, '', ?, '')""",
        (chat_guid, chat_identifier, service, str(uuid.uuid4()), ck_sync_state),
    )
    chat_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Link handle to chat
    conn.execute(
        "INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (?, ?)",
        (chat_rowid, handle_rowid),
    )

    # Insert sacrifice messages
    base_date = 700000000000000000  # iOS nanosecond timestamp
    message_rowids = []
    for i in range(count):
        msg_guid = f"original-msg-{uuid.uuid4()}"
        ck_record_id = f"{'a' * 60}{i:04d}" if ck_sync_state == 1 else ""
        ck_change_tag = "42" if ck_sync_state == 1 else ""
        msg_date = base_date + i * 1000000000

        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, date,
                                    date_read, date_delivered, is_from_me,
                                    is_read, is_delivered, is_finished,
                                    ck_sync_state, ck_record_id, ck_record_change_tag)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, 1, 1, ?, ?, ?)""",
            (
                msg_guid,
                f"sacrifice message {i}",
                handle_rowid,
                service,
                msg_date,
                msg_date,
                msg_date,
                ck_sync_state,
                ck_record_id,
                ck_change_tag,
            ),
        )
        rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        message_rowids.append(rowid)

        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (?, ?, ?)",
            (chat_rowid, rowid, msg_date),
        )

    conn.commit()
    conn.close()
    return chat_rowid, message_rowids


# --- Tests ---


class TestOverwriteContentUpdate:
    """Content columns are updated correctly."""

    def test_text_updated(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15552220000", "New Android text", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            stats = db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT text FROM message WHERE ROWID = ?", (msg_ids[0],)).fetchone()
        assert row["text"] == "New Android text"
        assert stats.messages_overwritten == 1
        conn.close()

    def test_attributed_body_updated(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15552220000", "Has attributedBody", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT attributedBody FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()
        assert row["attributedBody"] is not None
        conn.close()

    def test_has_dd_results_updated_for_rich_url_body(self, empty_sms_db: Path, monkeypatch):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message(
            "+15552220000",
            "See https://example.com/link",
            800000000000000000,
        )
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )
        monkeypatch.setattr(
            "green2blue.ios.sms_db.build_attributed_body_with_metadata",
            lambda display_text, *, attachment_guids=(): (b"rich-url-blob", True),
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT has_dd_results, attributedBody FROM message WHERE ROWID = ?",
            (msg_ids[0],),
        ).fetchone()
        assert row["has_dd_results"] == 1
        assert row["attributedBody"] == b"rich-url-blob"
        conn.close()

    def test_message_summary_info_updated(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15552220000", "Has MSI", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT message_summary_info FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()
        assert row["message_summary_info"] is not None
        conn.close()

    def test_dates_updated(self, empty_sms_db: Path):
        chat_id, _ = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        new_date = 850000000000000000
        android_msg = _make_message("+15552220000", "Date test", new_date)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT date, date_read, date_delivered FROM message").fetchone()
        assert row["date"] == new_date
        assert row["date_read"] == new_date
        assert row["date_delivered"] == new_date
        conn.close()

    def test_metadata_fields_updated(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)

        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "UPDATE message SET account = 'stale-account', account_guid = 'STALE-GUID', "
            "destination_caller_id = '+19995550000', ck_chat_id = 'STALE-CK' "
            "WHERE ROWID = ?",
            (msg_ids[0],),
        )
        for i in range(3):
            conn.execute(
                "INSERT INTO message (guid, text, service, account, account_guid, "
                "destination_caller_id, date) VALUES (?, 'real', 'SMS', ?, ?, ?, ?)",
                (
                    f"real-meta-{i}",
                    "P:+15052289549",
                    "AD9A6DB5-8CDA-48CD-9819-25C5F91E775D",
                    "+15052289549",
                    1000 + i,
                ),
            )
        conn.commit()
        conn.close()

        android_msg = _make_message("+15552220000", "Metadata test", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT account, account_guid, destination_caller_id, ck_chat_id "
            "FROM message WHERE ROWID = ?",
            (msg_ids[0],),
        ).fetchone()
        assert row["account"] == "P:+15052289549"
        assert row["account_guid"] == "AD9A6DB5-8CDA-48CD-9819-25C5F91E775D"
        assert row["destination_caller_id"] == "+15052289549"
        assert row["ck_chat_id"] == compute_ck_chat_id("SMS", "+15552220000")
        conn.close()


class TestOverwriteCKPreservation:
    """CloudKit metadata is preserved from sacrifice messages."""

    def test_ck_sync_state_preserved(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(
            empty_sms_db,
            "+15551110000",
            1,
            ck_sync_state=1,
        )
        android_msg = _make_message("+15552220000", "CK test", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_sync_state FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()
        assert row["ck_sync_state"] == 1
        conn.close()

    def test_ck_record_id_preserved(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(
            empty_sms_db,
            "+15551110000",
            1,
            ck_sync_state=1,
        )

        # Read the original ck_record_id
        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        original = conn.execute(
            "SELECT ck_record_id FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()["ck_record_id"]
        conn.close()

        android_msg = _make_message("+15552220000", "CK id test", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_record_id FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()
        assert row["ck_record_id"] == original
        conn.close()

    def test_ck_record_change_tag_preserved(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(
            empty_sms_db,
            "+15551110000",
            1,
            ck_sync_state=1,
        )
        android_msg = _make_message("+15552220000", "CK tag test", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ck_record_change_tag FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()
        assert row["ck_record_change_tag"] == "42"
        conn.close()


class TestOverwriteRowidPreservation:
    """ROWID and original GUID are preserved."""

    def test_rowid_preserved(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 3)
        messages = [
            _make_message("+15552220000", f"msg {i}", 800000000000000000 + i * 1000000000)
            for i in range(3)
        ]
        result = _make_result(
            messages,
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        rowids = [r[0] for r in conn.execute("SELECT ROWID FROM message ORDER BY ROWID").fetchall()]
        assert rowids == msg_ids
        conn.close()

    def test_original_guid_preserved(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)

        # Read original GUID
        conn = sqlite3.connect(empty_sms_db)
        original_guid = conn.execute(
            "SELECT guid FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()[0]
        conn.close()

        android_msg = _make_message("+15552220000", "guid test", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        new_guid = conn.execute(
            "SELECT guid FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()[0]
        assert new_guid == original_guid
        assert not new_guid.startswith("green2blue:")
        conn.close()


class TestOverwriteHandleUpdate:
    """handle_id is changed to the Android contact's handle."""

    def test_handle_id_changed(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15552220000", "handle test", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            stats = db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT handle_id FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()
        # The handle_id should point to the new Android contact handle
        new_handle = conn.execute("SELECT ROWID FROM handle WHERE id = '+15552220000'").fetchone()
        assert row["handle_id"] == new_handle[0]
        assert stats.handles_inserted == 1
        conn.close()


class TestOverwriteChatMessageJoin:
    """chat_message_join is updated correctly."""

    def test_message_moved_to_target_chat(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15552220000", "chat move", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        conn.row_factory = sqlite3.Row
        # Old join should be gone
        old_join = conn.execute(
            "SELECT * FROM chat_message_join WHERE chat_id = ? AND message_id = ?",
            (chat_id, msg_ids[0]),
        ).fetchone()
        assert old_join is None

        # New join should exist
        target_chat = conn.execute(
            "SELECT ROWID FROM chat WHERE guid = 'any;-;+15552220000'"
        ).fetchone()
        new_join = conn.execute(
            "SELECT * FROM chat_message_join WHERE chat_id = ? AND message_id = ?",
            (target_chat[0], msg_ids[0]),
        ).fetchone()
        assert new_join is not None
        assert new_join["message_date"] == 800000000000000000
        conn.close()


class TestOverwriteTargetCreation:
    """Target handles and chats are created for Android contacts."""

    def test_target_handles_created(self, empty_sms_db: Path):
        chat_id, _ = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15559990000", "new handle", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15559990000")],
            [_make_chat("+15559990000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            stats = db.overwrite(result, [chat_id])

        assert stats.handles_inserted == 1
        conn = sqlite3.connect(empty_sms_db)
        row = conn.execute("SELECT id FROM handle WHERE id = '+15559990000'").fetchone()
        assert row is not None
        conn.close()

    def test_target_chats_created(self, empty_sms_db: Path):
        chat_id, _ = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15559990000", "new chat", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15559990000")],
            [_make_chat("+15559990000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            stats = db.overwrite(result, [chat_id])

        assert stats.chats_inserted == 1
        conn = sqlite3.connect(empty_sms_db)
        row = conn.execute("SELECT guid FROM chat WHERE guid = 'any;-;+15559990000'").fetchone()
        assert row is not None
        conn.close()


class TestOverwriteInsufficientPool:
    """InsufficientSacrificeError when pool is too small."""

    def test_insufficient_sacrifice_raises(self, empty_sms_db: Path):
        chat_id, _ = _populate_sacrifice_db(empty_sms_db, "+15551110000", 2)
        messages = [
            _make_message("+15552220000", f"msg {i}", 800000000000000000 + i * 1000000000)
            for i in range(5)
        ]
        result = _make_result(
            messages,
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with (
            SMSDatabase(empty_sms_db) as db,
            pytest.raises(
                InsufficientSacrificeError,
                match="2 messages but 5 are needed",
            ),
        ):
            db.overwrite(result, [chat_id])


class TestOverwriteMultipleSacrificeChats:
    """Messages from multiple sacrifice chats are pooled correctly."""

    def test_multiple_sacrifice_chats(self, empty_sms_db: Path):
        chat_id_1, msg_ids_1 = _populate_sacrifice_db(
            empty_sms_db,
            "+15551110000",
            2,
        )
        chat_id_2, msg_ids_2 = _populate_sacrifice_db(
            empty_sms_db,
            "+15553330000",
            2,
        )
        messages = [
            _make_message("+15559990000", f"multi {i}", 800000000000000000 + i * 1000000000)
            for i in range(4)
        ]
        result = _make_result(
            messages,
            [_make_handle("+15559990000")],
            [_make_chat("+15559990000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            stats = db.overwrite(result, [chat_id_1, chat_id_2])

        assert stats.messages_overwritten == 4
        assert stats.sacrifice_pool_size == 4


class TestOverwriteAttachments:
    """Old attachment joins removed, new ones added."""

    def test_old_attachments_removed_new_added(self, empty_sms_db: Path):
        # Insert sacrifice with an attachment
        conn = sqlite3.connect(empty_sms_db)
        conn.execute(
            "INSERT INTO handle (id, country, service) VALUES ('+15551110000', 'us', 'SMS')",
        )
        handle_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO chat (guid, style, state, chat_identifier, service_name,
                                 account_login, group_id, server_change_token,
                                 ck_sync_state, cloudkit_record_id)
               VALUES ('any;-;+15551110000', 45, 3, '+15551110000', 'SMS', 'E:',
                        'g1', '', 1, '')""",
        )
        chat_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat_handle_join (chat_id, handle_id) VALUES (?, ?)",
            (chat_rowid, handle_rowid),
        )
        conn.execute(
            """INSERT INTO message (guid, text, handle_id, service, date,
                                    ck_sync_state, ck_record_id, ck_record_change_tag)
               VALUES ('old-msg', 'old text', ?, 'SMS', 700000000000000000, 1, 'rec1', '1')""",
            (handle_rowid,),
        )
        msg_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chat_message_join (chat_id, message_id, message_date) VALUES (?, ?, ?)",
            (chat_rowid, msg_rowid, 700000000000000000),
        )
        conn.execute(
            """INSERT INTO attachment (guid, created_date, filename, uti, mime_type,
                                       transfer_name, total_bytes)
               VALUES ('old-att', 700000000, 'old.jpg', 'public.jpeg', 'image/jpeg',
                        'old.jpg', 1000)""",
        )
        old_att_rowid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO message_attachment_join (message_id, attachment_id) VALUES (?, ?)",
            (msg_rowid, old_att_rowid),
        )
        conn.commit()
        conn.close()

        # Create Android message with new attachment
        new_att = iOSAttachment(
            guid=f"green2blue-att:{uuid.uuid4()}",
            filename="~/Library/SMS/Attachments/new.jpg",
            mime_type="image/jpeg",
            uti="public.jpeg",
            transfer_name="new.jpg",
            total_bytes=2000,
            created_date=800000000,
        )
        android_msg = _make_message(
            "+15559990000",
            "with attachment",
            800000000000000000,
            attachments=(new_att,),
        )
        result = _make_result(
            [android_msg],
            [_make_handle("+15559990000")],
            [_make_chat("+15559990000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            stats = db.overwrite(result, [chat_rowid])

        conn = sqlite3.connect(empty_sms_db)
        # Old attachment join should be removed
        old_join = conn.execute(
            "SELECT * FROM message_attachment_join WHERE attachment_id = ?",
            (old_att_rowid,),
        ).fetchone()
        assert old_join is None

        # New attachment join should exist
        new_join = conn.execute(
            "SELECT * FROM message_attachment_join WHERE message_id = ?",
            (msg_rowid,),
        ).fetchone()
        assert new_join is not None
        assert stats.attachments_inserted == 1
        conn.close()


class TestOverwriteTriggerRestore:
    """Triggers are restored after overwrite."""

    def test_triggers_restored_after_overwrite(self, empty_sms_db: Path):
        # Add a dummy trigger to the DB
        conn = sqlite3.connect(empty_sms_db)
        conn.execute("CREATE TRIGGER test_trigger AFTER INSERT ON handle BEGIN SELECT 1; END")
        conn.commit()
        conn.close()

        chat_id, _ = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)
        android_msg = _make_message("+15552220000", "trigger test", 800000000000000000)
        result = _make_result(
            [android_msg],
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            db.overwrite(result, [chat_id])

        conn = sqlite3.connect(empty_sms_db)
        triggers = conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        trigger_names = [t[0] for t in triggers]
        assert "test_trigger" in trigger_names
        conn.close()


class TestOverwriteTransactionRollback:
    """Transaction is rolled back on error."""

    def test_rollback_on_error(self, empty_sms_db: Path):
        chat_id, msg_ids = _populate_sacrifice_db(empty_sms_db, "+15551110000", 1)

        # Read original text
        conn = sqlite3.connect(empty_sms_db)
        original_text = conn.execute(
            "SELECT text FROM message WHERE ROWID = ?", (msg_ids[0],)
        ).fetchone()[0]
        conn.close()

        # Pool too small should raise and rollback
        messages = [
            _make_message("+15552220000", f"msg {i}", 800000000000000000 + i) for i in range(5)
        ]
        result = _make_result(
            messages,
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db, pytest.raises(InsufficientSacrificeError):
            db.overwrite(result, [chat_id])

        # Original data should be intact
        conn = sqlite3.connect(empty_sms_db)
        text = conn.execute("SELECT text FROM message WHERE ROWID = ?", (msg_ids[0],)).fetchone()[0]
        assert text == original_text
        conn.close()


class TestOverwriteStats:
    """OverwriteStats tracks all metrics correctly."""

    def test_stats_complete(self, empty_sms_db: Path):
        chat_id, _ = _populate_sacrifice_db(empty_sms_db, "+15551110000", 3)
        messages = [
            _make_message("+15552220000", f"stats {i}", 800000000000000000 + i * 1000000000)
            for i in range(2)
        ]
        result = _make_result(
            messages,
            [_make_handle("+15552220000")],
            [_make_chat("+15552220000")],
        )

        with SMSDatabase(empty_sms_db) as db:
            stats = db.overwrite(result, [chat_id])

        assert isinstance(stats, OverwriteStats)
        assert stats.sacrifice_pool_size == 3
        assert stats.messages_overwritten == 2
        assert stats.messages_skipped == 0
        assert stats.handles_inserted == 1
        assert stats.chats_inserted == 1
